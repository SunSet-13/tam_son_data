from airflow.decorators import dag, task
from airflow.sensors.base import PokeReturnValue
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.slack.notifications.slack_notifier import SlackNotifier
from include.stock_market.tasks import _get_stock_prices, _store_prices, _get_formatted_csv, BUCKET_NAME
from astro import sql as aql
from astro.sql.table import Table, Metadata
from astro.files import File 
import requests

from datetime import datetime, timedelta
SYMBOL = 'NVDA'

@dag(
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['stock_market'],
    on_success_callback=SlackNotifier(
        slack_conn_id='slack',
        text='The DAG stock_market has been succeeded!',
        channel = 'all-notification'
    ),
    on_failure_callback=SlackNotifier(
        slack_conn_id='slack',
        text='The DAG stock_market has been failed!',
        channel = 'all-notification'
    )
)

def stock_market():
    @task.sensor(poke_interval=30, timeout=300)
    def is_api_available():
        api = BaseHook.get_connection('stock_api')
        url = f"{api.host}{api.extra_dejson['endpoint']}"
        response = requests.get(url, headers=api.extra_dejson['headers'])
        condition = response.json()['finance']['result'] is None
        return PokeReturnValue(is_done=condition, xcom_value= url)
    
    get_stock_prices = PythonOperator(
        task_id = 'get_stock_prices',
        python_callable = _get_stock_prices,
        op_kwargs = {'url': '{{ ti.xcom_pull(task_ids="is_api_available") }}', 'symbol': SYMBOL}
    )

    store_prices = PythonOperator(
        task_id = 'store_prices',
        python_callable = _store_prices,
        op_kwargs = {'stock': '{{ ti.xcom_pull(task_ids="get_stock_prices") }}'}
    )

    format_prices = DockerOperator(
        task_id='format_prices',
        image = 'airflow/stock-app',
        api_version='auto',
        auto_remove='success',
        docker_url='tcp://docker-proxy:2375',
        network_mode='airflow_fd0587_airflow',
        tty=True,
        xcom_all = False,
        mount_tmp_dir=False,
        retries=2,
        retry_delay=timedelta(seconds=30),
        environment = {
            'SPARK_APPLICATION_ARGS': '{{ ti.xcom_pull(task_ids="store_prices") }}',
        }
    )    

    get_formatted_csv = PythonOperator(
        task_id = 'get_formatted_csv',
        python_callable = _get_formatted_csv,
        op_kwargs = {'path': '{{ ti.xcom_pull(task_ids="store_prices") }}'}
    )  

    load_to_dw = aql.load_file(
        task_id='load_to_dw',
        input_file=File(
            path = f"s3://{BUCKET_NAME}/{{{{ ti.xcom_pull(task_ids='get_formatted_csv') }}}}",
            conn_id='minio',
        ),
        output_table = Table(
            name='stock_market',
            conn_id='postgres',
            metadata=Metadata(schema='public')
        ),
        load_options = {
            "aws_access_key_id": "{{ conn.minio.login }}",
            "aws_secret_access_key": "{{ conn.minio.password }}",
            "endpoint_url": "{{ conn.minio.extra_dejson.endpoint_url }}",
        }
    )
    is_api_available() >> get_stock_prices >> store_prices >>format_prices >> get_formatted_csv >> load_to_dw 


stock_market()