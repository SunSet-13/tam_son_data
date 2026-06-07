from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from datetime import datetime


def _task_a():
    print('Task A')
    return 42
 
@dag(
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['taskflow']
)


def taskflow():

    task_a = PythonOperator(
        task_id='task_a',
        python_callable= _task_a
    )
    @task
    def task_b():
        print('Task B')
   
    
    task_a >> task_b()

taskflow()