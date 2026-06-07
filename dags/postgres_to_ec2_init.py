from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from pathlib import Path


@dag(
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=['postgres', 'ec2', 'init', 'setup'],
    description='Initialize tamson_ecommerce schema on EC2 PostgreSQL'
)
def postgres_ec2_init_table():

    @task
    def create_schema_from_file():
        """
        Apply the Tam Son schema to the EC2 database.
        This is the one-time initialization step before loading data.
        """
        schema_path = Path('/usr/local/airflow/tamson_schema_fixed.sql')
        if not schema_path.exists():
            schema_path = Path('/usr/local/airflow/dags/../tamson_schema_fixed.sql').resolve()

        sql_text = schema_path.read_text(encoding='utf-8')

        postgres_hook = PostgresHook(postgres_conn_id='postgres_ec2')
        connection = postgres_hook.get_conn()
        cursor = connection.cursor()

        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS cataloginventory_stock (
                    stock_id SMALLSERIAL PRIMARY KEY,
                    stock_name VARCHAR(255) NOT NULL UNIQUE
                )
            """)
            connection.commit()

            cursor.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """)
            existing_tables = [row[0] for row in cursor.fetchall()]
            if existing_tables:
                print(f'Schema already exists on EC2, skipping init: {existing_tables}')
                return {'skipped': True, 'tables_present': len(existing_tables)}

            print(f'Applying schema from {schema_path}')
            cursor.execute(sql_text)
            connection.commit()
            print('✓ Schema applied successfully on EC2')

            cursor.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                ORDER BY table_name
            """)
            tables = [row[0] for row in cursor.fetchall()]
            print(f'Tables on EC2: {tables}')
            return {'tables_created': len(tables)}

        except Exception as e:
            connection.rollback()
            print(f'Error applying schema: {e}')
            raise
        finally:
            cursor.close()
            connection.close()

    create_schema_from_file()


postgres_ec2_init_table()
