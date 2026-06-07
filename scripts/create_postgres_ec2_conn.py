import os

from airflow import settings
from airflow.models import Connection
from sqlalchemy.orm import Session


def create_ec2_postgres_connection():
    session = Session(bind=settings.engine)

    existing_conn = session.query(Connection).filter(
        Connection.conn_id == 'postgres_ec2'
    ).first()

    if existing_conn:
        session.delete(existing_conn)
        session.commit()
        print("Removed existing 'postgres_ec2' connection")

    new_conn = Connection(
        conn_id='postgres_ec2',
        conn_type='postgres',
        host=os.environ['EC2_DB_HOST'],
        schema=os.getenv('EC2_DB_NAME', 'tamson_ecommerce'),
        login=os.getenv('EC2_DB_USER', 'postgres'),
        password=os.environ['EC2_DB_PASSWORD'],
        port=int(os.getenv('EC2_DB_PORT', '5432')),
        extra=None
    )

    session.add(new_conn)
    session.commit()
    session.close()

    print(
        "Created connection 'postgres_ec2' -> "
        f"{new_conn.host}:{new_conn.port}/{new_conn.schema}"
    )


if __name__ == '__main__':
    create_ec2_postgres_connection()
