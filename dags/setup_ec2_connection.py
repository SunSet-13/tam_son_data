"""
Script để tạo connection EC2 PostgreSQL trong Airflow
Chạy script này một lần để tạo connection
"""
from airflow import settings
from airflow.models import Connection
from sqlalchemy.orm import Session
import os

def create_ec2_postgres_connection():
    """
    Tạo connection cho EC2 PostgreSQL
    """
    session = Session(bind=settings.engine)
    
    # Xóa connection cũ nếu có
    existing_conn = session.query(Connection).filter(
        Connection.conn_id == 'postgres_ec2'
    ).first()
    
    if existing_conn:
        session.delete(existing_conn)
        session.commit()
        print("Đã xóa connection cũ")
    
    # Tạo connection mới
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
    
    print("✓ Đã tạo connection 'postgres_ec2' thành công!")
    print(f"  Host: {new_conn.host}")
    print(f"  Database: {new_conn.schema}")
    print(f"  User: {new_conn.login}")
    print(f"  Port: {new_conn.port}")

if __name__ == "__main__":
    create_ec2_postgres_connection()
