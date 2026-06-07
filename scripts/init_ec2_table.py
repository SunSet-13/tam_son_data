import os

import psycopg2

ec2_config = {
    'host': os.environ['EC2_DB_HOST'],
    'database': os.getenv('EC2_DB_NAME', 'postgres'),
    'user': os.getenv('EC2_DB_USER', 'postgres'),
    'password': os.environ['EC2_DB_PASSWORD'],
    'port': int(os.getenv('EC2_DB_PORT', '5432')),
}

create_table_sql = '''
CREATE TABLE IF NOT EXISTS stock_market (
    timestamp BIGINT,
    close DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    open DOUBLE PRECISION,
    volume BIGINT,
    date TEXT,
    PRIMARY KEY (date)
)
'''

def main():
    try:
        print("Connecting to EC2 Postgres...")
        conn = psycopg2.connect(**ec2_config)
        cur = conn.cursor()
        print("Dropping table if exists...")
        cur.execute("DROP TABLE IF EXISTS stock_market CASCADE")
        conn.commit()
        print("Creating table...")
        cur.execute(create_table_sql)
        conn.commit()
        print("✓ Table created or already existed")
        cur.close()
        conn.close()
    except Exception as e:
        print("Error:", e)

if __name__ == '__main__':
    main()
