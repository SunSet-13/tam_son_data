import os

import psycopg2

cfg = dict(
    host=os.environ['EC2_DB_HOST'],
    database=os.getenv('EC2_DB_NAME', 'tamson_ecommerce'),
    user=os.getenv('EC2_DB_USER', 'postgres'),
    password=os.environ['EC2_DB_PASSWORD'],
    port=int(os.getenv('EC2_DB_PORT', '5432')),
)
conn = psycopg2.connect(**cfg)
cur = conn.cursor()
tables = ['sales_flat_order','customer_entity','stock_market']
for t in tables:
    cur.execute(f"SELECT COUNT(*) FROM {t}")
    print(t, cur.fetchone()[0])
cur.close(); conn.close()
