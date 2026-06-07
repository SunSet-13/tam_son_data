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
cur.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema='public'")
print('tables:', cur.fetchone()[0])
cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name")
print('\nTables:')
for r in cur.fetchall():
    print('-', r[0])
cur.close(); conn.close()
