import os

import psycopg2

cfg = dict(
    host=os.environ['EC2_DB_HOST'],
    database=os.getenv('EC2_DB_NAME', 'tamson_ecommerce'),
    user=os.getenv('EC2_DB_USER', 'postgres'),
    password=os.environ['EC2_DB_PASSWORD'],
    port=int(os.getenv('EC2_DB_PORT', '5432')),
    connect_timeout=5
)

print('Testing connection to', cfg['host'])
try:
    conn = psycopg2.connect(**cfg)
    cur = conn.cursor()
    cur.execute('SELECT version()')
    print('Connected -', cur.fetchone())
    cur.close()
    conn.close()
    print('SUCCESS')
except Exception as e:
    print('FAILED:', e)
    import traceback
    traceback.print_exc()
    raise
