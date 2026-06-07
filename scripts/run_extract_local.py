import pandas as pd
import psycopg2
import os

cfg = dict(
    host=os.getenv('LOCAL_DB_HOST', 'postgres'),
    database=os.getenv('LOCAL_DB_NAME', 'tamson_ecommerce'),
    user=os.getenv('LOCAL_DB_USER', 'postgres'),
    password=os.environ['LOCAL_DB_PASSWORD'],
    port=int(os.getenv('LOCAL_DB_PORT', '5432')),
)
print('Connecting to local Postgres at', cfg['host'])
try:
    conn = psycopg2.connect(**cfg)
    df = pd.read_sql('SELECT * FROM stock_market ORDER BY date DESC LIMIT 5', conn)
    print('Extracted rows:', len(df))
    print(df.head().to_string(index=False))
    conn.close()
except Exception as e:
    print('ERROR:', e)
    import traceback; traceback.print_exc()
    raise
