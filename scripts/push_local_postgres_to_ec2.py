#!/usr/bin/env python3
"""
Push all public tables from local Postgres Docker to EC2 Postgres.

Connections are read from environment variables.
"""

import os

import pandas as pd
import psycopg2
import psycopg2.extras


LOCAL_CFG = {
    'host': os.getenv('LOCAL_DB_HOST', 'postgres'),
    'database': os.getenv('LOCAL_DB_NAME', 'tamson_ecommerce'),
    'user': os.getenv('LOCAL_DB_USER', 'postgres'),
    'password': os.environ['LOCAL_DB_PASSWORD'],
    'port': int(os.getenv('LOCAL_DB_PORT', '5432')),
}

EC2_CFG = {
    'host': os.environ['EC2_DB_HOST'],
    'database': os.getenv('EC2_DB_NAME', 'tamson_ecommerce'),
    'user': os.getenv('EC2_DB_USER', 'postgres'),
    'password': os.environ['EC2_DB_PASSWORD'],
    'port': int(os.getenv('EC2_DB_PORT', '5432')),
}


def fetch_public_tables(cursor):
    cursor.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """
    )
    return [row[0] for row in cursor.fetchall()]


def fetch_columns(cursor, table_name):
    cursor.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position
        """,
        (table_name,),
    )
    return [row[0] for row in cursor.fetchall()]


def disable_triggers(cursor, tables):
    for table in tables:
        cursor.execute(f'ALTER TABLE "{table}" DISABLE TRIGGER ALL')


def enable_triggers(cursor, tables):
    for table in tables:
        cursor.execute(f'ALTER TABLE "{table}" ENABLE TRIGGER ALL')


def ensure_cataloginventory_stock_table(ec2_cur, target_tables):
    if 'cataloginventory_stock' in target_tables:
        return

    ec2_cur.execute(
        '''
        CREATE TABLE IF NOT EXISTS "cataloginventory_stock" (
            "stock_id" SMALLSERIAL PRIMARY KEY,
            "stock_name" VARCHAR(255) NOT NULL UNIQUE
        )
        '''
    )
    target_tables.add('cataloginventory_stock')


def truncate_tables(cursor, tables):
    for table in tables:
        cursor.execute(f'TRUNCATE TABLE "{table}" CASCADE')


def reset_serial_sequences(local_cur, ec2_cur, tables):
    for table in tables:
        local_cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
              AND column_default LIKE 'nextval(%%'
            """,
            (table,),
        )
        for (column_name,) in local_cur.fetchall():
            local_cur.execute(f'SELECT COALESCE(MAX("{column_name}"), 0) FROM "{table}"')
            max_value = local_cur.fetchone()[0] or 0
            local_cur.execute('SELECT pg_get_serial_sequence(%s, %s)', (table, column_name))
            sequence_name = local_cur.fetchone()[0]
            if sequence_name:
                ec2_cur.execute('SELECT setval(%s, %s, true)', (sequence_name, max_value))


def sync_table(local_conn, ec2_cur, table_name):
    local_cur = local_conn.cursor()
    columns = fetch_columns(local_cur, table_name)
    if not columns:
        print(f'No columns found for {table_name}, skipping')
        local_cur.close()
        return 0

    cols_sql = ', '.join([f'"{column}"' for column in columns])
    insert_sql = f'INSERT INTO "{table_name}" ({cols_sql}) VALUES %s'

    total_rows = 0
    for chunk in pd.read_sql_query(
        f'SELECT {cols_sql} FROM "{table_name}"',
        local_conn,
        chunksize=5000,
    ):
        if chunk.empty:
            continue
        chunk = chunk.where(pd.notnull(chunk), None)
        records = [tuple(row) for row in chunk.itertuples(index=False, name=None)]
        psycopg2.extras.execute_values(ec2_cur, insert_sql, records, page_size=1000)
        total_rows += len(records)
    local_cur.close()
    return total_rows


def main():
    print('Connecting to local DB:', LOCAL_CFG['host'], LOCAL_CFG['database'])
    print('Connecting to EC2 DB:', EC2_CFG['host'], EC2_CFG['database'])

    local_conn = psycopg2.connect(**LOCAL_CFG)
    ec2_conn = psycopg2.connect(**EC2_CFG)

    local_cur = local_conn.cursor()
    ec2_cur = ec2_conn.cursor()

    source_tables = fetch_public_tables(local_cur)
    target_tables = set(fetch_public_tables(ec2_cur))
    ensure_cataloginventory_stock_table(ec2_cur, target_tables)
    ec2_conn.commit()
    tables = [table for table in source_tables if table in target_tables]
    skipped_tables = [table for table in source_tables if table not in target_tables]
    print(f'Found {len(source_tables)} source tables')
    print(f'Syncing {len(tables)} tables, skipping {skipped_tables}')

    try:
        disable_triggers(ec2_cur, tables)
        ec2_conn.commit()

        truncate_tables(ec2_cur, tables)
        ec2_conn.commit()

        for table_name in tables:
            print(f'\n--- Syncing {table_name} ---')
            rows = sync_table(local_conn, ec2_cur, table_name)
            ec2_conn.commit()
            print(f'Loaded {rows} rows into {table_name}')

        reset_serial_sequences(local_cur, ec2_cur, tables)
        ec2_conn.commit()

        enable_triggers(ec2_cur, tables)
        ec2_conn.commit()

        print(f'✓ Sync completed for {len(tables)} tables')
    except Exception as exc:
        ec2_conn.rollback()
        print('ERROR during sync:', exc)
        raise
    finally:
        local_cur.close()
        ec2_cur.close()
        local_conn.close()
        ec2_conn.close()


if __name__ == '__main__':
    main()
