from datetime import datetime

import pandas as pd
import psycopg2.extras
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


def _fetch_public_tables(cursor):
    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """)
    return [row[0] for row in cursor.fetchall()]


def _fetch_columns(cursor, table_name):
    cursor.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position
    """, (table_name,))
    return [row[0] for row in cursor.fetchall()]


def _fetch_primary_key_columns(cursor, table_name):
    cursor.execute("""
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
         AND tc.table_name = kcu.table_name
        WHERE tc.table_schema = 'public'
          AND tc.table_name = %s
          AND tc.constraint_type = 'PRIMARY KEY'
        ORDER BY kcu.ordinal_position
    """, (table_name,))
    return [row[0] for row in cursor.fetchall()]


def _fetch_table_dependencies(cursor, table_names):
    cursor.execute("""
        SELECT
            tc.table_name AS child_table,
            ccu.table_name AS parent_table
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
         AND tc.table_name = kcu.table_name
        JOIN information_schema.constraint_column_usage ccu
          ON ccu.constraint_name = tc.constraint_name
         AND ccu.table_schema = tc.table_schema
        WHERE tc.table_schema = 'public'
          AND tc.constraint_type = 'FOREIGN KEY'
    """)

    table_set = set(table_names)
    dependencies = {table_name: set() for table_name in table_names}
    for child_table, parent_table in cursor.fetchall():
        if child_table in table_set and parent_table in table_set and child_table != parent_table:
            dependencies[child_table].add(parent_table)
    return dependencies


def _order_tables_by_dependencies(table_names, dependencies):
    remaining = list(table_names)
    ordered = []
    resolved = set()

    while remaining:
        ready = [table for table in remaining if dependencies.get(table, set()).issubset(resolved)]
        if not ready:
            ordered.extend(remaining)
            break

        for table in ready:
            ordered.append(table)
            resolved.add(table)
            remaining.remove(table)

    return ordered


def _get_single_primary_key(cursor, table_name):
    primary_key_columns = _fetch_primary_key_columns(cursor, table_name)
    if len(primary_key_columns) == 1:
        return primary_key_columns[0]
    return None


@dag(
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=['postgres', 'ec2', 'sync', 'tamson'],
    description='Sync all public tables from local tamson_ecommerce to EC2 PostgreSQL'
)
def postgres_to_ec2_transfer():
    @task
    def transfer_all_public_tables():
        """
        Append data from local Postgres database `tamson_ecommerce` to EC2.
        This DAG assumes the EC2 schema was initialized once beforehand.
        For tables with primary keys, duplicate rows are skipped.
        """
        local_hook = PostgresHook(postgres_conn_id='postgres')
        ec2_hook = PostgresHook(postgres_conn_id='postgres_ec2')

        local_conn = local_hook.get_conn()
        ec2_conn = ec2_hook.get_conn()
        local_cur = local_conn.cursor()
        ec2_cur = ec2_conn.cursor()

        try:
            source_tables = _fetch_public_tables(local_cur)
            target_tables = set(_fetch_public_tables(ec2_cur))
            dependencies = _fetch_table_dependencies(local_cur, source_tables)

            tables = [table for table in source_tables if table in target_tables]
            skipped_tables = [table for table in source_tables if table not in target_tables]
            ordered_tables = _order_tables_by_dependencies(tables, dependencies)
            print(f'Found {len(source_tables)} source tables, {len(tables)} will sync, {len(skipped_tables)} skipped: {skipped_tables}')
            print(f'Load order: {ordered_tables}')

            for table in ordered_tables:
                print(f'\n--- Syncing table: {table} ---')

                columns = _fetch_columns(local_cur, table)
                if not columns:
                    print(f'No columns found for {table}, skipping')
                    continue

                primary_key_columns = _fetch_primary_key_columns(local_cur, table)
                single_primary_key = _get_single_primary_key(local_cur, table)
                cols_sql = ', '.join([f'"{c}"' for c in columns])
                insert_sql = f'INSERT INTO "{table}" ({cols_sql}) VALUES %s'
                if primary_key_columns:
                    conflict_sql = ', '.join([f'"{c}"' for c in primary_key_columns])
                    insert_sql = f'{insert_sql} ON CONFLICT ({conflict_sql}) DO NOTHING'

                params = None
                query_sql = f'SELECT {cols_sql} FROM "{table}"'
                if single_primary_key:
                    ec2_cur.execute(f'SELECT COALESCE(MAX("{single_primary_key}"), NULL) FROM "{table}"')
                    target_max_pk = ec2_cur.fetchone()[0]
                    if target_max_pk is not None:
                        query_sql = f'SELECT {cols_sql} FROM "{table}" WHERE "{single_primary_key}" > %s ORDER BY "{single_primary_key}"'
                        params = (target_max_pk,)

                total_rows = 0
                chunk_size = 5000

                for chunk in pd.read_sql_query(
                    query_sql,
                    local_conn,
                    params=params,
                    chunksize=chunk_size,
                ):
                    if chunk.empty:
                        continue
                    chunk = chunk.where(pd.notnull(chunk), None)
                    records = [tuple(row) for row in chunk.itertuples(index=False, name=None)]
                    psycopg2.extras.execute_values(ec2_cur, insert_sql, records, page_size=1000)
                    ec2_conn.commit()
                    total_rows += len(records)
                    print(f'  loaded chunk of {len(records)} rows (total {total_rows})')

                if total_rows == 0:
                    print(f'✓ {table}: no new rows')
                else:
                    print(f'✓ {table}: {total_rows} new rows processed')

            # Reset serial sequences to the current max(id) values where applicable.
            for table in ordered_tables:
                local_cur.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = %s
                      AND column_default LIKE 'nextval(%%'
                """, (table,))
                sequence_columns = [row[0] for row in local_cur.fetchall()]
                for column in sequence_columns:
                    local_cur.execute(f'SELECT COALESCE(MAX("{column}"), 0) FROM "{table}"')
                    max_value = local_cur.fetchone()[0] or 0
                    local_cur.execute("SELECT pg_get_serial_sequence(%s, %s)", (table, column))
                    sequence_name = local_cur.fetchone()[0]
                    if sequence_name:
                        ec2_cur.execute("SELECT setval(%s, %s, true)", (sequence_name, max_value))
                        ec2_conn.commit()

            return {'tables_synced': len(ordered_tables)}

        except Exception:
            ec2_conn.rollback()
            raise
        finally:
            local_cur.close()
            ec2_cur.close()
            local_conn.close()
            ec2_conn.close()

    @task
    def verify_table_counts():
        local_hook = PostgresHook(postgres_conn_id='postgres')
        ec2_hook = PostgresHook(postgres_conn_id='postgres_ec2')

        local_conn = local_hook.get_conn()
        ec2_conn = ec2_hook.get_conn()
        local_cur = local_conn.cursor()
        ec2_cur = ec2_conn.cursor()

        try:
            source_tables = _fetch_public_tables(local_cur)
            target_tables = set(_fetch_public_tables(ec2_cur))
            dependencies = _fetch_table_dependencies(local_cur, source_tables)
            tables = [table for table in source_tables if table in target_tables]
            ordered_tables = _order_tables_by_dependencies(tables, dependencies)

            mismatches = []
            for table in ordered_tables:
                local_cur.execute(f'SELECT COUNT(*) FROM "{table}"')
                local_count = local_cur.fetchone()[0]
                ec2_cur.execute(f'SELECT COUNT(*) FROM "{table}"')
                ec2_count = ec2_cur.fetchone()[0]
                print(f'{table}: local={local_count} ec2={ec2_count}')
                if local_count != ec2_count:
                    mismatches.append((table, local_count, ec2_count))

            if mismatches:
                print(f'Warning: count mismatches found: {mismatches}')
            else:
                print('✓ All table counts match')

            return {'mismatches': mismatches}

        finally:
            local_cur.close()
            ec2_cur.close()
            local_conn.close()
            ec2_conn.close()

    transfer_all_public_tables() >> verify_table_counts()


postgres_to_ec2_transfer()
