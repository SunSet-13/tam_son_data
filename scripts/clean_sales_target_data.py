#!/usr/bin/env python3
"""Clean missing values in sales_target.

Rules:
- employee_id: fill from the Store Manager of the same store_id.
- brand: fill from core_store.name of the same store_id.

The script defaults to dry-run so you can inspect the effect first.
Use --apply to write the changes.
"""

import argparse
import os

import psycopg2


DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'postgres'),
    'port': int(os.getenv('DB_PORT', '5432')),
    'database': os.getenv('DB_NAME', 'tamson_ecommerce'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.environ['DB_PASSWORD'],
}


def get_null_counts(cursor):
    cursor.execute(
        """
        SELECT
            COUNT(*) AS total_rows,
            COUNT(employee_id) AS employee_filled,
            COUNT(brand) AS brand_filled,
            COUNT(*) - COUNT(employee_id) AS employee_nulls,
            COUNT(*) - COUNT(brand) AS brand_nulls
        FROM sales_target
        """
    )
    return cursor.fetchone()


def clean_sales_target(cursor):
    cursor.execute(
        """
        UPDATE sales_target st
        SET employee_id = sm.user_id
        FROM admin_user sm
        WHERE st.employee_id IS NULL
          AND st.store_id = sm.store_id
          AND sm.position = 'Store Manager'
        """
    )

    cursor.execute(
        """
        UPDATE sales_target st
        SET brand = cs.name
        FROM core_store cs
        WHERE (st.brand IS NULL OR btrim(st.brand) = '')
          AND st.store_id = cs.store_id
        """
    )


def main():
    parser = argparse.ArgumentParser(description='Clean missing values in sales_target')
    parser.add_argument('--apply', action='store_true', help='Write changes to the database')
    args = parser.parse_args()

    print('Connecting to', DB_CONFIG['host'], DB_CONFIG['database'])
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    before = get_null_counts(cursor)
    print(
        'Before:',
        f'total={before[0]}',
        f'employee_nulls={before[3]}',
        f'brand_nulls={before[4]}',
    )

    try:
        if args.apply:
            clean_sales_target(cursor)
            conn.commit()
            after = get_null_counts(cursor)
            print(
                'After:',
                f'total={after[0]}',
                f'employee_nulls={after[3]}',
                f'brand_nulls={after[4]}',
            )
            print('Done')
        else:
            print('Dry-run only. Re-run with --apply to update rows.')
    except Exception as exc:
        conn.rollback()
        print('ERROR:', exc)
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == '__main__':
    main()
