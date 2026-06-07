#!/usr/bin/env python3
"""Clean missing values in the Tam Son Postgres dataset.

This script uses deterministic rules from related tables instead of guessing.

Default behavior is dry-run. Re-run with --apply to write changes.

Rules currently covered:
- sales_target: fill employee_id from the Store Manager of the same store_id,
  and fill brand from core_store.name.
- sales_flat_order: fill employee_id from the Store Manager of the same store_id,
  and fill brand/category from the first matching order item.
- sales_flat_order_item: fill sku/name/brand/category from the product and
  category tables when the item points to a product_id.
- sales_flat_quote: fill customer_email from customer_entity.email.
- sales_flat_quote_item: fill sku/price/row_total from the product table when
  the item points to a product_id.
- customer_entity: fill missing profile text fields with a stable placeholder.
- admin_user: fill missing manager_id for non-managers, and normalize training
  fields to safe defaults.
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


def fetch_table_null_stats(cursor):
    cursor.execute(
        """
        SELECT
            'sales_target' AS table_name,
            COUNT(*) AS total_rows,
            COUNT(employee_id) AS employee_filled,
            COUNT(brand) AS brand_filled,
            COUNT(*) - COUNT(employee_id) AS employee_nulls,
            COUNT(*) - COUNT(brand) AS brand_nulls
        FROM sales_target
        UNION ALL
        SELECT
            'sales_flat_order' AS table_name,
            COUNT(*) AS total_rows,
            COUNT(employee_id) AS employee_filled,
            COUNT(brand) AS brand_filled,
            COUNT(*) - COUNT(employee_id) AS employee_nulls,
            COUNT(*) - COUNT(brand) AS brand_nulls
        FROM sales_flat_order
        UNION ALL
        SELECT
            'sales_flat_order_item' AS table_name,
            COUNT(*) AS total_rows,
            COUNT(sku) AS employee_filled,
            COUNT(brand) AS brand_filled,
            COUNT(*) - COUNT(sku) AS employee_nulls,
            COUNT(*) - COUNT(brand) AS brand_nulls
        FROM sales_flat_order_item
        ORDER BY table_name
        """
    )
    return cursor.fetchall()


def fill_sales_target(cursor):
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


def fill_sales_flat_order(cursor):
    cursor.execute(
        """
        UPDATE sales_flat_order o
        SET employee_id = sm.user_id
        FROM admin_user sm
        WHERE o.employee_id IS NULL
          AND o.store_id = sm.store_id
          AND sm.position = 'Store Manager'
        """
    )
    cursor.execute(
        """
        WITH item_pick AS (
            SELECT DISTINCT ON (order_id)
                order_id,
                brand,
                category
            FROM sales_flat_order_item
            WHERE brand IS NOT NULL OR category IS NOT NULL
            ORDER BY order_id, item_id
        )
        UPDATE sales_flat_order o
        SET brand = COALESCE(o.brand, item_pick.brand),
            category = COALESCE(o.category, item_pick.category)
        FROM item_pick
        WHERE o.entity_id = item_pick.order_id
          AND (o.brand IS NULL OR o.category IS NULL)
        """
    )


def fill_sales_flat_order_item(cursor):
    cursor.execute(
        """
        UPDATE sales_flat_order_item oi
        SET sku = COALESCE(oi.sku, p.sku),
            name = COALESCE(oi.name, p.product_name),
            brand = COALESCE(oi.brand, p.brand),
            category = COALESCE(oi.category, cce.name)
        FROM catalog_product_entity p
        LEFT JOIN catalog_category_entity cce
          ON cce.entity_id = p.category_id
        WHERE oi.product_id = p.entity_id
          AND (
              oi.sku IS NULL OR oi.name IS NULL OR oi.brand IS NULL OR oi.category IS NULL
          )
        """
    )


def fill_sales_flat_quote(cursor):
    cursor.execute(
        """
        UPDATE sales_flat_quote q
        SET customer_email = ce.email
        FROM customer_entity ce
        WHERE q.customer_email IS NULL
          AND q.customer_id = ce.entity_id
        """
    )


def fill_sales_flat_quote_item(cursor):
    cursor.execute(
        """
        UPDATE sales_flat_quote_item qi
        SET sku = COALESCE(qi.sku, p.sku),
            price = COALESCE(qi.price, p.list_price),
            row_total = COALESCE(qi.row_total, COALESCE(qi.qty, 1) * p.list_price)
        FROM catalog_product_entity p
        WHERE qi.product_id = p.entity_id
          AND (qi.sku IS NULL OR qi.price IS NULL OR qi.row_total IS NULL)
        """
    )


def fill_customer_entity(cursor):
    cursor.execute(
        """
        UPDATE customer_entity
        SET gender = COALESCE(NULLIF(btrim(gender), ''), 'Chua xac dinh'),
            age_group = COALESCE(NULLIF(btrim(age_group), ''), 'Chua xac dinh'),
            occupation = COALESCE(NULLIF(btrim(occupation), ''), 'Chua xac dinh'),
            province = COALESCE(NULLIF(btrim(province), ''), 'Chua xac dinh'),
            nationality = COALESCE(NULLIF(btrim(nationality), ''), 'Viet Nam')
        WHERE gender IS NULL
           OR age_group IS NULL
           OR occupation IS NULL
           OR province IS NULL
           OR nationality IS NULL
        """
    )


def fill_admin_user(cursor):
    cursor.execute(
        """
        UPDATE admin_user au
        SET manager_id = sm.user_id
        FROM admin_user sm
        WHERE au.manager_id IS NULL
          AND au.store_id = sm.store_id
          AND sm.position = 'Store Manager'
          AND au.position <> 'Store Manager'
        """
    )
    cursor.execute(
        """
        UPDATE admin_user
        SET has_training = COALESCE(has_training, 0),
            total_training_days = COALESCE(total_training_days, 0),
            training_score = COALESCE(training_score, 0),
            training_level = COALESCE(NULLIF(btrim(training_level), ''), 'Chua xac dinh')
        WHERE has_training IS NULL
           OR total_training_days IS NULL
           OR training_score IS NULL
           OR training_level IS NULL
        """
    )


def main():
    parser = argparse.ArgumentParser(description='Clean missing values in Tam Son tables')
    parser.add_argument('--apply', action='store_true', help='Write changes to the database')
    args = parser.parse_args()

    print('Connecting to', DB_CONFIG['host'], DB_CONFIG['database'])
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    before = fetch_table_null_stats(cursor)
    print('Before:')
    for row in before:
        print(
            f"- {row[0]}: total={row[1]}, employee_nulls={row[4]}, brand_nulls={row[5]}"
        )

    if not args.apply:
        print('Dry-run only. Re-run with --apply to update rows.')
        cursor.close()
        conn.close()
        return

    try:
        fill_sales_target(cursor)
        fill_sales_flat_order(cursor)
        fill_sales_flat_order_item(cursor)
        fill_sales_flat_quote(cursor)
        fill_sales_flat_quote_item(cursor)
        fill_customer_entity(cursor)
        fill_admin_user(cursor)
        conn.commit()

        after = fetch_table_null_stats(cursor)
        print('After:')
        for row in after:
            print(
                f"- {row[0]}: total={row[1]}, employee_nulls={row[4]}, brand_nulls={row[5]}"
            )
        print('Done')
    except Exception as exc:
        conn.rollback()
        print('ERROR:', exc)
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == '__main__':
    main()
