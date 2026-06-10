from __future__ import annotations

import uuid
from datetime import datetime
from io import StringIO
from pathlib import Path

import pandas as pd
import psycopg2.extras
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import sql


TABLE_FILES = [
    ("core_website.csv", "core_website"),
    ("core_store.csv", "core_store"),
    ("eav_attribute_set.csv", "eav_attribute_set"),
    ("catalog_category_entity.csv", "catalog_category_entity"),
    ("catalog_product_entity.csv", "catalog_product_entity"),
    ("catalog_product_entity_varchar.csv", "catalog_product_entity_varchar"),
    ("catalog_product_entity_decimal.csv", "catalog_product_entity_decimal"),
    ("catalog_category_product.csv", "catalog_category_product"),
    ("catalog_product_website.csv", "catalog_product_website"),
    ("cataloginventory_stock.csv", "cataloginventory_stock"),
    ("cataloginventory_stock_item.csv", "cataloginventory_stock_item"),
    ("cataloginventory_stock_status.csv", "cataloginventory_stock_status"),
    ("customer_entity.csv", "customer_entity"),
    ("customer_address_entity.csv", "customer_address_entity"),
    ("admin_user.csv", "admin_user"),
    ("sales_flat_quote.csv", "sales_flat_quote"),
    ("sales_flat_quote_item.csv", "sales_flat_quote_item"),
    ("sales_flat_order.csv", "sales_flat_order"),
    ("sales_flat_order_item.csv", "sales_flat_order_item"),
    ("sales_flat_order_address.csv", "sales_flat_order_address"),
    ("sales_flat_order_payment.csv", "sales_flat_order_payment"),
    ("sales_target.csv", "sales_target"),
]


LOGICAL_KEYS = {
    "core_website": ["code"],
    "core_store": ["code"],
    "eav_attribute_set": ["attribute_set_name"],
    "catalog_category_entity": ["entity_id"],
    "catalog_product_entity": ["sku"],
    "catalog_product_entity_varchar": ["entity_id", "attribute_id", "store_id", "value"],
    "catalog_product_entity_decimal": ["entity_id", "attribute_id", "store_id", "value"],
    "catalog_category_product": ["category_id", "product_id"],
    "catalog_product_website": ["product_id", "website_id"],
    "cataloginventory_stock": ["stock_name"],
    "cataloginventory_stock_item": ["product_id", "stock_id"],
    "cataloginventory_stock_status": ["product_id", "website_id", "stock_id"],
    "customer_entity": ["email"],
    "customer_address_entity": ["parent_id", "address_type", "street", "telephone"],
    "admin_user": ["username"],
    "sales_flat_quote": ["entity_id"],
    "sales_flat_quote_item": ["quote_id", "product_id", "sku", "row_total"],
    "sales_flat_order": ["increment_id"],
    "sales_flat_order_item": ["order_id", "product_id", "sku", "row_total"],
    "sales_flat_order_address": ["parent_id", "address_type", "street", "telephone"],
    "sales_flat_order_payment": ["parent_id", "method", "amount_ordered", "amount_paid"],
    "sales_target": ["period", "year", "month", "store_id", "employee_id", "brand"],
}


CONFLICT_ONLY_TABLES = {
    "core_website",
    "core_store",
    "eav_attribute_set",
    "catalog_category_entity",
    "catalog_product_entity",
    "catalog_category_product",
    "catalog_product_website",
    "cataloginventory_stock",
    "cataloginventory_stock_item",
    "cataloginventory_stock_status",
    "customer_entity",
    "customer_address_entity",
    "admin_user",
    "sales_flat_quote",
    "sales_flat_quote_item",
    "sales_flat_order",
    "sales_flat_order_item",
    "sales_flat_order_address",
    "sales_flat_order_payment",
}


SOURCE_ID_KEYS = {
    "catalog_category_entity": ["entity_id"],
    "catalog_product_entity_varchar": ["value_id"],
    "catalog_product_entity_decimal": ["value_id"],
    "customer_address_entity": ["entity_id"],
    "sales_flat_quote": ["entity_id"],
    "sales_flat_quote_item": ["item_id"],
    "sales_flat_order_item": ["item_id"],
    "sales_flat_order_address": ["entity_id"],
    "sales_flat_order_payment": ["entity_id"],
}


LOAD_INDEXES = {
    "catalog_product_entity_varchar": ["entity_id", "attribute_id", "store_id", "value"],
    "catalog_product_entity_decimal": ["entity_id", "attribute_id", "store_id", "value"],
    "customer_address_entity": ["parent_id", "address_type", "telephone"],
    "sales_flat_quote_item": ["quote_id", "product_id", "sku", "row_total"],
    "sales_flat_order_item": ["order_id", "product_id", "sku", "row_total"],
    "sales_flat_order_address": ["parent_id", "address_type", "telephone"],
    "sales_flat_order_payment": ["parent_id", "method", "amount_ordered", "amount_paid"],
    "sales_target": ["period", "year", "month", "store_id", "employee_id", "brand"],
}


def _project_path(*parts: str) -> Path:
    airflow_root = Path("/usr/local/airflow")
    if airflow_root.exists():
        return airflow_root.joinpath(*parts)
    return Path(__file__).resolve().parents[1].joinpath(*parts)


def _cleaned_dir() -> Path:
    return _project_path("include", "data", "tamson_cleaned")


def _raw_dir() -> Path:
    return _project_path("tamson_sales_data")


def _clean_columns(columns: list[str]) -> list[str]:
    return [column.strip().lower().replace(" ", "_") for column in columns]


def _clean_frame(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(how="all").copy()
    df.columns = _clean_columns(list(df.columns))

    for column in df.select_dtypes(include=["object"]).columns:
        df[column] = df[column].map(lambda value: value.strip() if isinstance(value, str) else value)
        df[column] = df[column].replace({"": None, "nan": None, "NaN": None, "NULL": None})

    df = df.where(pd.notnull(df), None)
    return df.dropna(how="all")


def _cleaning_change_stats(raw_df: pd.DataFrame, cleaned_df: pd.DataFrame) -> dict[str, object]:
    raw_non_empty = raw_df.dropna(how="all").copy()
    cleaned_columns = _clean_columns(list(raw_non_empty.columns))

    changed_columns = set()
    for raw_column, cleaned_column in zip(raw_non_empty.columns, cleaned_columns):
        if raw_column != cleaned_column:
            changed_columns.add(cleaned_column)

        raw_values = raw_non_empty[raw_column].astype("object").where(pd.notnull(raw_non_empty[raw_column]), None)
        cleaned_values = cleaned_df[cleaned_column].astype("object").where(pd.notnull(cleaned_df[cleaned_column]), None)
        raw_normalized = raw_values.map(lambda value: "__NULL__" if value is None else str(value))
        cleaned_normalized = cleaned_values.map(lambda value: "__NULL__" if value is None else str(value))
        if not raw_normalized.reset_index(drop=True).equals(cleaned_normalized.reset_index(drop=True)):
            changed_columns.add(cleaned_column)

    dropped_rows = len(raw_df) - len(cleaned_df)
    return {
        "changed_columns": changed_columns,
        "dropped_rows": dropped_rows,
    }


def _fetch_table_columns(cursor, table_name: str) -> list[str]:
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


def _fetch_table_column_types(cursor, table_name: str) -> dict[str, str]:
    cursor.execute(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        """,
        (table_name,),
    )
    return {row[0]: row[1] for row in cursor.fetchall()}


def _coerce_frame_for_table(df: pd.DataFrame, column_types: dict[str, str]) -> pd.DataFrame:
    coerced = df.copy()
    integer_types = {"smallint", "integer", "bigint"}

    for column in coerced.columns:
        if column_types.get(column) not in integer_types:
            continue

        numeric = pd.to_numeric(coerced[column], errors="coerce")
        coerced[column] = numeric.map(lambda value: None if pd.isna(value) else str(int(value)))

    return coerced


def _count_rows(cursor, table_name: str) -> int:
    cursor.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table_name)))
    return cursor.fetchone()[0]


def _copy_frame_to_temp(cursor, temp_table: str, columns: list[str], df: pd.DataFrame) -> None:
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False, sep="\t", na_rep="\\N")
    buffer.seek(0)

    copy_sql = sql.SQL(
        "COPY {} ({}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N')"
    ).format(
        sql.Identifier(temp_table),
        sql.SQL(", ").join(sql.Identifier(column) for column in columns),
    )
    cursor.copy_expert(copy_sql, buffer)


def _insert_from_temp_sql(table_name: str, temp_table: str, columns: list[str], keys: list[str]):
    source_cols = sql.SQL(", ").join(sql.SQL("s.{}").format(sql.Identifier(column)) for column in columns)
    insert_cols = sql.SQL(", ").join(sql.Identifier(column) for column in columns)
    key_cols = sql.SQL(", ").join(sql.Identifier(key) for key in keys)
    duplicate_conditions = sql.SQL(" AND ").join(
        sql.SQL("t.{} IS NOT DISTINCT FROM s.{}").format(sql.Identifier(key), sql.Identifier(key))
        for key in keys
    )

    return sql.SQL(
        """
        WITH candidates AS (
            SELECT DISTINCT ON ({key_cols}) {source_cols}
            FROM {temp_table} s
        )
        INSERT INTO {table_name} ({insert_cols})
        SELECT {select_cols}
        FROM candidates s
        WHERE NOT EXISTS (
            SELECT 1
            FROM {table_name} t
            WHERE {duplicate_conditions}
        )
        ON CONFLICT DO NOTHING
        RETURNING 1
        """
    ).format(
        key_cols=key_cols,
        source_cols=source_cols,
        temp_table=sql.Identifier(temp_table),
        table_name=sql.Identifier(table_name),
        insert_cols=insert_cols,
        select_cols=source_cols,
        duplicate_conditions=duplicate_conditions,
    )


def _insert_conflict_only_from_temp_sql(table_name: str, temp_table: str, columns: list[str], keys: list[str]):
    source_cols = sql.SQL(", ").join(sql.SQL("s.{}").format(sql.Identifier(column)) for column in columns)
    insert_cols = sql.SQL(", ").join(sql.Identifier(column) for column in columns)
    key_cols = sql.SQL(", ").join(sql.Identifier(key) for key in keys)

    return sql.SQL(
        """
        WITH candidates AS (
            SELECT DISTINCT ON ({key_cols}) {source_cols}
            FROM {temp_table} s
        )
        INSERT INTO {table_name} ({insert_cols})
        SELECT {select_cols}
        FROM candidates s
        ON CONFLICT DO NOTHING
        RETURNING 1
        """
    ).format(
        key_cols=key_cols,
        source_cols=source_cols,
        temp_table=sql.Identifier(temp_table),
        table_name=sql.Identifier(table_name),
        insert_cols=insert_cols,
        select_cols=source_cols,
    )


def _ensure_load_indexes(cursor) -> None:
    for table_name, columns in LOAD_INDEXES.items():
        index_name = f"idx_tamson_load_{table_name}"
        cursor.execute(
            sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} ({})").format(
                sql.Identifier(index_name),
                sql.Identifier(table_name),
                sql.SQL(", ").join(sql.Identifier(column) for column in columns),
            )
        )


def _reset_serial_sequences(cursor, table_names: list[str]) -> None:
    for table_name in table_names:
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
              AND column_default LIKE 'nextval(%%'
            """,
            (table_name,),
        )
        for (column_name,) in cursor.fetchall():
            cursor.execute(
                sql.SQL("SELECT COALESCE(MAX({}), 0) FROM {}").format(
                    sql.Identifier(column_name),
                    sql.Identifier(table_name),
                )
            )
            max_value = cursor.fetchone()[0] or 0
            cursor.execute("SELECT pg_get_serial_sequence(%s, %s)", (table_name, column_name))
            sequence_name = cursor.fetchone()[0]
            if sequence_name:
                cursor.execute("SELECT setval(%s, %s, true)", (sequence_name, max_value))


def _run_database_cleaning(cursor) -> list[dict[str, object]]:
    stats = []

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
    stats.append({"table": "sales_target", "columns": ["employee_id"], "rows_updated": cursor.rowcount})
    cursor.execute(
        """
        UPDATE sales_target st
        SET brand = cs.name
        FROM core_store cs
        WHERE (st.brand IS NULL OR btrim(st.brand) = '')
          AND st.store_id = cs.store_id
        """
    )
    stats.append({"table": "sales_target", "columns": ["brand"], "rows_updated": cursor.rowcount})
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
    stats.append({"table": "sales_flat_order", "columns": ["employee_id"], "rows_updated": cursor.rowcount})
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
    stats.append({"table": "sales_flat_order", "columns": ["brand", "category"], "rows_updated": cursor.rowcount})
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
          AND (oi.sku IS NULL OR oi.name IS NULL OR oi.brand IS NULL OR oi.category IS NULL)
        """
    )
    stats.append(
        {
            "table": "sales_flat_order_item",
            "columns": ["sku", "name", "brand", "category"],
            "rows_updated": cursor.rowcount,
        }
    )
    cursor.execute(
        """
        UPDATE sales_flat_quote q
        SET customer_email = ce.email
        FROM customer_entity ce
        WHERE q.customer_email IS NULL
          AND q.customer_id = ce.entity_id
        """
    )
    stats.append({"table": "sales_flat_quote", "columns": ["customer_email"], "rows_updated": cursor.rowcount})
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
    stats.append(
        {
            "table": "sales_flat_quote_item",
            "columns": ["sku", "price", "row_total"],
            "rows_updated": cursor.rowcount,
        }
    )
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
    stats.append(
        {
            "table": "customer_entity",
            "columns": ["gender", "age_group", "occupation", "province", "nationality"],
            "rows_updated": cursor.rowcount,
        }
    )
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
    stats.append({"table": "admin_user", "columns": ["manager_id"], "rows_updated": cursor.rowcount})
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
    stats.append(
        {
            "table": "admin_user",
            "columns": ["has_training", "total_training_days", "training_score", "training_level"],
            "rows_updated": cursor.rowcount,
        }
    )
    return stats


@dag(
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["postgres", "tamson", "csv", "clean", "load"],
    description="Clean Tam Son CSV files and idempotently load them into the local Postgres Docker database",
)
def tamson_csv_to_postgres():
    @task
    def validate_csv_files():
        raw_dir = _raw_dir()
        missing = []
        empty = []
        total_size = 0

        for csv_file, _table_name in TABLE_FILES:
            path = raw_dir / csv_file
            if not path.exists():
                missing.append(csv_file)
                continue
            size = path.stat().st_size
            total_size += size
            if size == 0:
                empty.append(csv_file)

        print(f"Raw CSV directory: {raw_dir}")
        print(f"Expected CSV files: {len(TABLE_FILES)}")
        print(f"Total raw CSV size: {total_size} bytes")
        if missing:
            raise FileNotFoundError(f"Missing CSV files: {missing}")
        if empty:
            raise ValueError(f"Empty CSV files: {empty}")
        return {"files": len(TABLE_FILES), "total_size": total_size}

    @task
    def clean_csv_files():
        raw_dir = _raw_dir()
        cleaned_dir = _cleaned_dir()
        cleaned_dir.mkdir(parents=True, exist_ok=True)

        stats = []
        for csv_file, table_name in TABLE_FILES:
            input_path = raw_dir / csv_file
            output_path = cleaned_dir / csv_file
            temp_output_path = cleaned_dir / f".{output_path.stem}.{uuid.uuid4().hex}.tmp"
            rows = 0
            dropped_rows = 0
            changed_columns = set()
            temp_output_path.unlink(missing_ok=True)

            for index, chunk in enumerate(pd.read_csv(input_path, chunksize=50000)):
                cleaned = _clean_frame(chunk)
                chunk_stats = _cleaning_change_stats(chunk, cleaned)
                changed_columns.update(chunk_stats["changed_columns"])
                dropped_rows += chunk_stats["dropped_rows"]
                rows += len(cleaned)
                cleaned.to_csv(
                    temp_output_path,
                    index=False,
                    mode="w" if index == 0 else "a",
                    header=index == 0,
                )

            temp_output_path.replace(output_path)

            changed_columns = sorted(changed_columns)
            print(
                f"{table_name}: cleaned_rows={rows}, dropped_empty_rows={dropped_rows}, "
                f"cleaned_columns={len(changed_columns)}, columns={changed_columns} -> {output_path}"
            )
            stats.append(
                {
                    "table": table_name,
                    "rows_cleaned": rows,
                    "dropped_empty_rows": dropped_rows,
                    "cleaned_columns": changed_columns,
                    "cleaned_column_count": len(changed_columns),
                }
            )

        cleaned_tables = [stat for stat in stats if stat["cleaned_column_count"] > 0 or stat["dropped_empty_rows"] > 0]
        total_cleaned_columns = sum(stat["cleaned_column_count"] for stat in stats)
        print("=== Tam Son CSV cleaning summary ===")
        print(f"Tables scanned for cleaning: {len(stats)}")
        print(f"Tables with cleaning changes: {len(cleaned_tables)}")
        print(f"Total cleaned columns across tables: {total_cleaned_columns}")
        for stat in stats:
            print(
                f"{stat['table']}: cleaned_column_count={stat['cleaned_column_count']}, "
                f"dropped_empty_rows={stat['dropped_empty_rows']}"
            )

        return {
            "cleaned_dir": str(cleaned_dir),
            "tables_scanned": len(stats),
            "tables_with_cleaning_changes": len(cleaned_tables),
            "total_cleaned_columns": total_cleaned_columns,
            "tables": stats,
        }

    @task
    def load_cleaned_csv_to_postgres():
        hook = PostgresHook(postgres_conn_id="postgres")
        conn = hook.get_conn()
        cursor = conn.cursor()
        cleaned_dir = _cleaned_dir()
        load_stats = []

        try:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS cataloginventory_stock (
                    stock_id SMALLSERIAL PRIMARY KEY,
                    stock_name VARCHAR(255) NOT NULL UNIQUE
                )
                """
            )
            _ensure_load_indexes(cursor)

            for csv_file, table_name in TABLE_FILES:
                table_columns = _fetch_table_columns(cursor, table_name)
                column_types = _fetch_table_column_types(cursor, table_name)
                if not table_columns:
                    raise RuntimeError(f"Table does not exist in Postgres Docker: {table_name}")

                input_path = cleaned_dir / csv_file
                before_count = _count_rows(cursor, table_name)
                temp_table = f"tmp_{table_name}"
                cursor.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(temp_table)))
                cursor.execute(
                    sql.SQL("CREATE TEMP TABLE {} (LIKE {} INCLUDING DEFAULTS)").format(
                        sql.Identifier(temp_table),
                        sql.Identifier(table_name),
                    )
                )

                read_rows = 0
                inserted_rows = 0
                loaded_columns = set()
                table_keys = [key for key in LOGICAL_KEYS[table_name] if key in table_columns]

                for chunk in pd.read_csv(input_path, chunksize=50000):
                    cleaned = _clean_frame(chunk)
                    insert_columns = [column for column in cleaned.columns if column in table_columns]
                    if not insert_columns:
                        raise RuntimeError(f"No CSV columns match table columns for {table_name}")

                    keys = [key for key in table_keys if key in insert_columns]
                    if table_name in CONFLICT_ONLY_TABLES:
                        keys = [
                            key
                            for key in SOURCE_ID_KEYS.get(table_name, table_keys)
                            if key in insert_columns
                        ]
                    if not keys:
                        keys = insert_columns

                    cleaned = cleaned[insert_columns]
                    cleaned = _coerce_frame_for_table(cleaned, column_types)
                    read_rows += len(cleaned)
                    loaded_columns.update(insert_columns)

                    cursor.execute(sql.SQL("TRUNCATE TABLE {}").format(sql.Identifier(temp_table)))
                    _copy_frame_to_temp(cursor, temp_table, insert_columns, cleaned)
                    if table_name in CONFLICT_ONLY_TABLES:
                        cursor.execute(
                            _insert_conflict_only_from_temp_sql(
                                table_name,
                                temp_table,
                                insert_columns,
                                keys,
                            )
                        )
                    else:
                        cursor.execute(_insert_from_temp_sql(table_name, temp_table, insert_columns, keys))
                    inserted_rows += len(cursor.fetchall())

                after_count = _count_rows(cursor, table_name)
                loaded_columns = sorted(loaded_columns)
                print(
                    f"{table_name}: read={read_rows}, inserted={inserted_rows}, "
                    f"before={before_count}, after={after_count}, "
                    f"loaded_columns={len(loaded_columns)}, duplicate_key={table_keys}"
                )
                load_stats.append(
                    {
                        "table": table_name,
                        "read_rows": read_rows,
                        "inserted_rows": inserted_rows,
                        "before_rows": before_count,
                        "after_rows": after_count,
                        "loaded_columns": loaded_columns,
                        "loaded_column_count": len(loaded_columns),
                        "duplicate_key": table_keys,
                    }
                )

            _reset_serial_sequences(cursor, [table_name for _csv_file, table_name in TABLE_FILES])
            conn.commit()

            inserted_total = sum(stat["inserted_rows"] for stat in load_stats)
            updated_tables = sum(1 for stat in load_stats if stat["inserted_rows"] > 0)
            total_loaded_columns = sum(stat["loaded_column_count"] for stat in load_stats)
            updated_columns = sum(
                stat["loaded_column_count"]
                for stat in load_stats
                if stat["inserted_rows"] > 0
            )
            print("=== Tam Son CSV -> Postgres Docker load summary ===")
            print(f"Tables checked: {len(load_stats)}")
            print(f"Tables mapped to Postgres columns: {sum(1 for stat in load_stats if stat['loaded_column_count'] > 0)}")
            print(f"Total mapped columns into Postgres: {total_loaded_columns}")
            print(f"Tables updated: {updated_tables}")
            print(f"Columns updated on tables with new rows: {updated_columns}")
            print(f"Total inserted rows: {inserted_total}")
            for stat in load_stats:
                print(
                    f"{stat['table']}: loaded_column_count={stat['loaded_column_count']}, "
                    f"inserted_rows={stat['inserted_rows']}, columns={stat['loaded_columns']}"
                )
            return {
                "tables_checked": len(load_stats),
                "tables_mapped_to_postgres": sum(1 for stat in load_stats if stat["loaded_column_count"] > 0),
                "total_mapped_columns": total_loaded_columns,
                "tables_updated": updated_tables,
                "columns_updated_on_tables_with_new_rows": updated_columns,
                "total_inserted_rows": inserted_total,
                "table_stats": load_stats,
            }
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()

    @task
    def clean_database_values():
        hook = PostgresHook(postgres_conn_id="postgres")
        conn = hook.get_conn()
        cursor = conn.cursor()
        try:
            cleanup_stats = _run_database_cleaning(cursor)
            conn.commit()
            print("Database cleanup rules applied to Postgres Docker")
            affected_stats = [stat for stat in cleanup_stats if stat["rows_updated"] > 0]
            affected_tables = sorted({stat["table"] for stat in affected_stats})
            affected_columns = {
                (stat["table"], column)
                for stat in affected_stats
                for column in stat["columns"]
            }
            print("=== Postgres Docker database cleanup summary ===")
            print(f"Cleanup rules checked: {len(cleanup_stats)}")
            print(f"Tables cleaned in Postgres: {len(affected_tables)}")
            print(f"Columns cleaned in Postgres: {len(affected_columns)}")
            print(f"Rows updated by cleanup rules: {sum(stat['rows_updated'] for stat in cleanup_stats)}")
            for stat in cleanup_stats:
                print(
                    f"{stat['table']}: columns={stat['columns']}, "
                    f"rows_updated={stat['rows_updated']}"
                )
            return {
                "cleanup_rules_checked": len(cleanup_stats),
                "tables_cleaned": len(affected_tables),
                "columns_cleaned": len(affected_columns),
                "rows_updated": sum(stat["rows_updated"] for stat in cleanup_stats),
                "rule_stats": cleanup_stats,
            }
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()

    @task
    def verify_table_counts():
        hook = PostgresHook(postgres_conn_id="postgres")
        conn = hook.get_conn()
        cursor = conn.cursor()
        try:
            counts = []
            for _csv_file, table_name in TABLE_FILES:
                row_count = _count_rows(cursor, table_name)
                print(f"{table_name}: rows={row_count}")
                counts.append({"table": table_name, "rows": row_count})

            zero_row_tables = [stat["table"] for stat in counts if stat["rows"] == 0]
            if zero_row_tables:
                raise RuntimeError(f"Tables have zero rows after load: {zero_row_tables}")

            return {"table_counts": counts}
        finally:
            cursor.close()
            conn.close()

    validate_csv_files() >> clean_csv_files() >> load_cleaned_csv_to_postgres() >> clean_database_values() >> verify_table_counts()


tamson_csv_to_postgres()
