#!/usr/bin/env python3
"""
Import dữ liệu từ CSV vào database Tam Sơn (disable constraints)
"""
import os
import pandas as pd
import psycopg2

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', '5432')),
    'database': os.getenv('DB_NAME', 'tamson_ecommerce'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.environ['DB_PASSWORD']
}

CSV_DIR = 'tamson_sales_data'

TABLE_MAPPING = {
    'core_website.csv': 'core_website',
    'core_store.csv': 'core_store',
    'eav_attribute_set.csv': 'eav_attribute_set',
    'catalog_category_entity.csv': 'catalog_category_entity',
    'catalog_product_entity.csv': 'catalog_product_entity',
    'catalog_product_entity_varchar.csv': 'catalog_product_entity_varchar',
    'catalog_product_entity_decimal.csv': 'catalog_product_entity_decimal',
    'catalog_category_product.csv': 'catalog_category_product',
    'catalog_product_website.csv': 'catalog_product_website',
    'cataloginventory_stock.csv': 'cataloginventory_stock',
    'cataloginventory_stock_item.csv': 'cataloginventory_stock_item',
    'cataloginventory_stock_status.csv': 'cataloginventory_stock_status',
    'customer_entity.csv': 'customer_entity',
    'customer_address_entity.csv': 'customer_address_entity',
    'admin_user.csv': 'admin_user',
    'sales_flat_quote.csv': 'sales_flat_quote',
    'sales_flat_quote_item.csv': 'sales_flat_quote_item',
    'sales_flat_order.csv': 'sales_flat_order',
    'sales_flat_order_item.csv': 'sales_flat_order_item',
    'sales_flat_order_address.csv': 'sales_flat_order_address',
    'sales_flat_order_payment.csv': 'sales_flat_order_payment',
    'sales_target.csv': 'sales_target',
}

def get_db_columns(cursor, table_name):
    """Lấy danh sách column trong DB"""
    try:
        cursor.execute(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position
        """)
        return set([row[0] for row in cursor.fetchall()])
    except:
        return set()

def import_csv_to_db(conn, csv_file, table_name):
    """Import CSV file"""
    try:
        filepath = os.path.join(CSV_DIR, csv_file)

        if table_name == 'cataloginventory_stock':
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS cataloginventory_stock (
                    stock_id SMALLSERIAL PRIMARY KEY,
                    stock_name VARCHAR(255) NOT NULL UNIQUE
                )
            """)
            conn.commit()
            cursor.close()
        
        if not os.path.exists(filepath):
            print(f"⚠️  {table_name}: File không tìm thấy")
            return False
        
        df = pd.read_csv(filepath)
        
        if df.empty:
            print(f"⏭️  {table_name}: Không có dữ liệu")
            return True
        
        # Làm sạch cột
        df.columns = [col.lower().replace(' ', '_') for col in df.columns]
        df = df.where(pd.notnull(df), None)
        
        # Lọc columns match DB
        cursor = conn.cursor()
        db_cols = get_db_columns(cursor, table_name)
        df_cols = [c for c in df.columns if c in db_cols]
        
        if not df_cols:
            print(f"⚠️  {table_name}: Không có matching columns")
            return False
        
        df = df[df_cols]
        
        # COPY từ CSV
        from io import StringIO
        buffer = StringIO()
        
        df.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
        buffer.seek(0)
        
        cols_str = ','.join(df_cols)
        cursor.copy_from(buffer, table_name, columns=df_cols, null='\\N')
        
        conn.commit()
        print(f"✅ {table_name}: {len(df)} rows")
        cursor.close()
        return True
        
    except Exception as e:
        conn.rollback()
        err = str(e)[:80]
        print(f"❌ {table_name}: {err}")
        return False

def main():
    print("=" * 60)
    print("Import dữ liệu Tam Sơn (No FK Constraints)")
    print("=" * 60)
    
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # Disable all constraints
    cursor.execute("ALTER TABLE catalog_category_entity DISABLE TRIGGER ALL")
    cursor.execute("ALTER TABLE catalog_product_entity DISABLE TRIGGER ALL")
    cursor.execute("ALTER TABLE catalog_product_entity_varchar DISABLE TRIGGER ALL")
    cursor.execute("ALTER TABLE catalog_product_entity_decimal DISABLE TRIGGER ALL")
    cursor.execute("ALTER TABLE catalog_category_product DISABLE TRIGGER ALL")
    cursor.execute("ALTER TABLE catalog_product_website DISABLE TRIGGER ALL")
    cursor.execute("ALTER TABLE cataloginventory_stock_item DISABLE TRIGGER ALL")
    cursor.execute("ALTER TABLE cataloginventory_stock_status DISABLE TRIGGER ALL")
    cursor.execute("ALTER TABLE customer_address_entity DISABLE TRIGGER ALL")
    cursor.execute("ALTER TABLE admin_user DISABLE TRIGGER ALL")
    cursor.execute("ALTER TABLE sales_flat_quote DISABLE TRIGGER ALL")
    cursor.execute("ALTER TABLE sales_flat_quote_item DISABLE TRIGGER ALL")
    cursor.execute("ALTER TABLE sales_flat_order DISABLE TRIGGER ALL")
    cursor.execute("ALTER TABLE sales_flat_order_item DISABLE TRIGGER ALL")
    cursor.execute("ALTER TABLE sales_flat_order_address DISABLE TRIGGER ALL")
    cursor.execute("ALTER TABLE sales_flat_order_payment DISABLE TRIGGER ALL")
    cursor.execute("ALTER TABLE sales_target DISABLE TRIGGER ALL")
    
    conn.commit()
    cursor.close()
    
    success = 0
    for csv_file, table_name in TABLE_MAPPING.items():
        if import_csv_to_db(conn, csv_file, table_name):
            success += 1
    
    # Re-enable constraints
    cursor = conn.cursor()
    cursor.execute("ALTER TABLE catalog_category_entity ENABLE TRIGGER ALL")
    cursor.execute("ALTER TABLE catalog_product_entity ENABLE TRIGGER ALL")
    cursor.execute("ALTER TABLE catalog_product_entity_varchar ENABLE TRIGGER ALL")
    cursor.execute("ALTER TABLE catalog_product_entity_decimal ENABLE TRIGGER ALL")
    cursor.execute("ALTER TABLE catalog_category_product ENABLE TRIGGER ALL")
    cursor.execute("ALTER TABLE catalog_product_website ENABLE TRIGGER ALL")
    cursor.execute("ALTER TABLE cataloginventory_stock_item ENABLE TRIGGER ALL")
    cursor.execute("ALTER TABLE cataloginventory_stock_status ENABLE TRIGGER ALL")
    cursor.execute("ALTER TABLE customer_address_entity ENABLE TRIGGER ALL")
    cursor.execute("ALTER TABLE admin_user ENABLE TRIGGER ALL")
    cursor.execute("ALTER TABLE sales_flat_quote ENABLE TRIGGER ALL")
    cursor.execute("ALTER TABLE sales_flat_quote_item ENABLE TRIGGER ALL")
    cursor.execute("ALTER TABLE sales_flat_order ENABLE TRIGGER ALL")
    cursor.execute("ALTER TABLE sales_flat_order_item ENABLE TRIGGER ALL")
    cursor.execute("ALTER TABLE sales_flat_order_address ENABLE TRIGGER ALL")
    cursor.execute("ALTER TABLE sales_flat_order_payment ENABLE TRIGGER ALL")
    cursor.execute("ALTER TABLE sales_target ENABLE TRIGGER ALL")
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print("=" * 60)
    print(f"Hoàn tất: {success}/{len(TABLE_MAPPING)}")
    print("=" * 60)

if __name__ == '__main__':
    main()
