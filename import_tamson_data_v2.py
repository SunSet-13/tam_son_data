#!/usr/bin/env python3
"""
Import dữ liệu từ CSV vào database tamson_ecommerce
"""
import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# Cấu hình kết nối database
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', '5432')),
    'database': os.getenv('DB_NAME', 'tamson_ecommerce'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.environ['DB_PASSWORD']
}

CSV_DIR = 'tamson_sales_data'

# Thứ tự import - dependencies trước
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
    'cataloginventory_stock_item.csv': 'cataloginventory_stock_item',
    'cataloginventory_stock_status.csv': 'cataloginventory_stock_status',
    'cataloginventory_stock.csv': 'cataloginventory_stock',
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

def get_db_columns(table_name):
    """Lấy danh sách column có trong database"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position
        """)
        columns = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return set(columns)
    except Exception as e:
        print(f"Lỗi lấy columns: {e}")
        return set()

def ensure_cataloginventory_stock_table():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS cataloginventory_stock (
            stock_id SMALLSERIAL PRIMARY KEY,
            stock_name VARCHAR(255) NOT NULL UNIQUE
        )
    """)
    conn.commit()
    cursor.close()
    conn.close()

def import_csv_to_db(csv_file, table_name):
    """Import một CSV file vào một bảng"""
    try:
        filepath = os.path.join(CSV_DIR, csv_file)

        if table_name == 'cataloginventory_stock':
            ensure_cataloginventory_stock_table()
        
        if not os.path.exists(filepath):
            print(f"⚠️  File không tìm thấy: {filepath}")
            return False
        
        # Đọc CSV
        df = pd.read_csv(filepath)
        
        if df.empty:
            print(f"⏭️  {table_name}: Không có dữ liệu")
            return True
        
        # Làm sạch tên cột
        df.columns = [col.lower().replace(' ', '_') for col in df.columns]
        
        # Replace NaN → None
        df = df.where(pd.notnull(df), None)
        
        # Lọc columns tồn tại trong DB
        db_columns = get_db_columns(table_name)
        df_columns = [col for col in df.columns if col in db_columns]
        
        if not df_columns:
            print(f"⚠️  {table_name}: Không có matching columns")
            return False
        
        df = df[df_columns]
        
        # Import với SQLAlchemy
        engine_url = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        engine = create_engine(engine_url)
        
        df.to_sql(table_name, engine, if_exists='append', index=False)
        
        print(f"✅ {table_name}: {len(df)} rows")
        return True
        
    except Exception as e:
        err_msg = str(e)[:80]
        print(f"❌ {table_name}: {err_msg}")
        return False

def main():
    print("=" * 60)
    print("Import dữ liệu Tam Sơn")
    print("=" * 60)
    
    success = 0
    for csv_file, table_name in TABLE_MAPPING.items():
        if import_csv_to_db(csv_file, table_name):
            success += 1
    
    print("=" * 60)
    print(f"Hoàn tất: {success}/{len(TABLE_MAPPING)}")
    print("=" * 60)

if __name__ == '__main__':
    main()
