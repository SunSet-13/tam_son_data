"""
Script kiểm tra dữ liệu trên EC2 PostgreSQL
"""
import os

import psycopg2

# Thông tin kết nối EC2
ec2_config = {
    'host': os.environ['EC2_DB_HOST'],
    'database': os.getenv('EC2_DB_NAME', 'postgres'),
    'user': os.getenv('EC2_DB_USER', 'postgres'),
    'password': os.environ['EC2_DB_PASSWORD'],
    'port': int(os.getenv('EC2_DB_PORT', '5432')),
}

def check_ec2_data():
    try:
        print("Đang kết nối tới EC2 PostgreSQL...")
        print(f"Host: {ec2_config['host']}")
        print(f"Database: {ec2_config['database']}")
        
        conn = psycopg2.connect(**ec2_config)
        cursor = conn.cursor()
        
        # Kiểm tra bảng có tồn tại
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'stock_market'
            )
        """)
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            print("\n❌ Bảng 'stock_market' chưa tồn tại trên EC2")
            return
        
        print("\n✓ Bảng 'stock_market' đã tồn tại")
        
        # Đếm số dòng
        cursor.execute("SELECT COUNT(*) FROM stock_market")
        count = cursor.fetchone()[0]
        print(f"✓ Số lượng records: {count}")
        
        # Lấy 5 dòng mới nhất
        cursor.execute("""
            SELECT date, open, high, low, close, volume 
            FROM stock_market 
            ORDER BY date DESC 
            LIMIT 5
        """)
        rows = cursor.fetchall()
        
        print("\n📊 5 records mới nhất:")
        print("Date       | Open    | High    | Low     | Close   | Volume")
        print("-" * 70)
        for row in rows:
            print(f"{row[0]:10} | {row[1]:7.2f} | {row[2]:7.2f} | {row[3]:7.2f} | {row[4]:7.2f} | {row[5]:,}")
        
        # Lấy thông tin ngày đầu và cuối
        cursor.execute("SELECT MIN(date), MAX(date) FROM stock_market")
        min_date, max_date = cursor.fetchone()
        print(f"\n📅 Khoảng thời gian: {min_date} → {max_date}")
        
        cursor.close()
        conn.close()
        
        print("\n✅ Kết nối EC2 PostgreSQL thành công!")
        
    except Exception as e:
        print(f"\n❌ Lỗi kết nối: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_ec2_data()
