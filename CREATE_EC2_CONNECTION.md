# Tạo EC2 PostgreSQL Connection

## Thông tin Connection:
- **Host**: `<EC2_DB_HOST>`
- **Database**: tamson_ecommerce
- **User**: `<EC2_DB_USER>`
- **Password**: `<EC2_DB_PASSWORD>`
- **Port**: 5432

## Cách 1: Tạo qua Airflow UI (Khuyến nghị)

1. Truy cập Airflow UI: http://localhost:8080
2. Vào **Admin** → **Connections**
3. Click nút **+** (Add a new record)
4. Điền thông tin:
   ```
   Connection Id: postgres_ec2
   Connection Type: Postgres
    Host: <EC2_DB_HOST>
    Schema: tamson_ecommerce
   Login: <EC2_DB_USER>
   Password: <EC2_DB_PASSWORD>
   Port: 5432
   ```
5. Click **Save**

## Cách 2: Tạo qua CLI (trong container)

```bash
docker exec -it <airflow-webserver-container> bash

airflow connections add 'postgres_ec2' \
    --conn-type 'postgres' \
    --conn-host "$EC2_DB_HOST" \
    --conn-schema "${EC2_DB_NAME:-tamson_ecommerce}" \
    --conn-login "${EC2_DB_USER:-postgres}" \
    --conn-password "$EC2_DB_PASSWORD" \
    --conn-port "${EC2_DB_PORT:-5432}"
```

## Cách 3: Chạy script Python

```bash
docker exec -it <airflow-webserver-container> python /opt/airflow/dags/setup_ec2_connection.py
```

## Test Connection

Sau khi tạo xong, test connection bằng cách:

1. Vào Airflow UI → Admin → Connections
2. Tìm connection `postgres_ec2`
3. Click vào Test button
4. Hoặc chạy DAG `postgres_to_ec2_transfer` để kiểm tra
