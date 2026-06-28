# Huong Dan Chay Cac DAG Tam Son

Tai lieu nay mo ta cach hoat dong va cach chay cac DAG chinh trong thu muc `dags/`.

## Tong Quan Luong Du Lieu

Pipeline hien tai di theo thu tu:

1. Tao/cap nhat Airflow connections tu file `.env`.
2. Nap du lieu CSV Tam Son vao Postgres local trong Docker.
3. Khoi tao schema tren Postgres EC2 neu database EC2 chua co bang.
4. Dong bo cac bang public tu Postgres local sang Postgres EC2.
5. Tinh forecast doanh so va de xuat phan bo ton kho, sau do sync ket qua forecast sang EC2.

Tat ca DAG deu de `schedule=None`, nen can trigger thu cong tu Airflow UI hoac CLI.

## Cai Dat Cong Cu Can Thiet

Project nay chay bang Astro/Airflow tren Docker. Truoc khi build project, can cai cac cong cu sau tren may local.

### 1. Cai Docker Desktop

Docker dung de chay Airflow webserver, scheduler, Postgres, Metabase va pgAdmin.

Tren Windows, co the cai Docker Desktop bang `winget`:

```powershell
winget install -e --id Docker.DockerDesktop
```

Hoac tai installer tu trang chinh thuc: `https://docs.docker.com/desktop/setup/install/windows-install/`.

Sau khi cai xong:

1. Mo Docker Desktop.
2. Bat WSL 2 backend neu Docker yeu cau.
3. Dam bao virtualization/Hyper-V/WSL 2 da duoc bat tren Windows.
4. Doi den khi Docker Desktop bao trang thai dang chay.

Kiem tra Docker:

```bash
docker --version
docker ps
```

Neu `docker ps` chay duoc va khong bao loi daemon, Docker da san sang.

### 2. Cai Astro CLI

Astro CLI dung de build va chay Airflow project local bang cac lenh `astro dev ...`.

Tren Windows, co the cai bang `winget`:

```powershell
winget install -e --id Astronomer.Astro
```

Hoac xem huong dan moi nhat tai trang chinh thuc: `https://www.astronomer.io/docs/astro/cli/install-cli/`.

Kiem tra Astro CLI:

```bash
astro version
```

Neu terminal khong nhan lenh `astro`, hay dong terminal hien tai, mo terminal moi va chay lai `astro version`.

### 3. Cai Git Neu Can Clone Project

Neu may chua co Git, cai bang `winget`:

```powershell
winget install -e --id Git.Git
```

Kiem tra Git:

```bash
git --version
```

### 4. Chuan Bi File Moi Truong

Tu thu muc goc project, tao file `.env` tu file mau:

```bash
cp .env.example .env
```

Tren PowerShell Windows, neu lenh `cp` khong dung duoc thi chay:

```powershell
Copy-Item .env.example .env
```

Sau do mo `.env` va dien dung thong tin ket noi local/EC2. Cac bien can co duoc mo ta o phan `Dieu Kien Can Co` ben duoi.

## Nguoi Tai File Zip Can Biet

Neu nguoi khac tai file zip cua project nay ve, ho khong can cai rieng Postgres, Airflow, Metabase hay pgAdmin tren may. Ho chi can cai Docker Desktop va Astro CLI, sau do chay project bang `astro dev start`.

Docker Desktop khong tu co san database Postgres cho project. Docker Desktop chi la nen de chay container. Khi chay `astro dev start`, Astro se doc cau hinh project va Docker se tu tao/cap nhat cac container can thiet.

Lan chay dau tien can co internet de Docker tai cac image can thiet, vi du:

- Airflow image tu `Dockerfile`: `quay.io/astronomer/astro-runtime:13.3.0`
- Postgres metadata/local database cua Astro Airflow project
- Metabase image: `metabase/metabase:v0.52.8.4`
- pgAdmin image: `dpage/pgadmin4:latest`

Sau khi image da tai xong, cac lan chay sau se nhanh hon va khong can tai lai neu image khong thay doi.

## Docker Stack Gom Nhung Gi

Khi chay `astro dev start`, stack local se co cac thanh phan chinh:

- Airflow webserver: giao dien quan ly DAG.
- Airflow scheduler: lap lich va chay task.
- Postgres trong Docker: database local cho Airflow va du lieu Tam Son, duoc map ra host qua cong `5433`.
- Metabase: dashboard/BI, chay o cong `3000`.
- pgAdmin: quan ly Postgres bang giao dien, chay o cong `5050`.

File `docker-compose.override.yml` trong project dang mo cong Postgres local ra `localhost:5433` va them Metabase, pgAdmin vao cung network Docker voi Airflow. Vi vay nguoi dung khong can tu tao container Postgres rieng neu chi muon chay project local.

Neu chay `docker ps` sau khi start, can thay cac container lien quan den Airflow va it nhat cac service nhu `postgres`, `scheduler`, `webserver`, `metabase`, `pgadmin`.

## File Zip Can Co Du Nhung Gi

De nguoi khac tai zip ve va chay duoc gan nhu day du, file zip nen gom cac thanh phan sau:

- `.astro/`: cau hinh Astro project.
- `Dockerfile`: image Airflow runtime cua project.
- `docker-compose.override.yml`: cau hinh them port Postgres, Metabase, pgAdmin.
- `requirements.txt`: Python packages cai vao Airflow image.
- `airflow_settings.yaml`: cau hinh Airflow local.
- `dags/`: cac DAG Airflow.
- `include/`: helper code, config va data dau vao/ra cua pipeline.
- `scripts/`: script tao/cap nhat Airflow connections.
- `tamson_schema.sql`, `tamson_schema_fixed.sql`: schema database.
- `.env.example`: file mau de nguoi dung tao `.env`.

Neu zip co du cac file tren, nguoi dung chi can giai nen, tao `.env`, mo Docker Desktop, roi chay `astro dev start`. Docker se tu tao container Postgres local cho project. Khong can cai Postgres rieng tren may host.

Luu y: cac chuc nang dong bo len Postgres EC2 van can EC2 database that, dung host/user/password trong `.env`, va EC2 phai mo ket noi port `5432`. Neu khong co EC2, nguoi dung van co the chay phan local nhu Airflow, Postgres local, load CSV va quan ly bang pgAdmin/Metabase, nhung cac DAG sync EC2 se loi ket noi.

## Build Va Khoi Dong Project

Image Airflow duoc build tu `Dockerfile` va cai them package trong `requirements.txt`. Khong can tu cai cac Python package nay tren may host vi chung se duoc cai trong Docker image.

Dam bao dang o thu muc goc project, noi co cac file `Dockerfile`, `requirements.txt` va `docker-compose.override.yml`.

Build va khoi dong toan bo stack Airflow:

```bash
astro dev start
```

Lenh nay se build image Airflow, cai dependencies, sau do chay cac container nhu webserver, scheduler, Postgres, Metabase va pgAdmin.

Neu thay doi `Dockerfile`, `requirements.txt` hoac dependency lien quan, chay lai:

```bash
astro dev restart
```

Kiem tra cac container da chay:

```bash
docker ps
```

Cac cong thuong dung sau khi start:

- Airflow UI: `http://localhost:8080`
- Metabase: `http://localhost:3000`
- pgAdmin: `http://localhost:5050`
- Postgres local tren host: `localhost:5433`

Dung project khi khong can chay nua:

```bash
astro dev stop
```

## Dieu Kien Can Co

Airflow va cac container can dang chay:

```bash
docker ps
```

File `.env` can co cac bien ket noi:

```env
LOCAL_DB_HOST=postgres
LOCAL_DB_PORT=5432
LOCAL_DB_NAME=tamson_ecommerce
LOCAL_DB_USER=postgres
LOCAL_DB_PASSWORD=postgres

EC2_DB_HOST=15.134.228.145
EC2_DB_PORT=5432
EC2_DB_NAME=tamson_ecommerce
EC2_DB_USER=postgres
EC2_DB_PASSWORD=postgres
```

Postgres EC2 phai mo port `5432` va cho phep Airflow container ket noi.

## Cap Nhat Airflow Connections

File `setup_ec2_connection.py` khong tao DAG tren UI. Day la script helper de goi chung logic tao/cap nhat Airflow connections tu `scripts/create_airflow_connections.py`.

Chay lenh sau sau khi thay doi `.env`:

```bash
docker cp .env airflow_fd0587-webserver-1:/usr/local/airflow/.env
docker cp .env airflow_fd0587-scheduler-1:/usr/local/airflow/.env
docker exec airflow_fd0587-webserver-1 python /usr/local/airflow/scripts/create_airflow_connections.py
```

Neu thanh cong, log se co dang:

```text
Upserted Connection: postgres_ec2 (postgres://15.134.228.145:5432/tamson_ecommerce)
TCP check passed: 15.134.228.145:5432
```

Kiem tra connection hien tai:

```bash
docker exec airflow_fd0587-webserver-1 airflow connections get postgres_ec2
```

## Thu Tu Chay Khuyen Nghi

Chay theo thu tu nay khi dung moi truong tu dau:

1. `tamson_csv_to_postgres`
2. `postgres_ec2_init_table`
3. `postgres_to_ec2_transfer`
4. `forecast_sales`

Lenh trigger bang CLI:

```bash
docker exec airflow_fd0587-webserver-1 airflow dags trigger tamson_csv_to_postgres
docker exec airflow_fd0587-webserver-1 airflow dags trigger postgres_ec2_init_table
docker exec airflow_fd0587-webserver-1 airflow dags trigger postgres_to_ec2_transfer
docker exec airflow_fd0587-webserver-1 airflow dags trigger forecast_sales
```

Hoac vao Airflow UI, bat DAG va bam Trigger.

## `tamson_csv_to_postgres.py`

DAG ID: `tamson_csv_to_postgres`

Muc dich: lam sach CSV Tam Son va load idempotent vao Postgres local trong Docker qua Airflow connection `postgres`.

Task flow:

```text
validate_csv_files
  -> clean_csv_files
  -> load_cleaned_csv_to_postgres
  -> clean_database_values
  -> verify_table_counts
```

Hoat dong chinh:

- Kiem tra cac file CSV raw theo cau hinh bang.
- Chuan hoa ten cot, trim text, doi gia tri rong thanh NULL.
- Ghi CSV da lam sach ra thu muc cleaned data.
- Load vao Postgres local bang temp table va conflict handling.
- Reset serial sequence sau khi load.
- Kiem tra row count sau khi load.

Nen chay DAG nay truoc khi dong bo len EC2.

## `postgres_to_ec2_init.py`

DAG ID: `postgres_ec2_init_table`

Muc dich: khoi tao schema `tamson_ecommerce` tren Postgres EC2 bang file schema SQL.

Task flow:

```text
create_schema_from_file
```

Hoat dong chinh:

- Doc file `tamson_schema_fixed.sql`.
- Ket noi EC2 bang Airflow connection `postgres_ec2`.
- Neu EC2 da co bang `core_website`, DAG bo qua buoc tao schema.
- Neu chua co schema, DAG chay SQL de tao bang, sequence, index va constraint.

Chi can chay khi database EC2 moi/chua co schema.

## `postgres_to_ec2.py`

DAG ID: `postgres_to_ec2_transfer`

Muc dich: dong bo tat ca bang public tu Postgres local sang Postgres EC2.

Task flow:

```text
transfer_all_public_tables -> verify_table_counts
```

Hoat dong chinh:

- Doc danh sach bang public tu Postgres local.
- Bo qua mot so bang output/ngoai pham vi sync theo cau hinh trong helper.
- Sap xep thu tu sync theo foreign key dependency.
- Insert du lieu theo chunk, dung primary key conflict de tranh nap trung.
- Reset sequence tren EC2 sau khi sync.
- So sanh row count local va EC2.

Can chay sau `tamson_csv_to_postgres` va sau khi EC2 da co schema.

## `forecast_sales.py`

DAG ID: `forecast_sales`

Muc dich: tinh du bao nhu cau ban hang va de xuat phan bo ton kho theo cua hang, sau do sync ket qua sang EC2.

Task flow:

```text
build_forecast_outputs -> sync_outputs_to_ec2
```

Hoat dong chinh:

- Doc doanh so theo ngay/cua hang tu `sales_flat_order`, `sales_flat_order_item`, `core_store`.
- Loai cac don bi huy.
- Tinh forecast 7 ngay va 30 ngay dua tren toi da 30 ngay ban gan nhat.
- Tinh `demand_share` cua tung cua hang.
- Doc tong ton kho kha dung tu `cataloginventory_stock_item`.
- Tao bang output neu chua co:
  - `forecast_sales`
  - `inventory_allocation`
- Ghi output vao Postgres local va CSV tai `include/data/forecast_outputs/`.
- Sync 2 bang output sang Postgres EC2.

Nen chay sau khi du lieu sales va inventory da duoc load/sync on dinh.

## Kiem Tra Log Va Loi Thuong Gap

Xem danh sach DAG:

```bash
docker exec airflow_fd0587-webserver-1 airflow dags list
```

Xem task failed trong Airflow UI:

```text
Airflow UI -> DAG -> Run -> Task -> Logs
```

Loi EC2 connection thuong gap:

```text
Cannot connect to Airflow connection "postgres_ec2"
connection refused
timed out
```

Can kiem tra:

- `EC2_DB_HOST`, `EC2_DB_PORT`, user/password trong `.env`.
- Airflow connection `postgres_ec2` da duoc upsert lai chua.
- Security Group EC2 da mo inbound TCP `5432` chua.
- PostgreSQL tren EC2 co listen `0.0.0.0:5432` chua.
- Neu Postgres chay bang Docker tren EC2, container co expose `-p 5432:5432` chua.

## Ghi Chu

- Nen chay tung DAG theo thu tu va doi DAG truoc `success` roi moi chay DAG tiep theo.
