# Huong Dan Chay He Thong Va Sync BigQuery

Tai lieu nay ghi lai thu tu lenh de dung lai toan bo local stack, chay pipeline Bronze/Silver/Gold, sync Gold len BigQuery, va ket noi Looker Studio hoac Superset.

Gia dinh:

- Ban dang dung PowerShell tren Windows.
- Docker Desktop da chay.
- Google Cloud SDK da cai.
- Repo nam o `D:\Do an tot nghiep` hoac duong dan tuong duong.

## 1. Lay code moi nhat

Neu clone moi:

```powershell
git clone https://github.com/Jayus52Hz/kltn-project.git
cd ".\kltn-project"
```

Neu da co repo local:

```powershell
cd "D:\Do an tot nghiep"
git pull origin main
```

## 2. Dang nhap Google Cloud va BigQuery

PowerShell co the chan `gcloud.ps1`, nen nen dung file `.cmd`:

```powershell
gcloud.cmd init
gcloud.cmd auth application-default login
gcloud.cmd config set project project-ef0c6db5-0765-4391-845
```

Kiem tra account va project:

```powershell
gcloud.cmd auth list
gcloud.cmd config get-value project
```

Kiem tra BigQuery dataset:

```powershell
bq.cmd ls project-ef0c6db5-0765-4391-845:kltn0710
```

File credential local phai ton tai tai:

```text
%APPDATA%\gcloud\application_default_credentials.json
```

Docker Compose se mount file nay vao container tai:

```text
/opt/gcp/application_default_credentials.json
```

## 3. Reset Docker neu muon build sach

Lenh nay xoa container, network va volume cua compose project hien tai. Dung khi muon chay lai tu dau, bao gom MongoDB, MinIO/Iceberg warehouse va Airflow metadata.

```powershell
docker compose -f ".\project\docker-compose.yml" down -v --remove-orphans
```

## 4. Build va start toan bo stack

```powershell
docker compose -f ".\project\docker-compose.yml" up -d --build
```

Neu chi vua thay doi cau hinh BigQuery/Superset, co the recreate cac service lien quan:

```powershell
docker compose -f ".\project\docker-compose.yml" up -d --build --force-recreate airflow spark-master spark-worker superset
```

## 5. Kiem tra trang thai container

```powershell
docker compose -f ".\project\docker-compose.yml" ps -a
```

Ket qua mong doi:

- Service chay dai han phai `Up`: `mongodb`, `postgres_metadata`, `zookeeper`, `kafka`, `debezium_connect`, `minio`, `spark-master`, `spark-worker`, `airflow`, `superset`, `telesales-dashboard`.
- Bootstrap job nen `Exited (0)`: `mongo-init`, `mongo-data-init`, `minio-mc`, `debezium-init`.
- `Exited (0)` voi cac job init la dung, khong phai loi.

Xem log bootstrap:

```powershell
docker compose -f ".\project\docker-compose.yml" logs --tail=120 mongo-init mongo-data-init minio-mc debezium-init
```

## 6. Kiem tra Debezium va Kafka

```powershell
docker exec debezium_connect curl -fsS http://localhost:8083/connectors/mongo-source/status
```

Trang thai mong doi: connector `RUNNING`, task `RUNNING`.

```powershell
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --list
```

Can thay cac topic:

```text
mongo-source.telesales.cust
mongo-source.telesales.offer
mongo-source.telesales.call_logs
```

## 7. Chay Airflow DAG day du

Trigger DAG:

```powershell
docker exec airflow airflow dags trigger telesales_lakehouse_pipeline
```

Lay run id moi nhat:

```powershell
docker exec airflow airflow dags list-runs -d telesales_lakehouse_pipeline --limit 5
```

Kiem tra task state, thay `<run_id>` bang run id vua lay:

```powershell
docker exec airflow airflow tasks states-for-dag-run telesales_lakehouse_pipeline <run_id>
```

Ket qua mong doi:

```text
wait_for_debezium_connector  success
bronze_cdc_ingestion         success
silver_etl                   success
gold_star_schema             success
bq_sync_gold                 success
```

## 8. Chi sync lai BigQuery neu Gold da co san

Neu Gold Iceberg tables da co du lieu, co the chay rieng task sync:

```powershell
docker exec airflow airflow tasks test telesales_lakehouse_pipeline bq_sync_gold 2026-05-29
```

Task nay doc:

```text
lakehouse.gold.dim_customer
lakehouse.gold.dim_offer
lakehouse.gold.dim_date
lakehouse.gold.fact_telesales_calls
```

Va ghi len BigQuery:

```text
project-ef0c6db5-0765-4391-845.kltn0710.dim_customer
project-ef0c6db5-0765-4391-845.kltn0710.dim_offer
project-ef0c6db5-0765-4391-845.kltn0710.dim_date
project-ef0c6db5-0765-4391-845.kltn0710.fact_telesales_calls
```

Mac dinh task se khong day `full_name` va `address` len BigQuery. Cac field phone/national id da la masked field:

```text
phone_number_masked
national_id_masked
```

## 9. Tao BigQuery serving view cho BI

Sau khi 4 table da co tren BigQuery, tao view:

```powershell
Get-Content -Raw ".\project\bigquery\create_serving_views.sql" | bq.cmd query --use_legacy_sql=false
```

View duoc tao:

```text
project-ef0c6db5-0765-4391-845.kltn0710.vw_telesales_performance
```

Kiem tra count:

```powershell
bq.cmd query --use_legacy_sql=false "SELECT COUNT(*) AS row_count FROM ``project-ef0c6db5-0765-4391-845.kltn0710.vw_telesales_performance``"
```

Ket qua mong doi:

```text
23447
```

## 10. Kiem tra count BigQuery

```powershell
bq.cmd query --use_legacy_sql=false "SELECT COUNT(*) AS row_count FROM ``project-ef0c6db5-0765-4391-845.kltn0710.dim_customer``"
bq.cmd query --use_legacy_sql=false "SELECT COUNT(*) AS row_count FROM ``project-ef0c6db5-0765-4391-845.kltn0710.dim_offer``"
bq.cmd query --use_legacy_sql=false "SELECT COUNT(*) AS row_count FROM ``project-ef0c6db5-0765-4391-845.kltn0710.dim_date``"
bq.cmd query --use_legacy_sql=false "SELECT COUNT(*) AS row_count FROM ``project-ef0c6db5-0765-4391-845.kltn0710.fact_telesales_calls``"
```

Count mong doi:

```text
dim_customer           4344
dim_offer              5072
dim_date               2
fact_telesales_calls   23447
```

## 11. Connect Looker Studio voi BigQuery

1. Mo Looker Studio.
2. Tao data source moi.
3. Chon connector `BigQuery`.
4. Chon:
   - Project: `project-ef0c6db5-0765-4391-845`
   - Dataset: `kltn0710`
   - View: `vw_telesales_performance`
5. Tao report dua tren view nay.

Goi y chart:

| Muc dich | Field |
|---|---|
| KPI tong calls | count `call_id` |
| KPI successful sales | sum/count `has_successful_sale` |
| Success rate | `has_successful_sale` / count `call_id` |
| Calls theo ngay | `full_date`, count `call_id` |
| Outcome breakdown | `outcome_category` |
| Product performance | `product_category`, `product_name` |
| Customer segment | `age_group`, `income_band`, `credit_tier` |
| Agent performance | `agent_id`, count calls, avg `talk_time_seconds` |

## 12. Connect Superset voi BigQuery

Superset local duoc build tu:

```text
project/superset/Dockerfile
```

Image nay cai them:

```text
sqlalchemy-bigquery
pandas-gbq
```

Mo Superset:

```text
http://localhost:8088
```

Dang nhap:

```text
admin / admin
```

Them database:

```text
SQLAlchemy URI: bigquery://project-ef0c6db5-0765-4391-845
```

Sau do tao dataset tu:

```text
kltn0710.vw_telesales_performance
```

Neu Superset khong nhan credential, recreate lai sau khi da login ADC:

```powershell
docker compose -f ".\project\docker-compose.yml" up -d --build --force-recreate superset
```

## 13. Kiem tra count trong Iceberg

```powershell
docker exec airflow bash -lc "python - <<'PY'
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName('check_lakehouse_counts')
    .master('local[*]')
    .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
    .config('spark.sql.catalog.lakehouse', 'org.apache.iceberg.spark.SparkCatalog')
    .config('spark.sql.catalog.lakehouse.type', 'hadoop')
    .config('spark.sql.catalog.lakehouse.warehouse', 's3a://lakehouse/warehouse')
    .config('spark.hadoop.fs.s3a.endpoint', 'http://minio:9000')
    .config('spark.hadoop.fs.s3a.access.key', 'minioadmin')
    .config('spark.hadoop.fs.s3a.secret.key', 'minioadmin')
    .config('spark.hadoop.fs.s3a.path.style.access', 'true')
    .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
    .config('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.261')
    .getOrCreate()
)

spark.sparkContext.setLogLevel('ERROR')

tables = [
    'lakehouse.silver.cust',
    'lakehouse.silver.offer',
    'lakehouse.silver.call_logs',
    'lakehouse.gold.dim_customer',
    'lakehouse.gold.dim_offer',
    'lakehouse.gold.dim_date',
    'lakehouse.gold.fact_telesales_calls',
]

for table in tables:
    print(f'{table}\t{spark.table(table).count()}')

spark.stop()
PY"
```

## 14. Tao dashboard demo nhanh

Sau khi DAG Airflow chay xong va Gold tables da co du lieu, xuat data cho dashboard tinh:

```powershell
docker compose -f ".\project\docker-compose.yml" --profile dashboard run --rm dashboard-export
```

Mo dashboard:

```powershell
docker compose -f ".\project\docker-compose.yml" up -d dashboard
```

Truy cap:

```text
http://localhost:8090
```

Dashboard nay doc file `project/dashboard/dashboard_data.json`. Moi lan chay lai pipeline va muon refresh so lieu, chay lai job `dashboard-export`.

## 15. Cac URL hay dung

```text
Airflow:        http://localhost:8081  admin / admin
Spark UI:       http://localhost:8080
MinIO Console:  http://localhost:9001  minioadmin / minioadmin
Superset:       http://localhost:8088  admin / admin
Demo dashboard: http://localhost:8090
Debezium REST:  http://localhost:8083
Kafka:          localhost:9092
```

## 16. Loi thuong gap

- `gcloud` bi PowerShell chan script: dung `gcloud.cmd` thay vi `gcloud`.
- `bq` khong tim thay credential: chay `gcloud.cmd auth application-default login`.
- Docker khong mount duoc credential: kiem tra `%APPDATA%\gcloud\application_default_credentials.json`.
- `mongo-data-init` hoac `debezium-init` khong `Up`: binh thuong neu status la `Exited (0)`.
- `mongo-data-init` bi `Exited (1)`: xem log bang `docker compose -f ".\project\docker-compose.yml" logs mongo-data-init`.
- Debezium connector khong `RUNNING`: chay `docker compose -f ".\project\docker-compose.yml" logs debezium_connect debezium-init`.
- Silver loi model: kiem tra `NLP model/models/bow_model.pkl` co ton tai va compose da mount vao `/opt/spark/work-dir/batch-etl/models`.
- Silver loi Python worker khac version: build lai image bang `docker compose -f ".\project\docker-compose.yml" up -d --build --force-recreate airflow spark-master spark-worker`.
- `bq_sync_gold` loi doc Iceberg: kiem tra `minio` dang `Up` va Gold tables da ton tai.
- `bq_sync_gold` loi BigQuery permission: kiem tra account co quyen BigQuery tren project `project-ef0c6db5-0765-4391-845`.
- Dashboard bao chua co data: chay `docker compose -f ".\project\docker-compose.yml" --profile dashboard run --rm dashboard-export`.
