# Huong dan build lai tu dau

File nay ghi lai thu tu lenh de dung lai toan bo local stack tu mot repo moi hoac tu mot moi truong Docker da reset. Cac lenh ben duoi gia dinh ban dang dung PowerShell tren Windows va Docker Desktop da chay.

## 1. Lay code moi nhat

```powershell
git clone https://github.com/Jayus52Hz/kltn-project.git
cd ".\kltn-project"
```

Neu da co repo local:

```powershell
cd "D:\Đồ án tốt nghiệp"
git pull origin main
```

## 2. Reset Docker state neu muon build sach

Lenh nay xoa container, network va volume cua compose project hien tai. Dung khi muon chay lai tu dau, bao gom MongoDB, MinIO/Iceberg warehouse va Airflow metadata.

```powershell
docker compose -f ".\project\docker-compose.yml" down -v --remove-orphans
```

## 3. Build va start toan bo stack

```powershell
docker compose -f ".\project\docker-compose.yml" up -d --build
```

## 4. Kiem tra trang thai container

```powershell
docker compose -f ".\project\docker-compose.yml" ps -a
```

Ket qua mong doi:

- Cac service chay dai han phai `Up`: `mongodb`, `postgres_metadata`, `zookeeper`, `kafka`, `debezium_connect`, `minio`, `spark-master`, `spark-worker`, `airflow`, `superset`.
- Cac bootstrap job nen `Exited (0)`: `mongo-init`, `mongo-data-init`, `minio-mc`, `debezium-init`.
- `Exited (0)` voi cac job init la dung, khong phai loi. Chung chi chay mot lan de khoi tao replica set, load data, tao bucket va dang ky connector.

Xem log bootstrap:

```powershell
docker compose -f ".\project\docker-compose.yml" logs --tail=120 mongo-init mongo-data-init minio-mc debezium-init
```

## 5. Kiem tra Debezium va Kafka

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

## 6. Chay Airflow DAG

Trigger DAG:

```powershell
docker exec airflow airflow dags trigger telesales_lakehouse_pipeline
```

Lay run id moi nhat:

```powershell
docker exec airflow airflow dags list-runs -d telesales_lakehouse_pipeline --limit 5
```

Kiem tra task state, thay `<run_id>` bang run id vua lay duoc:

```powershell
docker exec airflow airflow tasks states-for-dag-run telesales_lakehouse_pipeline <run_id>
```

Ket qua mong doi:

```text
wait_for_debezium_connector  success
bronze_cdc_ingestion         success
silver_etl                   success
gold_star_schema             success
```

## 7. Kiem tra count trong Iceberg

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

Count mong doi sau mot lan build sach:

```text
lakehouse.silver.cust                  4344
lakehouse.silver.offer                 5072
lakehouse.silver.call_logs             23447
lakehouse.gold.dim_customer            4344
lakehouse.gold.dim_offer               5072
lakehouse.gold.dim_date                2
lakehouse.gold.fact_telesales_calls    23447
```

## 8. Cac URL hay dung

```text
Airflow:       http://localhost:8081  admin / admin
Spark UI:      http://localhost:8080
MinIO Console: http://localhost:9001  minioadmin / minioadmin
Superset:      http://localhost:8088
Debezium REST: http://localhost:8083
Kafka:         localhost:9092
```

## 9. Ghi chu loi thuong gap

- `mongo-data-init` hoac `debezium-init` khong `Up`: binh thuong neu status la `Exited (0)`.
- `mongo-data-init` bi `Exited (1)`: xem log bang `docker compose -f ".\project\docker-compose.yml" logs mongo-data-init`.
- Debezium connector khong `RUNNING`: chay `docker compose -f ".\project\docker-compose.yml" logs debezium_connect debezium-init`.
- Silver loi model: kiem tra `NLP model/models/bow_model.pkl` co ton tai va compose da mount vao `/opt/spark/work-dir/batch-etl/models`.
- Silver loi Python worker khac version: build lai image bang `docker compose -f ".\project\docker-compose.yml" up -d --build --force-recreate airflow spark-master spark-worker`.
