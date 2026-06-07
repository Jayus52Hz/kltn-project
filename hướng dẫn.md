# Rebuild Guide

Short command list for a full local rebuild and BigQuery refresh.

## 1. Open PowerShell

```powershell
cd "<repo-root>"
```

## 2. Check Google Cloud auth

Use `.cmd` on Windows because PowerShell may block `gcloud.ps1`.

```powershell
gcloud.cmd auth list
gcloud.cmd config set project project-ef0c6db5-0765-4391-845
gcloud.cmd auth application-default login
```

## 3. Delete the old Docker stack

This removes containers, networks, volumes, and local images for this Compose project.

```powershell
docker compose -f ".\project\docker-compose.yml" down -v --remove-orphans --rmi local
```

## 4. Reset BigQuery

This deletes and recreates the dataset used by the pipeline.

```powershell
bq.cmd rm -r -f -d project-ef0c6db5-0765-4391-845:kltn0710
bq.cmd --location=asia-southeast1 mk --dataset project-ef0c6db5-0765-4391-845:kltn0710
```

## 5. Rebuild and start everything

```powershell
docker compose -f ".\project\docker-compose.yml" up -d --build
```

## 6. Check containers

```powershell
docker compose -f ".\project\docker-compose.yml" ps -a
```

Expected:

- Long-running services are `Up`.
- Init jobs are `Exited (0)`: `mongo-init`, `mongo-data-init`, `minio-mc`, `debezium-init`.

## 7. Check Debezium and Kafka

```powershell
docker exec debezium_connect curl -fsS http://localhost:8083/connectors/mongo-source/status
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --list
```

## 8. Run the Airflow pipeline

```powershell
docker exec airflow airflow dags trigger telesales_lakehouse_pipeline
```

Check task status:

```powershell
docker exec airflow airflow dags list-runs -d telesales_lakehouse_pipeline
docker exec airflow airflow tasks states-for-dag-run telesales_lakehouse_pipeline "<run_id>"
```

All tasks should be `success`:

```text
wait_for_debezium_connector
bronze_cdc_ingestion
silver_etl
gold_star_schema
bq_sync_gold
```

## 9. Create the BigQuery serving view

Run this after `bq_sync_gold` succeeds.

```powershell
Get-Content -Raw ".\project\bigquery\create_serving_views.sql" | bq.cmd query --use_legacy_sql=false
```

## 10. Validate BigQuery counts

```powershell
@'
SELECT 'dim_customer' AS object_name, COUNT(*) AS row_count FROM `project-ef0c6db5-0765-4391-845.kltn0710.dim_customer`
UNION ALL SELECT 'dim_offer', COUNT(*) FROM `project-ef0c6db5-0765-4391-845.kltn0710.dim_offer`
UNION ALL SELECT 'dim_date', COUNT(*) FROM `project-ef0c6db5-0765-4391-845.kltn0710.dim_date`
UNION ALL SELECT 'fact_telesales_calls', COUNT(*) FROM `project-ef0c6db5-0765-4391-845.kltn0710.fact_telesales_calls`
UNION ALL SELECT 'vw_telesales_performance', COUNT(*) FROM `project-ef0c6db5-0765-4391-845.kltn0710.vw_telesales_performance`
'@ | bq.cmd query --use_legacy_sql=false
```

Expected counts:

```text
dim_customer              4344
dim_offer                 5072
dim_date                     2
fact_telesales_calls     23447
vw_telesales_performance 23447
```

## Useful URLs

```text
Airflow:        http://localhost:8081  admin / admin
Spark UI:       http://localhost:8080
MinIO Console:  http://localhost:9001  minioadmin / minioadmin
Superset:       http://localhost:8088  admin / admin
Dashboard:      http://localhost:8090
Debezium REST:  http://localhost:8083
```
