# KLTN Hybrid Data Lakehouse for AGI Telesales

Graduation thesis project for building an end-to-end Hybrid Data Lakehouse for an AGI Telesales system.

The platform ingests operational data from MongoDB through Debezium CDC, streams it into Kafka, processes Bronze/Silver/Gold Iceberg tables with Spark, orchestrates the pipeline with Airflow, and serves BI-ready data through Superset plus a lightweight demo dashboard.

## Goals

- Avoid analytics workloads querying MongoDB directly and affecting operational calls.
- Store and process structured records plus unstructured call transcripts at lakehouse scale.
- Mask customer PII before data reaches analytics-facing layers.
- Build a BI-ready Star Schema for telesales performance reporting.
- Provide a local Docker-based demo that can be rebuilt and tested end to end.

## Architecture

```text
master_data/*.json
  -> split_to_entities.py
  -> master_data/output/*.csv
  -> project/init/load_data.py
  -> MongoDB telesales.{cust,offer,call_logs}
  -> Debezium CDC
  -> Kafka topics
  -> Spark Bronze ETL
  -> Iceberg lakehouse.bronze.*
  -> Spark Silver ETL
  -> Iceberg lakehouse.silver.*
  -> Spark Gold ETL
  -> Iceberg lakehouse.gold.*
  -> Superset / dashboard-export / static dashboard
```

Why "Hybrid":

- Hybrid deployment: raw and sensitive data stays in local MinIO/S3-compatible storage; clean serving data can later be published outward if needed.
- Hybrid workloads: CDC/streaming ingestion and batch ETL run in the same Dockerized platform.

## Tech Stack

| Layer | Technology | Container |
|---|---|---|
| Operational database | MongoDB 6.0 ReplicaSet | `mongodb` |
| Metadata database | PostgreSQL 13 | `postgres_metadata` |
| CDC | Debezium Connect 2.1 | `debezium_connect` |
| Queue | Kafka 7.3.0 | `kafka` |
| Coordination | Zookeeper 7.3.0 | `zookeeper` |
| Object storage | MinIO | `minio` |
| Table format | Apache Iceberg | via Spark |
| Compute | Spark 3.4.0 / PySpark | `spark-master`, `spark-worker` |
| Orchestration | Airflow 2.11.0 | `airflow` |
| BI | Superset | `superset` |
| Demo dashboard | Static Nginx app | `telesales-dashboard` |

## Repository Structure

```text
kltn-project/
|-- README.md
|-- hướng dẫn.md
|-- project_kltn_hybrid_lakehouse.md
|-- master_data/
|   |-- transcript_batch*.json
|   |-- split_to_entities.py
|   `-- output/
|       |-- customers.csv
|       |-- offers.csv
|       `-- calls.csv
|-- NLP model/
|   |-- NLP_model.ipynb
|   |-- train.csv
|   |-- valid.csv
|   |-- test.csv
|   `-- models/
|       |-- bow_model.pkl
|       `-- label_classes.json
`-- project/
    |-- docker-compose.yml
    |-- airflow/
    |   |-- Dockerfile
    |   `-- dags/telesales_pipeline.py
    |-- batch-etl/
    |   |-- bronze_job.py
    |   |-- silver_job.py
    |   `-- gold_job.py
    |-- dashboard/
    |   |-- index.html
    |   |-- styles.css
    |   |-- app.js
    |   `-- export_dashboard_data.py
    |-- init/
    |   |-- load_data.py
    |   |-- mongodb-connector.json
    |   `-- data/
    `-- spark/
        `-- Dockerfile
```

## Data Model

MongoDB source database: `telesales`

Collections:

- `cust`: customer profile data.
- `offer`: campaign and product offer data.
- `call_logs`: telesales call records and transcripts.

Current initialized source counts:

| Collection | Rows |
|---|---:|
| `cust` | 4,344 |
| `offer` | 5,072 |
| `call_logs` | 23,447 |

## Medallion Layers

| Layer | Tables | Main logic |
|---|---|---|
| Bronze | `lakehouse.bronze.{cust,offer,call_logs}` | Read Debezium CDC messages from Kafka, unwrap payloads, persist raw records to Iceberg. |
| Silver | `lakehouse.silver.{cust,offer,call_logs}` | Parse typed columns, mask PII, deduplicate by business key and CDC timestamp, run BoW NLP inference. |
| Gold | `lakehouse.gold.{dim_customer,dim_offer,dim_date,fact_telesales_calls}` | Build Star Schema, derived flags, product/category bands, and dashboard-ready metrics. |

## NLP

The thesis notebook under `NLP model/NLP_model.ipynb` contains two modeling approaches:

- Baseline: CountVectorizer + Logistic Regression.
- Primary experiment: fine-tuned RoBERTa.

The production Silver ETL currently uses the committed BoW artifact because it is small, deterministic, and suitable for the Docker demo:

- `NLP model/models/bow_model.pkl`
- `NLP model/models/label_classes.json`

The model directory is mounted into Airflow and Spark containers at:

```text
/opt/spark/work-dir/batch-etl/models
```

## Quick Start

Requirements:

- Docker Desktop is running.
- PowerShell on Windows.
- Git.

From the repository root:

```powershell
docker compose -f ".\project\docker-compose.yml" up -d --build
```

Check containers:

```powershell
docker compose -f ".\project\docker-compose.yml" ps -a
```

Expected result:

- Long-running services should be `Up`: `mongodb`, `postgres_metadata`, `zookeeper`, `kafka`, `debezium_connect`, `minio`, `spark-master`, `spark-worker`, `airflow`, `superset`, `telesales-dashboard`.
- Bootstrap jobs should be `Exited (0)`: `mongo-init`, `mongo-data-init`, `minio-mc`, `debezium-init`.
- `Exited (0)` for bootstrap jobs is expected; they only initialize ReplicaSet, load MongoDB data, create buckets, and register Debezium once.

## Service URLs

| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8081 | `admin / admin` |
| Spark UI | http://localhost:8080 | none |
| MinIO Console | http://localhost:9001 | `minioadmin / minioadmin` |
| Superset | http://localhost:8088 | configured in container |
| Demo dashboard | http://localhost:8090 | none |
| Debezium REST | http://localhost:8083 | none |
| Kafka external | `localhost:9092` | none |

## Run End-to-End Pipeline

Check Debezium:

```powershell
docker exec debezium_connect curl -fsS http://localhost:8083/connectors/mongo-source/status
```

Expected: connector `RUNNING`, task `RUNNING`.

Check Kafka topics:

```powershell
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --list
```

Expected topics:

```text
mongo-source.telesales.cust
mongo-source.telesales.offer
mongo-source.telesales.call_logs
```

Trigger the Airflow DAG:

```powershell
docker exec airflow airflow dags trigger telesales_lakehouse_pipeline
```

List DAG runs:

```powershell
docker exec airflow airflow dags list-runs -d telesales_lakehouse_pipeline
```

Check task states. Replace `<run_id>` with the run id from the previous command:

```powershell
docker exec airflow airflow tasks states-for-dag-run telesales_lakehouse_pipeline "<run_id>"
```

Expected task result:

```text
wait_for_debezium_connector  success
bronze_cdc_ingestion         success
silver_etl                   success
gold_star_schema             success
```

## Refresh Demo Dashboard

After Gold tables exist, export dashboard JSON:

```powershell
docker compose -f ".\project\docker-compose.yml" --profile dashboard run --rm dashboard-export
```

Open:

```text
http://localhost:8090
```

The static dashboard reads:

```text
project/dashboard/dashboard_data.json
```

This JSON file is generated locally and ignored by Git. Run `dashboard-export` again whenever the Gold layer changes.

## Sync Gold To BigQuery

The Airflow DAG includes a final `bq_sync_gold` task that publishes the Gold star schema to BigQuery. This is the recommended BI serving path for Looker Studio or Superset:

```text
lakehouse.gold.* -> bq_sync_gold -> BigQuery kltn0710.* -> BI tool
```

| BigQuery setting | Value |
|---|---|
| Project | `project-ef0c6db5-0765-4391-845` |
| Dataset | `kltn0710` |
| Location | `asia-southeast1` |

### Authentication

Install Google Cloud SDK, then initialize and create Application Default Credentials:

```powershell
gcloud.cmd init
gcloud.cmd auth application-default login
gcloud.cmd config set project project-ef0c6db5-0765-4391-845
```

PowerShell may block `gcloud.ps1` depending on execution policy, so using `gcloud.cmd` and `bq.cmd` is the safest Windows command form.

The Docker services mount your local ADC file from:

```text
%APPDATA%/gcloud/application_default_credentials.json
```

Verify local BigQuery access:

```powershell
bq.cmd ls project-ef0c6db5-0765-4391-845:kltn0710
```

### Run The Sync

Recreate the services after changing the Compose file or credentials:

```powershell
docker compose -f ".\project\docker-compose.yml" up -d --build --force-recreate airflow spark-master spark-worker superset
```

Run the full DAG from Airflow UI, or trigger it from CLI:

```powershell
docker exec airflow airflow dags trigger telesales_lakehouse_pipeline
```

If Gold tables already exist and you only want to republish to BigQuery:

```powershell
docker exec airflow airflow tasks test telesales_lakehouse_pipeline bq_sync_gold 2026-05-29
```

The sync writes these BigQuery objects:

| BigQuery table | Expected rows |
|---|---:|
| `kltn0710.dim_customer` | 4,344 |
| `kltn0710.dim_offer` | 5,072 |
| `kltn0710.dim_date` | 2 |
| `kltn0710.fact_telesales_calls` | 23,447 |

By default, `bq_sync_gold` excludes `dim_customer.full_name` and `dim_customer.address` before publishing to BigQuery. The phone number and national id fields are already masked in Silver/Gold (`phone_number_masked`, `national_id_masked`). Set `BQ_INCLUDE_PII=true` in the DAG task only for a private demo that needs raw name/address fields.

### Create The BI View

After the four BigQuery tables exist, create the joined serving view:

```powershell
Get-Content -Raw ".\project\bigquery\create_serving_views.sql" | bq.cmd query --use_legacy_sql=false
```

Validate counts:

```powershell
bq.cmd query --use_legacy_sql=false "SELECT COUNT(*) AS row_count FROM ``project-ef0c6db5-0765-4391-845.kltn0710.vw_telesales_performance``"
```

Expected:

```text
row_count = 23447
```

### Connect Looker Studio

1. Open Looker Studio.
2. Create a new data source.
3. Choose the BigQuery connector.
4. Select:
   - Project: `project-ef0c6db5-0765-4391-845`
   - Dataset: `kltn0710`
   - View: `vw_telesales_performance`
5. Use the view as the main reporting table.

Recommended charts:

| Chart | Fields |
|---|---|
| KPI cards | `call_id`, `has_successful_sale`, `talk_time_seconds` |
| Daily calls | `full_date`, count of `call_id` |
| Outcome breakdown | `outcome_category`, count of `call_id` |
| Product performance | `product_category`, `product_name`, success rate |
| Customer segments | `age_group`, `income_band`, `credit_tier` |
| Agent performance | `agent_id`, calls, sales, average talk time |

### Connect Superset To BigQuery

The local Superset image is built from `project/superset/Dockerfile` and includes:

- `sqlalchemy-bigquery`
- `pandas-gbq`

Superset also receives the same ADC mount:

```text
/opt/gcp/application_default_credentials.json
```

Open Superset at:

```text
http://localhost:8088
```

Default login:

```text
admin / admin
```

Add a database connection:

```text
SQLAlchemy URI: bigquery://project-ef0c6db5-0765-4391-845
```

Then create datasets from:

```text
kltn0710.vw_telesales_performance
```

If Superset cannot find credentials, recreate it after ADC login:

```powershell
docker compose -f ".\project\docker-compose.yml" up -d --build --force-recreate superset
```

## Full Rebuild From Zero

Use this when you want a clean local test, including fresh MongoDB, Airflow metadata, MinIO buckets, and Iceberg warehouse:

```powershell
docker compose -f ".\project\docker-compose.yml" down -v --remove-orphans
docker compose -f ".\project\docker-compose.yml" up -d --build
```

Detailed command order is documented in:

```text
hướng dẫn.md
```

## Latest End-to-End Validation

Validated locally on 2026-05-28 with a cold rebuild:

1. Ran `docker compose down -v --remove-orphans`.
2. Rebuilt with `docker compose up -d --build`.
3. Confirmed all bootstrap jobs completed with `Exited (0)`.
4. Confirmed Debezium connector and task are `RUNNING`.
5. Confirmed Kafka has all three MongoDB CDC topics.
6. Triggered Airflow DAG `telesales_lakehouse_pipeline`.
7. Confirmed all DAG tasks succeeded.
8. Queried Iceberg tables directly.
9. Exported dashboard JSON.
10. Confirmed dashboard returns HTTP `200 OK` and renders KPI values.

Validated Iceberg counts:

| Table | Rows |
|---|---:|
| `lakehouse.bronze.cust` | 4,344 |
| `lakehouse.bronze.offer` | 5,072 |
| `lakehouse.bronze.call_logs` | 23,447 |
| `lakehouse.silver.cust` | 4,344 |
| `lakehouse.silver.offer` | 5,072 |
| `lakehouse.silver.call_logs` | 23,447 |
| `lakehouse.gold.dim_customer` | 4,344 |
| `lakehouse.gold.dim_offer` | 5,072 |
| `lakehouse.gold.dim_date` | 2 |
| `lakehouse.gold.fact_telesales_calls` | 23,447 |

Validated dashboard KPI output:

| KPI | Value |
|---|---:|
| Total calls | 23,447 |
| Customers | 4,344 |
| Offers | 5,072 |
| Successful sales | 4,221 |
| Success rate | 18% |
| Average talk time | 4m 33s |

Validated BigQuery sync on 2026-05-29:

| BigQuery object | Rows |
|---|---:|
| `kltn0710.dim_customer` | 4,344 |
| `kltn0710.dim_offer` | 5,072 |
| `kltn0710.dim_date` | 2 |
| `kltn0710.fact_telesales_calls` | 23,447 |
| `kltn0710.vw_telesales_performance` | 23,447 |

## Implementation Notes

Recent fixes included in the repo:

- Airflow image now bakes in Spark provider, PySpark, pandas, NumPy, PyArrow, scikit-learn, and joblib.
- Airflow and Spark use a compatible Python 3.9 runtime for driver/executor consistency.
- Spark image includes Python 3.9 and dependencies required by the Silver ETL.
- Debezium MongoDB connector uses `rs0/mongodb:27017`.
- Bronze parsing supports Debezium events with and without a top-level `payload` wrapper.
- Gold `dim_date.is_weekend` logic is fixed.
- BoW model artifact was regenerated for compatible `scikit-learn` and `numpy` versions.
- Static dashboard and `dashboard-export` profile were added for quick demo visibility.

## Troubleshooting

- Init containers are not supposed to stay `Up`. If `mongo-init`, `mongo-data-init`, `minio-mc`, or `debezium-init` show `Exited (0)`, they completed successfully.
- If `mongo-data-init` exits with error, inspect:
  ```powershell
  docker compose -f ".\project\docker-compose.yml" logs mongo-data-init
  ```
- If Debezium is not running, inspect:
  ```powershell
  docker compose -f ".\project\docker-compose.yml" logs debezium_connect debezium-init
  ```
- If Airflow cannot submit Spark jobs, rebuild:
  ```powershell
  docker compose -f ".\project\docker-compose.yml" up -d --build --force-recreate airflow spark-master spark-worker
  ```
- If the dashboard has no data, run:
  ```powershell
  docker compose -f ".\project\docker-compose.yml" --profile dashboard run --rm dashboard-export
  ```

## License

Academic use only. Graduation thesis project in Data Engineering.
