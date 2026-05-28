---
name: KLTN Hybrid Data Lakehouse for Telesales
description: Graduation thesis project — End-to-End Hybrid Data Lakehouse for AGI Telesales system. Current repo-aligned architecture, pipeline flow, ETL implementation status, and local project context.
type: project
---
# KLTN: Hybrid Data Lakehouse for AGI Telesales

## Context & Goal

**Thesis title:** Xây dựng nền tảng dữ liệu Hybrid Data Lakehouse cho hệ thống Telesales  
**Major:** Data Engineering  
**Local repo:** `D:\Đồ án tốt nghiệp`  
**GitHub:** https://github.com/Jayus52Hz/kltn-project.git

This project builds an end-to-end Hybrid Data Lakehouse for an AGI Telesales system. The main goals are:
1. Prevent analytics workloads from querying MongoDB directly and overloading operations
2. Handle large volumes of unstructured call transcripts in a scalable analytics platform
3. Enforce PII masking before business users and analysts access customer data
4. Serve a BI-ready Star Schema for dashboards and reporting

---

## Tech Stack (Dockerized, Open-source)

| Layer | Technology | Container name |
|---|---|---|
| Operational DB | MongoDB 6.0 (ReplicaSet rs0) | `mongodb` |
| Metadata DB | PostgreSQL 13 | `postgres_metadata` |
| CDC | Debezium 2.1 | `debezium_connect` |
| Message Queue | Apache Kafka 7.3.0 (Confluent) | `kafka` |
| Coordination | Zookeeper 7.3.0 | `zookeeper` |
| Object Storage | MinIO (S3-compatible) | `minio` |
| Open Table Format | Apache Iceberg | via Spark |
| Compute | Apache Spark 3.4.0 (PySpark) | `spark-master`, `spark-worker` |
| Orchestration | Apache Airflow 2.11.0 | `airflow` |
| BI | Apache Superset | `superset` |

**Why "Hybrid":**
- Hybrid Deployment: raw and sensitive data stays on-premise in MinIO, while clean serving data can later be synced outward if needed
- Hybrid Workloads: streaming ingestion and batch ETL coexist on the same platform

---

## Infrastructure Details

**MinIO credentials:** `minioadmin / minioadmin`  
**MinIO buckets:** `bronze/`, `silver/`, `gold/`, `lakehouse/`  
**PostgreSQL:** `admin / admin`, database `metadata_db`, port `5432`  
**Airflow UI:** `http://localhost:8081`, `admin / admin`  
**Superset:** `http://localhost:8088`  
**Spark UI:** `http://localhost:8080`, cluster port `7077`  
**Kafka:** external `9092`, internal `29092`  
**Debezium REST API:** `http://localhost:8083`

**Kafka internal topics:**
- `my_connect_configs`
- `my_connect_offsets`
- `my_connect_statuses`

**Network:** `data-pipeline-network`

---

## Source Data Model

MongoDB source database: `telesales`

Collections:
1. `cust` — customer profiles
2. `offer` — campaign/product offers
3. `call_logs` — telesales call records and transcripts

Synthetic source data is stored under `master_data/` as `transcript_batch*.json` files.

---

## Mock Data and Normalization

`master_data/` contains thousands of transcript batch JSON files covering telesales calls over several days in March 2026.

Important normalization logic lives in:
- `master_data/split_to_entities.py`

What it does:
- loads all `transcript_batch*.json`
- derives a stable `customer_id` by hashing `full_name + phone_number + national_id`
- renames `unique_id` to `call_id`
- generates deterministic `offer_id` values per `(customer_id, campaign_id)` pair
- writes normalized CSV outputs to `master_data/output/`:
  - `customers.csv`
  - `offers.csv`
  - `calls.csv`

This fixes the original mock-data issue where source `customer_id` values were reused inconsistently across different profiles.

---

## Medallion Architecture

### Bronze Layer
Implementation file:
- `project/batch-etl/bronze_job.py`

Behavior:
- reads CDC events from Kafka topics produced by Debezium
- unwraps Debezium payloads
- keeps raw MongoDB documents as JSON strings
- writes Iceberg tables to `lakehouse.bronze.{cust,offer,call_logs}`
- supports continuous streaming mode and Airflow-compatible `TRIGGER_ONCE=true` mode

Kafka topics consumed:
- `mongo-source.telesales.cust`
- `mongo-source.telesales.offer`
- `mongo-source.telesales.call_logs`

### Silver Layer
Implementation file:
- `project/batch-etl/silver_job.py`

Behavior:
- reads Bronze Iceberg tables
- parses raw JSON into typed columns
- masks PII fields such as phone number and national ID
- deduplicates by keeping the latest record per business key using CDC timestamp
- runs BoW NLP inference on `call_transcript` to generate `call_code_predicted`
- merges results into Iceberg Silver tables:
  - `lakehouse.silver.cust`
  - `lakehouse.silver.offer`
  - `lakehouse.silver.call_logs`

### Gold Layer
Implementation file:
- `project/batch-etl/gold_job.py`

Behavior:
- reads Silver tables
- builds BI-facing Star Schema tables:
  - `lakehouse.gold.dim_customer`
  - `lakehouse.gold.dim_offer`
  - `lakehouse.gold.dim_date`
  - `lakehouse.gold.fact_telesales_calls`
- derives analytics-friendly dimensions and flags such as:
  - age group
  - income band
  - credit tier
  - product category
  - talk time band
  - outcome category
  - sale / rejection / objection indicators

---

## NLP Module

Training assets are under:
- `NLP model/NLP_model.ipynb`
- `NLP model/train.csv`
- `NLP model/valid.csv`
- `NLP model/test.csv`

Project notes from the repo indicate two modeling approaches:
- Baseline: CountVectorizer + Logistic Regression
- Primary: RoBERTa fine-tuned model

The implemented Silver ETL currently uses the saved BoW model artifact:
- `bow_model.pkl`

Operational note:
- `silver_job.py` expects model artifacts to be copied into the Spark container path before execution

---

## Pipeline Flow

```text
master_data/transcript_batch*.json
  -> split_to_entities.py
  -> master_data/output/*.csv
  -> project/init/load_data.py
  -> MongoDB telesales.{cust,offer,call_logs}
  -> Debezium CDC
  -> Kafka topics
  -> bronze_job.py
  -> lakehouse.bronze.*
  -> silver_job.py
  -> lakehouse.silver.*
  -> gold_job.py
  -> lakehouse.gold.*
  -> Superset dashboards / BI
```

---

## Orchestration

Airflow DAG file:
- `project/airflow/dags/telesales_pipeline.py`

Current DAG order:
1. `bronze_cdc_ingestion`
2. `silver_etl`
3. `gold_star_schema`

Bronze runs in trigger-once mode inside Airflow so the streaming job can complete as a bounded task.

---

## Initialization and Bootstrap

Relevant files:
- `project/docker-compose.yml`
- `project/init/load_data.py`
- `project/init/mongodb-connector.json`

Current bootstrap behavior in Docker Compose:
- initializes MongoDB ReplicaSet
- loads normalized CSV data into MongoDB via `mongo-data-init`
- registers Debezium connector via `debezium-init`
- creates MinIO buckets via `minio-mc`
- mounts Spark ETL scripts into Spark containers

Important operational note:
- `mongo-init`, `mongo-data-init`, `minio-mc`, and `debezium-init` are one-shot bootstrap jobs
- Seeing these containers as `Exited (0)` in `docker compose ps -a` is expected and means they completed successfully
- Long-running services are MongoDB, PostgreSQL, Zookeeper, Kafka, Debezium Connect, MinIO, Spark master/worker, Airflow, and Superset

Validated bootstrap outputs:
- MongoDB collections: `cust=4344`, `offer=5072`, `call_logs=23447`
- Debezium connector: `mongo-source`, connector `RUNNING`, task `RUNNING`
- Kafka topics:
  - `mongo-source.telesales.cust`
  - `mongo-source.telesales.offer`
  - `mongo-source.telesales.call_logs`

---

## Runtime Fixes and Validation, 2026-05-28

The local Docker stack was rebuilt and validated end-to-end on 2026-05-28.

Fixes applied:
- baked Airflow Spark provider, PySpark, pandas, NumPy, PyArrow, scikit-learn, and joblib into the Airflow image instead of installing through `_PIP_ADDITIONAL_REQUIREMENTS`
- standardized Airflow/Spark Python runtime to Python 3.9 for PySpark driver/executor compatibility
- installed Python 3.9 and the Silver-job dependencies into the Spark image
- mounted `NLP model/models` into Airflow, Spark master, and Spark worker at `/opt/spark/work-dir/batch-etl/models`
- fixed Airflow `spark_default` connection so SparkSubmit uses `spark://spark-master:7077`
- fixed Zookeeper healthcheck
- fixed Debezium MongoDB connector config to use `mongodb.hosts=rs0/mongodb:27017`
- fixed Debezium registration command in Docker Compose
- fixed Bronze parsing for Debezium messages emitted without a top-level `payload` wrapper
- fixed Gold `dim_date.is_weekend` expression
- retrained the BoW model artifact with `scikit-learn==1.3.0` and `numpy==1.24.4`; the original incompatible artifact is kept locally as `bow_model.pkl.numpy2.bak` and ignored by Git

Validated Airflow DAG run:
- DAG: `telesales_lakehouse_pipeline`
- Run id: `manual__2026-05-28T07:02:36+00:00`
- Result: all tasks succeeded
  - `wait_for_debezium_connector`
  - `bronze_cdc_ingestion`
  - `silver_etl`
  - `gold_star_schema`

Validated Iceberg counts after Silver/Gold:
- `lakehouse.silver.cust`: 4344
- `lakehouse.silver.offer`: 5072
- `lakehouse.silver.call_logs`: 23447
- `lakehouse.gold.dim_customer`: 4344
- `lakehouse.gold.dim_offer`: 5072
- `lakehouse.gold.dim_date`: 2
- `lakehouse.gold.fact_telesales_calls`: 23447

Bronze can contain more rows than the source counts during repeated local tests because Debezium emits snapshot/update events each time source data is reloaded. Silver deduplicates by business key and CDC timestamp, so the serving counts above are the expected values.

---

## Current Repo Status

Implemented in the current repository:
- Docker Compose infrastructure
- MongoDB ReplicaSet setup
- CSV-to-MongoDB initialization pipeline
- Debezium connector config
- Bronze ETL job
- Silver ETL job with PII masking and NLP inference
- Gold ETL job with Star Schema generation
- Airflow DAG for Bronze -> Silver -> Gold orchestration
- Synthetic data normalization script
- End-to-end local validation through Bronze, Silver, and Gold

Potential remaining work depends on thesis/demo needs:
- polishing Superset dashboard and demo story
- thesis write-up and demo packaging

---

## Repository Structure (Current)

```text
kltn-project/
├── README.md
├── NLP model/
│   ├── NLP_model.ipynb
│   ├── train.csv
│   ├── valid.csv
│   └── test.csv
├── master_data/
│   ├── transcript_batch*.json
│   ├── split_to_entities.py
│   └── output/
│       ├── customers.csv
│       ├── offers.csv
│       └── calls.csv
└── project/
    ├── docker-compose.yml
    ├── airflow/
    │   ├── dags/
    │   │   └── telesales_pipeline.py
    │   └── airflow_logs/
    ├── batch-etl/
    │   ├── bronze_job.py
    │   ├── silver_job.py
    │   ├── gold_job.py
    │   └── models/
    ├── init/
    │   ├── data/
    │   ├── load_data.py
    │   └── mongodb-connector.json
    └── spark/
        └── Dockerfile
```

---

## Working Assumptions for Future Sessions

When reasoning about this project, prefer the current repository state over older notes.

Assume:
- ETL scripts and Airflow DAG already exist
- this is no longer only an architecture draft; it is an implemented local prototype
- the key engineering focus is now correctness, orchestration, demo readiness, and end-to-end validation
