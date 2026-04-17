# Hybrid Data Lakehouse for AGI Telesales

> **Graduation Thesis — Data Engineering**  
> Topic: *Xây dựng nền tảng dữ liệu Hybrid Data Lakehouse cho hệ thống Telesales*

An end-to-end data platform that ingests real-time call events from an AGI Telesales system, processes and anonymizes PII, classifies call transcripts using NLP, and serves a Star Schema to a BI dashboard — all on open-source, containerized infrastructure.

---

## Problem Statement

| Pain Point | Impact |
|---|---|
| **Operational DB overload** | BI tools querying MongoDB directly cause system crashes and drop live AI calls |
| **Unstructured call transcripts** | Traditional DWH cannot store or query millions of free-text conversations efficiently |
| **PII compliance** | Customer phone numbers and national IDs must be masked before analysts access data |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  AGI Telesales System                                           │
│  MongoDB (cust / offer / call_logs)  ←  AI generates calls     │
└──────────────────┬──────────────────────────────────────────────┘
                   │ CDC (Debezium)
                   ▼
         ┌─────────────────┐
         │   Apache Kafka  │  (decouples operational load)
         └────────┬────────┘
                  │ PySpark Structured Streaming / Batch
         ┌────────▼────────────────────────────────────┐
         │              MinIO  (S3-compatible)          │
         │  ┌──────────┐  ┌──────────┐  ┌──────────┐  │
         │  │  Bronze  │→ │  Silver  │→ │   Gold   │  │
         │  │ Raw JSON │  │ Cleansed │  │   Star   │  │
         │  │  Iceberg │  │ PII mask │  │  Schema  │  │
         │  └──────────┘  │ NLP tags │  └──────────┘  │
         │                └──────────┘                 │
         └────────────────────────┬────────────────────┘
                                  │
              ┌───────────────────┼───────────────────┐
              │ Apache Airflow    │            Apache  │
              │ (orchestration)   │            Superset│
              └───────────────────┘            (BI)    │
                                               └───────┘
```

### Why "Hybrid"?
- **Hybrid Deployment** — sensitive/raw data stays on-premise (MinIO); clean Gold layer is ready to sync to Cloud for business users.
- **Hybrid Workloads** — Streaming ingestion (Kafka → Bronze) and Batch processing (Spark ETL Silver/Gold) run on the same architecture.

---

## Medallion Architecture

| Layer | Location | Processing |
|---|---|---|
| **Bronze** | `minio://bronze/` | Raw JSON from Kafka, written as Apache Iceberg — no transformation |
| **Silver** | `minio://silver/` | Flatten JSON · PII masking (`0987***456`) · NLP call code extraction |
| **Gold** | `minio://gold/` | Star Schema: `fact_telesales_calls` + `dim_customer` / `dim_offer` / `dim_date` |

---

## Tech Stack

| Layer | Technology | Version |
|---|---|---|
| Operational DB | MongoDB | 6.0 |
| Metadata DB | PostgreSQL | 13 |
| CDC | Debezium Connect | 2.1 |
| Message Queue | Apache Kafka (Confluent) | 7.3.0 |
| Object Storage | MinIO | latest |
| Open Table Format | Apache Iceberg | via Spark |
| Compute | Apache Spark (PySpark) | 3.4.0 |
| Orchestration | Apache Airflow | 2.11.0 |
| BI | Apache Superset | latest |

All services run as Docker containers on a single `data-pipeline-network` bridge network.

---

## Repository Structure

```
kltn-project/
├── project/
│   ├── docker-compose.yml          # Full infrastructure definition
│   ├── airflow/
│   │   └── dags/                   # Airflow DAG files (WIP)
│   └── batch-etl/                  # PySpark ETL scripts (WIP)
│       └── models/                 # Trained NLP model artifacts
├── NLP model/
│   ├── NLP_model.ipynb             # Training notebook (BoW + RoBERTa)
│   ├── train.csv                   # Training set  (~13,857 records)
│   ├── valid.csv                   # Validation set (~1,732 records)
│   └── test.csv                    # Test set       (~1,733 records)
└── master_data/
    ├── transcript_batch*.json      # Mock AGI call data (2,635 files, ~23,447 records)
    ├── split_to_entities.py        # ETL script: merge JSON → 3 normalized CSVs
    └── output/
        ├── customers.csv           # dim_customer (10 rows)
        ├── offers.csv              # dim_offer    (57 rows)
        └── calls.csv               # fact_call    (23,447 rows)
```

---

## Data Model

```
customers (10)          offers (57)              calls (23,447)
──────────────          ─────────────            ─────────────────
customer_id  PK ──┐     offer_id     PK ──┐      call_id        PK
full_name        │     customer_id  FK──┘      offer_id       FK ──┘
age              └──►  campaign_id            agent_id
gender                 product_name            call_timestamp
phone_number *         lead_source             call_status
national_id *          decile_group            talk_time_seconds
address                loan_amount             previous_contact_count
employment_status      interest_rate           call_code          (NLP output)
monthly_income                                 call_transcript    (raw text)
credit_score
is_existing_customer

* PII — masked in Silver layer
```

**Cardinality:** 1 customer → 2–9 offers · 1 offer → 3–2,532 calls

---

## NLP Module

Classifies raw `call_transcript` text into structured `call_code` labels (multi-label, 32 classes).

| Model | Method | F1 Micro | Precision | Recall |
|---|---|---|---|---|
| **Baseline** | CountVectorizer + Logistic Regression | 70.16% | 65.92% | 74.98% |
| **Primary** | RoBERTa (`roberta-base`) fine-tuned | **73.64%** | **80.78%** | 67.67% |

RoBERTa is used as the primary model due to higher precision (fewer false-positive call code assignments).

**Saved artifacts** (after running the notebook):
```
NLP model/models/
├── bow_model.pkl          ← sklearn bundle (vectorizer + classifier + mlb)
├── label_classes.json     ← 32 class names
└── roberta_saved/         ← HuggingFace save_pretrained output
```

---

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.9+

### 1. Start infrastructure

```bash
cd project
docker compose up -d
```

| Service | URL | Credentials |
|---|---|---|
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Spark UI | http://localhost:8080 | — |
| Airflow | http://localhost:8081 | admin / admin |
| Superset | http://localhost:8088 | — |
| Debezium REST | http://localhost:8083 | — |
| Kafka | localhost:9092 | — |

### 2. Register Debezium MongoDB connector

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @project/debezium/mongodb-connector.json
```

### 3. Prepare normalized data (mock)

```bash
cd master_data
python split_to_entities.py
# Output: master_data/output/customers.csv, offers.csv, calls.csv
```

### 4. Train NLP model

Open and run `NLP model/NLP_model.ipynb` (requires GPU for RoBERTa).

---

## Current Progress

- [x] Docker Compose infrastructure (all services)
- [x] Debezium CDC: MongoDB → Kafka
- [x] Mock data generation (~23,447 AGI call records)
- [x] Data normalization: JSON → customers / offers / calls
- [x] NLP model training (BoW baseline + RoBERTa fine-tuned)
- [ ] PySpark Bronze ETL (Kafka → MinIO Iceberg)
- [ ] PySpark Silver ETL (PII masking + NLP inference)
- [ ] PySpark Gold ETL (Star Schema)
- [ ] Airflow DAGs
- [ ] Superset dashboards

---

## License

For academic use only — Graduation Thesis, Data Engineering.
