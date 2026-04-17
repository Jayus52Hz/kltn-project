"""
telesales_pipeline.py
=====================
Airflow DAG — Telesales Lakehouse Pipeline

Orchestrates the full Medallion pipeline:
  Bronze (CDC ingestion) → Silver (ETL + NLP) → Gold (Star Schema)

Schedule: daily at 02:00 UTC
Trigger:  also runnable manually via Airflow UI

Task graph:
  bronze_cdc_ingestion
        │
        ▼
    silver_etl
        │
        ▼
  gold_star_schema

Bronze note:
  The bronze_job.py is a Spark Structured Streaming job.
  We run it with TRIGGER_ONCE=true so it processes all available
  Kafka offsets and exits, making it compatible with Airflow task model.

Spark connection:
  Airflow connection id: spark_default
  Set via env: AIRFLOW_CONN_SPARK_DEFAULT=spark://spark-master:7077
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# ── Common Spark config shared by all 3 jobs ──────────────────────────────────
SPARK_PACKAGES = ",".join([
    "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.0",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0",
])

SPARK_CONF = {
    # Iceberg extensions
    "spark.sql.extensions":
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    # Iceberg catalog
    "spark.sql.catalog.lakehouse":
        "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.lakehouse.type":      "hadoop",
    "spark.sql.catalog.lakehouse.warehouse": "s3a://lakehouse/warehouse",
    # MinIO / S3A
    "spark.hadoop.fs.s3a.endpoint":                       "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key":                     "minioadmin",
    "spark.hadoop.fs.s3a.secret.key":                     "minioadmin",
    "spark.hadoop.fs.s3a.path.style.access":              "true",
    "spark.hadoop.fs.s3a.impl":
        "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.aws.credentials.provider":
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
}

WORK_DIR = "/opt/spark/work-dir/batch-etl"

# ── DAG definition ─────────────────────────────────────────────────────────────
default_args = {
    "owner": "thinh-nguyen-ts",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="telesales_lakehouse_pipeline",
    description="Bronze → Silver → Gold Medallion ETL for Telesales Lakehouse",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 2 * * *",   # daily at 02:00 UTC
    catchup=False,
    tags=["lakehouse", "telesales", "iceberg"],
) as dag:

    # ── Task 1: Bronze — CDC ingestion from Kafka → Iceberg ───────────────────
    bronze_cdc_ingestion = SparkSubmitOperator(
        task_id="bronze_cdc_ingestion",
        conn_id="spark_default",
        application=f"{WORK_DIR}/bronze_job.py",
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        env_vars={
            "KAFKA_BOOTSTRAP_SERVERS": "kafka:29092",
            "MINIO_ENDPOINT":          "http://minio:9000",
            "MINIO_ACCESS_KEY":        "minioadmin",
            "MINIO_SECRET_KEY":        "minioadmin",
            "CHECKPOINT_BASE":         "s3a://bronze/_checkpoints",
            # TRIGGER_ONCE=true: Spark Structured Streaming processes all
            # available Kafka offsets then exits (compatible with Airflow tasks)
            "TRIGGER_ONCE":            "true",
        },
        name="bronze_cdc_ingestion",
        # Bronze may take longer when backfilling large Kafka topics
        execution_timeout=timedelta(hours=2),
    )

    # ── Task 2: Silver — Flatten, PII mask, Dedup, NLP inference ──────────────
    silver_etl = SparkSubmitOperator(
        task_id="silver_etl",
        conn_id="spark_default",
        application=f"{WORK_DIR}/silver_job.py",
        packages=",".join([
            "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.0",
            "org.apache.hadoop:hadoop-aws:3.3.4",
        ]),
        conf=SPARK_CONF,
        env_vars={
            "MINIO_ENDPOINT":   "http://minio:9000",
            "MINIO_ACCESS_KEY": "minioadmin",
            "MINIO_SECRET_KEY": "minioadmin",
            # bow_model.pkl must be pre-copied into the container:
            # docker cp "NLP model/models/" spark-master:/opt/spark/work-dir/batch-etl/models/
            "MODELS_PATH":      "/opt/spark/work-dir/batch-etl/models",
        },
        name="silver_etl",
        execution_timeout=timedelta(hours=1),
    )

    # ── Task 3: Gold — Star Schema build for BI / Superset ────────────────────
    gold_star_schema = SparkSubmitOperator(
        task_id="gold_star_schema",
        conn_id="spark_default",
        application=f"{WORK_DIR}/gold_job.py",
        packages=",".join([
            "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.0",
            "org.apache.hadoop:hadoop-aws:3.3.4",
        ]),
        conf=SPARK_CONF,
        env_vars={
            "MINIO_ENDPOINT":   "http://minio:9000",
            "MINIO_ACCESS_KEY": "minioadmin",
            "MINIO_SECRET_KEY": "minioadmin",
        },
        name="gold_star_schema",
        execution_timeout=timedelta(hours=1),
    )

    # ── Dependencies ───────────────────────────────────────────────────────────
    bronze_cdc_ingestion >> silver_etl >> gold_star_schema
