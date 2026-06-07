"""
bq_sync_job.py
==============
Sync Gold Iceberg tables to Google BigQuery for Looker Studio / Superset.

Input:  lakehouse.gold.{dim_customer, dim_offer, dim_date, fact_telesales_calls}
Output: BigQuery {BQ_PROJECT_ID}.{BQ_DATASET}.{same table names}
"""

import os

from google.cloud import bigquery
from pyspark.sql import SparkSession


MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

BQ_PROJECT_ID = os.environ["BQ_PROJECT_ID"]
BQ_DATASET = os.getenv("BQ_DATASET", "kltn0710")
BQ_WRITE_METHOD = os.getenv("BQ_WRITE_METHOD", "direct")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

BLOCKED_BIGQUERY_COLUMNS = {
    "call_transcript",
    "call_code_original",
    "call_code_predicted",
    "full_name",
    "address",
}


spark = (
    SparkSession.builder
    .appName("bq_sync_gold")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakehouse.type", "hadoop")
    .config("spark.sql.catalog.lakehouse.warehouse", "s3a://lakehouse/warehouse")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

bq_client = bigquery.Client(project=BQ_PROJECT_ID)


def gold_table(name):
    return spark.table(f"lakehouse.gold.{name}")


tables = {
    "dim_customer": gold_table("dim_customer"),
    "dim_offer": gold_table("dim_offer"),
    "dim_date": gold_table("dim_date"),
    "fact_telesales_calls": gold_table("fact_telesales_calls"),
}

for table_name, df in list(tables.items()):
    blocked_cols = [col for col in df.columns if col in BLOCKED_BIGQUERY_COLUMNS]
    if blocked_cols:
        print(f"Excluding non-analytic or sensitive columns from {table_name}: {blocked_cols}")
        tables[table_name] = df.drop(*blocked_cols)

for table_name, df in tables.items():
    target = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{table_name}"
    bq_client.delete_table(target, not_found_ok=True)
    print(f"Deleted existing BigQuery table if present: {target}")

    writer = (
        df.write
        .format("bigquery")
        .mode("overwrite")
        .option("writeMethod", BQ_WRITE_METHOD)
        .option("parentProject", BQ_PROJECT_ID)
    )
    if GOOGLE_APPLICATION_CREDENTIALS:
        writer = writer.option("credentialsFile", GOOGLE_APPLICATION_CREDENTIALS)

    writer.save(target)
    print(f"Synced lakehouse.gold.{table_name} -> {target} ({df.count():,} rows)")

print("BigQuery sync completed successfully.")
spark.stop()
