"""
bq_sync_job.py
==============
Sync Gold Iceberg tables to Google BigQuery for Looker Studio / Superset.

Input:  lakehouse.gold.{dim_customer, dim_offer, dim_date, fact_telesales_calls,
        customer_outcome_scripts}
        plus optional CallCenterEN serving/comparison/analytics tables
Output: BigQuery {BQ_PROJECT_ID}.{BQ_DATASET}.{same table names}
"""

import os
import time

from google.cloud import bigquery
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

BQ_PROJECT_ID = os.environ["BQ_PROJECT_ID"]
BQ_DATASET = os.getenv("BQ_DATASET", "kltn0710")
BQ_WRITE_METHOD = os.getenv("BQ_WRITE_METHOD", "direct")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
BQ_SYNC_MAX_ATTEMPTS = int(os.getenv("BQ_SYNC_MAX_ATTEMPTS", "3"))
BQ_SYNC_RETRY_DELAY_SECONDS = int(os.getenv("BQ_SYNC_RETRY_DELAY_SECONDS", "20"))

BLOCKED_BIGQUERY_COLUMNS = {
    "call_transcript",
    "text",
    "call_code_original",
    "call_code_predicted",
    "full_name",
    "address",
    "external_id",
    "source_entry",
    "pseudo_label_rationale",
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


def bigquery_type_for_spark_type(spark_type):
    if isinstance(spark_type, T.BooleanType):
        return "BOOLEAN"
    if isinstance(spark_type, (T.ByteType, T.ShortType, T.IntegerType, T.LongType)):
        return "INTEGER"
    if isinstance(spark_type, (T.FloatType, T.DoubleType, T.DecimalType)):
        return "FLOAT"
    if isinstance(spark_type, T.DateType):
        return "DATE"
    if isinstance(spark_type, T.ArrayType):
        return bigquery_type_for_spark_type(spark_type.elementType)
    return "STRING"


def bigquery_schema_for_dataframe(df):
    schema = []
    for field in df.schema.fields:
        mode = "REPEATED" if isinstance(field.dataType, T.ArrayType) else "NULLABLE"
        schema.append(
            bigquery.SchemaField(
                field.name,
                bigquery_type_for_spark_type(field.dataType),
                mode=mode,
            )
        )
    return schema


def dataframe_safe_for_bigquery_load(df):
    safe_df = df
    for field in df.schema.fields:
        if isinstance(field.dataType, T.TimestampType):
            safe_df = safe_df.withColumn(field.name, F.col(field.name).cast("string"))
    return safe_df


def load_with_bigquery_client(df, target):
    safe_df = dataframe_safe_for_bigquery_load(df)
    pdf = safe_df.toPandas()
    job_config = bigquery.LoadJobConfig(
        schema=bigquery_schema_for_dataframe(safe_df),
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    load_job = bq_client.load_table_from_dataframe(
        pdf,
        target,
        job_config=job_config,
    )
    load_job.result()


def gold_table(name):
    return spark.table(f"lakehouse.gold.{name}")


def optional_table(source_name):
    try:
        return spark.table(source_name)
    except Exception as exc:
        print(f"Skipping optional table {source_name}: {exc}")
        return None


def optional_gold_table(name):
    return optional_table(f"lakehouse.gold.{name}")


def optional_external_gold_table(name):
    return optional_table(f"lakehouse.gold_external.{name}")


def optional_silver_external_table(name):
    return optional_table(f"lakehouse.silver_external.{name}")


tables = {
    "dim_customer": ("lakehouse.gold.dim_customer", gold_table("dim_customer")),
    "dim_offer": ("lakehouse.gold.dim_offer", gold_table("dim_offer")),
    "dim_date": ("lakehouse.gold.dim_date", gold_table("dim_date")),
    "fact_telesales_calls": (
        "lakehouse.gold.fact_telesales_calls",
        gold_table("fact_telesales_calls"),
    ),
    "customer_outcome_scripts": (
        "lakehouse.gold.customer_outcome_scripts",
        gold_table("customer_outcome_scripts"),
    ),
}

optional_tables = {
    "dim_callcenter_source": (
        "lakehouse.gold_external.dim_callcenter_source",
        optional_external_gold_table("dim_callcenter_source"),
    ),
    "dim_callcenter_model": (
        "lakehouse.gold_external.dim_callcenter_model",
        optional_external_gold_table("dim_callcenter_model"),
    ),
    "dim_call_code": (
        "lakehouse.gold_external.dim_call_code",
        optional_external_gold_table("dim_call_code"),
    ),
    "fact_callcenter_calls": (
        "lakehouse.gold_external.fact_callcenter_calls",
        optional_external_gold_table("fact_callcenter_calls"),
    ),
    "bridge_callcenter_call_code": (
        "lakehouse.gold_external.bridge_callcenter_call_code",
        optional_external_gold_table("bridge_callcenter_call_code"),
    ),
    "dataset_profile_comparison": (
        "lakehouse.gold.dataset_profile_comparison",
        optional_gold_table("dataset_profile_comparison"),
    ),
    "call_code_distribution_comparison": (
        "lakehouse.gold.call_code_distribution_comparison",
        optional_gold_table("call_code_distribution_comparison"),
    ),
    "model_experiment_comparison": (
        "lakehouse.gold.model_experiment_comparison",
        optional_gold_table("model_experiment_comparison"),
    ),
    "callcenteren_call_analytics": (
        "lakehouse.gold_external.callcenteren_call_analytics",
        optional_external_gold_table("callcenteren_call_analytics"),
    ),
    "callcenteren_labeled": (
        "lakehouse.silver_external.callcenteren_labeled",
        optional_silver_external_table("callcenteren_labeled"),
    ),
}
tables.update({
    name: (source_name, df)
    for name, (source_name, df) in optional_tables.items()
    if df is not None
})

for table_name, (source_name, df) in list(tables.items()):
    blocked_cols = [col for col in df.columns if col in BLOCKED_BIGQUERY_COLUMNS]
    if blocked_cols:
        print(f"Excluding non-analytic or sensitive columns from {table_name}: {blocked_cols}")
        tables[table_name] = (source_name, df.drop(*blocked_cols))

for table_name, (source_name, df) in tables.items():
    target = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{table_name}"
    for attempt in range(1, BQ_SYNC_MAX_ATTEMPTS + 1):
        try:
            bq_client.delete_table(target, not_found_ok=True)
            print(f"Deleted existing BigQuery table if present: {target}")

            if BQ_WRITE_METHOD == "dataframe":
                load_with_bigquery_client(df, target)
            else:
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
            print(f"Synced {source_name} -> {target} ({df.count():,} rows)")
            break
        except Exception as exc:
            if attempt == BQ_SYNC_MAX_ATTEMPTS:
                print(f"Failed to sync {source_name} -> {target} after {attempt} attempts: {exc}")
                raise
            print(
                f"Retrying BigQuery sync for {target} after attempt {attempt} failed: {exc}"
            )
            time.sleep(BQ_SYNC_RETRY_DELAY_SECONDS)

print("BigQuery sync completed successfully.")
spark.stop()
