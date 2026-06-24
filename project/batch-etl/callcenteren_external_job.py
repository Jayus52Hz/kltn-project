"""
callcenteren_external_job.py
============================
Stage-aware batch ETL for the CallCenterEN external dataset.

Set CALLCENTEREN_STAGE to one of:
  - bronze: CSV -> lakehouse.bronze_external.callcenteren_raw
  - silver: bronze_external -> silver_external clean/labeled tables
  - gold: silver_external -> analytics and comparison Gold tables
  - all: run all stages, preserving the original one-shot behavior
"""

import os
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
CALLCENTEREN_STAGE = os.getenv("CALLCENTEREN_STAGE", "all").lower()
CALLCENTEREN_SCHEMA_CSV = os.getenv(
    "CALLCENTEREN_SCHEMA_CSV",
    "/opt/spark/work-dir/callcenteren-output/callcenteren_finetuned_max4/callcenteren_15k_with_model_callcodes.csv",
)
MODEL_METRICS_CSV = os.getenv(
    "MODEL_METRICS_CSV",
    "/opt/spark/work-dir/callcenteren-output/callcenteren_finetuned_max4/callcenteren_finetune_metrics.csv",
)

ALLOWED_STAGES = {"all", "bronze", "silver", "gold"}
if CALLCENTEREN_STAGE not in ALLOWED_STAGES:
    raise ValueError(f"Unsupported CALLCENTEREN_STAGE={CALLCENTEREN_STAGE!r}. Use one of {sorted(ALLOWED_STAGES)}")


spark = (
    SparkSession.builder
    .appName(f"callcenteren_external_{CALLCENTEREN_STAGE}")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
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


def read_csv(path: str):
    return (
        spark.read
        .option("header", "true")
        .option("multiLine", "true")
        .option("quote", '"')
        .option("escape", '"')
        .csv(path)
    )


def write_table(df, table_name: str) -> None:
    df.write.format("iceberg").mode("overwrite").saveAsTable(table_name)
    print(f"Wrote {table_name}: {df.count():,} rows")


def table_or_none(name: str):
    try:
        return spark.table(name)
    except Exception as exc:
        print(f"Skipping optional table {name}: {exc}")
        return None


def ensure_columns(df, column_names: list[str]):
    for column_name in column_names:
        if column_name not in df.columns:
            df = df.withColumn(column_name, F.lit(None).cast("string"))
    return df


def create_namespaces() -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze_external")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver_external")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold_external")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")


def run_bronze() -> None:
    raw = read_csv(CALLCENTEREN_SCHEMA_CSV)
    raw = ensure_columns(
        raw,
        [
            "text",
            "call_transcript",
            "call_code",
            "model_call_code",
            "model_call_code_confidence",
            "pseudo_label_confidence",
            "pseudo_label_confidence_existing",
            "should_use_for_training",
        ],
    )
    raw = raw.withColumn("_ingested_at", F.current_timestamp())
    write_table(raw, "lakehouse.bronze_external.callcenteren_raw")


def run_silver() -> None:
    raw = spark.table("lakehouse.bronze_external.callcenteren_raw")
    window = Window.partitionBy("text_hash").orderBy(
        F.desc(
            F.coalesce(
                F.col("model_call_code_confidence").cast("double"),
                F.col("pseudo_label_confidence").cast("double"),
                F.col("pseudo_label_confidence_existing").cast("double"),
            )
        )
    )

    clean = (
        raw
        .withColumn("call_transcript", F.coalesce(F.col("call_transcript"), F.col("text")))
        .withColumn("call_code_source", F.coalesce(F.col("model_call_code"), F.col("call_code")))
        .withColumn(
            "label_confidence",
            F.coalesce(
                F.col("model_call_code_confidence").cast("double"),
                F.col("pseudo_label_confidence").cast("double"),
                F.col("pseudo_label_confidence_existing").cast("double"),
            ),
        )
        .withColumn("audio_duration", F.col("audio_duration").cast("double"))
        .withColumn("asr_confidence", F.col("confidence").cast("double"))
        .withColumn("word_count", F.col("word_count").cast("int"))
        .withColumn("char_count", F.col("char_count").cast("int"))
        .withColumn("pii_token_count", F.col("pii_token_count").cast("int"))
        .withColumn("should_use_for_training", F.lower(F.col("should_use_for_training")).isin("true", "1", "yes"))
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "confidence")
    )
    write_table(clean, "lakehouse.silver_external.callcenteren_clean")

    labeled = (
        clean
        .withColumn(
            "call_code",
            F.expr("filter(transform(split(call_code_source, ','), x -> trim(x)), x -> x <> '')"),
        )
        .filter(F.size(F.col("call_code")) > 0)
    )
    write_table(labeled, "lakehouse.silver_external.callcenteren_labeled")


def run_gold() -> None:
    labeled = spark.table("lakehouse.silver_external.callcenteren_labeled")

    analytics = (
        labeled
        .groupBy("source_domain", "call_direction")
        .agg(
            F.count("*").alias("call_count"),
            F.avg("audio_duration").alias("avg_duration_seconds"),
            F.avg("asr_confidence").alias("avg_asr_confidence"),
            F.avg("word_count").alias("avg_word_count"),
            F.avg("char_count").alias("avg_char_count"),
            F.avg("pii_token_count").alias("avg_pii_token_count"),
            F.avg("label_confidence").alias("avg_label_confidence"),
        )
        .withColumn("_processed_at", F.current_timestamp())
    )
    write_table(analytics, "lakehouse.gold_external.callcenteren_call_analytics")

    callcenteren_profile = labeled.agg(
        F.count("*").alias("row_count"),
        F.avg("audio_duration").alias("avg_duration_seconds"),
        F.avg("word_count").alias("avg_word_count"),
        F.avg("char_count").alias("avg_char_count"),
        F.avg("pii_token_count").alias("avg_pii_token_count"),
    ).withColumn("dataset_name", F.lit("callcenteren")).select(
        "dataset_name",
        "row_count",
        "avg_duration_seconds",
        "avg_word_count",
        "avg_char_count",
        "avg_pii_token_count",
    )

    primary_calls = table_or_none("lakehouse.silver.call_logs")
    if primary_calls is not None:
        primary_profile = primary_calls.agg(
            F.count("*").alias("row_count"),
            F.avg("talk_time_seconds").alias("avg_duration_seconds"),
            F.avg(F.size(F.split(F.coalesce(F.col("call_transcript"), F.lit("")), r"\s+"))).alias("avg_word_count"),
            F.avg(F.length(F.coalesce(F.col("call_transcript"), F.lit("")))).alias("avg_char_count"),
            F.lit(None).cast("double").alias("avg_pii_token_count"),
        ).withColumn("dataset_name", F.lit("primary_telesales")).select(
            "dataset_name",
            "row_count",
            "avg_duration_seconds",
            "avg_word_count",
            "avg_char_count",
            "avg_pii_token_count",
        )
        profile_comparison = primary_profile.unionByName(callcenteren_profile)
    else:
        profile_comparison = callcenteren_profile

    write_table(
        profile_comparison.withColumn("_processed_at", F.current_timestamp()),
        "lakehouse.gold.dataset_profile_comparison",
    )

    callcenter_labels = (
        labeled
        .select(F.lit("callcenteren").alias("dataset_name"), F.explode("call_code").alias("call_code"))
        .groupBy("dataset_name", "call_code")
        .count()
        .withColumnRenamed("count", "label_count")
    )

    if primary_calls is not None:
        primary_labels = (
            primary_calls
            .select(F.lit("primary_telesales").alias("dataset_name"), F.explode("call_code").alias("call_code"))
            .groupBy("dataset_name", "call_code")
            .count()
            .withColumnRenamed("count", "label_count")
        )
        label_comparison = primary_labels.unionByName(callcenter_labels)
    else:
        label_comparison = callcenter_labels

    write_table(
        label_comparison.withColumn("_processed_at", F.current_timestamp()),
        "lakehouse.gold.call_code_distribution_comparison",
    )

    if Path(MODEL_METRICS_CSV).exists():
        model_metrics = read_csv(MODEL_METRICS_CSV).withColumn("_processed_at", F.current_timestamp())
        write_table(model_metrics, "lakehouse.gold.model_experiment_comparison")
    else:
        print(f"Skipping model_experiment_comparison; metrics CSV not found: {MODEL_METRICS_CSV}")


create_namespaces()

if CALLCENTEREN_STAGE in {"all", "bronze"}:
    print("Running CallCenterEN bronze stage")
    run_bronze()

if CALLCENTEREN_STAGE in {"all", "silver"}:
    print("Running CallCenterEN silver stage")
    run_silver()

if CALLCENTEREN_STAGE in {"all", "gold"}:
    print("Running CallCenterEN gold stage")
    run_gold()

print(f"CallCenterEN external {CALLCENTEREN_STAGE} stage completed successfully.")
spark.stop()
