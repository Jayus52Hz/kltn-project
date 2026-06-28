"""
callcenteren_external_job.py
============================
Stage-aware batch ETL for the CallCenterEN external dataset.

Set CALLCENTEREN_STAGE to one of:
  - bronze: CSV -> lakehouse.bronze_external.callcenteren_raw
  - silver: bronze_external -> silver_external clean/labeled tables
  - gold: silver_external -> Star Schema, analytics, and comparison Gold tables
  - all: run all stages, preserving the original one-shot behavior
"""

import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


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
CALLCENTEREN_MODEL_PATH = os.getenv(
    "CALLCENTEREN_MODEL_PATH",
    "/opt/spark/work-dir/callcenteren-output/callcenteren_finetuned_max4/callcenteren_best_finetuned_model.pkl",
)

ALLOWED_STAGES = {"all", "bronze", "silver", "gold"}
if CALLCENTEREN_STAGE not in ALLOWED_STAGES:
    raise ValueError(f"Unsupported CALLCENTEREN_STAGE={CALLCENTEREN_STAGE!r}. Use one of {sorted(ALLOWED_STAGES)}")

RUN_SILVER = CALLCENTEREN_STAGE in {"all", "silver"}
if RUN_SILVER and not Path(CALLCENTEREN_MODEL_PATH).exists():
    print(f"[ERROR] CallCenterEN BoW model not found: {CALLCENTEREN_MODEL_PATH}")
    sys.exit(1)

# The committed CallCenterEN model can be produced from a newer NumPy runtime
# whose pickle references numpy._core. The Docker Spark runtime uses NumPy 1.24,
# where the equivalent module is numpy.core.
sys.modules.setdefault("numpy._core", np.core)
sys.modules.setdefault("numpy._core.multiarray", np.core.multiarray)
sys.modules.setdefault("numpy._core.numeric", np.core.numeric)
if hasattr(np.core, "_multiarray_umath"):
    sys.modules.setdefault("numpy._core._multiarray_umath", np.core._multiarray_umath)


@dataclass
class ThresholdedTextModel:
    name: str
    labels: list[str]
    vectorizer: Any
    classifiers: dict[str, Any]
    thresholds: dict[str, float]
    min_labels: int
    max_labels: int

    def _score_matrix(self, texts: pd.Series) -> np.ndarray:
        x_eval = self.vectorizer.transform(texts.fillna("").astype(str))
        scores = np.zeros((x_eval.shape[0], len(self.labels)), dtype=float)
        for idx, label in enumerate(self.labels):
            clf = self.classifiers.get(label)
            if clf == "always_one":
                scores[:, idx] = 1.0
            elif clf is not None:
                scores[:, idx] = clf.predict_proba(x_eval)[:, 1]
        return scores

    def predict_matrix(self, texts: pd.Series) -> np.ndarray:
        scores = self._score_matrix(texts)
        pred = np.zeros_like(scores, dtype=int)
        thresholds = np.array([self.thresholds.get(label, 0.5) for label in self.labels])
        pred[scores >= thresholds] = 1

        for row_idx in range(scores.shape[0]):
            order = np.argsort(scores[row_idx])[::-1]
            positive_count = int(pred[row_idx].sum())
            if positive_count < self.min_labels:
                pred[row_idx, order[: self.min_labels]] = 1
            if int(pred[row_idx].sum()) > self.max_labels:
                keep = set(order[: self.max_labels])
                for label_idx in range(pred.shape[1]):
                    if label_idx not in keep:
                        pred[row_idx, label_idx] = 0
        return pred

    def predict_labels_and_confidence(self, texts: pd.Series) -> tuple[list[list[str]], list[float]]:
        scores = self._score_matrix(texts)
        pred = self.predict_matrix(texts)
        labels_out: list[list[str]] = []
        confidences: list[float] = []
        for row_idx in range(pred.shape[0]):
            selected = [idx for idx, value in enumerate(pred[row_idx]) if value == 1]
            selected = sorted(selected, key=lambda idx: scores[row_idx, idx], reverse=True)
            labels_out.append([self.labels[idx] for idx in selected])
            confidences.append(float(np.mean([scores[row_idx, idx] for idx in selected])) if selected else 0.0)
        return labels_out, confidences


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

CALLCENTEREN_MODEL = None
CALLCENTEREN_MODEL_NAME = None

if RUN_SILVER:
    print(f"Using CallCenterEN BoW model from {CALLCENTEREN_MODEL_PATH} ...")
    CALLCENTEREN_MODEL = joblib.load(CALLCENTEREN_MODEL_PATH)
    CALLCENTEREN_MODEL_NAME = getattr(CALLCENTEREN_MODEL, "name", "callcenteren_bow")
    print(f"CallCenterEN BoW model loaded for Silver inference: {CALLCENTEREN_MODEL_NAME}")


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


def create_gold_star_schema_tables() -> None:
    spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.gold_external.dim_callcenter_source (
        source_key     STRING COMMENT 'Natural hash for source_zip, source_domain, and call_direction',
        source_zip     STRING,
        source_domain  STRING,
        call_direction STRING,
        _processed_at  TIMESTAMP
    )
    USING iceberg
    """)

    spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.gold_external.dim_callcenter_model (
        model_key     STRING COMMENT 'Natural hash for model_name',
        model_name    STRING,
        _processed_at TIMESTAMP
    )
    USING iceberg
    """)

    spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.gold_external.dim_call_code (
        call_code_key STRING COMMENT 'Natural hash for call_code',
        call_code     STRING,
        _processed_at TIMESTAMP
    )
    USING iceberg
    """)

    spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.gold_external.fact_callcenter_calls (
        callcenter_call_id       STRING COMMENT 'Natural key from CallCenterEN text_hash',
        source_key               STRING COMMENT 'FK -> dim_callcenter_source',
        model_key                STRING COMMENT 'FK -> dim_callcenter_model',
        audio_duration           DOUBLE,
        asr_confidence           DOUBLE,
        word_count               INT,
        char_count               INT,
        pii_token_count          INT,
        pii_types                STRING,
        model_call_code          STRING,
        model_call_code_confidence DOUBLE,
        has_existing_pseudo_label BOOLEAN,
        pseudo_call_code_existing STRING,
        pseudo_label_confidence_existing DOUBLE,
        should_use_for_training BOOLEAN,
        call_code_source         STRING,
        label_confidence         DOUBLE,
        call_code                ARRAY<STRING>,
        _processed_at            TIMESTAMP
    )
    USING iceberg
    """)

    spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.gold_external.bridge_callcenter_call_code (
        callcenter_call_id STRING COMMENT 'FK -> fact_callcenter_calls',
        call_code_key      STRING COMMENT 'FK -> dim_call_code',
        call_code          STRING,
        source_key         STRING,
        model_key          STRING,
        label_confidence   DOUBLE,
        _processed_at      TIMESTAMP
    )
    USING iceberg
    """)


def merge_into_table(df, table_name: str, pk_col: str) -> None:
    view_name = f"_new_{table_name.split('.')[-1]}"
    df.createOrReplaceTempView(view_name)
    spark.sql(f"""
        MERGE INTO {table_name} AS target
        USING {view_name} AS source
        ON target.{pk_col} = source.{pk_col}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"Merged {table_name}: {df.count():,} source rows")


def run_gold_star_schema(labeled) -> None:
    create_gold_star_schema_tables()

    enriched = (
        labeled
        .withColumn(
            "source_key",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.coalesce(F.col("source_zip"), F.lit("UNKNOWN")),
                    F.coalesce(F.col("source_domain"), F.lit("UNKNOWN")),
                    F.coalesce(F.col("call_direction"), F.lit("UNKNOWN")),
                ),
                256,
            ),
        )
        .withColumn(
            "model_key",
            F.sha2(F.coalesce(F.col("model_name"), F.lit("UNKNOWN")), 256),
        )
        .withColumn("callcenter_call_id", F.col("text_hash"))
        .filter(F.col("callcenter_call_id").isNotNull())
    )

    dim_source = (
        enriched
        .select("source_key", "source_zip", "source_domain", "call_direction")
        .dropDuplicates(["source_key"])
        .withColumn("_processed_at", F.current_timestamp())
    )
    merge_into_table(
        dim_source,
        "lakehouse.gold_external.dim_callcenter_source",
        "source_key",
    )

    dim_model = (
        enriched
        .select("model_key", "model_name")
        .dropDuplicates(["model_key"])
        .withColumn("_processed_at", F.current_timestamp())
    )
    merge_into_table(
        dim_model,
        "lakehouse.gold_external.dim_callcenter_model",
        "model_key",
    )

    exploded_labels = (
        enriched
        .select(
            "callcenter_call_id",
            "source_key",
            "model_key",
            "label_confidence",
            F.explode("call_code").alias("call_code"),
        )
        .filter(F.col("call_code").isNotNull())
        .withColumn("call_code_key", F.sha2(F.col("call_code"), 256))
    )

    dim_call_code = (
        exploded_labels
        .select("call_code_key", "call_code")
        .dropDuplicates(["call_code_key"])
        .withColumn("_processed_at", F.current_timestamp())
    )
    merge_into_table(
        dim_call_code,
        "lakehouse.gold_external.dim_call_code",
        "call_code_key",
    )

    fact = (
        enriched
        .select(
            "callcenter_call_id",
            "source_key",
            "model_key",
            "audio_duration",
            "asr_confidence",
            "word_count",
            "char_count",
            "pii_token_count",
            "pii_types",
            "model_call_code",
            F.col("model_call_code_confidence").cast("double").alias("model_call_code_confidence"),
            F.col("has_existing_pseudo_label").cast("boolean").alias("has_existing_pseudo_label"),
            "pseudo_call_code_existing",
            F.col("pseudo_label_confidence_existing").cast("double").alias("pseudo_label_confidence_existing"),
            F.col("should_use_for_training").cast("boolean").alias("should_use_for_training"),
            "call_code_source",
            "label_confidence",
            "call_code",
        )
        .withColumn("_processed_at", F.current_timestamp())
    )
    merge_into_table(
        fact,
        "lakehouse.gold_external.fact_callcenter_calls",
        "callcenter_call_id",
    )

    bridge = (
        exploded_labels
        .select(
            "callcenter_call_id",
            "call_code_key",
            "call_code",
            "source_key",
            "model_key",
            "label_confidence",
        )
        .dropDuplicates(["callcenter_call_id", "call_code_key"])
        .withColumn("_processed_at", F.current_timestamp())
    )
    write_table(bridge, "lakehouse.gold_external.bridge_callcenter_call_code")


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
            "model_name",
            "has_existing_pseudo_label",
            "pseudo_call_code_existing",
            "pseudo_label_confidence",
            "pseudo_label_confidence_existing",
            "should_use_for_training",
            "pseudo_label_rationale",
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
        .withColumn("audio_duration", F.col("audio_duration").cast("double"))
        .withColumn("asr_confidence", F.col("confidence").cast("double"))
        .withColumn("word_count", F.col("word_count").cast("int"))
        .withColumn("char_count", F.col("char_count").cast("int"))
        .withColumn("pii_token_count", F.col("pii_token_count").cast("int"))
        .withColumn("pseudo_label_confidence", F.col("pseudo_label_confidence").cast("double"))
        .withColumn(
            "pseudo_label_confidence_existing",
            F.col("pseudo_label_confidence_existing").cast("double"),
        )
        .withColumn(
            "has_existing_pseudo_label",
            F.lower(F.col("has_existing_pseudo_label").cast("string")).isin("true", "1", "yes"),
        )
        .withColumn("should_use_for_training", F.lower(F.col("should_use_for_training")).isin("true", "1", "yes"))
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "confidence")
    )
    write_table(clean, "lakehouse.silver_external.callcenteren_clean")

    prediction_input = clean.select("text_hash", "call_transcript")
    clean_pdf = prediction_input.toPandas()
    labels, confidences = CALLCENTEREN_MODEL.predict_labels_and_confidence(
        clean_pdf["call_transcript"].fillna("").astype(str)
    )
    label_pdf = pd.DataFrame(
        {
            "text_hash": clean_pdf["text_hash"].astype(str),
            "call_code": labels,
            "model_call_code": [", ".join(row) for row in labels],
            "model_call_code_confidence": [float(value) for value in confidences],
            "model_name": CALLCENTEREN_MODEL_NAME,
            "call_code_source": [", ".join(row) for row in labels],
            "label_confidence": [float(value) for value in confidences],
        }
    )
    label_schema = T.StructType(
        [
            T.StructField("text_hash", T.StringType(), False),
            T.StructField("call_code", T.ArrayType(T.StringType()), False),
            T.StructField("model_call_code", T.StringType(), True),
            T.StructField("model_call_code_confidence", T.DoubleType(), True),
            T.StructField("model_name", T.StringType(), True),
            T.StructField("call_code_source", T.StringType(), True),
            T.StructField("label_confidence", T.DoubleType(), True),
        ]
    )
    label_updates = spark.createDataFrame(label_pdf, schema=label_schema)

    labeled = (
        clean
        .drop(
            "call_code",
            "model_call_code",
            "model_call_code_confidence",
            "model_name",
            "call_code_source",
            "label_confidence",
        )
        .join(label_updates, on="text_hash", how="inner")
        .filter(F.size(F.col("call_code")) > 0)
    )
    write_table(labeled, "lakehouse.silver_external.callcenteren_labeled")


def run_gold() -> None:
    labeled = spark.table("lakehouse.silver_external.callcenteren_labeled")
    run_gold_star_schema(labeled)

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
