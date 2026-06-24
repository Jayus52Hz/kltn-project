"""
silver_job.py
=============
Batch ETL — Silver Layer

Đọc từ Bronze Iceberg tables, áp dụng 4 bước xử lý, ghi ra Silver.

Transformations:
  1. Flatten   — parse raw_doc JSON string → typed columns
  2. PII Mask  — phone_number: 555-***-4567 | national_id: XXX-XX-****
  3. Deduplicate — giữ record mới nhất theo ts_ms, bỏ DELETE (op='d')
  4. NLP       — call_transcript → call_code (BoW + Logistic Regression by
                 default, RoBERTa fine-tuned available as experimental baseline)

Input:  lakehouse.bronze.{cust, offer, call_logs}
Output: lakehouse.silver.{cust, offer, call_logs}

Ghi theo chiến lược MERGE INTO (Iceberg ACID upsert):
  - Nếu entity đã tồn tại trong Silver → UPDATE
  - Nếu chưa tồn tại → INSERT

Spark-submit:
  docker exec spark-master spark-submit \\
    --master spark://spark-master:7077 \\
    --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.0,\\
               org.apache.hadoop:hadoop-aws:3.3.4 \\
    /opt/spark/work-dir/batch-etl/silver_job.py

Yêu cầu:
  Mount/copy NLP model artifacts vào container trước khi chạy:
    /opt/spark/work-dir/batch-etl/models/bow_model.pkl
  Nếu cần chạy baseline RoBERTa:
    NLP_MODEL_TYPE=roberta và /opt/spark/work-dir/batch-etl/models/roberta_saved/
"""

import os
import sys
import json
import joblib
import pandas as pd
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, DoubleType, BooleanType,
    ArrayType, TimestampType,
)

# ── Cấu hình ──────────────────────────────────────────────────────────────────
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MODELS_PATH      = os.getenv("MODELS_PATH",      "/opt/spark/work-dir/batch-etl/models")
NLP_MODEL_TYPE   = os.getenv("NLP_MODEL_TYPE",   "bow").lower()
BOW_MODEL_PATH   = os.path.join(MODELS_PATH, "bow_model.pkl")
ROBERTA_PATH     = os.path.join(MODELS_PATH, "roberta_saved")
LABELS_PATH      = os.path.join(MODELS_PATH, "label_classes.json")
ROBERTA_THRESHOLD = float(os.getenv("ROBERTA_THRESHOLD", "0.5"))
ROBERTA_BATCH_SIZE = int(os.getenv("ROBERTA_BATCH_SIZE", "8"))
ROBERTA_NUM_PARTITIONS = int(os.getenv("ROBERTA_NUM_PARTITIONS", "2"))
TORCH_NUM_THREADS = int(os.getenv("TORCH_NUM_THREADS", "1"))
ALLOWED_ENTITIES = ["cust", "offer", "call_logs"]


def parse_list_env(name, default_values, allowed_values):
    raw_value = os.getenv(name)
    if not raw_value:
        return list(default_values)

    selected_values = [
        value.strip()
        for value in raw_value.split(",")
        if value.strip()
    ]
    unknown_values = sorted(set(selected_values) - set(allowed_values))
    if unknown_values:
        print(f"[ERROR] Unsupported {name} value(s): {unknown_values}. Allowed values: {allowed_values}")
        sys.exit(1)
    return selected_values


SELECTED_ENTITIES = parse_list_env("SILVER_ENTITIES", ALLOWED_ENTITIES, ALLOWED_ENTITIES)
RUN_CALL_LOGS = "call_logs" in SELECTED_ENTITIES
print(f"Silver selected entities: {', '.join(SELECTED_ENTITIES)}")

# ── Kiểm tra model tồn tại trước khi khởi động Spark ──────────────────────────
if RUN_CALL_LOGS and NLP_MODEL_TYPE == "roberta":
    missing = [
        path for path in [ROBERTA_PATH, LABELS_PATH]
        if not os.path.exists(path)
    ]
    if missing:
        print("[ERROR] RoBERTa artifacts not found:")
        for path in missing:
            print(f"  - {path}")
        print("  Mount/copy: \"NLP model/models\" -> /opt/spark/work-dir/batch-etl/models")
        sys.exit(1)
elif RUN_CALL_LOGS and NLP_MODEL_TYPE == "bow":
    if not os.path.exists(BOW_MODEL_PATH):
        print(f"[ERROR] BoW model not found: {BOW_MODEL_PATH}")
        print("  Run: docker cp \"NLP model/models/\" spark-master:/opt/spark/work-dir/batch-etl/models/")
        sys.exit(1)
elif RUN_CALL_LOGS:
    print(f"[ERROR] Unsupported NLP_MODEL_TYPE={NLP_MODEL_TYPE!r}. Use 'roberta' or 'bow'.")
    sys.exit(1)

# ── Spark Session ──────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("silver_job")
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lakehouse",
            "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakehouse.type",      "hadoop")
    .config("spark.sql.catalog.lakehouse.warehouse", "s3a://lakehouse/warehouse")
    .config("spark.hadoop.fs.s3a.endpoint",           MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key",         MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key",         MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access",  "true")
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print(f"Spark version: {spark.version}")

# ── Tạo namespace ──────────────────────────────────────────────────────────────
spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
print("Namespace lakehouse.silver ready")

# ── DDL Silver tables ──────────────────────────────────────────────────────────

spark.sql("""
CREATE TABLE IF NOT EXISTS lakehouse.silver.cust (
    customer_id          STRING  COMMENT 'PK — hash(name+phone+national_id)',
    full_name            STRING,
    age                  INT,
    gender               STRING,
    phone_number_masked  STRING  COMMENT 'PII masked: 555-***-4567',
    national_id_masked   STRING  COMMENT 'PII masked: XXX-XX-****',
    address              STRING,
    employment_status    STRING,
    monthly_income       DOUBLE,
    credit_score         INT,
    is_existing_customer BOOLEAN,
    _op                  STRING  COMMENT 'CDC op: c/u/d/r',
    _ts_ms               BIGINT  COMMENT 'CDC timestamp ms',
    _processed_at        TIMESTAMP
)
USING iceberg
PARTITIONED BY (_op)
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lakehouse.silver.offer (
    offer_id     STRING  COMMENT 'PK',
    customer_id  STRING  COMMENT 'FK -> silver.cust',
    campaign_id  STRING,
    product_name STRING,
    lead_source  STRING,
    decile_group INT,
    loan_amount  DOUBLE,
    interest_rate DOUBLE,
    _op           STRING,
    _ts_ms        BIGINT,
    _processed_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (product_name)
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lakehouse.silver.call_logs (
    call_id                STRING    COMMENT 'PK — UUID',
    offer_id               STRING    COMMENT 'FK -> silver.offer',
    agent_id               STRING,
    call_timestamp         TIMESTAMP,
    call_status            STRING,
    talk_time_seconds      INT,
    previous_contact_count INT,
    call_code              ARRAY<STRING> COMMENT 'Model-generated source of truth for downstream analytics',
    call_transcript        STRING,
    _op                    STRING,
    _ts_ms                 BIGINT,
    _processed_at          TIMESTAMP
)
USING iceberg
PARTITIONED BY (call_status)
""")

print("Silver tables ready")


def reconcile_call_log_schema():
    table = "lakehouse.silver.call_logs"
    columns = spark.table(table).columns

    if "call_code_predicted" in columns and "call_code" not in columns:
        spark.sql(f"ALTER TABLE {table} RENAME COLUMN call_code_predicted TO call_code")
        print(f"Renamed legacy {table}.call_code_predicted -> call_code")
        columns = spark.table(table).columns

    if "call_code_predicted" in columns:
        spark.sql(f"ALTER TABLE {table} DROP COLUMN call_code_predicted")
        print(f"Dropped legacy model output column {table}.call_code_predicted")
        columns = spark.table(table).columns

    if "call_code_original" in columns:
        spark.sql(f"ALTER TABLE {table} DROP COLUMN call_code_original")
        print(f"Dropped legacy training label column {table}.call_code_original")


reconcile_call_log_schema()

# ── JSON schemas cho from_json() ───────────────────────────────────────────────

CUST_SCHEMA = StructType([
    StructField("customer_id",          StringType(),  True),
    StructField("full_name",            StringType(),  True),
    StructField("age",                  IntegerType(), True),
    StructField("gender",               StringType(),  True),
    StructField("phone_number",         StringType(),  True),
    StructField("national_id",          StringType(),  True),
    StructField("address",              StringType(),  True),
    StructField("employment_status",    StringType(),  True),
    StructField("monthly_income",       DoubleType(),  True),
    StructField("credit_score",         IntegerType(), True),
    StructField("is_existing_customer", BooleanType(), True),
])

OFFER_SCHEMA = StructType([
    StructField("offer_id",      StringType(),  True),
    StructField("customer_id",   StringType(),  True),
    StructField("campaign_id",   StringType(),  True),
    StructField("product_name",  StringType(),  True),
    StructField("lead_source",   StringType(),  True),
    StructField("decile_group",  IntegerType(), True),
    StructField("loan_amount",   DoubleType(),  True),
    StructField("interest_rate", DoubleType(),  True),
])

CALL_SCHEMA = StructType([
    StructField("call_id",                StringType(),              True),
    StructField("offer_id",               StringType(),              True),
    StructField("agent_id",               StringType(),              True),
    StructField("call_timestamp",         StringType(),              True),
    StructField("call_status",            StringType(),              True),
    StructField("talk_time_seconds",      IntegerType(),             True),
    StructField("previous_contact_count", IntegerType(),             True),
    StructField("call_transcript",        StringType(),              True),
])

# ── Hàm helper: deduplicate giữ record mới nhất per entity ────────────────────
def dedup_latest(df, pk_col):
    """Giữ record có ts_ms lớn nhất cho mỗi giá trị pk_col, bỏ DELETE events."""
    window = Window.partitionBy(pk_col).orderBy(F.col("ts_ms").desc())
    return (
        df
        .withColumn("_rn", F.row_number().over(window))
        .filter((F.col("_rn") == 1) & (F.col("op") != "d"))
        .drop("_rn")
    )

# ── NLP: BoW chính, RoBERTa baseline/thực nghiệm ──────────────────────────────
#
# BoW + Logistic Regression được chọn cho pipeline vận hành vì thời gian
# inference ổn định hơn nhiều trong full rebuild CPU, trong khi chênh lệch chất
# lượng so với RoBERTa không đủ lớn để bù chi phí vận hành.
if RUN_CALL_LOGS and NLP_MODEL_TYPE == "roberta":
    with open(LABELS_PATH, "r", encoding="utf-8") as f:
        label_classes = json.load(f)
    labels_broadcast = spark.sparkContext.broadcast(label_classes)
    print(f"Using RoBERTa model from {ROBERTA_PATH}")
    print(
        f"RoBERTa threshold={ROBERTA_THRESHOLD}, "
        f"batch_size={ROBERTA_BATCH_SIZE}, "
        f"partitions={ROBERTA_NUM_PARTITIONS}, "
        f"torch_threads={TORCH_NUM_THREADS}"
    )

    _roberta_model = None
    _roberta_tokenizer = None
    _roberta_device = None

    @F.pandas_udf(ArrayType(StringType()))
    def predict_call_codes(transcripts: pd.Series) -> pd.Series:
        global _roberta_model, _roberta_tokenizer, _roberta_device

        if _roberta_model is None or _roberta_tokenizer is None:
            import torch
            from transformers import AutoModelForSequenceClassification, AutoTokenizer

            torch.set_num_threads(TORCH_NUM_THREADS)
            torch.set_num_interop_threads(1)
            _roberta_device = "cuda" if torch.cuda.is_available() else "cpu"
            _roberta_tokenizer = AutoTokenizer.from_pretrained(ROBERTA_PATH)
            _roberta_model = AutoModelForSequenceClassification.from_pretrained(ROBERTA_PATH)
            _roberta_model.to(_roberta_device)
            _roberta_model.eval()

        import torch

        labels = labels_broadcast.value
        values = transcripts.fillna("").astype(str).tolist()
        outputs = []

        with torch.no_grad():
            for start in range(0, len(values), ROBERTA_BATCH_SIZE):
                batch = values[start:start + ROBERTA_BATCH_SIZE]
                encoded = _roberta_tokenizer(
                    batch,
                    truncation=True,
                    padding=True,
                    max_length=512,
                    return_tensors="pt",
                )
                encoded = {k: v.to(_roberta_device) for k, v in encoded.items()}
                probs = torch.sigmoid(_roberta_model(**encoded).logits).cpu().numpy()
                for row in probs:
                    pred = [labels[i] for i, score in enumerate(row) if score >= ROBERTA_THRESHOLD]
                    outputs.append(pred)

        return pd.Series(outputs)

elif RUN_CALL_LOGS and NLP_MODEL_TYPE == "bow":
    print(f"Using BoW production model from {BOW_MODEL_PATH} ...")
    bow_broadcast = spark.sparkContext.broadcast(joblib.load(BOW_MODEL_PATH))
    print("BoW model loaded and broadcast")

    @F.pandas_udf(ArrayType(StringType()))
    def predict_call_codes(transcripts: pd.Series) -> pd.Series:
        bundle     = bow_broadcast.value
        X          = bundle["vectorizer"].transform(transcripts.fillna(""))
        y_pred     = bundle["classifier"].predict(X)
        label_list = bundle["mlb"].inverse_transform(y_pred)
        return pd.Series([list(labels) for labels in label_list])

# ── PII masking ────────────────────────────────────────────────────────────────
def mask_phone(col_expr):
    """555-123-4567 → 555-***-4567"""
    return F.regexp_replace(col_expr, r"(\d{3})-\d{3}-(\d{4})", r"$1-***-$2")

def mask_national_id(col_expr):
    """XXX-XX-1234 → XXX-XX-****"""
    return F.regexp_replace(col_expr, r"(\w{3}-\w{2})-\d{4}", r"$1-****")

# ── MERGE INTO helper ──────────────────────────────────────────────────────────
def merge_into_silver(new_df, table, pk_col):
    """
    Upsert new_df vào Silver table dùng Iceberg MERGE INTO.
    Đây là điểm quan trọng để demo ACID transaction của Iceberg:
      - UPDATE nếu pk đã tồn tại
      - INSERT nếu pk chưa có
    """
    view = f"_new_{table.split('.')[-1]}"
    new_df.createOrReplaceTempView(view)

    spark.sql(f"""
        MERGE INTO {table} AS target
        USING {view} AS source
        ON target.{pk_col} = source.{pk_col}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"  MERGE INTO {table} completed ({new_df.count():,} source records)")

# ══════════════════════════════════════════════════════════════════════════════
# 1. PROCESS CUST
# ══════════════════════════════════════════════════════════════════════════════
if "cust" in SELECTED_ENTITIES:
    print("\n[1/3] Processing cust ...")

    bronze_cust = spark.table("lakehouse.bronze.cust")

    silver_cust = (
        bronze_cust
        # Flatten raw_doc JSON string → typed struct
        .withColumn("doc", F.from_json(F.col("raw_doc"), CUST_SCHEMA))
        .select(
            F.col("doc.customer_id"),
            F.col("doc.full_name"),
            F.col("doc.age"),
            F.col("doc.gender"),
            # PII masking
            mask_phone(F.col("doc.phone_number")).alias("phone_number_masked"),
            mask_national_id(F.col("doc.national_id")).alias("national_id_masked"),
            F.col("doc.address"),
            F.col("doc.employment_status"),
            F.col("doc.monthly_income"),
            F.col("doc.credit_score"),
            F.col("doc.is_existing_customer"),
            # CDC metadata
            F.col("op"),
            F.col("ts_ms"),
            F.current_timestamp().alias("_processed_at"),
        )
        # Bỏ rows parse lỗi (customer_id null)
        .filter(F.col("customer_id").isNotNull())
    )

    silver_cust = dedup_latest(silver_cust, "customer_id")

    # Rename để khớp Silver schema
    silver_cust = silver_cust.withColumnRenamed("op", "_op").withColumnRenamed("ts_ms", "_ts_ms")

    merge_into_silver(silver_cust, "lakehouse.silver.cust", "customer_id")
else:
    print('\\n[1/3] Skipping cust')

# ══════════════════════════════════════════════════════════════════════════════
# 2. PROCESS OFFER
# ══════════════════════════════════════════════════════════════════════════════
if "offer" in SELECTED_ENTITIES:
    print("\n[2/3] Processing offer ...")

    bronze_offer = spark.table("lakehouse.bronze.offer")

    silver_offer = (
        bronze_offer
        .withColumn("doc", F.from_json(F.col("raw_doc"), OFFER_SCHEMA))
        .select(
            F.col("doc.offer_id"),
            F.col("doc.customer_id"),
            F.col("doc.campaign_id"),
            F.col("doc.product_name"),
            F.col("doc.lead_source"),
            F.col("doc.decile_group"),
            F.col("doc.loan_amount"),
            F.col("doc.interest_rate"),
            F.col("op"),
            F.col("ts_ms"),
            F.current_timestamp().alias("_processed_at"),
        )
        .filter(F.col("offer_id").isNotNull())
    )

    silver_offer = dedup_latest(silver_offer, "offer_id")
    silver_offer = silver_offer.withColumnRenamed("op", "_op").withColumnRenamed("ts_ms", "_ts_ms")

    merge_into_silver(silver_offer, "lakehouse.silver.offer", "offer_id")
else:
    print('\\n[2/3] Skipping offer')

# ══════════════════════════════════════════════════════════════════════════════
# 3. PROCESS CALL_LOGS
# ══════════════════════════════════════════════════════════════════════════════
if "call_logs" in SELECTED_ENTITIES:
    print("\n[3/3] Processing call_logs ...")

    bronze_calls = spark.table("lakehouse.bronze.call_logs")

    parsed_calls = (
        bronze_calls
        .withColumn("doc", F.from_json(F.col("raw_doc"), CALL_SCHEMA))
        .select(
            F.col("doc.call_id"),
            F.col("doc.offer_id"),
            F.col("doc.agent_id"),
            F.to_timestamp(F.col("doc.call_timestamp")).alias("call_timestamp"),
            F.col("doc.call_status"),
            F.col("doc.talk_time_seconds"),
            F.col("doc.previous_contact_count"),
            F.col("doc.call_transcript"),
            F.col("op"),
            F.col("ts_ms"),
        )
        .filter(F.col("call_id").isNotNull())
    )

    # Deduplicate trước khi chạy NLP (tránh inference trùng lặp)
    parsed_calls = dedup_latest(parsed_calls, "call_id")
    if NLP_MODEL_TYPE == "roberta" and ROBERTA_NUM_PARTITIONS > 1:
        parsed_calls = parsed_calls.repartition(ROBERTA_NUM_PARTITIONS, "call_id")

    # NLP inference: downstream call_code is generated by the model, not copied from
    # the synthetic source label used during training.
    silver_calls = (
        parsed_calls
        .withColumn("call_code", predict_call_codes(F.col("call_transcript")))
        .withColumn("_processed_at", F.current_timestamp())
        .withColumnRenamed("op", "_op")
        .withColumnRenamed("ts_ms", "_ts_ms")
    )

    merge_into_silver(silver_calls, "lakehouse.silver.call_logs", "call_id")
else:
    print('\\n[3/3] Skipping call_logs')

# ── Done ───────────────────────────────────────────────────────────────────────
print("\nSilver job completed successfully.")
for entity in SELECTED_ENTITIES:
    print(f"  lakehouse.silver.{entity} ready")
spark.stop()
