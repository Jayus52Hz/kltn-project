"""
gold_job.py
===========
Batch ETL — Gold Layer

Đọc từ Silver Iceberg tables, xây dựng Star Schema phục vụ BI (Superset).

Input:  lakehouse.silver.{cust, offer, call_logs}
Output: lakehouse.gold.{dim_customer, dim_offer, dim_date, fact_telesales_calls}

Star Schema:
                    ┌─────────────┐
                    │  dim_date   │
                    │  (date_key) │
                    └──────┬──────┘
                           │
  ┌──────────────┐   ┌─────▼──────────────┐   ┌─────────────┐
  │ dim_customer │◄──│ fact_telesales_calls│──►│  dim_offer  │
  │(customer_id) │   │                    │   │ (offer_id)  │
  └──────────────┘   └────────────────────┘   └─────────────┘

Derived columns thêm vào Gold:
  dim_customer : age_group, income_band, credit_tier
  dim_offer    : product_category
  fact         : date_key, outcome_category, talk_time_band,
                 has_successful_sale, has_hard_rejection,
                 has_soft_rejection, has_do_not_call, has_objection

Ghi chiến lược: MERGE INTO (Iceberg ACID upsert) trên natural key.

Spark-submit:
  docker exec spark-master spark-submit \\
    --master spark://spark-master:7077 \\
    --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.0,\\
               org.apache.hadoop:hadoop-aws:3.3.4 \\
    /opt/spark/work-dir/batch-etl/gold_job.py
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# ── Cấu hình ──────────────────────────────────────────────────────────────────
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

# ── Spark Session ──────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("gold_job")
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
spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")
print("Namespace lakehouse.gold ready")

# ── DDL Gold tables ────────────────────────────────────────────────────────────

spark.sql("""
CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_customer (
    customer_id          STRING   COMMENT 'Natural key',
    full_name            STRING,
    age                  INT,
    age_group            STRING   COMMENT 'Young/Mid/Senior/Elder',
    gender               STRING,
    phone_number_masked  STRING,
    national_id_masked   STRING,
    address              STRING,
    employment_status    STRING,
    monthly_income       DOUBLE,
    income_band          STRING   COMMENT 'Low/Mid/High',
    credit_score         INT,
    credit_tier          STRING   COMMENT 'Poor/Fair/Good/Excellent',
    is_existing_customer BOOLEAN,
    _processed_at        TIMESTAMP
)
USING iceberg
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_offer (
    offer_id         STRING   COMMENT 'Natural key',
    customer_id      STRING   COMMENT 'FK -> dim_customer',
    campaign_id      STRING,
    product_name     STRING,
    product_category STRING   COMMENT 'Loan / Card',
    lead_source      STRING,
    decile_group     INT,
    loan_amount      DOUBLE,
    interest_rate    DOUBLE,
    _processed_at    TIMESTAMP
)
USING iceberg
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_date (
    date_key     INT      COMMENT 'PK — format YYYYMMDD',
    full_date    DATE,
    year         INT,
    quarter      INT,
    month        INT,
    month_name   STRING,
    week_of_year INT,
    day_of_month INT,
    day_of_week  INT      COMMENT '1=Sunday … 7=Saturday',
    day_name     STRING,
    is_weekend   BOOLEAN
)
USING iceberg
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lakehouse.gold.fact_telesales_calls (
    call_id                STRING    COMMENT 'Natural key',
    customer_id            STRING    COMMENT 'FK -> dim_customer',
    offer_id               STRING    COMMENT 'FK -> dim_offer',
    date_key               INT       COMMENT 'FK -> dim_date',
    agent_id               STRING,
    call_status            STRING,
    talk_time_seconds      INT,
    talk_time_band         STRING    COMMENT 'SHORT/MEDIUM/LONG',
    previous_contact_count INT,
    call_code_original     ARRAY<STRING> COMMENT 'Labels từ hệ thống nguồn',
    call_code_predicted    ARRAY<STRING> COMMENT 'Dự đoán từ NLP model',
    has_successful_sale    BOOLEAN,
    has_hard_rejection     BOOLEAN,
    has_soft_rejection     BOOLEAN,
    has_do_not_call        BOOLEAN,
    has_objection          BOOLEAN,
    outcome_category       STRING    COMMENT 'SALE/HARD_REJECTION/SOFT_REJECTION/DO_NOT_CALL/CALLBACK/IN_PROGRESS',
    _processed_at          TIMESTAMP
)
USING iceberg
PARTITIONED BY (date_key)
""")

print("Gold tables ready")

# ── Helper: MERGE INTO ─────────────────────────────────────────────────────────
def merge_into_gold(df, table, pk_col):
    view = f"_new_{table.split('.')[-1]}"
    df.createOrReplaceTempView(view)
    spark.sql(f"""
        MERGE INTO {table} AS target
        USING {view} AS source
        ON target.{pk_col} = source.{pk_col}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"  MERGE INTO {table} ({df.count():,} source records)")

# ══════════════════════════════════════════════════════════════════════════════
# 1. DIM_CUSTOMER
# ══════════════════════════════════════════════════════════════════════════════
print("\n[1/4] Building dim_customer ...")

dim_customer = (
    spark.table("lakehouse.silver.cust")
    .select(
        F.col("customer_id"),
        F.col("full_name"),
        F.col("age"),
        F.when(F.col("age") <= 30, "Young (<=30)")
         .when(F.col("age") <= 45, "Mid (31-45)")
         .when(F.col("age") <= 60, "Senior (46-60)")
         .otherwise("Elder (60+)")
         .alias("age_group"),
        F.col("gender"),
        F.col("phone_number_masked"),
        F.col("national_id_masked"),
        F.col("address"),
        F.col("employment_status"),
        F.col("monthly_income"),
        F.when(F.col("monthly_income") <  4000, "Low")
         .when(F.col("monthly_income") <  8000, "Mid")
         .otherwise("High")
         .alias("income_band"),
        F.col("credit_score"),
        F.when(F.col("credit_score") < 580, "Poor")
         .when(F.col("credit_score") < 670, "Fair")
         .when(F.col("credit_score") < 740, "Good")
         .otherwise("Excellent")
         .alias("credit_tier"),
        F.col("is_existing_customer"),
        F.current_timestamp().alias("_processed_at"),
    )
)

merge_into_gold(dim_customer, "lakehouse.gold.dim_customer", "customer_id")

# ══════════════════════════════════════════════════════════════════════════════
# 2. DIM_OFFER
# ══════════════════════════════════════════════════════════════════════════════
print("\n[2/4] Building dim_offer ...")

dim_offer = (
    spark.table("lakehouse.silver.offer")
    .select(
        F.col("offer_id"),
        F.col("customer_id"),
        F.col("campaign_id"),
        F.col("product_name"),
        F.when(F.lower(F.col("product_name")).contains("loan"), "Loan")
         .when(F.lower(F.col("product_name")).contains("card"), "Card")
         .otherwise("Other")
         .alias("product_category"),
        F.col("lead_source"),
        F.col("decile_group"),
        F.col("loan_amount"),
        F.col("interest_rate"),
        F.current_timestamp().alias("_processed_at"),
    )
)

merge_into_gold(dim_offer, "lakehouse.gold.dim_offer", "offer_id")

# ══════════════════════════════════════════════════════════════════════════════
# 3. DIM_DATE
# ══════════════════════════════════════════════════════════════════════════════
print("\n[3/4] Building dim_date ...")

# Sinh date dimension từ toàn bộ ngày thực tế có trong call_logs
dim_date = (
    spark.table("lakehouse.silver.call_logs")
    .select(F.to_date(F.col("call_timestamp")).alias("full_date"))
    .distinct()
    .filter(F.col("full_date").isNotNull())
    .select(
        F.date_format(F.col("full_date"), "yyyyMMdd")
         .cast(IntegerType()).alias("date_key"),
        F.col("full_date"),
        F.year(F.col("full_date")).alias("year"),
        F.quarter(F.col("full_date")).alias("quarter"),
        F.month(F.col("full_date")).alias("month"),
        F.date_format(F.col("full_date"), "MMMM").alias("month_name"),
        F.weekofyear(F.col("full_date")).alias("week_of_year"),
        F.dayofmonth(F.col("full_date")).alias("day_of_month"),
        F.dayofweek(F.col("full_date")).alias("day_of_week"),
        F.date_format(F.col("full_date"), "EEEE").alias("day_name"),
        # dayofweek: 1=Sunday, 7=Saturday
        F.col("full_date").isin(
            F.when(F.dayofweek(F.col("full_date")).isin(1, 7), True).otherwise(False)
        ).alias("is_weekend"),
    )
    # Ghi lại is_weekend đúng cách
    .drop("is_weekend")
    .withColumn(
        "is_weekend",
        F.dayofweek(F.col("full_date")).isin(1, 7),
    )
)

merge_into_gold(dim_date, "lakehouse.gold.dim_date", "date_key")

# ══════════════════════════════════════════════════════════════════════════════
# 4. FACT_TELESALES_CALLS
# ══════════════════════════════════════════════════════════════════════════════
print("\n[4/4] Building fact_telesales_calls ...")

silver_calls = spark.table("lakehouse.silver.call_logs")
silver_offer = spark.table("lakehouse.silver.offer").select("offer_id", "customer_id")

# Join để lấy customer_id vào fact (thông qua offer)
calls_with_customer = silver_calls.join(silver_offer, on="offer_id", how="left")

fact = (
    calls_with_customer
    .select(
        F.col("call_id"),
        F.col("customer_id"),
        F.col("offer_id"),

        # FK → dim_date
        F.date_format(F.col("call_timestamp"), "yyyyMMdd")
         .cast(IntegerType()).alias("date_key"),

        F.col("agent_id"),
        F.col("call_status"),
        F.col("talk_time_seconds"),

        # Derived: talk_time_band
        F.when(F.col("talk_time_seconds") <=  60, "SHORT (<=60s)")
         .when(F.col("talk_time_seconds") <= 180, "MEDIUM (61-180s)")
         .otherwise("LONG (>180s)")
         .alias("talk_time_band"),

        F.col("previous_contact_count"),
        F.col("call_code_original"),
        F.col("call_code_predicted"),

        # Derived: outcome flags từ call_code_original
        F.array_contains(F.col("call_code_original"), "SUCCESSFUL_SALE")
         .alias("has_successful_sale"),
        F.array_contains(F.col("call_code_original"), "HARD_REJECTION")
         .alias("has_hard_rejection"),
        F.array_contains(F.col("call_code_original"), "SOFT_REJECTION")
         .alias("has_soft_rejection"),
        F.array_contains(F.col("call_code_original"), "DO_NOT_CALL_REQUEST")
         .alias("has_do_not_call"),
        F.array_contains(F.col("call_code_original"), "OBJECTION_HANDLING")
         .alias("has_objection"),

        # Derived: outcome_category (priority-based)
        F.when(F.array_contains(F.col("call_code_original"), "SUCCESSFUL_SALE"),   "SALE")
         .when(F.array_contains(F.col("call_code_original"), "DO_NOT_CALL_REQUEST"),"DO_NOT_CALL")
         .when(F.array_contains(F.col("call_code_original"), "HARD_REJECTION"),    "HARD_REJECTION")
         .when(F.array_contains(F.col("call_code_original"), "SOFT_REJECTION"),    "SOFT_REJECTION")
         .when(F.array_contains(F.col("call_code_original"), "WARM_LEAD"),         "CALLBACK")
         .otherwise("IN_PROGRESS")
         .alias("outcome_category"),

        F.current_timestamp().alias("_processed_at"),
    )
)

merge_into_gold(fact, "lakehouse.gold.fact_telesales_calls", "call_id")

# ── Done ───────────────────────────────────────────────────────────────────────
print("\nGold job completed successfully.")
print("  lakehouse.gold.dim_customer        ✓")
print("  lakehouse.gold.dim_offer           ✓")
print("  lakehouse.gold.dim_date            ✓")
print("  lakehouse.gold.fact_telesales_calls ✓")
print()
print("Star Schema ready for Superset BI.")

spark.stop()
