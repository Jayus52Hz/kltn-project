"""
outcome_script_job.py
=====================
Build deterministic customer-facing telesales scripts from Gold outcomes.

Input:  lakehouse.gold.{fact_telesales_calls, dim_customer, dim_offer}
Output: lakehouse.gold.customer_outcome_scripts

The job intentionally uses fixed templates instead of an LLM so pipeline output
is reproducible, inexpensive, and easy to validate in the lakehouse demo.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from outcome_script_rules import DEFAULT_OUTCOME, OUTCOME_SCRIPT_RULES, rule_for_outcome


MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
SCRIPT_VERSION = os.getenv("OUTCOME_SCRIPT_VERSION", "v1")


spark = (
    SparkSession.builder
    .appName("outcome_script_job")
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

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")

spark.sql("""
CREATE TABLE IF NOT EXISTS lakehouse.gold.customer_outcome_scripts (
    script_id           STRING COMMENT 'Deterministic hash for call + outcome + script version',
    call_id             STRING COMMENT 'FK -> fact_telesales_calls',
    customer_id         STRING COMMENT 'FK -> dim_customer',
    offer_id            STRING COMMENT 'FK -> dim_offer',
    outcome_category    STRING COMMENT 'SALE/HARD_REJECTION/SOFT_REJECTION/DO_NOT_CALL/CALLBACK/IN_PROGRESS',
    outcome_strategy    STRING COMMENT 'Action strategy derived from outcome when upstream strategy is absent',
    script_template_id  STRING,
    script_version      STRING,
    script_title        STRING,
    opening_line        STRING,
    main_pitch          STRING,
    objection_response  STRING,
    next_action         STRING,
    closing_line        STRING,
    variables_json      STRING COMMENT 'Rendered customer/offer variables used by the template',
    _processed_at       TIMESTAMP
)
USING iceberg
PARTITIONED BY (outcome_category)
""")


def safe_text(column_name, fallback):
    value = F.col(column_name)
    return (
        F.when(value.isNull() | (F.trim(value.cast("string")) == ""), F.lit(fallback))
        .otherwise(F.trim(value.cast("string")))
    )


def money_text(column_name):
    return (
        F.when(F.col(column_name).isNull(), F.lit("chua xac dinh"))
        .otherwise(F.concat(F.lit("$"), F.format_number(F.col(column_name), 0)))
    )


def percent_text(column_name):
    return (
        F.when(F.col(column_name).isNull(), F.lit("chua xac dinh"))
        .otherwise(F.concat(F.format_number(F.col(column_name), 2), F.lit("%")))
    )


def outcome_rule_expr(field_name):
    expression = None
    for outcome_category, rule in OUTCOME_SCRIPT_RULES.items():
        branch = F.when(F.col("outcome_category") == outcome_category, F.lit(rule[field_name]))
        expression = branch if expression is None else expression.when(
            F.col("outcome_category") == outcome_category,
            F.lit(rule[field_name]),
        )
    return expression.otherwise(F.lit(rule_for_outcome(DEFAULT_OUTCOME)[field_name]))


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
    print(f"MERGE INTO {table} ({df.count():,} source records)")


fact = spark.table("lakehouse.gold.fact_telesales_calls")
customers = spark.table("lakehouse.gold.dim_customer")
offers = spark.table("lakehouse.gold.dim_offer")

if "outcome_strategy" in fact.columns:
    fact_with_strategy = fact
else:
    fact_with_strategy = fact.withColumn(
        "outcome_strategy",
        outcome_rule_expr("outcome_strategy"),
    )

base = (
    fact_with_strategy.alias("f")
    .join(customers.alias("c"), on="customer_id", how="left")
    .join(offers.alias("o"), on="offer_id", how="left")
    .select(
        F.col("f.call_id"),
        F.col("f.customer_id"),
        F.col("f.offer_id"),
        F.coalesce(F.col("f.outcome_category"), F.lit(DEFAULT_OUTCOME)).alias("outcome_category"),
        F.coalesce(
            F.col("f.outcome_strategy"),
            F.lit(rule_for_outcome(DEFAULT_OUTCOME)["outcome_strategy"]),
        ).alias("outcome_strategy"),
        F.col("c.full_name"),
        F.col("c.income_band"),
        F.col("c.credit_tier"),
        F.col("c.is_existing_customer"),
        F.col("o.product_name"),
        F.col("o.product_category"),
        F.col("o.lead_source"),
        F.col("o.loan_amount"),
        F.col("o.interest_rate"),
    )
    .filter(F.col("call_id").isNotNull())
)

enriched = (
    base
    .withColumn("customer_name_text", safe_text("full_name", "quy khach"))
    .withColumn("product_name_text", safe_text("product_name", "san pham tai chinh"))
    .withColumn("income_band_text", safe_text("income_band", "chua xac dinh"))
    .withColumn("credit_tier_text", safe_text("credit_tier", "chua xac dinh"))
    .withColumn("lead_source_text", safe_text("lead_source", "kenh dang ky"))
    .withColumn("loan_amount_text", money_text("loan_amount"))
    .withColumn("interest_rate_text", percent_text("interest_rate"))
    .withColumn(
        "customer_status_text",
        F.when(F.col("is_existing_customer") == True, "khach hang hien huu")
        .when(F.col("is_existing_customer") == False, "khach hang moi")
        .otherwise("chua xac dinh"),
    )
)

scripts = (
    enriched
    .withColumn(
        "script_template_id",
        outcome_rule_expr("script_template_id"),
    )
    .withColumn("script_version", F.lit(SCRIPT_VERSION))
    .withColumn(
        "script_title",
        outcome_rule_expr("script_title"),
    )
    .withColumn(
        "opening_line",
        F.when(
            F.col("outcome_category") == "SALE",
            F.concat(
                F.lit("Chao "),
                F.col("customer_name_text"),
                F.lit(", em goi de xac nhan minh se tiep tuc hoan tat ho so cho "),
                F.col("product_name_text"),
                F.lit("."),
            ),
        )
        .when(
            F.col("outcome_category") == "CALLBACK",
            F.concat(
                F.lit("Chao "),
                F.col("customer_name_text"),
                F.lit(", em cam on anh/chi da quan tam den "),
                F.col("product_name_text"),
                F.lit("."),
            ),
        )
        .when(
            F.col("outcome_category") == "DO_NOT_CALL",
            F.concat(
                F.lit("Chao "),
                F.col("customer_name_text"),
                F.lit(", em xin loi vi cuoc goi lam phien anh/chi."),
            ),
        )
        .otherwise(
            F.concat(
                F.lit("Chao "),
                F.col("customer_name_text"),
                F.lit(", em goi de ho tro anh/chi xem lai thong tin ve "),
                F.col("product_name_text"),
                F.lit("."),
            )
        ),
    )
    .withColumn(
        "main_pitch",
        F.when(
            F.col("outcome_category") == "DO_NOT_CALL",
            F.lit("Khong tiep tuc gioi thieu san pham; chi ghi nhan yeu cau ngung lien he."),
        )
        .when(
            F.col("outcome_category") == "HARD_REJECTION",
            F.lit("Khong ep khach tiep tuc trao doi; uu tien giu trai nghiem lich su va ton trong quyet dinh cua khach."),
        )
        .otherwise(
            F.concat(
                F.col("product_name_text"),
                F.lit(" hien co gia tri/han muc "),
                F.col("loan_amount_text"),
                F.lit(" voi lai suat "),
                F.col("interest_rate_text"),
                F.lit("; thong tin nay phu hop de anh/chi can nhac theo ho so "),
                F.col("credit_tier_text"),
                F.lit(" va nhom thu nhap "),
                F.col("income_band_text"),
                F.lit("."),
            )
        ),
    )
    .withColumn(
        "objection_response",
        F.when(
            F.col("outcome_category") == "SALE",
            F.lit("Neu khach con lo ve phi/lai suat, nhac lai cac dieu khoan da thong nhat va xac nhan buoc cung cap ho so."),
        )
        .when(
            F.col("outcome_category") == "CALLBACK",
            F.lit("Neu khach ban, de nghi mot khung gio goi lai cu the va gui tom tat thong tin truoc."),
        )
        .when(
            F.col("outcome_category") == "SOFT_REJECTION",
            F.lit("Neu khach can suy nghi, de nghi gui bang tom tat loi ich/chi phi va hen follow-up ngan."),
        )
        .when(
            F.col("outcome_category") == "HARD_REJECTION",
            F.lit("Ghi nhan khach khong quan tam; khong tranh luan hoac lap lai uu dai."),
        )
        .when(
            F.col("outcome_category") == "DO_NOT_CALL",
            F.lit("Xac nhan se cap nhat danh sach ngung lien he; khong xu ly phan doi bang pitch san pham."),
        )
        .otherwise(
            F.lit("Hoi ngan ve nhu cau, ngan sach, thoi diem quyet dinh va rao can hien tai cua khach."),
        ),
    )
    .withColumn(
        "next_action",
        outcome_rule_expr("next_action"),
    )
    .withColumn(
        "closing_line",
        F.when(
            F.col("outcome_category") == "DO_NOT_CALL",
            F.lit("Em da ghi nhan yeu cau cua anh/chi va se cap nhat de khong tiep tuc lien he. Em xin loi vi bat tien nay."),
        )
        .when(
            F.col("outcome_category") == "HARD_REJECTION",
            F.lit("Em cam on anh/chi da danh thoi gian. Em se ghi nhan trang thai khong quan tam va khong lam phien them ve uu dai nay."),
        )
        .when(
            F.col("outcome_category") == "SALE",
            F.lit("Em se gui buoc tiep theo ngay sau cuoc goi de anh/chi hoan tat ho so."),
        )
        .otherwise(
            F.lit("Em se gui thong tin tom tat va lien he lai theo thoi diem phu hop voi anh/chi."),
        ),
    )
    .withColumn(
        "variables_json",
        F.to_json(
            F.struct(
                F.col("customer_name_text").alias("full_name"),
                F.col("product_name_text").alias("product_name"),
                F.col("loan_amount_text").alias("loan_amount"),
                F.col("interest_rate_text").alias("interest_rate"),
                F.col("income_band_text").alias("income_band"),
                F.col("credit_tier_text").alias("credit_tier"),
                F.col("customer_status_text").alias("is_existing_customer"),
                F.col("lead_source_text").alias("lead_source"),
            )
        ),
    )
    .withColumn(
        "script_id",
        F.sha2(
            F.concat_ws(
                "||",
                F.col("call_id"),
                F.col("outcome_category"),
                F.col("outcome_strategy"),
                F.col("script_version"),
            ),
            256,
        ),
    )
    .select(
        "script_id",
        "call_id",
        "customer_id",
        "offer_id",
        "outcome_category",
        "outcome_strategy",
        "script_template_id",
        "script_version",
        "script_title",
        "opening_line",
        "main_pitch",
        "objection_response",
        "next_action",
        "closing_line",
        "variables_json",
        F.current_timestamp().alias("_processed_at"),
    )
)

merge_into_gold(scripts, "lakehouse.gold.customer_outcome_scripts", "script_id")

print("Outcome script job completed successfully.")
spark.stop()
