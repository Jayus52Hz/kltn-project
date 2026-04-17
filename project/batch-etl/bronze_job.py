"""
bronze_job.py
=============
Spark Structured Streaming — Bronze Layer

Đọc CDC events từ Kafka (Debezium → MongoDB) và ghi raw documents vào
Apache Iceberg tables trên MinIO. Không transform business logic,
chỉ unwrap Debezium envelope và thêm metadata.

Luồng:
  MongoDB (rs0) → Debezium CDC → Kafka → [bronze_job] → MinIO bronze/ (Iceberg)

Kafka topics consumed:
  mongo-source.telesales.cust
  mongo-source.telesales.offer
  mongo-source.telesales.call_logs

Iceberg tables written (partitioned by day):
  lakehouse.bronze.cust
  lakehouse.bronze.offer
  lakehouse.bronze.call_logs

Debezium MongoDB payload format (v2.x):
  {
    "payload": {
      "after":  "<full MongoDB document as JSON string>",  -- null on DELETE
      "before": null,                                       -- null for MongoDB
      "op":     "c | u | d | r",                           -- c=insert, u=update, d=delete, r=snapshot
      "ts_ms":  1234567890000,                              -- CDC timestamp ms
      "source": { "db": "telesales", "collection": "..." }
    }
  }

Spark-submit:
  docker exec spark-master spark-submit \\
    --master spark://spark-master:7077 \\
    --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.0,\\
               org.apache.hadoop:hadoop-aws:3.3.4,\\
               org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \\
    /opt/spark/work-dir/batch-etl/bronze_job.py
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType,
)

# ── Cấu hình (lấy từ env, fallback về giá trị mặc định trong Docker network) ──
KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",          "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY",        "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY",        "minioadmin")
CHECKPOINT_BASE  = os.getenv("CHECKPOINT_BASE",         "s3a://bronze/_checkpoints")

# Debezium connector name phải khớp với field "name" trong mongodb-connector.json
CONNECTOR_NAME = "mongo-source"
DATABASE_NAME  = "telesales"
COLLECTIONS    = ["cust", "offer", "call_logs"]

# ── Spark Session ──────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("bronze_job")

    # Iceberg extensions
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

    # Iceberg Hadoop catalog — dùng MinIO làm warehouse
    .config("spark.sql.catalog.lakehouse",
            "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakehouse.type",      "hadoop")
    .config("spark.sql.catalog.lakehouse.warehouse", "s3a://bronze/warehouse")

    # MinIO / S3A
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

# ── Tạo Iceberg namespace ──────────────────────────────────────────────────────
spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")
print("Namespace lakehouse.bronze ready")

# ── DDL: tạo Iceberg table cho từng collection nếu chưa có ────────────────────
#
# Schema thống nhất cho cả 3 collections:
#   raw_doc     — toàn bộ MongoDB document dạng JSON string (payload.after)
#   op          — loại CDC event: c/u/d/r
#   ts_ms       — timestamp của CDC event (milliseconds)
#   topic       — Kafka topic nguồn
#   partition   — Kafka partition
#   offset      — Kafka offset (dùng để deduplicate nếu cần)
#   ingested_at — thời điểm Spark đọc và ghi record này (dùng để partition)
#
DDL = """
CREATE TABLE IF NOT EXISTS lakehouse.bronze.{collection} (
    raw_doc     STRING    COMMENT 'Full MongoDB document as JSON string (payload.after)',
    op          STRING    COMMENT 'CDC op: c=insert, u=update, d=delete, r=snapshot',
    ts_ms       BIGINT    COMMENT 'CDC event timestamp in milliseconds',
    topic       STRING    COMMENT 'Source Kafka topic',
    partition   INT       COMMENT 'Kafka partition number',
    offset      BIGINT    COMMENT 'Kafka offset (unique per partition)',
    ingested_at TIMESTAMP COMMENT 'When Spark ingested this record'
)
USING iceberg
PARTITIONED BY (days(ingested_at))
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy'
)
"""

for col in COLLECTIONS:
    spark.sql(DDL.format(collection=col))
    print(f"Table lakehouse.bronze.{col} ready")

# ── Schema của Debezium MongoDB payload (chỉ parse phần cần thiết) ────────────
DEBEZIUM_SCHEMA = StructType([
    StructField("payload", StructType([
        StructField("after",  StringType(), True),
        StructField("op",     StringType(), True),
        StructField("ts_ms",  LongType(),   True),
    ]), True)
])

# ── Tạo streaming query cho từng collection ────────────────────────────────────
queries = []

for collection in COLLECTIONS:
    topic      = f"{CONNECTOR_NAME}.{DATABASE_NAME}.{collection}"
    checkpoint = f"{CHECKPOINT_BASE}/{collection}"

    # 1. Đọc từ Kafka
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe",               topic)
        .option("startingOffsets",         "earliest")
        .option("failOnDataLoss",          "false")
        .load()
    )

    # 2. Parse Debezium envelope → extract raw_doc + metadata
    parsed = (
        raw_stream
        .withColumn(
            "envelope",
            F.from_json(F.col("value").cast("string"), DEBEZIUM_SCHEMA)
        )
        .select(
            F.col("envelope.payload.after").alias("raw_doc"),
            F.col("envelope.payload.op").alias("op"),
            F.col("envelope.payload.ts_ms").alias("ts_ms"),
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.current_timestamp().alias("ingested_at"),
        )
        # Bỏ DELETE events (after = null) — không có document để lưu
        .filter(F.col("raw_doc").isNotNull())
    )

    # 3. Ghi vào Iceberg table, micro-batch mỗi 30 giây
    query = (
        parsed.writeStream
        .format("iceberg")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .option("checkpointLocation", checkpoint)
        .toTable(f"lakehouse.bronze.{collection}")
    )

    queries.append(query)
    print(f"Stream started: {topic}  →  lakehouse.bronze.{collection}")

# ── Chờ tất cả queries ────────────────────────────────────────────────────────
print(f"\nBronze job running — {len(queries)} active streams")
print("Press Ctrl+C to stop\n")

for q in queries:
    q.awaitTermination()
