"""
test_silver.py
==============
Unit tests for Silver layer transformations.

Tests are self-contained — no Iceberg, no Kafka, no model file required.
The same expressions used in silver_job.py are reproduced here so tests
remain independent of module-level side effects (SparkSession init, model load).
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType, DoubleType, BooleanType,
)


# ── Replicated helpers from silver_job.py ────────────────────────────────────

def mask_phone(col_expr):
    return F.regexp_replace(col_expr, r"(\d{3})-\d{3}-(\d{4})", r"$1-***-$2")


def mask_national_id(col_expr):
    return F.regexp_replace(col_expr, r"(\w{3}-\w{2})-\d{4}", r"$1-****")


def dedup_latest(df, pk_col):
    window = Window.partitionBy(pk_col).orderBy(F.col("ts_ms").desc())
    return (
        df
        .withColumn("_rn", F.row_number().over(window))
        .filter((F.col("_rn") == 1) & (F.col("op") != "d"))
        .drop("_rn")
    )


# ── PII Masking ───────────────────────────────────────────────────────────────

class TestPiiMasking:
    def test_phone_masks_middle_segment(self, spark):
        df = spark.createDataFrame([("555-123-4567",)], ["phone"])
        result = df.select(mask_phone(F.col("phone")).alias("masked")).first()["masked"]
        assert result == "555-***-4567"

    def test_phone_preserves_area_code_and_last4(self, spark):
        df = spark.createDataFrame([("800-999-0001",)], ["phone"])
        result = df.select(mask_phone(F.col("phone")).alias("masked")).first()["masked"]
        assert result == "800-***-0001"

    def test_phone_null_returns_null(self, spark):
        df = spark.createDataFrame([(None,)], ["phone"])
        result = df.select(mask_phone(F.col("phone")).alias("masked")).first()["masked"]
        assert result is None

    def test_national_id_masks_last4(self, spark):
        df = spark.createDataFrame([("ABC-12-9876",)], ["nid"])
        result = df.select(mask_national_id(F.col("nid")).alias("masked")).first()["masked"]
        assert result == "ABC-12-****"

    def test_national_id_preserves_prefix(self, spark):
        df = spark.createDataFrame([("XYZ-99-1111",)], ["nid"])
        result = df.select(mask_national_id(F.col("nid")).alias("masked")).first()["masked"]
        assert result == "XYZ-99-****"


# ── Deduplication ─────────────────────────────────────────────────────────────

DEDUP_SCHEMA = StructType([
    StructField("customer_id", StringType(), True),
    StructField("full_name",   StringType(), True),
    StructField("ts_ms",       LongType(),   True),
    StructField("op",          StringType(), True),
])

class TestDedupLatest:
    def test_keeps_latest_record(self, spark):
        rows = [
            ("C001", "Alice v1", 1000, "c"),
            ("C001", "Alice v2", 2000, "u"),   # ← should be kept
            ("C001", "Alice v3", 1500, "u"),
        ]
        df = spark.createDataFrame(rows, DEDUP_SCHEMA)
        result = dedup_latest(df, "customer_id")
        assert result.count() == 1
        assert result.first()["full_name"] == "Alice v2"

    def test_drops_delete_events(self, spark):
        rows = [
            ("C002", "Bob",     1000, "c"),
            ("C002", "Bob del", 2000, "d"),   # ← delete is latest but must be dropped
        ]
        df = spark.createDataFrame(rows, DEDUP_SCHEMA)
        result = dedup_latest(df, "customer_id")
        assert result.count() == 0

    def test_multiple_customers_independent(self, spark):
        rows = [
            ("C001", "Alice v1", 1000, "c"),
            ("C001", "Alice v2", 3000, "u"),   # ← keep
            ("C002", "Bob v1",   2000, "c"),   # ← keep (only one)
        ]
        df = spark.createDataFrame(rows, DEDUP_SCHEMA)
        result = dedup_latest(df, "customer_id")
        assert result.count() == 2
        names = {r["full_name"] for r in result.collect()}
        assert names == {"Alice v2", "Bob v1"}

    def test_snapshot_record_not_dropped(self, spark):
        rows = [("C003", "Carol", 1000, "r")]  # r = snapshot read
        df = spark.createDataFrame(rows, DEDUP_SCHEMA)
        result = dedup_latest(df, "customer_id")
        assert result.count() == 1


# ── JSON parsing (Bronze → Silver flatten) ────────────────────────────────────

class TestJsonParsing:
    CUST_SCHEMA = StructType([
        StructField("customer_id",    StringType(),  True),
        StructField("age",            IntegerType(), True),
        StructField("monthly_income", DoubleType(),  True),
    ])

    def test_valid_json_parsed_correctly(self, spark):
        raw = '[{"raw_doc": "{\\"customer_id\\":\\"C001\\",\\"age\\":30,\\"monthly_income\\":5000.0}"}]'
        df = spark.read.json(spark.sparkContext.parallelize([raw]))
        result = (
            df.withColumn("doc", F.from_json(F.col("raw_doc"), self.CUST_SCHEMA))
              .select("doc.customer_id", "doc.age", "doc.monthly_income")
              .first()
        )
        assert result["customer_id"] == "C001"
        assert result["age"] == 30
        assert result["monthly_income"] == 5000.0

    def test_malformed_json_returns_null_struct(self, spark):
        df = spark.createDataFrame([("not-json",)], ["raw_doc"])
        result = (
            df.withColumn("doc", F.from_json(F.col("raw_doc"), self.CUST_SCHEMA))
              .select(F.col("doc.customer_id").alias("cid"))
              .first()
        )
        assert result["cid"] is None
