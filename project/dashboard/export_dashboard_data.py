import json
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


OUTPUT_PATH = Path("/dashboard/dashboard_data.json")


def build_spark():
    return (
        SparkSession.builder
        .appName("export_telesales_dashboard_data")
        .master("local[*]")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://lakehouse/warehouse")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config(
            "spark.jars.packages",
            ",".join(
                [
                    "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.0",
                    "org.apache.hadoop:hadoop-aws:3.3.4",
                    "com.amazonaws:aws-java-sdk-bundle:1.12.261",
                ]
            ),
        )
        .getOrCreate()
    )


def collect_pairs(df, label_col, value_col="value", limit=20):
    return [
        {"label": str(row[label_col]) if row[label_col] is not None else "Unknown", "value": int(row[value_col] or 0)}
        for row in df.limit(limit).collect()
    ]


def count_by(df, col_name, limit=20):
    return collect_pairs(
        df.groupBy(col_name)
        .count()
        .withColumnRenamed("count", "value")
        .orderBy(F.desc("value"), F.asc(col_name)),
        col_name,
        limit=limit,
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")

    fact = spark.table("lakehouse.gold.fact_telesales_calls")
    offers = spark.table("lakehouse.gold.dim_offer")
    customers = spark.table("lakehouse.gold.dim_customer")
    dates = spark.table("lakehouse.gold.dim_date")

    fact_offer = fact.join(
        offers.select("offer_id", "campaign_id", "product_name", "product_category"),
        on="offer_id",
        how="left",
    )

    total_calls = fact.count()
    total_customers = customers.count()
    total_offers = offers.count()
    successful_sales = fact.filter(F.col("has_successful_sale") == True).count()
    hard_rejections = fact.filter(F.col("has_hard_rejection") == True).count()
    soft_rejections = fact.filter(F.col("has_soft_rejection") == True).count()
    avg_talk_time = fact.agg(F.avg("talk_time_seconds").alias("avg")).first()["avg"] or 0

    daily_calls = (
        fact.join(dates.select("date_key", "full_date"), on="date_key", how="left")
        .groupBy("full_date")
        .count()
        .withColumnRenamed("count", "value")
        .orderBy("full_date")
    )

    top_campaigns = (
        fact_offer.groupBy("campaign_id")
        .agg(
            F.count("*").alias("calls"),
            F.sum(F.col("has_successful_sale").cast("int")).alias("sales"),
            F.avg("talk_time_seconds").alias("avg_talk_time"),
        )
        .orderBy(F.desc("calls"), F.asc("campaign_id"))
        .limit(10)
    )

    top_products = (
        fact_offer.groupBy("product_name")
        .agg(
            F.count("*").alias("calls"),
            F.sum(F.col("has_successful_sale").cast("int")).alias("sales"),
        )
        .orderBy(F.desc("calls"), F.asc("product_name"))
        .limit(10)
    )

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "kpis": {
            "total_calls": total_calls,
            "total_customers": total_customers,
            "total_offers": total_offers,
            "successful_sales": successful_sales,
            "hard_rejections": hard_rejections,
            "soft_rejections": soft_rejections,
            "avg_talk_time_seconds": round(float(avg_talk_time), 1),
            "success_rate": round(successful_sales / total_calls * 100, 2) if total_calls else 0,
        },
        "charts": {
            "daily_calls": [
                {
                    "label": row["full_date"].isoformat() if row["full_date"] else "Unknown",
                    "value": int(row["value"] or 0),
                }
                for row in daily_calls.collect()
            ],
            "outcome_category": count_by(fact, "outcome_category", limit=12),
            "call_status": count_by(fact, "call_status", limit=12),
            "talk_time_band": count_by(fact, "talk_time_band", limit=12),
            "product_category": count_by(fact_offer, "product_category", limit=12),
            "credit_tier": count_by(customers, "credit_tier", limit=12),
        },
        "tables": {
            "top_campaigns": [
                {
                    "campaign_id": row["campaign_id"] or "Unknown",
                    "calls": int(row["calls"] or 0),
                    "sales": int(row["sales"] or 0),
                    "avg_talk_time_seconds": round(float(row["avg_talk_time"] or 0), 1),
                }
                for row in top_campaigns.collect()
            ],
            "top_products": [
                {
                    "product_name": row["product_name"] or "Unknown",
                    "calls": int(row["calls"] or 0),
                    "sales": int(row["sales"] or 0),
                }
                for row in top_products.collect()
            ],
        },
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Dashboard data exported to {OUTPUT_PATH}")
    spark.stop()


if __name__ == "__main__":
    main()
