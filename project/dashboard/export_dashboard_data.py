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


def table_or_none(spark, table_name):
    try:
        return spark.table(table_name)
    except Exception:
        return None


def count_table(df):
    return df.count() if df is not None else 0


def collect_profile_rows(df):
    if df is None:
        return []

    rows = []
    for row in df.collect():
        dataset_name = row["dataset_name"]
        rows.append(
            {
                "dataset_name": dataset_name,
                "dataset_label": (
                    "Primary AGI Telesales"
                    if dataset_name == "primary_telesales"
                    else "CallCenterEN"
                ),
                "role": (
                    "Business dataset for conversion and campaign analytics"
                    if dataset_name == "primary_telesales"
                    else "External corpus for domain-shift and multi-source serving checks"
                ),
                "row_count": int(row["row_count"] or 0),
                "avg_duration_seconds": round(float(row["avg_duration_seconds"] or 0), 1),
                "avg_word_count": round(float(row["avg_word_count"] or 0), 1),
                "avg_char_count": round(float(row["avg_char_count"] or 0), 1),
                "avg_pii_token_count": (
                    None
                    if row["avg_pii_token_count"] is None
                    else round(float(row["avg_pii_token_count"]), 1)
                ),
            }
        )
    return rows


def collect_label_distribution(df, dataset_name, limit=12):
    if df is None:
        return []
    return [
        {"label": row["call_code"], "value": int(row["label_count"] or 0)}
        for row in (
            df.filter(F.col("dataset_name") == dataset_name)
            .orderBy(F.desc("label_count"), F.asc("call_code"))
            .limit(limit)
            .collect()
        )
    ]


def collect_model_rows(df):
    if df is None:
        return []

    label_map = {
        "M0_primary_bow": "Primary BoW",
        "M1_primary_to_callcenteren": "Primary -> CallCenterEN",
        "M2_callcenteren_bow": "CallCenterEN BoW",
        "M4_combined_bow": "Combined BoW",
    }
    wanted = {
        ("M0_primary_bow", "primary_test"),
        ("M1_primary_to_callcenteren", "callcenteren_test"),
        ("M2_callcenteren_bow", "callcenteren_test"),
        ("M4_combined_bow", "callcenteren_test"),
    }
    rows = []
    for row in df.collect():
        key = (row["model"], row["eval_dataset"])
        if key not in wanted:
            continue
        rows.append(
            {
                "model": row["model"],
                "model_label": label_map.get(row["model"], row["model"]),
                "train_dataset": row["train_dataset"],
                "eval_dataset": row["eval_dataset"],
                "eval_rows": int(row["eval_rows"] or 0),
                "micro_f1": round(float(row["micro_f1"] or 0), 4),
                "exact_match_rate": round(float(row["exact_match_rate"] or 0), 4),
            }
        )
    return rows


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
    fact_callcenter = table_or_none(spark, "lakehouse.gold_external.fact_callcenter_calls")
    bridge_callcenter = table_or_none(spark, "lakehouse.gold_external.bridge_callcenter_call_code")
    callcenter_analytics = table_or_none(spark, "lakehouse.gold_external.callcenteren_call_analytics")
    dataset_profiles = table_or_none(spark, "lakehouse.gold.dataset_profile_comparison")
    label_distribution = table_or_none(spark, "lakehouse.gold.call_code_distribution_comparison")
    model_experiments = table_or_none(spark, "lakehouse.gold.model_experiment_comparison")
    callcenteren_calls = count_table(fact_callcenter)
    bridge_rows = count_table(bridge_callcenter)

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
    callcenter_domain = []
    callcenter_direction = []
    if callcenter_analytics is not None:
        callcenter_domain = collect_pairs(
            callcenter_analytics.groupBy("source_domain")
            .agg(F.sum("call_count").alias("value"))
            .orderBy(F.desc("value"), F.asc("source_domain")),
            "source_domain",
            limit=12,
        )
        callcenter_direction = collect_pairs(
            callcenter_analytics.groupBy("call_direction")
            .agg(F.sum("call_count").alias("value"))
            .orderBy(F.desc("value"), F.asc("call_direction")),
            "call_direction",
            limit=4,
        )

    profile_rows = collect_profile_rows(dataset_profiles)
    primary_profile = next(
        (row for row in profile_rows if row["dataset_name"] == "primary_telesales"),
        {},
    )
    callcenter_profile = next(
        (row for row in profile_rows if row["dataset_name"] == "callcenteren"),
        {},
    )
    duration_gap = (
        (callcenter_profile.get("avg_duration_seconds") or 0)
        - (primary_profile.get("avg_duration_seconds") or float(avg_talk_time))
    )
    total_serving_rows = total_calls + callcenteren_calls + bridge_rows

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_artifacts": {
            "primary_dashboard": "lakehouse.gold.fact_telesales_calls",
            "callcenteren_fact": "lakehouse.gold_external.fact_callcenter_calls",
            "callcenteren_bridge": "lakehouse.gold_external.bridge_callcenter_call_code",
            "model_metrics": "lakehouse.gold.model_experiment_comparison",
        },
        "kpis": {
            "total_calls": total_calls,
            "total_customers": total_customers,
            "total_offers": total_offers,
            "successful_sales": successful_sales,
            "hard_rejections": hard_rejections,
            "soft_rejections": soft_rejections,
            "avg_talk_time_seconds": round(float(avg_talk_time), 1),
            "success_rate": round(successful_sales / total_calls * 100, 2) if total_calls else 0,
            "callcenteren_calls": callcenteren_calls,
            "callcenteren_bridge_rows": bridge_rows,
            "total_serving_rows": total_serving_rows,
            "duration_gap_seconds": round(float(duration_gap), 1),
        },
        "insights": [
            {
                "label": "Scale insight",
                "value": f"{total_calls + callcenteren_calls:,} calls",
                "detail": "Gold serving covers the primary dataset and CallCenterEN, so the dashboard validates a multi-source lakehouse.",
            },
            {
                "label": "Conversation depth",
                "value": f"+{round(duration_gap):,}s",
                "detail": "CallCenterEN conversations are longer on average, making it useful for domain-shift and transcript-processing checks.",
            },
            {
                "label": "Business signal",
                "value": f"{round(successful_sales / total_calls * 100, 2) if total_calls else 0}%",
                "detail": "The primary dataset remains the source for conversion, product, campaign, and customer analytics.",
            },
            {
                "label": "Model signal",
                "value": f"{bridge_rows:,} links",
                "detail": "CallCenterEN Gold uses a bridge table because one call can have multiple inferred call_code labels.",
            },
        ],
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
        "comparison": {
            "dataset_profiles": profile_rows,
            "primary_top_call_codes": collect_label_distribution(
                label_distribution,
                "primary_telesales",
                limit=10,
            ),
        },
        "callcenteren": {
            "domain_distribution": {
                row["label"]: row["value"]
                for row in callcenter_domain
            },
            "direction_distribution": {
                row["label"]: row["value"]
                for row in callcenter_direction
            },
            "top_model_call_codes": collect_label_distribution(
                label_distribution,
                "callcenteren",
                limit=12,
            ),
        },
        "models": collect_model_rows(model_experiments),
        "evidence": [
            {
                "layer": "Primary Gold",
                "object": "fact_telesales_calls",
                "rows": total_calls,
                "status": "verified",
            },
            {
                "layer": "CallCenterEN Gold",
                "object": "fact_callcenter_calls",
                "rows": callcenteren_calls,
                "status": "verified" if callcenteren_calls else "not exported",
            },
            {
                "layer": "CallCenterEN Gold",
                "object": "bridge_callcenter_call_code",
                "rows": bridge_rows,
                "status": "verified" if bridge_rows else "not exported",
            },
        ],
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Dashboard data exported to {OUTPUT_PATH}")
    spark.stop()


if __name__ == "__main__":
    main()
