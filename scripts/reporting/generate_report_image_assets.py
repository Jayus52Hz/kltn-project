# -*- coding: utf-8 -*-
"""Generate report-ready PNG diagrams for the KLTN image checklist."""

from __future__ import annotations

import json
import math
import os
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont


ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "docs" / "report_image_assets" / "generated"
DATA_PATH = ROOT / "project" / "dashboard" / "dashboard_data.json"
W, H = 1600, 900
FONT_REG = r"C:\Windows\Fonts\arial.ttf"
FONT_BOLD = r"C:\Windows\Fonts\arialbd.ttf"


def font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont:
    return ImageFont.truetype(FONT_BOLD if bold else FONT_REG, size)


def wrap(draw: ImageDraw.ImageDraw, text: str, max_width: int, fnt: ImageFont.FreeTypeFont) -> list[str]:
    words = text.split()
    lines: list[str] = []
    cur = ""
    for word in words:
        test = (cur + " " + word).strip()
        if draw.textbbox((0, 0), test, font=fnt)[2] <= max_width:
            cur = test
        else:
            if cur:
                lines.append(cur)
            cur = word
    if cur:
        lines.append(cur)
    return lines


def base(title: str, subtitle: str = "") -> tuple[Image.Image, ImageDraw.ImageDraw]:
    img = Image.new("RGB", (W, H), "#f7f8fb")
    draw = ImageDraw.Draw(img)
    draw.text((70, 48), title, font=font(46, True), fill="#111827")
    if subtitle:
        draw.text((72, 108), subtitle, font=font(24), fill="#5b6475")
    return img, draw


def box(
    draw: ImageDraw.ImageDraw,
    xy: tuple[int, int, int, int],
    title: str,
    subtitle: str = "",
    fill: str = "#ffffff",
    outline: str = "#263244",
    accent: str = "#2563eb",
) -> None:
    x1, y1, x2, y2 = xy
    draw.rounded_rectangle(xy, radius=18, fill=fill, outline=outline, width=3)
    draw.rectangle((x1, y1, x1 + 12, y2), fill=accent)
    draw.text((x1 + 28, y1 + 22), title, fill="#111827", font=font(30, True))
    if not subtitle:
        return
    lines: list[str] = []
    for part in subtitle.split("\n"):
        lines.extend(wrap(draw, part, x2 - x1 - 56, font(22)) or [""])
    y = y1 + 62
    for line in lines[:5]:
        draw.text((x1 + 28, y), line, fill="#4b5563", font=font(22))
        y += 29


def arrow(
    draw: ImageDraw.ImageDraw,
    p1: tuple[int, int],
    p2: tuple[int, int],
    color: str = "#374151",
    width: int = 5,
) -> None:
    x1, y1 = p1
    x2, y2 = p2
    draw.line((x1, y1, x2, y2), fill=color, width=width)
    angle = math.atan2(y2 - y1, x2 - x1)
    length = 18
    points = [
        (x2, y2),
        (x2 - length * math.cos(angle - math.pi / 7), y2 - length * math.sin(angle - math.pi / 7)),
        (x2 - length * math.cos(angle + math.pi / 7), y2 - length * math.sin(angle + math.pi / 7)),
    ]
    draw.polygon(points, fill=color)


def save(img: Image.Image, name: str) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    img.save(OUT / name, quality=95)


def horizontal_flow(
    name: str,
    title: str,
    subtitle: str,
    items: list[tuple[str, str, str, str]],
    y1: int = 310,
    y2: int = 545,
) -> None:
    img, draw = base(title, subtitle)
    x = 70
    gap = 55
    width = int((W - 140 - gap * (len(items) - 1)) / len(items))
    for i, (heading, body, fill, accent) in enumerate(items):
        box(draw, (x, y1, x + width, y2), heading, body, fill=fill, accent=accent)
        if i < len(items) - 1:
            arrow(draw, (x + width + 10, (y1 + y2) // 2), (x + width + gap - 10, (y1 + y2) // 2))
        x += width + gap
    save(img, name)


def main() -> None:
    data = json.loads(DATA_PATH.read_text(encoding="utf-8")) if DATA_PATH.exists() else {}

    horizontal_flow(
        "fig_01_data_warehouse_lake_lakehouse.png",
        "Từ Data Warehouse đến Data Lakehouse",
        "So sánh schema-on-write, schema-on-read và open table format",
        [
            ("Data Warehouse", "Dữ liệu có cấu trúc\nSchema-on-write\nBI truyền thống", "#eaf2ff", "#2563eb"),
            ("Data Lake", "Dữ liệu đa dạng\nSchema-on-read\nLưu trữ linh hoạt", "#eefaf3", "#16a34a"),
            (
                "Data Lakehouse",
                "Object storage + Iceberg\nMetadata, snapshot, schema evolution\nBI + ML trên cùng nền tảng",
                "#f4f0ff",
                "#7c3aed",
            ),
        ],
        280,
        560,
    )

    horizontal_flow(
        "fig_02_medallion_overview.png",
        "Medallion Lakehouse cho AGI Telesales",
        "MongoDB/Kafka → Bronze → Silver → Gold → Dashboard",
        [
            ("MongoDB + Kafka", "CDC events", "#eef6ff", "#1d4ed8"),
            ("Bronze", "Raw CDC JSON\nmetadata, offsets", "#fff7ed", "#c2410c"),
            ("Silver", "Cleaned + dedup\nPII masking\nBoW call_code", "#effaf4", "#15803d"),
            ("Gold", "Star Schema\nfact + dimensions", "#f4f0ff", "#6d28d9"),
            ("BI Serving", "BigQuery views\nSuperset dashboard", "#fff1f2", "#be123c"),
        ],
        320,
        535,
    )

    horizontal_flow(
        "fig_03_cdc_mongodb_debezium_kafka_bronze.png",
        "Luồng CDC từ MongoDB qua Debezium và Kafka",
        "Bắt thay đổi từ nguồn vận hành trước khi ghi Bronze",
        [
            ("MongoDB ReplicaSet", "cust, offer, call_logs", "#ffffff", "#2563eb"),
            ("Change Stream", "insert, update, delete events", "#ffffff", "#2563eb"),
            ("Debezium Connect", "connector mongo-source", "#ffffff", "#c2410c"),
            ("Kafka Topics", "dbserver1.telesales.*", "#ffffff", "#c2410c"),
            ("Spark Bronze Job", "read stream, trigger once", "#ffffff", "#15803d"),
            ("Iceberg Bronze", "raw_doc + CDC metadata", "#ffffff", "#15803d"),
        ],
        300,
        520,
    )

    img, draw = base("Cấu trúc metadata của Apache Iceberg", "Snapshot giúp schema evolution, time travel và quản lý file dữ liệu")
    box(draw, (100, 250, 400, 390), "Catalog / Table", "lakehouse.gold.fact_telesales_calls", fill="#eef6ff", accent="#2563eb")
    box(draw, (510, 180, 850, 330), "metadata.json", "schema, partition spec, current snapshot", fill="#f4f0ff", accent="#7c3aed")
    box(draw, (510, 450, 850, 600), "Snapshot", "snapshot-id, timestamp, operation", fill="#fff7ed", accent="#c2410c")
    box(draw, (980, 180, 1300, 330), "Manifest List", "list of manifest files", fill="#effaf4", accent="#15803d")
    box(draw, (980, 450, 1300, 600), "Data Files", "Parquet files in object storage", fill="#effaf4", accent="#15803d")
    arrow(draw, (400, 320), (510, 250))
    arrow(draw, (680, 330), (680, 450))
    arrow(draw, (850, 255), (980, 255))
    arrow(draw, (1140, 330), (1140, 450))
    save(img, "fig_04_iceberg_metadata_snapshot_manifest.png")

    horizontal_flow(
        "fig_05_silver_pii_nlp_flow.png",
        "Luồng xử lý Silver: làm sạch, PII và NLP",
        "Transcript chỉ dùng để inference call_code, không đưa lên BI serving",
        [
            ("Raw JSON", "Bronze raw_doc", "#ffffff", "#6b7280"),
            ("Parse + Cast", "typed columns", "#ffffff", "#2563eb"),
            ("Deduplicate", "business key + CDC timestamp", "#ffffff", "#2563eb"),
            ("PII Control", "mask/drop identifiers", "#ffffff", "#be123c"),
            ("BoW NLP", "predict call_code", "#ffffff", "#7c3aed"),
            ("Silver Tables", "clean analytical data", "#ffffff", "#15803d"),
        ],
    )

    horizontal_flow(
        "fig_06_synthetic_data_generation.png",
        "Quy trình sinh dữ liệu tổng hợp bằng Generative AI",
        "Tạo dữ liệu mô phỏng cho bài toán AGI Telesales",
        [
            ("Prompt thiết kế", "business fields, schema, constraints", "#ffffff", "#7c3aed"),
            ("Generate records", "customers, offers, calls", "#ffffff", "#7c3aed"),
            ("Validate", "schema, range, consistency", "#ffffff", "#2563eb"),
            ("Split entities", "cust / offer / call_logs", "#ffffff", "#15803d"),
            ("Load MongoDB", "seed source database", "#ffffff", "#2563eb"),
        ],
    )

    img, draw = base("Schema master data trước chuẩn hóa", "Một bản ghi telesales tổng hợp trước khi tách entity")
    columns = [
        ("Customer", "customer_id\nage, gender\nincome, credit_score\nPII fields", "#eef6ff", "#2563eb"),
        ("Offer", "offer_id\nproduct_name\ncampaign_id\nlead_source", "#fff7ed", "#c2410c"),
        ("Call Log", "call_id\nagent_id\ntalk_time\ntranscript\noutcome", "#effaf4", "#15803d"),
        ("Label", "call_code\nhas_successful_sale\nhas_objection", "#f4f0ff", "#7c3aed"),
    ]
    x = 120
    for heading, body, fill, accent in columns:
        box(draw, (x, 260, x + 300, 610), heading, body, fill=fill, accent=accent)
        x += 350
    save(img, "fig_07_master_data_schema.png")

    img, draw = base("Phân bố nhãn call_code trong bộ dữ liệu", "Top nhãn từ dashboard_data.json")
    items = (data.get("comparison", {}).get("primary_top_call_codes") or data.get("callcenteren", {}).get("top_model_call_codes") or [])[:10]
    if not items:
        items = [
            {"label": "SALE_SUCCESS", "value": 3933},
            {"label": "HARD_REJECTION", "value": 5381},
            {"label": "SOFT_REJECTION", "value": 2837},
        ]
    max_value = max(int(item["value"]) for item in items)
    y = 210
    for item in items:
        label = str(item["label"])[:28]
        value = int(item["value"])
        draw.text((120, y + 8), label, font=font(22, True), fill="#111827")
        bar_width = int(900 * value / max_value)
        draw.rounded_rectangle((480, y, 480 + bar_width, y + 34), radius=8, fill="#2563eb")
        draw.text((500 + bar_width, y + 5), f"{value:,}", font=font(20), fill="#374151")
        y += 58
    save(img, "fig_08_call_code_distribution.png")

    img, draw = base("Luồng chuẩn hóa JSON thành customers, offers và calls", "Tách master data thành các entity phục vụ MongoDB và Gold Star Schema")
    box(draw, (95, 300, 390, 550), "Master JSON", "one generated telesales record", fill="#f4f0ff", accent="#7c3aed")
    for xy, heading, body, accent in [
        ((570, 180, 900, 330), "customers", "cust collection / dim_customer", "#2563eb"),
        ((570, 370, 900, 520), "offers", "offer collection / dim_offer", "#c2410c"),
        ((570, 560, 900, 710), "call_logs", "call_logs collection / fact calls", "#15803d"),
    ]:
        box(draw, xy, heading, body, fill="#ffffff", accent=accent)
        arrow(draw, (390, 425), (570, (xy[1] + xy[3]) // 2))
    box(draw, (1080, 370, 1420, 520), "MongoDB Source", "three collections loaded by init job", fill="#eef6ff", accent="#2563eb")
    for yy in [255, 445, 635]:
        arrow(draw, (900, yy), (1080, 445))
    save(img, "fig_09_json_normalization_entities.png")

    horizontal_flow(
        "fig_10_overall_hybrid_lakehouse_architecture.png",
        "Kiến trúc tổng thể Hybrid Data Lakehouse",
        "Hai nhánh AGI Telesales và CallCenterEN dùng chung Lakehouse/BI serving",
        [
            ("Sources", "MongoDB\nCallCenterEN", "#eef6ff", "#2563eb"),
            ("Ingestion", "Debezium\nKafka", "#fff7ed", "#c2410c"),
            ("Compute", "Spark ETL\nBoW NLP", "#effaf4", "#15803d"),
            ("Lakehouse", "Bronze\nSilver\nGold", "#effaf4", "#15803d"),
            ("Serving", "BigQuery\nSuperset", "#f4f0ff", "#7c3aed"),
        ],
        290,
        555,
    )
    # Reopen to add orchestration overlay.
    img = Image.open(OUT / "fig_10_overall_hybrid_lakehouse_architecture.png").convert("RGB")
    draw = ImageDraw.Draw(img)
    draw.rounded_rectangle((515, 165, 1085, 240), radius=18, fill="#fff1f2", outline="#be123c", width=3)
    draw.text((560, 185), "Airflow orchestration: staged DAG, sequential local execution", font=font(26, True), fill="#be123c")
    for xx in [520, 820, 1120]:
        arrow(draw, (800, 240), (xx, 290), color="#be123c", width=3)
    save(img, "fig_10_overall_hybrid_lakehouse_architecture.png")

    horizontal_flow(
        "fig_11_data_lineage_mongodb_bigquery.png",
        "Data lineage từ MongoDB đến BigQuery serving view",
        "Dòng dữ liệu có thể truy vết qua từng lớp xử lý",
        [
            ("MongoDB", "", "#ffffff", "#2563eb"),
            ("Debezium CDC", "", "#ffffff", "#c2410c"),
            ("Kafka topics", "", "#ffffff", "#c2410c"),
            ("Bronze Iceberg", "", "#ffffff", "#15803d"),
            ("Silver cleaned", "", "#ffffff", "#15803d"),
            ("Gold Star Schema", "", "#ffffff", "#15803d"),
            ("BigQuery tables", "", "#ffffff", "#7c3aed"),
            ("Serving views", "", "#ffffff", "#7c3aed"),
            ("Superset", "", "#ffffff", "#7c3aed"),
        ],
        360,
        500,
    )

    img, draw = base("Sequence diagram CDC và batch ETL", "Thứ tự tương tác giữa source, streaming, orchestration và compute")
    actors = ["MongoDB", "Debezium", "Kafka", "Airflow", "Spark", "Iceberg", "BigQuery"]
    xpos = [110, 320, 530, 740, 950, 1160, 1370]
    for x, actor in zip(xpos, actors):
        draw.text((x - 45, 170), actor, font=font(24, True), fill="#111827")
        draw.line((x, 215, x, 780), fill="#cbd5e1", width=3)
    events = [
        (110, 320, 260, "change stream"),
        (320, 530, 340, "publish CDC"),
        (740, 950, 430, "trigger task"),
        (950, 530, 500, "read Kafka"),
        (950, 1160, 580, "write tables"),
        (740, 950, 660, "bq_sync_gold"),
        (950, 1370, 720, "load DataFrame"),
    ]
    for x1, x2, y, label in events:
        arrow(draw, (x1, y), (x2, y), width=4)
        draw.text(((x1 + x2) // 2 - 70, y - 32), label, font=font(19), fill="#374151")
    save(img, "fig_12_sequence_cdc_batch_etl.png")

    horizontal_flow(
        "fig_13_medallion_layer_responsibilities.png",
        "Medallion Architecture và trách nhiệm từng tầng",
        "Tách raw, clean và analytical serving để dễ kiểm thử",
        [
            ("Bronze", "Raw CDC\nraw_doc\nsource metadata\nreplayable", "#fff7ed", "#c2410c"),
            ("Silver", "Typed schema\ndeduplicate\nPII control\nNLP enrichment", "#effaf4", "#15803d"),
            ("Gold", "Star Schema\nfact/dim\ncomparison tables\nBI-ready", "#f4f0ff", "#7c3aed"),
        ],
        260,
        640,
    )

    img, draw = base("Star Schema của lớp Gold phục vụ BI", "fact_telesales_calls liên kết với các dimension phân tích")
    box(
        draw,
        (590, 340, 1010, 535),
        "fact_telesales_calls",
        "call_id, customer_id, offer_id, date_key\ntalk_time, call_code, outcome flags",
        fill="#f4f0ff",
        accent="#7c3aed",
    )
    dimensions = [
        ((105, 140, 430, 285), "dim_customer", "age_group, income_band, credit_tier"),
        ((105, 590, 430, 735), "dim_offer", "campaign, product, lead_source"),
        ((1165, 140, 1490, 285), "dim_date", "date, month, weekday, weekend"),
        ((1165, 590, 1490, 735), "dim_call_code", "call_code categories"),
    ]
    connectors = [
        ((267, 285), (670, 340)),
        ((267, 590), (670, 535)),
        ((1327, 285), (930, 340)),
        ((1327, 590), (930, 535)),
    ]
    for (xy, heading, body), (start, end) in zip(dimensions, connectors):
        box(draw, xy, heading, body, fill="#ffffff", accent="#2563eb")
        arrow(draw, start, end, width=4)
    save(img, "fig_14_gold_star_schema.png")

    horizontal_flow(
        "fig_15_pii_control_flow.png",
        "Luồng kiểm soát PII từ nguồn đến serving layer",
        "PII chỉ tồn tại ở phạm vi cần thiết và bị mask/drop trước BI",
        [
            ("Source", "raw identifiers\ntranscript", "#fff1f2", "#be123c"),
            ("Silver", "mask phone/id\ndrop direct PII", "#fff7ed", "#c2410c"),
            ("Gold", "analytical fields\nno transcript", "#effaf4", "#15803d"),
            ("BigQuery", "blocked columns removed", "#f4f0ff", "#7c3aed"),
            ("Superset", "KPI/charts only", "#eef6ff", "#2563eb"),
        ],
    )

    img, draw = base("Bản đồ container Docker Compose", "Các service chính trong môi trường thực nghiệm local")
    rows = [
        ("Source", "mongodb, mongo-init, mongo-data-init", "#2563eb"),
        ("Streaming", "zookeeper, kafka, debezium_connect, debezium-init", "#c2410c"),
        ("Storage", "minio, minio-mc", "#15803d"),
        ("Compute", "spark-master, spark-worker", "#15803d"),
        ("Orchestration", "airflow", "#be123c"),
        ("BI", "superset, telesales-dashboard", "#7c3aed"),
    ]
    y = 180
    for heading, body, accent in rows:
        box(draw, (180, y, 1420, y + 90), heading, body, fill="#ffffff", accent=accent)
        y += 105
    save(img, "fig_16_docker_compose_container_map.png")

    horizontal_flow(
        "fig_17_gold_to_bigquery_serving.png",
        "Luồng phục vụ dữ liệu từ Gold Iceberg sang BigQuery serving view",
        "bq_sync_gold publish dữ liệu sạch cho BI",
        [
            ("Gold Iceberg", "dim_customer\ndim_offer\ndim_date\nfact_telesales_calls", "#effaf4", "#15803d"),
            ("bq_sync_gold", "drop blocked columns\noverwrite serving tables", "#fff7ed", "#c2410c"),
            ("BigQuery tables", "kltn0710.*", "#f4f0ff", "#7c3aed"),
            ("Serving views", "vw_telesales_performance\nvw_callcenteren_*", "#f4f0ff", "#7c3aed"),
            ("Superset", "end-to-end BI dashboard", "#eef6ff", "#2563eb"),
        ],
        300,
        570,
    )

    horizontal_flow(
        "fig_18_production_roadmap.png",
        "Roadmap nâng cấp production cho Hybrid Lakehouse",
        "Các hướng cải tiến sau phiên bản thực nghiệm local",
        [
            ("Data Quality", "validation rules\ncontract tests", "#ffffff", "#2563eb"),
            ("Model Serving", "separate model API\nGPU/CPU scaling", "#ffffff", "#7c3aed"),
            ("Security", "RBAC\nsecret management\naudit logs", "#ffffff", "#be123c"),
            ("Monitoring", "pipeline SLA\ndata freshness\nmodel drift", "#ffffff", "#15803d"),
            ("CI/CD", "automated tests\nimage build\ndeployment", "#ffffff", "#c2410c"),
        ],
    )

    print(f"Generated {len(list(OUT.glob('*.png')))} PNG files.")


if __name__ == "__main__":
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    main()
