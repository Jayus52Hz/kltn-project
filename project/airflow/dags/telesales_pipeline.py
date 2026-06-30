"""
telesales_pipeline.py
=====================
Airflow DAG for the Telesales Lakehouse pipeline.

The DAG is split by dataset and stage so a manual trigger no longer appears as
one coarse Spark task. Each primary dataset has Bronze and Silver stages, while
Gold tables and the CallCenterEN branch are exposed as separate stage tasks.
"""

from datetime import datetime, timedelta

import requests

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.task_group import TaskGroup


SPARK_PACKAGES = ",".join([
    "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.0",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.261",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0",
])

ICEBERG_PACKAGES = ",".join([
    "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.0",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.261",
])

ICEBERG_BQ_PACKAGES = ",".join([
    "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.0",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.261",
    "com.google.cloud.spark:spark-3.4-bigquery:0.44.2",
])

SPARK_CONF = {
    "spark.pyspark.python": "python3.9",
    "spark.executorEnv.PYSPARK_PYTHON": "/usr/bin/python3.9",
    "spark.cores.max": "1",
    "spark.executor.cores": "1",
    "spark.executor.instances": "1",
    "spark.default.parallelism": "1",
    "spark.sql.shuffle.partitions": "1",
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.lakehouse": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.lakehouse.type": "hadoop",
    "spark.sql.catalog.lakehouse.warehouse": "s3a://lakehouse/warehouse",
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key": "minioadmin",
    "spark.hadoop.fs.s3a.secret.key": "minioadmin",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.aws.credentials.provider": (
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    ),
}

BASE_ENV = {
    "MINIO_ENDPOINT": "http://minio:9000",
    "MINIO_ACCESS_KEY": "minioadmin",
    "MINIO_SECRET_KEY": "minioadmin",
}

WORK_DIR = "/opt/spark/work-dir/batch-etl"
GCP_CREDENTIALS_FILE = "/opt/gcp/application_default_credentials.json"
CALLCENTEREN_OUTPUT_DIR = "/opt/spark/work-dir/callcenteren-output/callcenteren_finetuned_max4"
CALLCENTEREN_SCHEMA_CSV = f"{CALLCENTEREN_OUTPUT_DIR}/callcenteren_15k_with_model_callcodes.csv"
MODEL_METRICS_CSV = f"{CALLCENTEREN_OUTPUT_DIR}/callcenteren_finetune_metrics.csv"
CALLCENTEREN_MODEL_PATH = f"{CALLCENTEREN_OUTPUT_DIR}/callcenteren_best_finetuned_model.pkl"
BQ_PROJECT_ID = "project-ef0c6db5-0765-4391-845"
BQ_DATASET = "kltn0710"
BQ_SYNC_COMMAND = " ".join([
    "spark-submit",
    "--master 'local[1]'",
    f"--packages {ICEBERG_BQ_PACKAGES}",
    "--conf spark.default.parallelism=1",
    "--conf spark.sql.shuffle.partitions=1",
    "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "--conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog",
    "--conf spark.sql.catalog.lakehouse.type=hadoop",
    "--conf spark.sql.catalog.lakehouse.warehouse=s3a://lakehouse/warehouse",
    "--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000",
    "--conf spark.hadoop.fs.s3a.access.key=minioadmin",
    "--conf spark.hadoop.fs.s3a.secret.key=minioadmin",
    "--conf spark.hadoop.fs.s3a.path.style.access=true",
    "--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
    "--conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    f"{WORK_DIR}/bq_sync_job.py",
])

PRIMARY_DATASETS = {
    "cust": {
        "bronze_collection": "cust",
        "silver_entity": "cust",
        "gold_entity": "dim_customer",
    },
    "offer": {
        "bronze_collection": "offer",
        "silver_entity": "offer",
        "gold_entity": "dim_offer",
    },
    "call_logs": {
        "bronze_collection": "call_logs",
        "silver_entity": "call_logs",
        "gold_entities": ["dim_date", "fact_telesales_calls"],
    },
}

default_args = {
    "owner": "thinh-nguyen-ts",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def _debezium_connector_ready():
    """Return True once the mongo-source connector is registered and reachable."""
    try:
        response = requests.get(
            "http://debezium_connect:8083/connectors/mongo-source",
            timeout=5,
        )
        return response.status_code == 200
    except Exception:
        return False


def spark_task(
    *,
    task_id: str,
    application: str,
    packages: str,
    env_vars: dict[str, str],
    name: str,
    execution_timeout: timedelta,
) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id=task_id,
        conn_id="spark_default",
        application=application,
        packages=packages,
        conf=SPARK_CONF,
        env_vars=env_vars,
        name=name,
        execution_timeout=execution_timeout,
    )


def primary_dataset_group(dataset_name: str, config: dict[str, str]) -> dict[str, SparkSubmitOperator]:
    with TaskGroup(group_id=f"{dataset_name}_dataset") as group:
        bronze = spark_task(
            task_id="bronze",
            application=f"{WORK_DIR}/bronze_job.py",
            packages=SPARK_PACKAGES,
            env_vars={
                **BASE_ENV,
                "KAFKA_BOOTSTRAP_SERVERS": "kafka:29092",
                "CHECKPOINT_BASE": "s3a://bronze/_checkpoints",
                "TRIGGER_ONCE": "true",
                "BRONZE_COLLECTIONS": config["bronze_collection"],
            },
            name=f"bronze_{dataset_name}",
            execution_timeout=timedelta(hours=2),
        )

        silver = spark_task(
            task_id="silver",
            application=f"{WORK_DIR}/silver_job.py",
            packages=ICEBERG_PACKAGES,
            env_vars={
                **BASE_ENV,
                "MODELS_PATH": "/opt/spark/work-dir/batch-etl/models",
                "NLP_MODEL_TYPE": "bow",
                "SILVER_ENTITIES": config["silver_entity"],
            },
            name=f"silver_{dataset_name}",
            execution_timeout=timedelta(hours=2),
        )

        bronze >> silver

    return {"group": group, "bronze": bronze, "silver": silver}


def gold_task(task_id: str, gold_entity: str) -> SparkSubmitOperator:
    return spark_task(
        task_id=task_id,
        application=f"{WORK_DIR}/gold_job.py",
        packages=ICEBERG_PACKAGES,
        env_vars={
            **BASE_ENV,
            "GOLD_ENTITIES": gold_entity,
        },
        name=task_id,
        execution_timeout=timedelta(hours=1),
    )


def outcome_script_task() -> SparkSubmitOperator:
    return spark_task(
        task_id="customer_outcome_scripts",
        application=f"{WORK_DIR}/outcome_script_job.py",
        packages=ICEBERG_PACKAGES,
        env_vars={
            **BASE_ENV,
            "OUTCOME_SCRIPT_VERSION": "v1",
        },
        name="customer_outcome_scripts",
        execution_timeout=timedelta(hours=1),
    )


def callcenteren_stage_task(stage: str) -> SparkSubmitOperator:
    return spark_task(
        task_id=stage,
        application=f"{WORK_DIR}/callcenteren_external_job.py",
        packages=ICEBERG_PACKAGES,
        env_vars={
            **BASE_ENV,
            "CALLCENTEREN_STAGE": stage,
            "CALLCENTEREN_SCHEMA_CSV": CALLCENTEREN_SCHEMA_CSV,
            "MODEL_METRICS_CSV": MODEL_METRICS_CSV,
            "CALLCENTEREN_MODEL_PATH": CALLCENTEREN_MODEL_PATH,
        },
        name=f"callcenteren_{stage}",
        execution_timeout=timedelta(hours=1),
    )


with DAG(
    dag_id="telesales_lakehouse_pipeline",
    description="Dataset-staged Bronze/Silver/Gold ETL for Telesales Lakehouse",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 2 * * *",
    catchup=False,
    max_active_tasks=1,
    max_active_runs=1,
    tags=["lakehouse", "telesales", "iceberg"],
) as dag:
    wait_for_debezium = PythonSensor(
        task_id="wait_for_debezium_connector",
        python_callable=_debezium_connector_ready,
        poke_interval=15,
        timeout=300,
        mode="reschedule",
    )

    with TaskGroup(group_id="primary_telesales") as primary_telesales:
        primary_tasks = {
            dataset_name: primary_dataset_group(dataset_name, config)
            for dataset_name, config in PRIMARY_DATASETS.items()
        }

        with TaskGroup(group_id="gold") as primary_gold:
            dim_customer = gold_task("dim_customer", "dim_customer")
            dim_offer = gold_task("dim_offer", "dim_offer")
            dim_date = gold_task("dim_date", "dim_date")
            fact_telesales_calls = gold_task("fact_telesales_calls", "fact_telesales_calls")
            customer_outcome_scripts = outcome_script_task()

        primary_tasks["cust"]["silver"] >> dim_customer
        primary_tasks["offer"]["silver"] >> dim_offer
        primary_tasks["call_logs"]["silver"] >> dim_date
        dim_customer >> dim_offer >> dim_date >> fact_telesales_calls >> customer_outcome_scripts
        primary_tasks["cust"]["group"] >> primary_tasks["offer"]["group"]
        primary_tasks["offer"]["group"] >> primary_tasks["call_logs"]["group"]
        primary_tasks["call_logs"]["group"] >> primary_gold

    with TaskGroup(group_id="callcenteren_external") as callcenteren_external:
        callcenteren_bronze = callcenteren_stage_task("bronze")
        callcenteren_silver = callcenteren_stage_task("silver")
        callcenteren_gold = callcenteren_stage_task("gold")

        callcenteren_bronze >> callcenteren_silver >> callcenteren_gold

    bq_sync_gold = BashOperator(
        task_id="bq_sync_gold",
        bash_command=BQ_SYNC_COMMAND,
        env={
            **BASE_ENV,
            "GOOGLE_APPLICATION_CREDENTIALS": GCP_CREDENTIALS_FILE,
            "BQ_PROJECT_ID": BQ_PROJECT_ID,
            "BQ_DATASET": BQ_DATASET,
            "BQ_WRITE_METHOD": "dataframe",
        },
        append_env=True,
        execution_timeout=timedelta(hours=1),
    )

    wait_for_debezium >> primary_telesales
    primary_telesales >> callcenteren_external
    [customer_outcome_scripts, callcenteren_gold] >> bq_sync_gold
