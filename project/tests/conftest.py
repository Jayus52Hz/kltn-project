"""
conftest.py — shared pytest fixtures for all test modules.
SparkSession is session-scoped: created once, reused across all tests.
"""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("telesales-tests")
        # Small shuffle partitions for unit tests
        .config("spark.sql.shuffle.partitions", "4")
        # Disable Spark UI to avoid port conflicts in CI
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()
