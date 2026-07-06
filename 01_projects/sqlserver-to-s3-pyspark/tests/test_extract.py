# ============================================================
# TEST EXTRACT MODULE (test_extract.py)
# ============================================================
# PURPOSE:
# Test the Extract phase of the PySpark ETL pipeline.
#
# WHAT IS TESTED:
# - Spark can call extract_data()
# - SQL Server data is returned as a Spark DataFrame
# - DataFrame has rows
# - Expected columns exist
#
# NOTE:
# This is an integration test because it connects to SQL Server.
# ============================================================

import os
from pathlib import Path

import pytest
from pyspark.sql import DataFrame

from sqlserver_to_s3.config import load_config
from sqlserver_to_s3.spark_session import create_spark_session
from sqlserver_to_s3.extract import extract_data


CONFIG_FILE = (
    Path(__file__).resolve().parents[1]
    / "configs"
    / "dev.yaml"
)


pytestmark = pytest.mark.skipif(
    not os.getenv("SQLSERVER_PASSWORD"),
    reason="SQLSERVER_PASSWORD not set"
)


def get_spark_and_config():
    cfg = load_config(CONFIG_FILE)

    spark = create_spark_session(
        app_name=cfg["spark"]["app_name"],
        master=cfg["spark"]["master"],
        jdbc_driver=cfg["spark"]["jdbc_driver"],
    )

    return spark, cfg


def test_extract_data_returns_dataframe():
    spark, cfg = get_spark_and_config()

    try:
        df = extract_data(
            spark=spark,
            config=cfg
        )

        assert isinstance(df, DataFrame)

    finally:
        spark.stop()


def test_extract_data_has_rows():
    spark, cfg = get_spark_and_config()

    try:
        df = extract_data(
            spark=spark,
            config=cfg
        )

        assert df.count() > 0

    finally:
        spark.stop()


def test_extract_data_has_expected_columns():
    spark, cfg = get_spark_and_config()

    try:
        df = extract_data(
            spark=spark,
            config=cfg
        )

        expected_columns = [
            "athlete_sk"
        ]

        for column in expected_columns:
            assert column in df.columns

    finally:
        spark.stop()


# ============================================================
# INTERVIEW QUESTION & ANSWER
# ============================================================
#
# Q:
# Why is test_extract.py considered an integration test?
#
# A:
# Because it connects to an external system: SQL Server.
# Unlike a pure unit test, this test validates that Spark,
# JDBC, SQL Server credentials, and the source table all work
# together successfully.
# ============================================================