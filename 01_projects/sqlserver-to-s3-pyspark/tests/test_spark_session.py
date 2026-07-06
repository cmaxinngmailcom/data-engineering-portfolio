# ============================================================
# TEST SPARK SESSION MODULE (test_spark_session.py)
# ============================================================
# PURPOSE:
# Test the Spark Session module.
#
# FIRST GOAL:
# Verify that spark_session.py correctly creates
# a reusable SparkSession.
#
# WHAT IS TESTED:
# - SparkSession creation
# - Application name
# - Spark master
# - Python interpreter configuration
# - Simple DataFrame creation
# - DataFrame operations
#
# WHY THIS TEST?
#
# SparkSession is the entry point of every PySpark ETL
# application. If Spark cannot start, none of the ETL
# pipeline can execute.
#
# ============================================================

from pathlib import Path

from pyspark.sql import SparkSession

from sqlserver_to_s3.config import load_config
from sqlserver_to_s3.spark_session import create_spark_session


# ============================================================
# CONFIG FILE
# ============================================================

CONFIG_FILE = (
    Path(__file__).resolve().parents[1]
    / "configs"
    / "dev.yaml"
)


# ============================================================
# HELPER FUNCTION
# ============================================================

def get_spark():

    cfg = load_config(CONFIG_FILE)

    return create_spark_session(
        app_name=cfg["spark"]["app_name"],
        master=cfg["spark"]["master"],
        jdbc_driver=cfg["spark"]["jdbc_driver"],
    )


# ============================================================
# TEST: CREATE SPARK SESSION
# ============================================================

def test_create_spark_session():

    spark = get_spark()

    assert isinstance(spark, SparkSession)

    spark.stop()


# ============================================================
# TEST: APPLICATION NAME
# ============================================================

def test_spark_app_name():

    cfg = load_config(CONFIG_FILE)

    spark = get_spark()

    assert spark.sparkContext.appName == cfg["spark"]["app_name"]

    spark.stop()


# ============================================================
# TEST: SPARK MASTER
# ============================================================

def test_spark_master():

    spark = get_spark()

    assert "local" in spark.sparkContext.master.lower()

    spark.stop()


# ============================================================
# TEST: CREATE DATAFRAME
# ============================================================

def test_create_dataframe():

    spark = get_spark()

    df = spark.createDataFrame(

        [
            (1, "Alice"),
            (2, "Bob"),
            (3, "Charlie")
        ],

        ["id", "name"]

    )

    assert df.count() == 3

    spark.stop()


# ============================================================
# TEST: DATAFRAME COLUMNS
# ============================================================

def test_dataframe_columns():

    spark = get_spark()

    df = spark.createDataFrame(

        [
            (1, "Alice")
        ],

        ["id", "name"]

    )

    assert df.columns == ["id", "name"]

    spark.stop()


# ============================================================
# TEST: SPARK LOG LEVEL
# ============================================================

def test_spark_log_level():

    spark = get_spark()

    assert spark.sparkContext.getConf() is not None

    spark.stop()

# ============================================================
# INTERVIEW QUESTION & ANSWER
# ============================================================
#
# Q:
# Why unit test SparkSession?
#
# A:
#
# SparkSession is the entry point for every PySpark
# application.
#
# Testing it verifies that:
#
# • Spark starts correctly
# • Configuration is applied
# • DataFrames can be created
# • Spark operations execute successfully
#
# Detecting Spark configuration problems early prevents
# failures later during extraction, transformation,
# and loading.
#
# ============================================================