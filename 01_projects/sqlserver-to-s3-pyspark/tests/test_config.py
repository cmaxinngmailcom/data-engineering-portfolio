# ============================================================
# TEST CONFIG MODULE (test_config.py)
# ============================================================
# PURPOSE:
# Test the configuration module.
#
# FIRST GOAL:
# Verify that config.py correctly loads dev.yaml.
#
# WHAT IS TESTED:
# - Configuration file loads successfully
# - Spark configuration
# - SQL Server configuration
# - Output configuration
#
# WHY THIS TEST?
# If the configuration cannot be loaded correctly,
# the ETL pipeline cannot start.
#
# ============================================================

from pathlib import Path

from sqlserver_to_s3.config import load_config


# ============================================================
# CONFIG FILE LOCATION
# ============================================================

CONFIG_FILE = (
    Path(__file__).resolve().parents[1]
    / "configs"
    / "dev.yaml"
)


# ============================================================
# TEST: LOAD CONFIGURATION
# ============================================================

def test_load_config():
    """
    Verify the YAML configuration loads successfully.
    """

    cfg = load_config(CONFIG_FILE)

    assert isinstance(cfg, dict)


# ============================================================
# TEST: SPARK CONFIGURATION
# ============================================================

def test_spark_config():
    """
    Verify Spark configuration values.
    """

    cfg = load_config(CONFIG_FILE)

    assert cfg["spark"]["app_name"] == \
        "SQLServer to S3 PySpark Pipeline"

    assert cfg["spark"]["master"] == "local[*]"


# ============================================================
# TEST: SQL SERVER CONFIGURATION
# ============================================================

def test_sqlserver_config():
    """
    Verify SQL Server configuration values.
    """

    cfg = load_config(CONFIG_FILE)

    assert cfg["sqlserver"]["database"] == \
        "AdventureWorksDW2014"

    assert cfg["sqlserver"]["table"] == \
        "dbo.Athletes_Big"


# ============================================================
# TEST: OUTPUT CONFIGURATION
# ============================================================

def test_output_config():
    """
    Verify output configuration values.
    """

    cfg = load_config(CONFIG_FILE)

    assert cfg["output"]["local_parquet_path"] == \
        "output/parquet/athletes_big"


# ============================================================
# INTERVIEW QUESTION & ANSWER
# ============================================================
#
# Q:
# Why unit test the configuration module?
#
# A:
#
# Configuration files are the entry point of an ETL pipeline.
# If configuration values are missing or incorrect, the
# pipeline may fail before any data is processed.
#
# Testing config.py verifies that:
#
# • The YAML file loads correctly
# • Required configuration sections exist
# • Expected values are returned
#
# Detecting configuration problems early makes the pipeline
# more reliable and easier to maintain.
#
# ============================================================