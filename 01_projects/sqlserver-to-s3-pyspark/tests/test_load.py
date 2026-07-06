"""
test_load.py

Unit tests for load.py.

These tests verify that the Load phase correctly writes
Parquet files to the target location.
"""

from pathlib import Path

from sqlserver_to_s3.load import load_data


# ============================================================
# TEST: PARQUET FILES ARE CREATED
# ============================================================

def test_load_creates_parquet_files(spark, tmp_path):
    """
    Verify that Parquet files are written.
    """

    output_path = tmp_path / "parquet_output"

    config = {
        "output": {
            "local_parquet_path": str(output_path)
        }
    }

    dataframe = spark.createDataFrame(

        [
            (1, "Alice"),
            (2, "Bob")
        ],

        ["id", "name"]

    )

    load_data(
        dataframe=dataframe,
        config=config
    )

    assert output_path.exists()


# ============================================================
# TEST: READ BACK PARQUET
# ============================================================

def test_read_written_parquet(spark, tmp_path):
    """
    Verify that the written Parquet can be read.
    """

    output_path = tmp_path / "parquet_output"

    config = {
        "output": {
            "local_parquet_path": str(output_path)
        }
    }

    dataframe = spark.createDataFrame(

        [
            (1, "Alice"),
            (2, "Bob")
        ],

        ["id", "name"]

    )

    load_data(
        dataframe=dataframe,
        config=config
    )

    parquet_df = spark.read.parquet(str(output_path))

    assert parquet_df.count() == 2
    assert parquet_df.columns == ["id", "name"]


# ============================================================
# TEST: DATA INTEGRITY
# ============================================================

def test_loaded_data_matches_source(spark, tmp_path):
    """
    Verify that the loaded data matches the source data.
    """

    output_path = tmp_path / "parquet_output"

    config = {
        "output": {
            "local_parquet_path": str(output_path)
        }
    }

    source_df = spark.createDataFrame(

        [
            (1, "Alice"),
            (2, "Bob"),
            (3, "Charlie")
        ],

        ["id", "name"]

    )

    load_data(
        dataframe=source_df,
        config=config
    )

    loaded_df = spark.read.parquet(str(output_path))

    assert loaded_df.count() == source_df.count()

    loaded_rows = loaded_df.orderBy("id").collect()
    source_rows = source_df.orderBy("id").collect()

    assert loaded_rows == source_rows


# ============================================================
# INTERVIEW QUESTION & ANSWER
# ============================================================
#
# Q:
# Why unit test the Load phase?
#
# A:
#
# The Load phase is responsible for persisting transformed
# data into the target storage.
#
# These tests verify:
#
# • Parquet files are created
# • Files can be read back
# • No data is lost during writing
#
# This provides confidence that downstream analytics
# systems will consume complete and accurate datasets.
#
# ============================================================