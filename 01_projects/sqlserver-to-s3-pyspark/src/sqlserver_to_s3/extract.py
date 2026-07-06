"""
extract.py

Extract data from source systems.

This module coordinates the extraction phase of the ETL pipeline.

Responsibilities

- Call the SQL Server reader.
- Return a Spark DataFrame.
- Log extraction progress.

No transformations are performed here.
"""

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

from sqlserver_to_s3.sqlserver_reader import read_sqlserver_table


def extract_data(
    spark: SparkSession,
    config: dict
) -> DataFrame:
    """
    Execute the Extract phase.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.

    config : dict
        Project configuration.

    Returns
    -------
    DataFrame
        Spark DataFrame containing source data.
    """

    print("=" * 60)
    print("Starting Extract Phase...")
    print("=" * 60)

    # ---------------------------------------------------------
    # Read SQL Server table into Spark.
    # ---------------------------------------------------------
    dataframe = read_sqlserver_table(
        spark=spark,
        config=config
    )

    print("Extract completed successfully.")

    print("=" * 60)

    return dataframe


# =====================================================================
# Interview Notes
# =====================================================================
#
# Question:
#
# Why create extract.py when sqlserver_reader.py already
# reads SQL Server?
#
# Suggested Answer:
#
# sqlserver_reader.py has one responsibility:
#
#     Read data from SQL Server.
#
# extract.py represents the Extract layer of the ETL
# architecture.
#
# Today it simply calls sqlserver_reader.py.
#
# Tomorrow it could:
#
# • Read multiple tables
# • Read multiple databases
# • Read Oracle
# • Read PostgreSQL
# • Read CSV files
# • Read APIs
#
# main.py does not need to know where the data comes from.
#
# It simply calls:
#
#     extract_data()
#
# This keeps the ETL pipeline modular and follows the
# Single Responsibility Principle.
#
# =====================================================================