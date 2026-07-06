"""
sqlserver_reader.py

Reads data from Microsoft SQL Server using Apache Spark JDBC.

This module is responsible for:

1. Building the SQL Server JDBC connection.
2. Reading a SQL Server table into a Spark DataFrame.
3. Returning the DataFrame to the ETL pipeline.

No transformations are performed here.
This module is responsible only for data extraction.
"""
import os
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


def read_sqlserver_table(
    spark: SparkSession,
    config: dict
) -> DataFrame:
    """
    Read a SQL Server table into a Spark DataFrame.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.

    config : dict
        Configuration loaded from dev.yaml.

    Returns
    -------
    DataFrame
        Spark DataFrame containing SQL Server data.
    """

    # ---------------------------------------------------------
    # Read SQL Server configuration.
    # ---------------------------------------------------------
    sql_config = config["sqlserver"]

    server = sql_config["server"]
    port = sql_config["port"]
    database = sql_config["database"]
    table = sql_config["table"]
    user = sql_config["user"]
    password_env_var = sql_config["password_env_var"]
    password = os.getenv(password_env_var)
    if not password:
        raise RuntimeError(f"Missing environment variable: {password_env_var}")
    driver = sql_config["driver"]

    partition_column = sql_config["partition_column"]
    lower_bound = sql_config["lower_bound"]
    upper_bound = sql_config["upper_bound"]
    num_partitions = sql_config["num_partitions"]
    fetch_size = sql_config["fetch_size"]

    # ---------------------------------------------------------
    # Build JDBC connection string.
    # ---------------------------------------------------------
    jdbc_url = (
        f"jdbc:sqlserver://{server}:{port};"
        f"databaseName={database};"
        "encrypt=true;"
        "trustServerCertificate=true;"
    )

    print("=" * 60)
    print("Connecting to SQL Server...")
    print(f"Server   : {server}")
    print(f"Database : {database}")
    print(f"Table    : {table}")
    print("=" * 60)

    # ---------------------------------------------------------
    # Read SQL Server table.
    # ---------------------------------------------------------
    dataframe = (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table)
        .option("user", user)
        .option("password", password)
        .option("driver", driver)

        # Parallel JDBC extraction
        .option("partitionColumn", partition_column)
        .option("lowerBound", lower_bound)
        .option("upperBound", upper_bound)
        .option("numPartitions", num_partitions)
        .option("fetchsize", fetch_size)

        .load()
    )

    print("SQL Server table successfully loaded into Spark.")

    return dataframe


# =====================================================================
# Interview Notes
# =====================================================================
#
# Question:
#
# Why did you create sqlserver_reader.py instead of placing
# the JDBC code inside main.py?
#
# Suggested Answer:
#
# I separated SQL Server access into its own module to follow
# the Single Responsibility Principle.
#
# The responsibility of this module is only to connect to
# SQL Server and return a Spark DataFrame.
#
# This makes the code easier to:
#
# • Test
# • Reuse
# • Maintain
# • Extend
#
# For example, in the future I can create:
#
# oracle_reader.py
# postgres_reader.py
# mysql_reader.py
#
# without changing the rest of the ETL pipeline.
#
# main.py simply orchestrates the pipeline and does not need
# to know how each database connection is implemented.
#
# This modular approach follows enterprise software
# engineering best practices.
#
# =====================================================================