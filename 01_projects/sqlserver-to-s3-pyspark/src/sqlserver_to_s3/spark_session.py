"""
spark_session.py

Creates and returns a reusable Apache Spark session.

This module centralizes all Spark configuration so the rest of
the ETL application can simply request a Spark session without
duplicating setup code.

For Windows, Spark sometimes searches for "python3", which does
not exist by default. We explicitly tell Spark to use the same
Python interpreter that is running this application.
"""

import os
import sys

from pyspark.sql import SparkSession


def create_spark_session(
    app_name: str,
    master: str,
    jdbc_driver: str
) -> SparkSession:
    """
    Create a reusable SparkSession.

    Parameters
    ----------
    app_name : str
        Name displayed in the Spark UI.

    master : str
        Spark execution mode.
        local[*] = Use all available CPU cores.

    jdbc_driver : str
        Path to the Microsoft SQL Server JDBC driver.

    Returns
    -------
    SparkSession
        Configured Spark session.
    """

    # ---------------------------------------------------------
    # Tell Spark which Python executable to use.
    #
    # This prevents the common Windows error where Spark
    # searches for "python3" instead of the active Python
    # interpreter.
    # ---------------------------------------------------------
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    # ---------------------------------------------------------
    # Create the Spark session.
    #
    # The SparkSession is the entry point to every PySpark
    # application.
    #
    # The SQL Server JDBC driver is loaded from dev.yaml so
    # Spark can connect to SQL Server without hardcoding the
    # driver location in the application code.
    #
    # Additional Spark configuration (memory, executors,
    # Amazon EMR settings, AWS Glue settings, etc.) can be
    # added here in the future.
    # ---------------------------------------------------------
    spark = (
        SparkSession.builder
        .master(master)
        .appName(app_name)

        # SQL Server JDBC Driver
        .config(
            "spark.jars",
            jdbc_driver
        )
        .getOrCreate()
    )



    # ---------------------------------------------------------
    # Reduce Spark logging.
    #
    # WARN displays only warnings and errors, making the
    # console output easier to read during development.
    # ---------------------------------------------------------
    spark.sparkContext.setLogLevel("WARN")

    # ---------------------------------------------------------
    # Return the configured SparkSession.
    #
    # The same SparkSession can be reused throughout the ETL
    # pipeline instead of creating multiple sessions.
    # ---------------------------------------------------------

    return spark

# =====================================================================
# Interview Notes
# =====================================================================
#
# Question:
#
# Why did you create spark_session.py instead of putting
# everything inside main.py?
#
# Suggested Answer:
#
# I separated Spark session creation into its own module to
# improve maintainability and code reuse.
#
# Any future ETL job, unit test, or pipeline can simply import
# create_spark_session() without duplicating Spark
# configuration.
#
# Centralizing Spark configuration also makes future changes
# easier, such as:
#
# • Executor memory
# • Driver memory
# • Logging level
# • JDBC drivers
# • AWS Glue configuration
# • Amazon EMR configuration
#
# This follows the Single Responsibility Principle (SRP),
# making the code easier to maintain, test, and extend.
#
# =====================================================================