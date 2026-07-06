"""
load.py

Load the transformed Spark DataFrame into the target destination.

Current Target
--------------
- Local Parquet files

Future Targets
--------------
- Amazon S3
- AWS Glue Catalog
- Amazon Redshift
- Delta Lake
- Apache Iceberg

This module is responsible only for loading data.
No extraction or transformations are performed here.
"""

from pathlib import Path

from pyspark.sql import DataFrame


def load_data(
    dataframe: DataFrame,
    config: dict
) -> None:
    """
    Write the transformed Spark DataFrame to Parquet.

    Parameters
    ----------
    dataframe : DataFrame
        Transformed Spark DataFrame.

    config : dict
        Project configuration loaded from dev.yaml.
    """

    print("=" * 60)
    print("Starting Load Phase...")
    print("=" * 60)

    # ---------------------------------------------------------
    # Read output configuration.
    # ---------------------------------------------------------
    output_path = config["output"]["local_parquet_path"]

    # ---------------------------------------------------------
    # Create the output directory if it does not already exist.
    # ---------------------------------------------------------
    Path(output_path).parent.mkdir(
        parents=True,
        exist_ok=True
    )

    print(f"Writing Parquet files to:\n{output_path}")

    # ---------------------------------------------------------
    # Write the DataFrame as Parquet.
    #
    # overwrite
    #     Replace previous output during development.
    #
    # Snappy
    #     Default Parquet compression.
    # ---------------------------------------------------------

    dataframe = dataframe.coalesce(4)

    (
        dataframe.write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(output_path)
    )

    print("Parquet files successfully created.")

    print("=" * 60)
    print("Load Phase Completed Successfully")
    print("=" * 60)


# =====================================================================
# Interview Notes
# =====================================================================
#
# Question:
#
# Why did you create load.py instead of writing the
# DataFrame directly inside main.py?
#
# Suggested Answer:
#
# I wanted the ETL pipeline to follow a modular architecture
# where each phase has a single responsibility.
#
# load.py is responsible only for writing data to the
# destination.
#
# Today the destination is Parquet.
#
# Tomorrow the same ETL pipeline could support:
#
# • Amazon S3
# • Azure Data Lake
# • Google Cloud Storage
# • Delta Lake
# • Apache Iceberg
# • Snowflake
#
# The rest of the ETL pipeline would remain unchanged.
#
# This improves maintainability, extensibility,
# and follows the Single Responsibility Principle.
#
# =====================================================================