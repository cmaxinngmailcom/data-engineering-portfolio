"""
main.py

Main orchestration file for the SQL Server to Parquet PySpark pipeline.

This module coordinates the full ETL flow:

1. Load configuration
2. Create Spark session
3. Extract data from SQL Server
4. Transform data
5. Load data to Parquet
6. Stop Spark session
"""

from sqlserver_to_s3.config import load_config
from sqlserver_to_s3.spark_session import create_spark_session
from sqlserver_to_s3.extract import extract_data
from sqlserver_to_s3.transform import transform_data
from sqlserver_to_s3.load import load_data
from sqlserver_to_s3.logger import get_logger


logger = get_logger()


def main() -> None:
    """
    Run the complete ETL pipeline.
    """

    spark = None

    try:
        logger.info("=" * 60)
        logger.info("Starting SQL Server to Parquet PySpark Pipeline")
        logger.info("=" * 60)

        # -----------------------------------------------------
        # 1. Load configuration from dev.yaml.
        # -----------------------------------------------------
        config = load_config("configs/dev.yaml")

        # -----------------------------------------------------
        # 2. Create Spark session.
        #
        # create_spark_session() is defined in spark_session.py.
        # It uses values from dev.yaml to configure Spark.
        #
        # main.py calls the function and receives a reusable
        # SparkSession for the rest of the ETL pipeline.
        # -----------------------------------------------------
        spark_config = config["spark"]

        spark = create_spark_session(
            app_name=spark_config["app_name"],
            master=spark_config["master"],
            jdbc_driver=spark_config["jdbc_driver"],
        )

        

        # -----------------------------------------------------
        # 3. Extract data from SQL Server.
        # -----------------------------------------------------
        source_df = extract_data(
            spark=spark,
            config=config,
        )

        logger.info("Source schema:")
        source_df.printSchema()

        logger.info("Source sample records:")
        source_df.show(10, truncate=False)

        # -----------------------------------------------------
        # 4. Transform data.
        # -----------------------------------------------------
        transformed_df = transform_data(source_df)

        # -----------------------------------------------------
        # 5. Load data to local Parquet.
        # -----------------------------------------------------
        load_data(
            dataframe=transformed_df,
            config=config,
        )

        logger.info("=" * 60)
        logger.info("Pipeline completed successfully.")
        logger.info("=" * 60)

    except Exception:
        logger.exception("Pipeline failed.")
        raise

    finally:
        # -----------------------------------------------------
        # Always stop Spark, even if the pipeline fails.
        # -----------------------------------------------------
        if spark is not None:
            spark.stop()
            logger.info("Spark stopped successfully.")


if __name__ == "__main__":
    main()


# =====================================================================
# Interview Notes
# =====================================================================
#
# Question:
#
# Why did you keep main.py focused on orchestration?
#
# Suggested Answer:
#
# main.py should coordinate the pipeline, not contain all the
# business logic.
#
# The Extract, Transform, and Load logic is separated into
# dedicated modules:
#
# extract.py
# transform.py
# load.py
#
# This keeps the pipeline easier to read, test, maintain,
# and extend.
#
# If the source changes from SQL Server to Oracle, or if the
# destination changes from local Parquet to Amazon S3, main.py
# can remain mostly unchanged.
#
# This approach follows enterprise ETL design principles and
# the Single Responsibility Principle.
#
# =====================================================================