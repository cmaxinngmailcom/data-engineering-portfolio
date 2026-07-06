"""
transform.py

Apply transformations to the extracted Spark DataFrame.

This module is responsible for preparing the data before it is
written to the target data lake.

Typical transformations include:

- Trimming leading and trailing spaces
- Renaming columns
- Standardizing data types
- Adding derived columns
- Applying business rules
- Performing data quality checks

For this first version, only lightweight transformations are
performed to establish the ETL framework.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.functions import trim


def transform_data(dataframe: DataFrame) -> DataFrame:
    """
    Apply transformations to the Spark DataFrame.

    Parameters
    ----------
    dataframe : DataFrame
        Data extracted from SQL Server.

    Returns
    -------
    DataFrame
        Transformed Spark DataFrame.
    """

    print("=" * 60)
    print("Starting Transform Phase...")
    print("=" * 60)

    # ---------------------------------------------------------
    # Remove leading and trailing spaces from string columns.
    # ---------------------------------------------------------

    dataframe = (
        dataframe
        .withColumn("name", trim(col("name")))
        .withColumn("born_city", trim(col("born_city")))
        .withColumn("born_region", trim(col("born_region")))
        .withColumn("born_country", trim(col("born_country")))
        .withColumn("NOC", trim(col("NOC")))
    )

    # ---------------------------------------------------------
    # Display the schema after transformations.
    # ---------------------------------------------------------

    print("Schema after transformation:")

    dataframe.printSchema()

    print("Transform phase completed successfully.")

    print("=" * 60)

    return dataframe


# =====================================================================
# Interview Notes
# =====================================================================
#
# Question:
#
# Why did you create transform.py instead of placing
# transformations inside extract.py?
#
# Suggested Answer:
#
# I wanted each module to have a single responsibility.
#
# extract.py is responsible only for extracting data.
#
# transform.py is responsible only for applying business
# rules and preparing data for analytics.
#
# Separating these responsibilities makes the code easier
# to maintain, test, and extend.
#
# As business requirements grow, new transformations can be
# added without modifying the extraction or loading logic.
#
# This modular approach follows the Single Responsibility
# Principle and reflects common enterprise ETL design
# patterns.
#
# =====================================================================