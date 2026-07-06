"""
test_transform.py

Unit tests for transform.py.

These tests verify that business transformations are correctly
applied to a Spark DataFrame.
"""

from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import StringType

from sqlserver_to_s3.transform import transform_data


def test_trim_string_columns(spark):
    """
    Verify that leading/trailing spaces are removed.
    """

    schema = StructType([
        StructField("name", StringType(), True),
        StructField("born_city", StringType(), True),
        StructField("born_region", StringType(), True),
        StructField("born_country", StringType(), True),
        StructField("NOC", StringType(), True),
    ])

    source_df = spark.createDataFrame(
        [
            (
                "  Michael Jordan  ",
                "  Brooklyn ",
                " New York ",
                " USA ",
                " USA "
            )
        ],
        schema=schema
    )

    transformed_df = transform_data(source_df)

    row = transformed_df.first()

    assert row.name == "Michael Jordan"
    assert row.born_city == "Brooklyn"
    assert row.born_region == "New York"
    assert row.born_country == "USA"
    assert row.NOC == "USA"


def test_row_count_not_changed(spark):
    """
    Transformations should not change the number of rows.
    """

    schema = StructType([
        StructField("name", StringType(), True),
        StructField("born_city", StringType(), True),
        StructField("born_region", StringType(), True),
        StructField("born_country", StringType(), True),
        StructField("NOC", StringType(), True),
    ])

    source_df = spark.createDataFrame(
        [
            ("Alice", "Paris", "Ile-de-France", "France", "FRA"),
            ("Bob", "London", "England", "UK", "GBR"),
        ],
        schema=schema
    )

    transformed_df = transform_data(source_df)

    assert transformed_df.count() == source_df.count()


def test_columns_preserved(spark):
    """
    Verify that no columns are accidentally removed.
    """

    schema = StructType([
        StructField("name", StringType(), True),
        StructField("born_city", StringType(), True),
        StructField("born_region", StringType(), True),
        StructField("born_country", StringType(), True),
        StructField("NOC", StringType(), True),
    ])

    source_df = spark.createDataFrame(
        [
            ("Alice", "Paris", "Ile-de-France", "France", "FRA")
        ],
        schema=schema
    )

    transformed_df = transform_data(source_df)

    assert transformed_df.columns == source_df.columns