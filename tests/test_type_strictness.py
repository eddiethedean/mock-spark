"""
Tests for strict type checking in functions.

This test suite verifies that functions enforce strict type requirements,
matching PySpark's behavior exactly.
"""

import pytest
from mock_spark import SparkSession, functions as F


class TestTypeStrictness:
    """Test strict type checking in functions."""

    def test_to_timestamp_requires_string(self):
        """Test that to_timestamp requires string or timestamp input."""
        spark = SparkSession("test")
        try:
            from mock_spark.spark_types import IntegerType, StructType, StructField

            # Create DataFrame with explicit IntegerType schema (not string or timestamp)
            schema = StructType([StructField("date", IntegerType(), True)])
            df = spark.createDataFrame([{"date": 12345}], schema=schema)

            # This should fail - to_timestamp requires StringType or TimestampType
            with pytest.raises(
                TypeError, match="requires StringType or TimestampType input"
            ):
                df.withColumn("parsed", F.to_timestamp(F.col("date")))
        finally:
            spark.stop()

    def test_to_timestamp_works_with_string(self):
        """Test that to_timestamp works with string input."""
        spark = SparkSession("test")
        try:
            # Create DataFrame with string column
            df = spark.createDataFrame(
                [{"date_str": "2023-01-01 12:00:00"}], schema=["date_str"]
            )

            # This should work
            result = df.withColumn("parsed", F.to_timestamp(F.col("date_str")))
            assert result is not None
        finally:
            spark.stop()

    def test_to_date_requires_string(self):
        """Test that to_date requires string or date input."""
        spark = SparkSession("test")
        try:
            from mock_spark.spark_types import IntegerType, StructType, StructField

            # Create DataFrame with explicit IntegerType schema (not string or date)
            schema = StructType([StructField("date", IntegerType(), True)])
            df = spark.createDataFrame([{"date": 12345}], schema=schema)

            # This should fail - to_date requires StringType or DateType
            with pytest.raises(
                TypeError, match="requires StringType or DateType input"
            ):
                df.withColumn("parsed", F.to_date(F.col("date")))
        finally:
            spark.stop()

    def test_to_date_works_with_string(self):
        """Test that to_date works with string input."""
        spark = SparkSession("test")
        try:
            # Create DataFrame with string column
            df = spark.createDataFrame(
                [{"date_str": "2023-01-01"}], schema=["date_str"]
            )

            # This should work
            result = df.withColumn("parsed", F.to_date(F.col("date_str")))
            assert result is not None
        finally:
            spark.stop()
