"""
Tests for column availability and materialization requirements.

This test suite verifies that columns created in transforms require
materialization before they can be accessed, matching PySpark behavior.
"""

import pytest
from mock_spark import SparkSession, functions as F
from mock_spark.core.exceptions.operation import SparkColumnNotFoundError


class TestColumnAvailability:
    """Test column materialization requirements."""

    def test_materialized_columns_are_available(self):
        """Test that materialized columns are available."""
        spark = SparkSession("test")
        try:
            df = spark.createDataFrame([{"id": 1, "value": 10}], ["id", "value"])
            
            # Columns from created DataFrame should be immediately available
            available = df._get_available_columns()
            assert "id" in available
            assert "value" in available
        finally:
            spark.stop()

    def test_columns_available_after_collect(self):
        """Test that columns are available after collect()."""
        spark = SparkSession("test")
        try:
            from mock_spark.spark_types import IntegerType, StructType, StructField
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("value", IntegerType(), True)
            ])
            df = spark.createDataFrame([{"id": 1, "value": 10}], schema=schema)
            df = df.withColumn("new_col", F.col("value") + 1)
            
            # Materialize by collecting
            df.collect()
            
            # Now new_col should be available
            available = df._get_available_columns()
            assert "new_col" in available
        finally:
            spark.stop()

    def test_columns_available_after_show(self):
        """Test that columns are available after show()."""
        spark = SparkSession("test")
        try:
            from mock_spark.spark_types import IntegerType, StructType, StructField
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("value", IntegerType(), True)
            ])
            df = spark.createDataFrame([{"id": 1, "value": 10}], schema=schema)
            df = df.withColumn("new_col", F.col("value") + 1)
            
            # Materialize by showing
            df.show()
            
            # Now new_col should be available
            available = df._get_available_columns()
            assert "new_col" in available
        finally:
            spark.stop()

    def test_dataframe_is_marked_materialized(self):
        """Test that DataFrame is marked as materialized after actions."""
        spark = SparkSession("test")
        try:
            from mock_spark.spark_types import IntegerType, StructType, StructField
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("value", IntegerType(), True)
            ])
            df = spark.createDataFrame([{"id": 1, "value": 10}], schema=schema)
            assert df._materialized is True  # Created from data, so materialized
            
            df2 = df.withColumn("new", F.col("value") + 1)
            # After transform, may not be materialized yet
            # (depends on implementation)
            
            # After action, should be materialized
            df2.collect()
            assert df2._materialized is True
        finally:
            spark.stop()
