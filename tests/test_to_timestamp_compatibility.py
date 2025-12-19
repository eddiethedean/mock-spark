"""
Tests for to_timestamp() compatibility with multiple input types.

This test suite verifies that to_timestamp() accepts all input types
that PySpark supports, matching PySpark's behavior exactly.

Issue #131: to_timestamp() should accept TimestampType input for PySpark compatibility
"""

import pytest
from sparkless import SparkSession, functions as F
from datetime import datetime, date
from sparkless.spark_types import (
    IntegerType,
    LongType,
    DateType,
    DoubleType,
    StructType,
    StructField,
)


class TestToTimestampCompatibility:
    """Test to_timestamp() compatibility with PySpark."""

    def test_to_timestamp_timestamp_type_pass_through(self):
        """Test that to_timestamp() accepts TimestampType input (pass-through behavior).

        This is the exact scenario from issue #131.
        """
        spark = SparkSession("test")
        try:
            # Create DataFrame with timestamp string
            data = [("2024-01-01T10:00:00", "test")]
            df = spark.createDataFrame(data, ["timestamp_str", "name"])

            # Convert to timestamp
            df = df.withColumn(
                "ts", F.to_timestamp(df["timestamp_str"], "yyyy-MM-dd'T'HH:mm:ss")
            )

            # Try to_timestamp on TimestampType column - should work now
            result = df.withColumn(
                "ts2", F.to_timestamp(df["ts"], "yyyy-MM-dd'T'HH:mm:ss")
            )

            # Verify both columns are TimestampType
            rows = result.collect()
            assert len(rows) == 1
            assert isinstance(rows[0]["ts"], datetime)
            assert isinstance(rows[0]["ts2"], datetime)
            # ts2 should be the same as ts (pass-through behavior)
            assert rows[0]["ts"] == rows[0]["ts2"]
        finally:
            spark.stop()

    def test_to_timestamp_string_type_with_format(self):
        """Test that to_timestamp() works with StringType input and format string."""
        spark = SparkSession("test")
        try:
            data = [("2024-01-01T10:00:00",)]
            df = spark.createDataFrame(data, ["timestamp_str"])

            result = df.withColumn(
                "ts", F.to_timestamp(F.col("timestamp_str"), "yyyy-MM-dd'T'HH:mm:ss")
            )

            rows = result.collect()
            assert len(rows) == 1
            assert isinstance(rows[0]["ts"], datetime)
            assert rows[0]["ts"] == datetime(2024, 1, 1, 10, 0, 0)
        finally:
            spark.stop()

    def test_to_timestamp_string_type_without_format(self):
        """Test that to_timestamp() works with StringType input without format."""
        spark = SparkSession("test")
        try:
            data = [("2024-01-01 10:00:00",)]
            df = spark.createDataFrame(data, ["timestamp_str"])

            result = df.withColumn("ts", F.to_timestamp(F.col("timestamp_str")))

            rows = result.collect()
            assert len(rows) == 1
            assert isinstance(rows[0]["ts"], datetime)
        finally:
            spark.stop()

    def test_to_timestamp_integer_type_unix_timestamp(self):
        """Test that to_timestamp() accepts IntegerType input (Unix timestamp in seconds)."""
        spark = SparkSession("test")
        try:
            # Unix timestamp for 2024-01-01 10:00:00 UTC
            unix_ts = 1704110400
            schema = StructType([StructField("unix_ts", IntegerType(), True)])
            df = spark.createDataFrame([{"unix_ts": unix_ts}], schema=schema)

            result = df.withColumn("ts", F.to_timestamp(F.col("unix_ts")))

            rows = result.collect()
            assert len(rows) == 1
            assert isinstance(rows[0]["ts"], datetime)
        finally:
            spark.stop()

    def test_to_timestamp_long_type_unix_timestamp(self):
        """Test that to_timestamp() accepts LongType input (Unix timestamp in seconds)."""
        spark = SparkSession("test")
        try:
            # Unix timestamp for 2024-01-01 10:00:00 UTC
            unix_ts = 1704110400
            schema = StructType([StructField("unix_ts", LongType(), True)])
            df = spark.createDataFrame([{"unix_ts": unix_ts}], schema=schema)

            result = df.withColumn("ts", F.to_timestamp(F.col("unix_ts")))

            rows = result.collect()
            assert len(rows) == 1
            assert isinstance(rows[0]["ts"], datetime)
        finally:
            spark.stop()

    def test_to_timestamp_date_type_conversion(self):
        """Test that to_timestamp() accepts DateType input (converts Date to Timestamp)."""
        spark = SparkSession("test")
        try:
            schema = StructType([StructField("date_col", DateType(), True)])
            df = spark.createDataFrame([{"date_col": date(2024, 1, 1)}], schema=schema)

            result = df.withColumn("ts", F.to_timestamp(F.col("date_col")))

            rows = result.collect()
            assert len(rows) == 1
            assert isinstance(rows[0]["ts"], datetime)
            # Date should be converted to timestamp at midnight
            assert rows[0]["ts"].date() == date(2024, 1, 1)
        finally:
            spark.stop()

    def test_to_timestamp_double_type_unix_timestamp(self):
        """Test that to_timestamp() accepts DoubleType input (Unix timestamp with decimal seconds)."""
        spark = SparkSession("test")
        try:
            # Unix timestamp with decimals for 2024-01-01 10:00:00.5 UTC
            unix_ts = 1704110400.5
            schema = StructType([StructField("unix_ts", DoubleType(), True)])
            df = spark.createDataFrame([{"unix_ts": unix_ts}], schema=schema)

            result = df.withColumn("ts", F.to_timestamp(F.col("unix_ts")))

            rows = result.collect()
            assert len(rows) == 1
            assert isinstance(rows[0]["ts"], datetime)
        finally:
            spark.stop()

    def test_to_timestamp_rejects_unsupported_type(self):
        """Test that to_timestamp() rejects unsupported input types."""
        spark = SparkSession("test")
        try:
            from sparkless.spark_types import BooleanType

            schema = StructType([StructField("bool_col", BooleanType(), True)])
            df = spark.createDataFrame([{"bool_col": True}], schema=schema)

            with pytest.raises(
                TypeError,
                match="requires StringType, TimestampType, IntegerType, LongType, DateType, or DoubleType",
            ):
                df.withColumn("ts", F.to_timestamp(F.col("bool_col")))
        finally:
            spark.stop()
