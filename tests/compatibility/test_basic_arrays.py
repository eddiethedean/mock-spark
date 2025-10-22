"""
Compatibility tests for Basic Array Functions (PySpark 3.2).

Tests array_compact, slice, element_at, array_append, array_prepend,
array_insert, array_size, array_sort, and arrays_overlap functions.
"""

import pytest

try:
    from pyspark.sql import SparkSession  # noqa: F401
    from pyspark.sql import functions as PySparkF  # noqa: F401

    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

from mock_spark import MockSparkSession
from mock_spark import functions as F
from mock_spark.spark_types import (
    MockStructType,
    MockStructField,
    ArrayType,
    IntegerType,
)


@pytest.mark.fast
class TestBasicArrayFunctionsUnit:
    """Fast unit tests without PySpark dependency."""

    def test_array_compact(self):
        """Test array_compact removes nulls."""
        spark = MockSparkSession("test")
        data = [{"nums": [1, None, 2, None, 3]}]
        schema = MockStructType([MockStructField("nums", ArrayType(IntegerType()))])
        df = spark.createDataFrame(data, schema)

        result = df.select(F.array_compact(F.col("nums")).alias("compact")).collect()

        assert result[0]["compact"] == [1, 2, 3]

    def test_slice(self):
        """Test slice extracts array slice."""
        spark = MockSparkSession("test")
        data = [{"nums": [1, 2, 3, 4, 5]}]
        schema = MockStructType([MockStructField("nums", ArrayType(IntegerType()))])
        df = spark.createDataFrame(data, schema)

        result = df.select(F.slice(F.col("nums"), 2, 3).alias("sliced")).collect()

        assert result[0]["sliced"] == [2, 3, 4]

    def test_element_at(self):
        """Test element_at gets element at index."""
        spark = MockSparkSession("test")
        data = [{"nums": [10, 20, 30, 40, 50]}]
        schema = MockStructType([MockStructField("nums", ArrayType(IntegerType()))])
        df = spark.createDataFrame(data, schema)

        result = df.select(
            F.element_at(F.col("nums"), 1).alias("first"),
            F.element_at(F.col("nums"), -1).alias("last"),
        ).collect()

        assert result[0]["first"] == 10
        assert result[0]["last"] == 50

    def test_array_append(self):
        """Test array_append adds element to end."""
        spark = MockSparkSession("test")
        data = [{"nums": [1, 2, 3]}]
        schema = MockStructType([MockStructField("nums", ArrayType(IntegerType()))])
        df = spark.createDataFrame(data, schema)

        result = df.select(F.array_append(F.col("nums"), 4).alias("appended")).collect()

        assert result[0]["appended"] == [1, 2, 3, 4]

    def test_array_prepend(self):
        """Test array_prepend adds element to start."""
        spark = MockSparkSession("test")
        data = [{"nums": [2, 3, 4]}]
        schema = MockStructType([MockStructField("nums", ArrayType(IntegerType()))])
        df = spark.createDataFrame(data, schema)

        result = df.select(
            F.array_prepend(F.col("nums"), 1).alias("prepended")
        ).collect()

        assert result[0]["prepended"] == [1, 2, 3, 4]

    def test_array_insert(self):
        """Test array_insert inserts element at position."""
        spark = MockSparkSession("test")
        data = [{"nums": [1, 2, 4, 5]}]
        schema = MockStructType([MockStructField("nums", ArrayType(IntegerType()))])
        df = spark.createDataFrame(data, schema)

        result = df.select(
            F.array_insert(F.col("nums"), 3, 3).alias("inserted")
        ).collect()

        assert result[0]["inserted"] == [1, 2, 3, 4, 5]

    def test_array_size(self):
        """Test array_size returns array length."""
        spark = MockSparkSession("test")
        data = [{"nums": [1, 2, 3, 4, 5]}]
        schema = MockStructType([MockStructField("nums", ArrayType(IntegerType()))])
        df = spark.createDataFrame(data, schema)

        result = df.select(F.array_size(F.col("nums")).alias("size")).collect()

        assert result[0]["size"] == 5

    def test_array_sort(self):
        """Test array_sort sorts elements."""
        spark = MockSparkSession("test")
        data = [{"nums": [3, 1, 4, 1, 5, 9, 2, 6]}]
        schema = MockStructType([MockStructField("nums", ArrayType(IntegerType()))])
        df = spark.createDataFrame(data, schema)

        result = df.select(F.array_sort(F.col("nums")).alias("sorted")).collect()

        assert result[0]["sorted"] == [1, 1, 2, 3, 4, 5, 6, 9]

    def test_arrays_overlap(self):
        """Test arrays_overlap checks for common elements."""
        spark = MockSparkSession("test")
        data = [{"a": [1, 2, 3], "b": [3, 4, 5]}, {"a": [1, 2, 3], "b": [4, 5, 6]}]
        schema = MockStructType(
            [
                MockStructField("a", ArrayType(IntegerType())),
                MockStructField("b", ArrayType(IntegerType())),
            ]
        )
        df = spark.createDataFrame(data, schema)

        result = df.select(
            F.arrays_overlap(F.col("a"), F.col("b")).alias("overlap")
        ).collect()

        assert result[0]["overlap"] is True  # Has 3 in common
        assert result[1]["overlap"] is False  # No common elements
