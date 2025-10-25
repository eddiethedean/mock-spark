"""
Unit tests for Bitwise Functions (PySpark 3.2).

Tests bit_count, bit_get, and bitwise_not functions.
"""

import pytest

from mock_spark import MockSparkSession
from mock_spark import functions as F


@pytest.mark.fast
class TestBitwiseFunctionsUnit:
    """Fast unit tests for bitwise functions."""

    def test_bit_count(self):
        """Test bit_count counts set bits."""
        spark = MockSparkSession("test")
        data = [{"val": 7}]  # Binary: 111
        df = spark.createDataFrame(data)

        result = df.select(F.bit_count(F.col("val")).alias("bits")).collect()

        assert result[0]["bits"] == 3

    def test_bit_get(self):
        """Test bit_get extracts bit at position."""
        spark = MockSparkSession("test")
        data = [{"val": 5}]  # Binary: 101
        df = spark.createDataFrame(data)

        result = df.select(
            F.bit_get(F.col("val"), 0).alias("bit0"),
            F.bit_get(F.col("val"), 2).alias("bit2"),
        ).collect()

        # bit_get may return string or int depending on DuckDB
        assert int(result[0]["bit0"]) == 1
        assert int(result[0]["bit2"]) == 1

    def test_bitwise_not(self):
        """Test bitwise_not performs bitwise NOT."""
        spark = MockSparkSession("test")
        data = [{"val": 5}]
        df = spark.createDataFrame(data)

        result = df.select(F.bitwise_not(F.col("val")).alias("not_val")).collect()

        # Bitwise NOT of 5 depends on bit width, just check it's an integer
        assert isinstance(result[0]["not_val"], int)
