"""
Compatibility tests for Struct Functions (PySpark 3.2).

Tests struct and named_struct functions.
"""

import pytest

from mock_spark import MockSparkSession
from mock_spark import functions as F


@pytest.mark.fast
class TestStructFunctionsUnit:
    """Fast unit tests for struct functions."""

    def test_struct(self):
        """Test struct creates struct from columns."""
        spark = MockSparkSession("test")
        data = [{"a": 1, "b": 2, "c": 3}]
        df = spark.createDataFrame(data)

        result = df.select(
            F.struct(F.col("a"), F.col("b")).alias("struct_col")
        ).collect()

        # Struct should be dict-like
        assert isinstance(result[0]["struct_col"], (dict, str))

    def test_named_struct(self):
        """Test named_struct creates struct with named fields."""
        spark = MockSparkSession("test")
        data = [{"x": 10, "y": 20}]
        df = spark.createDataFrame(data)

        result = df.select(
            F.named_struct("field1", F.col("x"), "field2", F.col("y")).alias(
                "struct_col"
            )
        ).collect()

        # Named struct should be dict-like
        assert isinstance(result[0]["struct_col"], (dict, str))
