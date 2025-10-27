"""
Compatibility tests for bitwise functions.

Tests bitwise operations against expected outputs generated from PySpark.
"""

import pytest
from tests.tools.output_loader import load_expected_output
from tests.tools.comparison_utils import assert_dataframes_equal


class TestBitwiseFunctionsCompatibility:
    """Test bitwise functions compatibility with PySpark."""

    @pytest.mark.skip(reason="not yet implemented")
    def test_bitwise_not(self, spark):
        """Test bitwise_not function."""
        expected = load_expected_output("functions", "bitwise_bitwise_not")

        df = spark.createDataFrame(expected["input_data"])
        result = df.select(df.num.bitwise_not())

        assert_dataframes_equal(result, expected)
