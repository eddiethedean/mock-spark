"""
PySpark parity tests for DataFrame filter operations.

Tests validate that Sparkless filter operations behave identically to PySpark.
"""

import pytest
from tests.fixtures.parity_base import ParityTestBase
from sparkless import F


class TestFilterParity(ParityTestBase):
    """Test DataFrame filter operations parity with PySpark."""

    def test_filter_operations(self, spark):
        """Test filter matches PySpark behavior."""
        expected = self.load_expected("dataframe_operations", "filter_operations")
        
        df = spark.createDataFrame(expected["input_data"])
        result = df.filter(df.age > 30)
        
        self.assert_parity(result, expected)

    def test_filter_with_boolean(self, spark):
        """Test filter with boolean matches PySpark behavior."""
        expected = self.load_expected("dataframe_operations", "filter_with_boolean")
        
        df = spark.createDataFrame(expected["input_data"])
        result = df.filter(df.salary > 60000)
        
        self.assert_parity(result, expected)

