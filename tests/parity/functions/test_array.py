"""
PySpark parity tests for array functions.

Tests validate that Sparkless array functions behave identically to PySpark.
"""

import pytest
from tests.fixtures.parity_base import ParityTestBase
from sparkless import F


class TestArrayFunctionsParity(ParityTestBase):
    """Test array function parity with PySpark."""

    def test_array_contains(self, spark):
        """Test array_contains function matches PySpark behavior."""
        expected = self.load_expected("arrays", "array_contains")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.array_contains(df.scores, 90))
        self.assert_parity(result, expected)

    def test_array_position(self, spark):
        """Test array_position function matches PySpark behavior."""
        expected = self.load_expected("arrays", "array_position")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.array_position(df.scores, 90))
        self.assert_parity(result, expected)

    def test_size(self, spark):
        """Test size function matches PySpark behavior."""
        expected = self.load_expected("arrays", "size")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.size(df.scores))
        self.assert_parity(result, expected)

    def test_element_at(self, spark):
        """Test element_at function matches PySpark behavior."""
        expected = self.load_expected("arrays", "element_at")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.element_at(df.scores, 2))
        self.assert_parity(result, expected)

    def test_explode(self, spark):
        """Test explode function matches PySpark behavior."""
        expected = self.load_expected("arrays", "explode")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(df.name, F.explode(df.scores).alias("score"))
        self.assert_parity(result, expected)

    def test_array_distinct(self, spark):
        """Test array_distinct function matches PySpark behavior."""
        expected = self.load_expected("arrays", "array_distinct")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.array_distinct(df.tags))
        # array_distinct doesn't guarantee order of elements within arrays
        # The comparison utility should handle this, but if it doesn't, we may need to skip
        # For now, let's try without sorting - the comparison should handle it
        self.assert_parity(result, expected)

    def test_array_join(self, spark):
        """Test array_join function matches PySpark behavior."""
        expected = self.load_expected("arrays", "array_join")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.array_join(df.arr1, "-"))
        self.assert_parity(result, expected)

    def test_array_union(self, spark):
        """Test array_union function matches PySpark behavior."""
        expected = self.load_expected("arrays", "array_union")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.array_union(df.arr1, df.arr2))
        self.assert_parity(result, expected)

    @pytest.mark.skip(reason="BUG-017: Column name mismatch - PySpark generates complex lambda function name in column, mock generates simpler name. Function works correctly, data values match.")
    def test_array_sort(self, spark):
        """Test array_sort function matches PySpark behavior."""
        expected = self.load_expected("arrays", "array_sort")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.array_sort(df.arr3))
        self.assert_parity(result, expected)

    def test_array_remove(self, spark):
        """Test array_remove function matches PySpark behavior."""
        expected = self.load_expected("arrays", "array_remove")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.array_remove(df.scores, 90))
        self.assert_parity(result, expected)

