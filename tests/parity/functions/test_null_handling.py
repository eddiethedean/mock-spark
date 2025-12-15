"""
PySpark parity tests for null handling functions.

Tests validate that Sparkless null handling functions behave identically to PySpark.
"""

import pytest
from tests.fixtures.parity_base import ParityTestBase
from sparkless import F


class TestNullHandlingFunctionsParity(ParityTestBase):
    """Test null handling function parity with PySpark."""

    def test_coalesce(self, spark):
        """Test coalesce function matches PySpark behavior."""
        expected = self.load_expected("null_handling", "coalesce")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.coalesce(df.col1, df.col2, df.col3).alias("first_non_null"))
        self.assert_parity(result, expected)

    def test_isnull(self, spark):
        """Test isnull function matches PySpark behavior."""
        expected = self.load_expected("null_handling", "isnull")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.isnull(df.value))
        self.assert_parity(result, expected)

    def test_isnotnull(self, spark):
        """Test isnotnull function matches PySpark behavior."""
        expected = self.load_expected("null_handling", "isnotnull")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.isnotnull(df.value))
        self.assert_parity(result, expected)

    def test_when_otherwise(self, spark):
        """Test when/otherwise function matches PySpark behavior."""
        expected = self.load_expected("null_handling", "when_otherwise")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.when(df.age > 30, "Senior").otherwise("Junior").alias("level"))
        self.assert_parity(result, expected)

    def test_nvl(self, spark):
        """Test nvl function matches PySpark behavior."""
        expected = self.load_expected("null_handling", "nvl")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.nvl(df.value, 0))
        self.assert_parity(result, expected)

    def test_nullif(self, spark):
        """Test nullif function matches PySpark behavior."""
        expected = self.load_expected("null_handling", "nullif")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.nullif(df.col1, df.col2))
        self.assert_parity(result, expected)

    def test_ifnull(self, spark):
        """Test ifnull function matches PySpark behavior."""
        expected = self.load_expected("functions", "ifnull")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.when(df.salary.isNull(), 0).otherwise(df.salary))
        self.assert_parity(result, expected)

    def test_nanvl(self, spark):
        """Test nanvl function matches PySpark behavior."""
        expected = self.load_expected("functions", "nanvl")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.nanvl(df.salary, 0))
        self.assert_parity(result, expected)

