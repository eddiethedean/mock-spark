"""
PySpark parity tests for aggregate functions.

Tests validate that Sparkless aggregate functions behave identically to PySpark.
"""

from tests.fixtures.parity_base import ParityTestBase
from sparkless import F


class TestAggregateFunctionsParity(ParityTestBase):
    """Test aggregate function parity with PySpark."""

    def test_agg_sum(self, spark):
        """Test sum aggregation matches PySpark behavior."""
        expected = self.load_expected("functions", "agg_sum")
        df = spark.createDataFrame(expected["input_data"])
        result = df.groupBy("active").agg(F.sum(df.salary))
        self.assert_parity(result, expected)

    def test_agg_avg(self, spark):
        """Test avg aggregation matches PySpark behavior."""
        expected = self.load_expected("functions", "agg_avg")
        df = spark.createDataFrame(expected["input_data"])
        result = df.groupBy("active").agg(F.avg(df.salary))
        self.assert_parity(result, expected)

    def test_agg_count(self, spark):
        """Test count aggregation matches PySpark behavior."""
        expected = self.load_expected("functions", "agg_count")
        df = spark.createDataFrame(expected["input_data"])
        result = df.groupBy("active").agg(F.count(df.id))
        self.assert_parity(result, expected)

    def test_agg_max(self, spark):
        """Test max aggregation matches PySpark behavior."""
        expected = self.load_expected("functions", "agg_max")
        df = spark.createDataFrame(expected["input_data"])
        result = df.groupBy("active").agg(F.max(df.salary))
        self.assert_parity(result, expected)

    def test_agg_min(self, spark):
        """Test min aggregation matches PySpark behavior."""
        expected = self.load_expected("functions", "agg_min")
        df = spark.createDataFrame(expected["input_data"])
        result = df.groupBy("active").agg(F.min(df.salary))
        self.assert_parity(result, expected)
