"""
PySpark parity tests for DataFrame filter operations.

Tests validate that Sparkless filter operations behave identically to PySpark.
"""

from tests.fixtures.parity_base import ParityTestBase


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

    def test_filter_with_and_operator(self, spark):
        """Test filter with combined expressions using & (AND) operator.

        Tests fix for issue #108: Combined ColumnOperation expressions with & operator
        should be properly translated, not treated as column names.
        """
        from sparkless import functions as F

        df = spark.createDataFrame(
            [{"a": 1, "b": 2}, {"a": 2, "b": 3}, {"a": 3, "b": 1}], "a int, b int"
        )

        expr1 = F.col("a") > 1
        expr2 = F.col("b") > 1
        combined = expr1 & expr2

        # Should not raise ColumnNotFoundError
        result = df.filter(combined)
        assert result.count() == 1, "Should return 1 row where both conditions are true"
        rows = result.collect()
        assert rows[0]["a"] == 2
        assert rows[0]["b"] == 3

    def test_filter_with_or_operator(self, spark):
        """Test filter with combined expressions using | (OR) operator.

        Tests fix for issue #109: Combined ColumnOperation expressions with | operator
        should be properly translated, not treated as column names.
        """
        from sparkless import functions as F

        df = spark.createDataFrame(
            [{"a": 1, "b": 2}, {"a": 2, "b": 3}, {"a": 3, "b": 1}], "a int, b int"
        )

        expr1 = F.col("a") > 1
        expr2 = F.col("b") > 1
        combined = expr1 | expr2

        # Should not raise ColumnNotFoundError
        result = df.filter(combined)
        assert result.count() == 3, (
            "Should return 3 rows where at least one condition is true"
        )
