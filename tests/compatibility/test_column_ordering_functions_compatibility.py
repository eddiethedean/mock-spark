"""
Compatibility tests for column/ordering functions.

Tests column and ordering operations against expected outputs generated from PySpark.
"""

import pytest
from tests.tools.output_loader import load_expected_output
from tests.tools.comparison_utils import assert_dataframes_equal
from mock_spark import F


class TestColumnOrderingFunctionsCompatibility:
    """Test column/ordering functions compatibility with PySpark."""

    @pytest.mark.skip(reason="asc ordering not yet implemented correctly")
    def test_asc(self, spark):
        """Test asc ordering function."""
        expected = load_expected_output("functions", "column_asc")

        df = spark.createDataFrame(expected["input_data"])
        result = df.select(df.age.asc())

        assert_dataframes_equal(result, expected)

    @pytest.mark.skip(reason="desc ordering not yet implemented correctly")
    def test_desc(self, spark):
        """Test desc ordering function."""
        expected = load_expected_output("functions", "column_desc")

        df = spark.createDataFrame(expected["input_data"])
        result = df.select(df.age.desc())

        assert_dataframes_equal(result, expected)

    def test_col(self, spark):
        """Test col function."""
        expected = load_expected_output("functions", "column_col")

        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.col("name"))

        assert_dataframes_equal(result, expected)

    def test_column(self, spark):
        """Test column function."""
        expected = load_expected_output("functions", "column_column")

        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.col("name"))

        assert_dataframes_equal(result, expected)

    def test_lit(self, spark):
        """Test lit function."""
        expected = load_expected_output("functions", "column_lit")

        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.lit("test"))

        assert_dataframes_equal(result, expected)

    @pytest.mark.skip(reason="expr not yet implemented correctly")
    def test_expr(self, spark):
        """Test expr function."""
        expected = load_expected_output("functions", "column_expr")

        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.expr("age + 1"))

        assert_dataframes_equal(result, expected)
