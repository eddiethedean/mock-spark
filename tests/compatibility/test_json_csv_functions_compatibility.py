"""
Compatibility tests for JSON/CSV functions.

This module validates JSON/CSV functions against pre-generated PySpark outputs.
"""

import pytest
from tests.tools.output_loader import load_expected_output
from tests.tools.comparison_utils import assert_dataframes_equal
from mock_spark import F


class TestJsonCsvFunctionsCompatibility:
    """Test JSON/CSV functions against expected PySpark outputs."""

    @pytest.fixture
    def spark(self):
        """Create a MockSparkSession for testing."""
        from mock_spark import MockSparkSession

        session = MockSparkSession("json_csv_functions_test")
        yield session
        session.stop()

    @pytest.mark.skip(reason="not yet implemented")
    def test_from_json(self, spark):
        """Test from_json function."""
        expected = load_expected_output("functions", "from_json")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.from_json(df.json_str, F.lit("name string, age int")))
        assert_dataframes_equal(result, expected)

    @pytest.mark.skip(reason="not yet implemented")
    def test_to_json(self, spark):
        """Test to_json function."""
        expected = load_expected_output("functions", "to_json")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.to_json(F.struct(df.name, df.age)))
        assert_dataframes_equal(result, expected)

    @pytest.mark.skip(reason="not yet implemented")
    def test_to_csv(self, spark):
        """Test to_csv function."""
        expected = load_expected_output("functions", "to_csv")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.to_csv(F.struct(df.name, df.age)))
        assert_dataframes_equal(result, expected)
