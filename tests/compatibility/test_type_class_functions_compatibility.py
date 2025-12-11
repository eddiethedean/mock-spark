"""
Compatibility tests for type/class functions.

Tests type and class operations against expected outputs generated from PySpark.
"""

from tests.tools.output_loader import load_expected_output
from tests.tools.comparison_utils import assert_dataframes_equal
from mock_spark import F


class TestTypeClassFunctionsCompatibility:
    """Test type/class functions compatibility with PySpark."""

    def test_string_type(self, spark):
        """Test string type casting."""
        expected = load_expected_output("functions", "type_string_type")

        df = spark.createDataFrame(expected["input_data"])
        result = df.select(df.name.cast("string"))

        assert_dataframes_equal(result, expected)

    def test_array_type(self, spark):
        """Test array type creation."""
        expected = load_expected_output("functions", "type_array_type")

        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.array(F.lit(1), F.lit(2)))

        assert_dataframes_equal(result, expected)

    def test_struct_type(self, spark):
        """Test struct type creation."""
        expected = load_expected_output("functions", "type_struct_type")

        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.struct(df.name, df.age))

        assert_dataframes_equal(result, expected)
