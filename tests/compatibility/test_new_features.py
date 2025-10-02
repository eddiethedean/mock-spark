"""
Compatibility tests for new features in mock_spark.

This module tests all the new functionality that has been added to ensure
it works exactly like PySpark and doesn't regress.
"""

import pytest
from tests.compatibility.utils.comparison import assert_dataframes_equal


class TestArithmeticOperations:
    """Test arithmetic operations compatibility."""

    def test_addition_operation(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test addition operation (col + value)."""
        mock_result = mock_dataframe.select(mock_functions.col("age") + 1)
        pyspark_result = pyspark_dataframe.select(pyspark_functions.col("age") + 1)
        assert_dataframes_equal(mock_result, pyspark_result)

    def test_subtraction_operation(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test subtraction operation (col - value)."""
        mock_result = mock_dataframe.select(mock_functions.col("age") - 1)
        pyspark_result = pyspark_dataframe.select(pyspark_functions.col("age") - 1)
        assert_dataframes_equal(mock_result, pyspark_result)

    def test_multiplication_operation(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test multiplication operation (col * value)."""
        mock_result = mock_dataframe.select(mock_functions.col("age") * 2)
        pyspark_result = pyspark_dataframe.select(pyspark_functions.col("age") * 2)
        assert_dataframes_equal(mock_result, pyspark_result)

    def test_division_operation(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test division operation (col / value)."""
        mock_result = mock_dataframe.select(mock_functions.col("salary") / 1000)
        pyspark_result = pyspark_dataframe.select(pyspark_functions.col("salary") / 1000)
        assert_dataframes_equal(mock_result, pyspark_result)

    def test_modulo_operation(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test modulo operation (col % value)."""
        mock_result = mock_dataframe.select(mock_functions.col("age") % 10)
        pyspark_result = pyspark_dataframe.select(pyspark_functions.col("age") % 10)
        assert_dataframes_equal(mock_result, pyspark_result)


class TestStringFunctions:
    """Test string functions compatibility."""

    def test_upper_function(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test upper() function."""
        mock_result = mock_dataframe.select(mock_functions.upper(mock_functions.col("name")))
        pyspark_result = pyspark_dataframe.select(
            pyspark_functions.upper(pyspark_functions.col("name"))
        )
        assert_dataframes_equal(mock_result, pyspark_result)

    def test_lower_function(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test lower() function."""
        mock_result = mock_dataframe.select(mock_functions.lower(mock_functions.col("name")))
        pyspark_result = pyspark_dataframe.select(
            pyspark_functions.lower(pyspark_functions.col("name"))
        )
        assert_dataframes_equal(mock_result, pyspark_result)

    def test_length_function(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test length() function."""
        mock_result = mock_dataframe.select(mock_functions.length(mock_functions.col("name")))
        pyspark_result = pyspark_dataframe.select(
            pyspark_functions.length(pyspark_functions.col("name"))
        )
        assert_dataframes_equal(mock_result, pyspark_result)


class TestMathematicalFunctions:
    """Test mathematical functions compatibility."""

    def test_abs_function(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test abs() function."""
        # Create a DataFrame with negative values
        mock_data_with_neg = mock_dataframe.withColumn("age_neg", mock_functions.col("age") * -1)
        pyspark_data_with_neg = pyspark_dataframe.withColumn(
            "age_neg", pyspark_functions.col("age") * -1
        )

        mock_result = mock_data_with_neg.select(mock_functions.abs(mock_functions.col("age_neg")))
        pyspark_result = pyspark_data_with_neg.select(
            pyspark_functions.abs(pyspark_functions.col("age_neg"))
        )
        assert_dataframes_equal(mock_result, pyspark_result)

    def test_round_function(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test round() function."""
        mock_result = mock_dataframe.select(mock_functions.round(mock_functions.col("salary"), 2))
        pyspark_result = pyspark_dataframe.select(
            pyspark_functions.round(pyspark_functions.col("salary"), 2)
        )
        assert_dataframes_equal(mock_result, pyspark_result)


class TestAggregationFunctions:
    """Test aggregation functions compatibility."""

    def test_count_function_with_star(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test count("*") function."""
        mock_result = mock_dataframe.select(mock_functions.count("*"))
        pyspark_result = pyspark_dataframe.select(pyspark_functions.count("*"))
        assert_dataframes_equal(mock_result, pyspark_result)

    def test_count_with_column(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test count() function with specific column."""
        mock_result = mock_dataframe.select(mock_functions.count("name"))
        pyspark_result = pyspark_dataframe.select(pyspark_functions.count("name"))
        assert_dataframes_equal(mock_result, pyspark_result)

    def test_sum_function(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test sum() function."""
        mock_result = mock_dataframe.select(mock_functions.sum("salary"))
        pyspark_result = pyspark_dataframe.select(pyspark_functions.sum("salary"))
        assert_dataframes_equal(mock_result, pyspark_result)

    def test_avg_function(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test avg() function."""
        mock_result = mock_dataframe.select(mock_functions.avg("salary"))
        pyspark_result = pyspark_dataframe.select(pyspark_functions.avg("salary"))
        assert_dataframes_equal(mock_result, pyspark_result)

    def test_max_function(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test max() function."""
        mock_result = mock_dataframe.select(mock_functions.max("salary"))
        pyspark_result = pyspark_dataframe.select(pyspark_functions.max("salary"))
        assert_dataframes_equal(mock_result, pyspark_result)

    def test_min_function(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test min() function."""
        mock_result = mock_dataframe.select(mock_functions.min("salary"))
        pyspark_result = pyspark_dataframe.select(pyspark_functions.min("salary"))
        assert_dataframes_equal(mock_result, pyspark_result)

    def test_count_distinct_function(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test countDistinct() function."""
        mock_result = mock_dataframe.select(mock_functions.countDistinct("department"))
        pyspark_result = pyspark_dataframe.select(pyspark_functions.countDistinct("department"))
        assert_dataframes_equal(mock_result, pyspark_result)


class TestDataFrameMethods:
    """Test new DataFrame methods compatibility."""

    def test_take_method(self, mock_dataframe, pyspark_dataframe):
        """Test take() method."""
        mock_result = mock_dataframe.take(2)
        pyspark_result = pyspark_dataframe.take(2)

        # Convert both to dictionaries for comparison
        mock_dicts = [row.asDict() for row in mock_result]
        pyspark_dicts = [row.asDict() for row in pyspark_result]

        assert len(mock_dicts) == len(pyspark_dicts)
        for mock_row, pyspark_row in zip(mock_dicts, pyspark_dicts):
            assert mock_row == pyspark_row

    def test_dtypes_property(self, mock_dataframe, pyspark_dataframe):
        """Test dtypes property."""
        mock_dtypes = mock_dataframe.dtypes
        pyspark_dtypes = pyspark_dataframe.dtypes

        assert len(mock_dtypes) == len(pyspark_dtypes)
        for mock_dtype, pyspark_dtype in zip(mock_dtypes, pyspark_dtypes):
            assert mock_dtype[0] == pyspark_dtype[0]  # Column name
            # Type names should match PySpark format
            assert mock_dtype[1] == pyspark_dtype[1]  # Type name should match exactly

    def test_desc_ordering(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test descending order with desc() method."""
        mock_result = mock_dataframe.orderBy(mock_functions.col("age").desc())
        pyspark_result = pyspark_dataframe.orderBy(pyspark_functions.col("age").desc())
        assert_dataframes_equal(mock_result, pyspark_result)

    def test_asc_ordering(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test ascending order with asc() method."""
        mock_result = mock_dataframe.orderBy(mock_functions.col("age").asc())
        pyspark_result = pyspark_dataframe.orderBy(pyspark_functions.col("age").asc())
        assert_dataframes_equal(mock_result, pyspark_result)


class TestLiteralFunctions:
    """Test literal function compatibility."""

    def test_lit_with_integer(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test lit() with integer value."""
        mock_result = mock_dataframe.select(mock_functions.lit(42))
        pyspark_result = pyspark_dataframe.select(pyspark_functions.lit(42))
        assert_dataframes_equal(mock_result, pyspark_result)

    def test_lit_with_string(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test lit() with string value."""
        mock_result = mock_dataframe.select(mock_functions.lit("constant"))
        pyspark_result = pyspark_dataframe.select(pyspark_functions.lit("constant"))
        assert_dataframes_equal(mock_result, pyspark_result)

    def test_lit_with_float(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test lit() with float value."""
        mock_result = mock_dataframe.select(mock_functions.lit(3.14))
        pyspark_result = pyspark_dataframe.select(pyspark_functions.lit(3.14))
        assert_dataframes_equal(mock_result, pyspark_result)


class TestEmptyDataFrame:
    """Test empty DataFrame handling."""

    def test_empty_dataframe_creation(self, mock_environment, pyspark_environment):
        """Test creating empty DataFrame."""
        empty_data = []

        # Mock_spark can create empty DataFrame with empty schema
        mock_df = mock_environment["session"].createDataFrame(empty_data)
        assert mock_df.count() == 0
        assert len(mock_df.schema.fields) == 0

        # Test with explicit schema - should work
        from mock_spark.spark_types import MockStructType, MockStructField, StringType

        schema = MockStructType([MockStructField("name", StringType())])
        mock_df = mock_environment["session"].createDataFrame(empty_data, schema)
        assert mock_df.count() == 0
        assert len(mock_df.schema.fields) == 1


class TestRowObjects:
    """Test Row object compatibility."""

    def test_collect_returns_rows(self, mock_dataframe, pyspark_dataframe):
        """Test that collect() returns Row objects."""
        mock_result = mock_dataframe.collect()
        pyspark_result = pyspark_dataframe.collect()

        assert len(mock_result) == len(pyspark_result)

        # Check that mock returns Row-like objects
        for mock_row in mock_result:
            assert hasattr(mock_row, "__getitem__")
            assert hasattr(mock_row, "keys")
            assert hasattr(mock_row, "values")
            assert hasattr(mock_row, "items")

        # Compare first row
        if mock_result and pyspark_result:
            mock_first = mock_result[0]
            pyspark_first = pyspark_result[0]

            # Convert PySpark Row to dict for comparison
            pyspark_dict = pyspark_first.asDict()

            # Compare the data
            for key in mock_first.keys():
                assert key in pyspark_dict
                assert mock_first[key] == pyspark_dict[key]


class TestSchemaCompatibility:
    """Test schema compatibility."""

    def test_schema_field_types(self, mock_dataframe, pyspark_dataframe):
        """Test that schema field types match PySpark."""
        mock_schema = mock_dataframe.schema
        pyspark_schema = pyspark_dataframe.schema

        assert len(mock_schema.fields) == len(pyspark_schema.fields)

        for mock_field, pyspark_field in zip(mock_schema.fields, pyspark_schema.fields):
            assert mock_field.name == pyspark_field.name
            # Type names should be compatible
            mock_type_name = mock_field.dataType.__class__.__name__
            pyspark_type_name = pyspark_field.dataType.__class__.__name__

            # Map mock types to PySpark types
            type_mapping = {
                "StringType": "StringType",
                "IntegerType": "IntegerType",
                "LongType": "LongType",
                "DoubleType": "DoubleType",
                "BooleanType": "BooleanType",
            }

            expected_pyspark_type = type_mapping.get(mock_type_name, mock_type_name)
            assert expected_pyspark_type == pyspark_type_name


class TestRegressionPrevention:
    """Test to prevent regressions in existing functionality."""

    def test_basic_select_still_works(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test that basic select operations still work."""
        mock_result = mock_dataframe.select("name", "age")
        pyspark_result = pyspark_dataframe.select("name", "age")
        assert_dataframes_equal(mock_result, pyspark_result)

    def test_basic_filter_still_works(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test that basic filter operations still work."""
        mock_result = mock_dataframe.filter(mock_functions.col("age") > 25)
        pyspark_result = pyspark_dataframe.filter(pyspark_functions.col("age") > 25)
        assert_dataframes_equal(mock_result, pyspark_result)

    def test_basic_groupby_still_works(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test that basic groupBy operations still work."""
        mock_result = mock_dataframe.groupBy("department").count()
        pyspark_result = pyspark_dataframe.groupBy("department").count()
        assert_dataframes_equal(mock_result, pyspark_result)
