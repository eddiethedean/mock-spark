"""
Compatibility tests for column functions between mock_spark and PySpark.
"""

import pytest
from tests.compatibility.utils.comparison import assert_dataframes_equal


class TestColumnFunctions:
    """Test column functions compatibility."""

    def test_col_function(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test col() function."""
        mock_result = mock_dataframe.select(mock_functions.col("name"))
        pyspark_result = pyspark_dataframe.select(pyspark_functions.col("name"))
        assert_dataframes_equal(mock_result, pyspark_result)

    def test_lit_function(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test lit() function."""
        mock_result = mock_dataframe.select(mock_functions.lit("constant_value"))
        pyspark_result = pyspark_dataframe.select(
            pyspark_functions.lit("constant_value")
        )
        assert_dataframes_equal(mock_result, pyspark_result)

    def test_lit_function_with_existing_column(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test lit() function combined with existing column."""
        mock_result = mock_dataframe.select(
            mock_functions.col("name"), mock_functions.lit("constant_value")
        )
        pyspark_result = pyspark_dataframe.select(
            pyspark_functions.col("name"), pyspark_functions.lit("constant_value")
        )
        assert_dataframes_equal(mock_result, pyspark_result)

    def test_lit_function_different_types(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test lit() function with different data types."""
        # Test with integer literal
        mock_result = mock_dataframe.select(mock_functions.lit(42))
        pyspark_result = pyspark_dataframe.select(pyspark_functions.lit(42))
        assert_dataframes_equal(mock_result, pyspark_result)

        # Test with float literal
        mock_result = mock_dataframe.select(mock_functions.lit(3.14))
        pyspark_result = pyspark_dataframe.select(pyspark_functions.lit(3.14))
        assert_dataframes_equal(mock_result, pyspark_result)

        # Test with boolean literal
        mock_result = mock_dataframe.select(mock_functions.lit(True))
        pyspark_result = pyspark_dataframe.select(pyspark_functions.lit(True))
        assert_dataframes_equal(mock_result, pyspark_result)

    def test_lit_function_column_naming(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test that lit() function creates columns with correct names."""
        mock_result = mock_dataframe.select(mock_functions.lit("test_value"))
        pyspark_result = pyspark_dataframe.select(pyspark_functions.lit("test_value"))

        # Check that column names match
        assert mock_result.columns == pyspark_result.columns
        assert "test_value" in mock_result.columns
        assert "test_value" in pyspark_result.columns

        # Check that all values are the literal value
        mock_data = mock_result.collect()
        pyspark_data = pyspark_result.collect()

        for mock_row, pyspark_row in zip(mock_data, pyspark_data):
            assert mock_row["test_value"] == pyspark_row["test_value"]
            assert mock_row["test_value"] == "test_value"

    def test_col_operations(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test column operations."""
        # Test equality
        mock_result = mock_dataframe.filter(mock_functions.col("age") == 25)
        pyspark_result = pyspark_dataframe.filter(pyspark_functions.col("age") == 25)
        assert_dataframes_equal(mock_result, pyspark_result)

        # Test inequality
        mock_result = mock_dataframe.filter(mock_functions.col("age") != 25)
        pyspark_result = pyspark_dataframe.filter(pyspark_functions.col("age") != 25)
        assert_dataframes_equal(mock_result, pyspark_result)

        # Test greater than
        mock_result = mock_dataframe.filter(mock_functions.col("age") > 30)
        pyspark_result = pyspark_dataframe.filter(pyspark_functions.col("age") > 30)
        assert_dataframes_equal(mock_result, pyspark_result)

        # Test less than
        mock_result = mock_dataframe.filter(mock_functions.col("age") < 30)
        pyspark_result = pyspark_dataframe.filter(pyspark_functions.col("age") < 30)
        assert_dataframes_equal(mock_result, pyspark_result)

        # Test greater than or equal
        mock_result = mock_dataframe.filter(mock_functions.col("age") >= 30)
        pyspark_result = pyspark_dataframe.filter(pyspark_functions.col("age") >= 30)
        assert_dataframes_equal(mock_result, pyspark_result)

        # Test less than or equal
        mock_result = mock_dataframe.filter(mock_functions.col("age") <= 30)
        pyspark_result = pyspark_dataframe.filter(pyspark_functions.col("age") <= 30)
        assert_dataframes_equal(mock_result, pyspark_result)

    def test_arithmetic_operations(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test arithmetic operations on columns."""
        # Addition
        mock_result = mock_dataframe.select(mock_functions.col("age") + 1)
        pyspark_result = pyspark_dataframe.select(pyspark_functions.col("age") + 1)
        assert_dataframes_equal(mock_result, pyspark_result)

        # Subtraction
        mock_result = mock_dataframe.select(mock_functions.col("age") - 1)
        pyspark_result = pyspark_dataframe.select(pyspark_functions.col("age") - 1)
        assert_dataframes_equal(mock_result, pyspark_result)

        # Multiplication
        mock_result = mock_dataframe.select(mock_functions.col("age") * 2)
        pyspark_result = pyspark_dataframe.select(pyspark_functions.col("age") * 2)
        assert_dataframes_equal(mock_result, pyspark_result)

        # Division
        mock_result = mock_dataframe.select(mock_functions.col("age") / 2)
        pyspark_result = pyspark_dataframe.select(pyspark_functions.col("age") / 2)
        assert_dataframes_equal(mock_result, pyspark_result)

    def test_logical_operations(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test logical operations."""
        # AND operation
        mock_result = mock_dataframe.filter(
            (mock_functions.col("age") > 25) & (mock_functions.col("age") < 35)
        )
        pyspark_result = pyspark_dataframe.filter(
            (pyspark_functions.col("age") > 25) & (pyspark_functions.col("age") < 35)
        )
        assert_dataframes_equal(mock_result, pyspark_result)

        # OR operation
        mock_result = mock_dataframe.filter(
            (mock_functions.col("age") == 25) | (mock_functions.col("age") == 35)
        )
        pyspark_result = pyspark_dataframe.filter(
            (pyspark_functions.col("age") == 25) | (pyspark_functions.col("age") == 35)
        )
        assert_dataframes_equal(mock_result, pyspark_result)

    def test_null_check_operations(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test null check operations."""
        # isNull
        mock_result = mock_dataframe.filter(mock_functions.col("name").isNull())
        pyspark_result = pyspark_dataframe.filter(
            pyspark_functions.col("name").isNull()
        )
        assert_dataframes_equal(mock_result, pyspark_result)

        # isNotNull
        mock_result = mock_dataframe.filter(mock_functions.col("name").isNotNull())
        pyspark_result = pyspark_dataframe.filter(
            pyspark_functions.col("name").isNotNull()
        )
        assert_dataframes_equal(mock_result, pyspark_result)


class TestAggregateFunctions:
    """Test aggregate functions compatibility."""

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

    def test_count_column_naming_compatibility(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test that count function column naming matches PySpark."""
        # Test count("*") column naming
        mock_result_star = mock_dataframe.select(mock_functions.count("*"))
        pyspark_result_star = pyspark_dataframe.select(pyspark_functions.count("*"))

        assert mock_result_star.columns == pyspark_result_star.columns
        assert "count(1)" in mock_result_star.columns
        assert "count(1)" in pyspark_result_star.columns

        # Test count("column") column naming
        mock_result_col = mock_dataframe.select(mock_functions.count("name"))
        pyspark_result_col = pyspark_dataframe.select(pyspark_functions.count("name"))

        assert mock_result_col.columns == pyspark_result_col.columns
        assert "count(name)" in mock_result_col.columns
        assert "count(name)" in pyspark_result_col.columns

    def test_groupby_count_method_vs_agg_compatibility(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test that groupBy().count() vs groupBy().agg(count("*")) have different column names."""
        # Test groupBy().count() method
        mock_result_method = mock_dataframe.groupBy("department").count()
        pyspark_result_method = pyspark_dataframe.groupBy("department").count()

        assert mock_result_method.columns == pyspark_result_method.columns
        assert "count" in mock_result_method.columns
        assert "count" in pyspark_result_method.columns

        # Test groupBy().agg(count("*"))
        mock_result_agg = mock_dataframe.groupBy("department").agg(
            mock_functions.count("*")
        )
        pyspark_result_agg = pyspark_dataframe.groupBy("department").agg(
            pyspark_functions.count("*")
        )

        assert mock_result_agg.columns == pyspark_result_agg.columns
        assert "count(1)" in mock_result_agg.columns
        assert "count(1)" in pyspark_result_agg.columns

        # Verify the column names are different between method and agg
        assert "count" != "count(1)"

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


class TestWindowFunctions:
    """Test window functions compatibility."""

    def test_row_number_function(
        self,
        mock_dataframe,
        pyspark_dataframe,
        mock_functions,
        pyspark_functions,
        mock_types,
        pyspark_types,
    ):
        """Test row_number() function."""
        # Note: Window functions require Window specification
        try:
            from pyspark.sql.window import Window as PySparkWindow
            from mock_spark.window import MockWindow

            # Create window specification
            mock_window = MockWindow.orderBy(mock_functions.col("age"))
            pyspark_window = PySparkWindow.orderBy(pyspark_functions.col("age"))

            mock_result = mock_dataframe.select(
                mock_functions.col("*"),
                mock_functions.row_number().over(mock_window).alias("row_num"),
            )
            pyspark_result = pyspark_dataframe.select(
                pyspark_functions.col("*"),
                pyspark_functions.row_number().over(pyspark_window).alias("row_num"),
            )
            assert_dataframes_equal(mock_result, pyspark_result)
        except ImportError:
            # Skip if window functions are not implemented in mock_spark yet
            # Window functions should now be implemented
            raise AssertionError("Window functions should be implemented")


class TestStringFunctions:
    """Test string functions compatibility."""

    def test_upper_function(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test upper() function."""
        mock_result = mock_dataframe.select(
            mock_functions.upper(mock_functions.col("name"))
        )
        pyspark_result = pyspark_dataframe.select(
            pyspark_functions.upper(pyspark_functions.col("name"))
        )
        assert_dataframes_equal(mock_result, pyspark_result)

    def test_lower_function(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test lower() function."""
        mock_result = mock_dataframe.select(
            mock_functions.lower(mock_functions.col("name"))
        )
        pyspark_result = pyspark_dataframe.select(
            pyspark_functions.lower(pyspark_functions.col("name"))
        )
        assert_dataframes_equal(mock_result, pyspark_result)

    def test_length_function(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test length() function."""
        mock_result = mock_dataframe.select(
            mock_functions.length(mock_functions.col("name"))
        )
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
        mock_data_with_neg = mock_dataframe.withColumn(
            "age_neg", mock_functions.col("age") * -1
        )
        pyspark_data_with_neg = pyspark_dataframe.withColumn(
            "age_neg", pyspark_functions.col("age") * -1
        )

        mock_result = mock_data_with_neg.select(
            mock_functions.abs(mock_functions.col("age_neg"))
        )
        pyspark_result = pyspark_data_with_neg.select(
            pyspark_functions.abs(pyspark_functions.col("age_neg"))
        )
        assert_dataframes_equal(mock_result, pyspark_result)

    def test_round_function(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test round() function."""
        mock_result = mock_dataframe.select(
            mock_functions.round(mock_functions.col("salary"), 2)
        )
        pyspark_result = pyspark_dataframe.select(
            pyspark_functions.round(pyspark_functions.col("salary"), 2)
        )
        assert_dataframes_equal(mock_result, pyspark_result)


# Test that will help identify discrepancies in function behavior
class TestFunctionDiscrepancyDetection:
    """Tests specifically designed to detect function discrepancies."""

    def test_function_return_types(self, mock_functions, pyspark_functions):
        """Test that functions return compatible types."""
        # Test col() function
        mock_col = mock_functions.col("test_column")
        pyspark_col = pyspark_functions.col("test_column")

        # Both should be column-like objects
        assert hasattr(mock_col, "name") or hasattr(mock_col, "column_name")
        assert hasattr(pyspark_col, "name")

        # Test lit() function
        mock_lit = mock_functions.lit("test_value")
        pyspark_lit = pyspark_functions.lit("test_value")

        # Both should be column-like objects
        assert hasattr(mock_lit, "name") or hasattr(mock_lit, "column_name")
        assert hasattr(pyspark_lit, "name")

    def test_aggregate_function_types(self, mock_functions, pyspark_functions):
        """Test that aggregate functions return compatible types."""
        # Test count() function
        mock_count = mock_functions.count("*")
        pyspark_count = pyspark_functions.count("*")

        # Both should be aggregate function objects
        assert hasattr(mock_count, "function_name") or hasattr(mock_count, "name")
        assert hasattr(pyspark_count, "name")

        # Test sum() function
        mock_sum = mock_functions.sum("test_column")
        pyspark_sum = pyspark_functions.sum("test_column")

        # Both should be aggregate function objects
        assert hasattr(mock_sum, "function_name") or hasattr(mock_sum, "name")
        assert hasattr(pyspark_sum, "name")

    def test_operation_chaining(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test complex operation chaining."""
        # Complex chained operations
        mock_result = (
            mock_dataframe.select(
                mock_functions.col("name"),
                mock_functions.col("age"),
                (mock_functions.col("age") + 1).alias("age_plus_one"),
                mock_functions.upper(mock_functions.col("name")).alias("name_upper"),
            )
            .filter(mock_functions.col("age") > 25)
            .orderBy(mock_functions.col("age"))
        )

        pyspark_result = (
            pyspark_dataframe.select(
                pyspark_functions.col("name"),
                pyspark_functions.col("age"),
                (pyspark_functions.col("age") + 1).alias("age_plus_one"),
                pyspark_functions.upper(pyspark_functions.col("name")).alias(
                    "name_upper"
                ),
            )
            .filter(pyspark_functions.col("age") > 25)
            .orderBy(pyspark_functions.col("age"))
        )

        assert_dataframes_equal(mock_result, pyspark_result)
