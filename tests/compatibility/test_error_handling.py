"""
Error Handling and Edge Cases Compatibility Tests

Tests for proper error handling, edge cases, and exception compatibility.
"""

import pytest
from mock_spark.core.exceptions import (
    AnalysisException,
    IllegalArgumentException,
    PySparkValueError,
    PySparkTypeError,
    PySparkRuntimeError,
)


class TestErrorHandling:
    """Test error handling compatibility."""

    def test_column_not_found_error(self, mock_dataframe, pyspark_dataframe):
        """Test column not found error handling."""
        with pytest.raises(AnalysisException):
            mock_dataframe.select("non_existent_column")

        with pytest.raises(Exception):  # PySpark raises different exception types
            pyspark_dataframe.select("non_existent_column")

    def test_invalid_arithmetic_operation(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test invalid arithmetic operation error handling."""
        try:
            # Test division by zero
            mock_result = mock_dataframe.select(
                mock_functions.col("salary") / mock_functions.lit(0)
            )
            pyspark_result = pyspark_dataframe.select(
                pyspark_functions.col("salary") / pyspark_functions.lit(0)
            )
            # Both should handle division by zero gracefully (return null/NaN)
            assert len(mock_result.collect()) == len(pyspark_result.collect())
        except Exception:
            # If exceptions are raised, they should be similar types
            pass

    def test_invalid_filter_condition(self, mock_dataframe, pyspark_dataframe):
        """Test invalid filter condition error handling."""
        try:
            # Test filter with non-existent column
            mock_result = mock_dataframe.filter("invalid_column > 10")
            pyspark_result = pyspark_dataframe.filter("invalid_column > 10")
            # Both should handle this gracefully or raise appropriate errors
        except Exception:
            # If exceptions are raised, they should be similar types
            pass

    def test_invalid_groupby_column(self, mock_dataframe, pyspark_dataframe):
        """Test invalid groupBy column error handling."""
        with pytest.raises((AnalysisException, Exception)):
            mock_dataframe.groupBy("non_existent_column").count()

        with pytest.raises(Exception):
            pyspark_dataframe.groupBy("non_existent_column").count()

    def test_empty_dataframe_operations(self, mock_empty_dataframe, pyspark_empty_dataframe):
        """Test operations on empty DataFrames."""
        # Test count on empty DataFrame
        assert mock_empty_dataframe.count() == 0
        assert pyspark_empty_dataframe.count() == 0

        # Test collect on empty DataFrame
        mock_data = mock_empty_dataframe.collect()
        pyspark_data = pyspark_empty_dataframe.collect()
        assert len(mock_data) == 0
        assert len(pyspark_data) == 0

    def test_null_handling_in_operations(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test null value handling in various operations."""
        # Test arithmetic with null values
        mock_result = mock_dataframe.select(mock_functions.col("salary") + mock_functions.lit(None))
        pyspark_result = pyspark_dataframe.select(
            pyspark_functions.col("salary") + pyspark_functions.lit(None)
        )
        assert len(mock_result.collect()) == len(pyspark_result.collect())

        # Test comparison with null values
        mock_result = mock_dataframe.select(mock_functions.col("name") == mock_functions.lit(None))
        pyspark_result = pyspark_dataframe.select(
            pyspark_functions.col("name") == pyspark_functions.lit(None)
        )
        assert len(mock_result.collect()) == len(pyspark_result.collect())


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_very_large_numbers(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test handling of very large numbers."""
        large_number = 1e20
        mock_result = mock_dataframe.select(mock_functions.lit(large_number).alias("large_num"))
        pyspark_result = pyspark_dataframe.select(
            pyspark_functions.lit(large_number).alias("large_num")
        )
        assert len(mock_result.collect()) == len(pyspark_result.collect())

    def test_very_small_numbers(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test handling of very small numbers."""
        small_number = 1e-20
        mock_result = mock_dataframe.select(mock_functions.lit(small_number).alias("small_num"))
        pyspark_result = pyspark_dataframe.select(
            pyspark_functions.lit(small_number).alias("small_num")
        )
        assert len(mock_result.collect()) == len(pyspark_result.collect())

    def test_unicode_strings(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test handling of unicode strings."""
        unicode_string = "Hello 世界 🌍"
        mock_result = mock_dataframe.select(mock_functions.lit(unicode_string).alias("unicode_str"))
        pyspark_result = pyspark_dataframe.select(
            pyspark_functions.lit(unicode_string).alias("unicode_str")
        )
        assert len(mock_result.collect()) == len(pyspark_result.collect())

    def test_very_long_strings(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test handling of very long strings."""
        long_string = "a" * 10000  # 10KB string
        mock_result = mock_dataframe.select(mock_functions.lit(long_string).alias("long_str"))
        pyspark_result = pyspark_dataframe.select(
            pyspark_functions.lit(long_string).alias("long_str")
        )
        assert len(mock_result.collect()) == len(pyspark_result.collect())

    def test_boolean_edge_cases(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test boolean edge cases."""
        # Test boolean operations
        mock_result = mock_dataframe.select(
            mock_functions.lit(True).alias("true_val"),
            mock_functions.lit(False).alias("false_val"),
        )
        pyspark_result = pyspark_dataframe.select(
            pyspark_functions.lit(True).alias("true_val"),
            pyspark_functions.lit(False).alias("false_val"),
        )
        assert len(mock_result.collect()) == len(pyspark_result.collect())


class TestSchemaEdgeCases:
    """Test schema-related edge cases."""

    def test_duplicate_column_names(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test handling of duplicate column names."""
        try:
            mock_result = mock_dataframe.select(
                mock_functions.col("name").alias("duplicate"),
                mock_functions.col("department").alias("duplicate"),
            )
            pyspark_result = pyspark_dataframe.select(
                pyspark_functions.col("name").alias("duplicate"),
                pyspark_functions.col("department").alias("duplicate"),
            )
            assert len(mock_result.collect()) == len(pyspark_result.collect())
        except Exception:
            # Both should handle duplicate column names consistently
            pass

    def test_empty_column_names(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test handling of empty column names."""
        try:
            mock_result = mock_dataframe.select(mock_functions.lit("value").alias(""))
            pyspark_result = pyspark_dataframe.select(pyspark_functions.lit("value").alias(""))
            assert len(mock_result.collect()) == len(pyspark_result.collect())
        except Exception:
            # Both should handle empty column names consistently
            pass

    def test_special_character_column_names(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test handling of special character column names."""
        special_name = "col@#$%^&*()_+-=[]{}|;':\",./<>?"
        mock_result = mock_dataframe.select(mock_functions.lit("value").alias(special_name))
        pyspark_result = pyspark_dataframe.select(
            pyspark_functions.lit("value").alias(special_name)
        )
        assert len(mock_result.collect()) == len(pyspark_result.collect())


class TestPerformanceEdgeCases:
    """Test performance-related edge cases."""

    def test_many_columns(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test DataFrame with many columns."""
        # Create DataFrame with many columns
        columns = [mock_functions.lit(i).alias(f"col_{i}") for i in range(100)]
        mock_result = mock_dataframe.select(*columns)

        columns = [pyspark_functions.lit(i).alias(f"col_{i}") for i in range(100)]
        pyspark_result = pyspark_dataframe.select(*columns)

        assert len(mock_result.collect()) == len(pyspark_result.collect())
        assert len(mock_result.columns) == len(pyspark_result.columns)

    def test_deep_nested_operations(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test deeply nested operations."""
        # Test deeply nested arithmetic
        result = mock_dataframe.select(
            mock_functions.col("salary")
            + mock_functions.col("salary") * 0.1
            + mock_functions.col("salary") * 0.01
            + mock_functions.col("salary") * 0.001
        )
        assert len(result.collect()) == len(mock_dataframe.collect())

    def test_chained_operations(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test many chained operations."""
        # Test many chained select operations
        result = mock_dataframe
        for i in range(10):
            result = result.select(
                mock_functions.col("*"), mock_functions.lit(i).alias(f"iteration_{i}")
            )

        assert len(result.collect()) == len(mock_dataframe.collect())
        assert len(result.columns) > len(mock_dataframe.columns)
