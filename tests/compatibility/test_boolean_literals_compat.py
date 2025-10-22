"""
Compatibility tests for boolean literal support in Mock-Spark 2.10.0.

Tests the specific SQL generation fix for F.lit(True) and F.lit(False) that
was causing "Binder Error: Referenced column 'true' not found" issues.
"""

import pytest
from tests.compatibility.utils.comparison import assert_dataframes_equal


class TestBooleanLiteralsCompatibility:
    """Test boolean literal compatibility between Mock-Spark and PySpark."""

    def test_boolean_literal_true_compatibility(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test F.lit(True) compatibility - the main fix for SQL generation."""
        # Test basic boolean literal
        mock_result = mock_dataframe.select(mock_functions.lit(True).alias("is_active"))
        pyspark_result = pyspark_dataframe.select(pyspark_functions.lit(True).alias("is_active"))
        assert_dataframes_equal(mock_result, pyspark_result)

    def test_boolean_literal_false_compatibility(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test F.lit(False) compatibility."""
        # Test basic boolean literal
        mock_result = mock_dataframe.select(mock_functions.lit(False).alias("is_deleted"))
        pyspark_result = pyspark_dataframe.select(pyspark_functions.lit(False).alias("is_deleted"))
        assert_dataframes_equal(mock_result, pyspark_result)

    def test_boolean_literals_in_withcolumn(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test boolean literals in withColumn operations."""
        # Test adding boolean columns
        mock_result = mock_dataframe.withColumn("active", mock_functions.lit(True))
        pyspark_result = pyspark_dataframe.withColumn("active", pyspark_functions.lit(True))
        assert_dataframes_equal(mock_result, pyspark_result)

        # Test adding false boolean columns
        mock_result = mock_result.withColumn("deleted", mock_functions.lit(False))
        pyspark_result = pyspark_result.withColumn("deleted", pyspark_functions.lit(False))
        assert_dataframes_equal(mock_result, pyspark_result)

    def test_boolean_literals_in_filter(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test boolean literals in filter operations."""
        # Test filtering with boolean literals
        mock_result = mock_dataframe.filter(mock_functions.lit(True))
        pyspark_result = pyspark_dataframe.filter(pyspark_functions.lit(True))
        assert_dataframes_equal(mock_result, pyspark_result)

    def test_boolean_literals_mixed_operations(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test complex operations with boolean literals."""
        # Test multiple boolean literals in one operation
        mock_result = mock_dataframe.select(
            mock_functions.col("id"),
            mock_functions.lit(True).alias("is_valid"),
            mock_functions.lit(False).alias("is_processed")
        )
        pyspark_result = pyspark_dataframe.select(
            pyspark_functions.col("id"),
            pyspark_functions.lit(True).alias("is_valid"),
            pyspark_functions.lit(False).alias("is_processed")
        )
        assert_dataframes_equal(mock_result, pyspark_result)

    def test_boolean_literals_schema_compatibility(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test that boolean literals have correct schema types."""
        # Test schema inference for boolean literals
        mock_result = mock_dataframe.select(
            mock_functions.lit(True).alias("bool_true"),
            mock_functions.lit(False).alias("bool_false")
        )
        pyspark_result = pyspark_dataframe.select(
            pyspark_functions.lit(True).alias("bool_true"),
            pyspark_functions.lit(False).alias("bool_false")
        )
        
        # Check that schemas match
        mock_schema = mock_result.schema
        pyspark_schema = pyspark_result.schema
        
        assert len(mock_schema.fields) == len(pyspark_schema.fields)
        # Compare data type classes, not instances (nullable is field-level, not type-level)
        assert type(mock_schema.fields[0].dataType).__name__ == type(pyspark_schema.fields[0].dataType).__name__
        assert type(mock_schema.fields[1].dataType).__name__ == type(pyspark_schema.fields[1].dataType).__name__
        # Also verify field nullable properties
        assert mock_schema.fields[0].nullable == pyspark_schema.fields[0].nullable
        assert mock_schema.fields[1].nullable == pyspark_schema.fields[1].nullable

    def test_boolean_literals_with_conditions(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test boolean literals in conditional expressions."""
        # Test when().otherwise() with boolean literals
        mock_result = mock_dataframe.select(
            mock_functions.when(mock_functions.col("id") > 2, mock_functions.lit(True))
            .otherwise(mock_functions.lit(False))
            .alias("condition_result")
        )
        pyspark_result = pyspark_dataframe.select(
            pyspark_functions.when(pyspark_functions.col("id") > 2, pyspark_functions.lit(True))
            .otherwise(pyspark_functions.lit(False))
            .alias("condition_result")
        )
        assert_dataframes_equal(mock_result, pyspark_result)

    def test_boolean_literals_aggregation_compatibility(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test boolean literals in aggregation functions."""
        # Test boolean literals with aggregation
        mock_result = mock_dataframe.agg(
            mock_functions.lit(True).alias("all_true"),
            mock_functions.lit(False).alias("all_false")
        )
        pyspark_result = pyspark_dataframe.agg(
            pyspark_functions.lit(True).alias("all_true"),
            pyspark_functions.lit(False).alias("all_false")
        )
        assert_dataframes_equal(mock_result, pyspark_result)

    def test_boolean_literals_table_persistence(
        self, mock_spark, real_spark, mock_functions, pyspark_functions
    ):
        """Test boolean literals with table persistence (saveAsTable/spark.table)."""
        # Create test data with boolean literals
        test_data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        
        # Mock-Spark version
        mock_df = mock_spark.createDataFrame(test_data)
        mock_result = mock_df.withColumn("active", mock_functions.lit(True))
        mock_result.write.mode("overwrite").saveAsTable("test_boolean_table")
        mock_read = mock_spark.table("test_boolean_table")
        
        # PySpark version
        pyspark_df = real_spark.createDataFrame(test_data)
        pyspark_result = pyspark_df.withColumn("active", pyspark_functions.lit(True))
        pyspark_result.write.mode("overwrite").saveAsTable("test_boolean_table_pyspark")
        pyspark_read = real_spark.table("test_boolean_table_pyspark")
        
        # Compare results
        assert_dataframes_equal(mock_read, pyspark_read)
        
        # Clean up
        mock_spark.sql("DROP TABLE IF EXISTS test_boolean_table")
        real_spark.sql("DROP TABLE IF EXISTS test_boolean_table_pyspark")

    def test_boolean_literals_edge_cases(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test edge cases for boolean literals."""
        # Test empty DataFrame with boolean literals
        empty_mock = mock_dataframe.filter(mock_functions.lit(False))
        empty_pyspark = pyspark_dataframe.filter(pyspark_functions.lit(False))
        assert_dataframes_equal(empty_mock, empty_pyspark)
        
        # Test boolean literals with null handling
        mock_result = mock_dataframe.select(
            mock_functions.lit(True).alias("not_null_bool"),
            mock_functions.lit(False).alias("false_bool")
        )
        pyspark_result = pyspark_dataframe.select(
            pyspark_functions.lit(True).alias("not_null_bool"),
            pyspark_functions.lit(False).alias("false_bool")
        )
        assert_dataframes_equal(mock_result, pyspark_result)
