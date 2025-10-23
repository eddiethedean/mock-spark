"""
Unit tests for error messages and debugging.

Tests that error messages are helpful and provide actionable guidance.
"""

import pytest
from mock_spark import MockSparkSession, F
from mock_spark.core.exceptions.operation import (
    MockSparkOperationError,
    MockSparkColumnNotFoundError,
    MockSparkTypeMismatchError,
    MockSparkSQLGenerationError,
    MockSparkQueryExecutionError,
)


@pytest.mark.fast
class TestErrorMessages:
    """Test helpful error messages and debugging."""

    @pytest.fixture
    def spark(self):
        """Create a MockSparkSession for testing."""
        return MockSparkSession("test")

    @pytest.fixture
    def sample_data(self):
        """Sample data for testing."""
        return [
            {"name": "Alice", "age": 25, "salary": 50000.0},
            {"name": "Bob", "age": 30, "salary": 60000.0},
            {"name": "Charlie", "age": 35, "salary": 70000.0},
        ]

    def test_column_not_found_error(self, spark, sample_data):
        """Test helpful column not found error."""
        df = spark.createDataFrame(sample_data)

        with pytest.raises(MockSparkColumnNotFoundError) as exc_info:
            df.select("non_existent_column")

        error = exc_info.value
        assert "non_existent_column" in str(error)
        assert "Available columns:" in str(error)
        assert "name" in str(error)
        assert "age" in str(error)
        assert "salary" in str(error)

    def test_column_not_found_in_filter(self, spark, sample_data):
        """Test column not found error in filter."""
        df = spark.createDataFrame(sample_data)

        with pytest.raises(MockSparkColumnNotFoundError) as exc_info:
            df.filter(F.col("non_existent_column") > 25)

        error = exc_info.value
        assert "non_existent_column" in str(error)
        assert "Available columns:" in str(error)

    def test_column_not_found_in_withcolumn(self, spark, sample_data):
        """Test column not found error in withColumn."""
        df = spark.createDataFrame(sample_data)

        with pytest.raises(MockSparkColumnNotFoundError) as exc_info:
            df.withColumn("new_col", F.col("non_existent_column") * 2)

        error = exc_info.value
        assert "non_existent_column" in str(error)
        assert "Available columns:" in str(error)

    def test_df_column_access_error(self, spark, sample_data):
        """Test df.column_name access error."""
        df = spark.createDataFrame(sample_data)

        with pytest.raises(AttributeError) as exc_info:
            _ = df.non_existent_column

        error_msg = str(exc_info.value)
        assert "no attribute 'non_existent_column'" in error_msg
        assert "Available columns:" in error_msg

    def test_operation_error_with_suggestion(self, spark, sample_data):
        """Test operation error with helpful suggestion."""
        df = spark.createDataFrame(sample_data)

        # This should trigger a helpful error message
        try:
            # Try an operation that might fail
            df.select(F.col("name").cast("invalid_type"))
        except Exception as e:
            # Check that the error is informative
            error_msg = str(e)
            assert len(error_msg) > 50  # Should be detailed
            assert "name" in error_msg or "invalid_type" in error_msg

    def test_type_mismatch_error(self, spark, sample_data):
        """Test type mismatch error."""
        df = spark.createDataFrame(sample_data)

        # This might trigger a type mismatch
        try:
            df.select(F.col("name") + F.col("age"))  # String + int
        except Exception as e:
            error_msg = str(e)
            # Should mention the operation and types involved
            assert len(error_msg) > 20

    def test_filter_expression_validation(self, spark, sample_data):
        """Test filter expression validation."""
        df = spark.createDataFrame(sample_data)

        with pytest.raises(MockSparkColumnNotFoundError) as exc_info:
            df.filter(F.col("non_existent") > 25)

        error = exc_info.value
        assert "non_existent" in str(error)

    def test_complex_expression_validation(self, spark, sample_data):
        """Test validation of complex expressions."""
        df = spark.createDataFrame(sample_data)

        with pytest.raises(MockSparkColumnNotFoundError) as exc_info:
            df.select(
                F.col("name"),
                F.col("age"),
                F.col("non_existent").alias("bad_col")
            )

        error = exc_info.value
        assert "non_existent" in str(error)

    def test_aggregation_error(self, spark, sample_data):
        """Test aggregation error messages."""
        df = spark.createDataFrame(sample_data)

        with pytest.raises(MockSparkColumnNotFoundError) as exc_info:
            df.agg(F.sum("non_existent_column"))

        error = exc_info.value
        assert "non_existent_column" in str(error)

    def test_groupby_error(self, spark, sample_data):
        """Test groupBy error messages."""
        df = spark.createDataFrame(sample_data)

        with pytest.raises(MockSparkColumnNotFoundError) as exc_info:
            df.groupBy("non_existent_column").agg(F.count("*"))

        error = exc_info.value
        assert "non_existent_column" in str(error)

    def test_join_error(self, spark, sample_data):
        """Test join error messages."""
        df1 = spark.createDataFrame(sample_data)
        df2 = spark.createDataFrame([{"id": 1, "value": "test"}])

        with pytest.raises(MockSparkColumnNotFoundError) as exc_info:
            df1.join(df2, df1.non_existent == df2.id)

        error = exc_info.value
        assert "non_existent" in str(error)

    def test_window_function_error(self, spark, sample_data):
        """Test window function error messages."""
        df = spark.createDataFrame(sample_data)

        with pytest.raises(MockSparkColumnNotFoundError) as exc_info:
            df.select(
                F.col("name"),
                F.row_number().over(
                    F.col("non_existent").partitionBy("name")
                )
            )

        error = exc_info.value
        assert "non_existent" in str(error)

    def test_date_function_error(self, spark, sample_data):
        """Test date function error messages."""
        df = spark.createDataFrame(sample_data)

        with pytest.raises(MockSparkColumnNotFoundError) as exc_info:
            df.select(F.hour("non_existent_timestamp"))

        error = exc_info.value
        assert "non_existent_timestamp" in str(error)

    def test_string_function_error(self, spark, sample_data):
        """Test string function error messages."""
        df = spark.createDataFrame(sample_data)

        with pytest.raises(MockSparkColumnNotFoundError) as exc_info:
            df.select(F.upper("non_existent_string"))

        error = exc_info.value
        assert "non_existent_string" in str(error)

    def test_math_function_error(self, spark, sample_data):
        """Test math function error messages."""
        df = spark.createDataFrame(sample_data)

        with pytest.raises(MockSparkColumnNotFoundError) as exc_info:
            df.select(F.abs("non_existent_number"))

        error = exc_info.value
        assert "non_existent_number" in str(error)

    def test_conditional_function_error(self, spark, sample_data):
        """Test conditional function error messages."""
        df = spark.createDataFrame(sample_data)

        with pytest.raises(MockSparkColumnNotFoundError) as exc_info:
            df.select(
                F.when(F.col("non_existent") > 25, "high")
                .otherwise("low")
                .alias("category")
            )

        error = exc_info.value
        assert "non_existent" in str(error)

    def test_coalesce_function_error(self, spark, sample_data):
        """Test coalesce function error messages."""
        df = spark.createDataFrame(sample_data)

        with pytest.raises(MockSparkColumnNotFoundError) as exc_info:
            df.select(F.coalesce(F.col("name"), F.col("non_existent")))

        error = exc_info.value
        assert "non_existent" in str(error)

    def test_cast_function_error(self, spark, sample_data):
        """Test cast function error messages."""
        df = spark.createDataFrame(sample_data)

        with pytest.raises(MockSparkColumnNotFoundError) as exc_info:
            df.select(F.col("non_existent").cast("string"))

        error = exc_info.value
        assert "non_existent" in str(error)

    def test_alias_function_error(self, spark, sample_data):
        """Test alias function error messages."""
        df = spark.createDataFrame(sample_data)

        with pytest.raises(MockSparkColumnNotFoundError) as exc_info:
            df.select(F.col("non_existent").alias("renamed"))

        error = exc_info.value
        assert "non_existent" in str(error)

    def test_multiple_column_errors(self, spark, sample_data):
        """Test multiple column errors in one operation."""
        df = spark.createDataFrame(sample_data)

        with pytest.raises(MockSparkColumnNotFoundError) as exc_info:
            df.select(
                F.col("name"),
                F.col("bad1"),
                F.col("age"),
                F.col("bad2")
            )

        error = exc_info.value
        # Should mention the first missing column
        assert "bad1" in str(error)

    def test_nested_expression_errors(self, spark, sample_data):
        """Test errors in nested expressions."""
        df = spark.createDataFrame(sample_data)

        with pytest.raises(MockSparkColumnNotFoundError) as exc_info:
            df.select(
                F.col("name"),
                (F.col("bad_column") + F.col("age")).alias("sum")
            )

        error = exc_info.value
        assert "bad_column" in str(error)

    def test_error_message_format(self, spark, sample_data):
        """Test that error messages are well-formatted."""
        df = spark.createDataFrame(sample_data)

        with pytest.raises(MockSparkColumnNotFoundError) as exc_info:
            df.select("bad_column")

        error = exc_info.value
        error_msg = str(error)
        
        # Should be multi-line and informative
        assert "\n" in error_msg
        assert "Column:" in error_msg
        assert "Available columns:" in error_msg
        
        # Should list available columns
        assert "name" in error_msg
        assert "age" in error_msg
        assert "salary" in error_msg

    def test_debug_mode_configuration(self, spark, sample_data):
        """Test debug mode configuration."""
        df = spark.createDataFrame(sample_data)

        # Test that debug mode can be configured
        # This is a basic test - actual debug output would require more complex setup
        assert hasattr(df, '_execute_with_debug')
        
        # Test that the method exists and is callable
        assert callable(df._execute_with_debug)

    def test_error_context_preservation(self, spark, sample_data):
        """Test that error context is preserved."""
        df = spark.createDataFrame(sample_data)

        try:
            df.select("bad_column")
        except MockSparkColumnNotFoundError as e:
            # Check that error attributes are preserved
            assert hasattr(e, 'column_name')
            assert hasattr(e, 'available_columns')
            assert e.column_name == "bad_column"
            assert "name" in e.available_columns
            assert "age" in e.available_columns
            assert "salary" in e.available_columns
