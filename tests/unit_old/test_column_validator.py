"""
Unit tests for ColumnValidator.

Tests column validation logic extracted from MockDataFrame.
"""

import pytest
from mock_spark.dataframe.validation.column_validator import ColumnValidator
from mock_spark.spark_types import (
    MockStructType,
    MockStructField,
    StringType,
    IntegerType,
)
from mock_spark.functions import MockColumn, MockColumnOperation
from mock_spark.core.exceptions.operation import MockSparkColumnNotFoundError


class TestColumnValidator:
    """Test cases for ColumnValidator."""

    def setup_method(self):
        """Set up test fixtures."""
        self.schema = MockStructType(
            [
                MockStructField("name", StringType()),
                MockStructField("age", IntegerType()),
                MockStructField("salary", IntegerType()),
            ]
        )

    def test_validate_column_exists_success(self):
        """Test successful column validation."""
        # Should not raise any exception
        ColumnValidator.validate_column_exists(self.schema, "name", "select")
        ColumnValidator.validate_column_exists(self.schema, "age", "filter")
        ColumnValidator.validate_column_exists(self.schema, "salary", "groupBy")

    def test_validate_column_exists_failure(self):
        """Test column validation failure."""
        with pytest.raises(MockSparkColumnNotFoundError) as exc_info:
            ColumnValidator.validate_column_exists(self.schema, "nonexistent", "select")

        assert "nonexistent" in str(exc_info.value)
        assert "name" in str(exc_info.value)  # Should list available columns

    def test_validate_columns_exist_success(self):
        """Test successful multiple column validation."""
        # Should not raise any exception
        ColumnValidator.validate_columns_exist(self.schema, ["name", "age"], "select")
        ColumnValidator.validate_columns_exist(self.schema, ["salary"], "filter")

    def test_validate_columns_exist_failure(self):
        """Test multiple column validation failure."""
        with pytest.raises(MockSparkColumnNotFoundError) as exc_info:
            ColumnValidator.validate_columns_exist(
                self.schema, ["name", "nonexistent"], "select"
            )

        assert "nonexistent" in str(exc_info.value)

    def test_validate_filter_expression_simple_column(self):
        """Test filter expression validation with simple column reference."""
        # Simple column reference
        col = MockColumn("name")
        condition = MockColumnOperation("==", col, "Alice")

        # Should not raise exception
        ColumnValidator.validate_filter_expression(self.schema, condition, "filter")

    def test_validate_filter_expression_complex_operation(self):
        """Test filter expression validation with complex operations."""
        # Complex operations should be skipped
        col = MockColumn("name")
        condition = MockColumnOperation("and", col, MockColumn("age"))

        # Should not raise exception (complex operations are skipped)
        ColumnValidator.validate_filter_expression(self.schema, condition, "filter")

    def test_validate_filter_expression_empty_schema(self):
        """Test filter expression validation with empty schema."""
        empty_schema = MockStructType([])
        col = MockColumn("any_column")
        condition = MockColumnOperation("==", col, "value")

        # Should not raise exception (empty schemas skip validation)
        ColumnValidator.validate_filter_expression(empty_schema, condition, "filter")

    def test_validate_expression_columns_simple_column(self):
        """Test expression column validation with simple column."""
        col = MockColumn("name")

        # Should not raise exception
        ColumnValidator.validate_expression_columns(self.schema, col, "select")

    def test_validate_expression_columns_nested_operation(self):
        """Test expression column validation with nested operations."""
        col1 = MockColumn("name")
        col2 = MockColumn("age")
        nested_op = MockColumnOperation("+", col1, col2)

        # Should not raise exception
        ColumnValidator.validate_expression_columns(self.schema, nested_op, "select")

    def test_validate_expression_columns_mockdataframe_skip(self):
        """Test that MockDataFrame objects are skipped in validation."""
        # Create a mock object that looks like a MockDataFrame
        mock_df = type("MockDataFrame", (), {"data": [], "schema": self.schema})()

        # Should not raise exception (MockDataFrame objects are skipped)
        ColumnValidator.validate_expression_columns(self.schema, mock_df, "select")

    def test_validate_expression_columns_lazy_materialization(self):
        """Test expression column validation during lazy materialization."""
        col = MockColumn("name")

        # Should not raise exception when in lazy materialization
        ColumnValidator.validate_expression_columns(
            self.schema, col, "select", in_lazy_materialization=True
        )

    def test_validate_expression_columns_aliased_column(self):
        """Test expression column validation with aliased columns."""
        original_col = MockColumn("name")
        aliased_col = MockColumn("full_name")
        aliased_col._original_column = original_col

        # Should not raise exception
        ColumnValidator.validate_expression_columns(self.schema, aliased_col, "select")

    def test_validate_expression_columns_nested_validation(self):
        """Test recursive validation of nested expressions."""
        col1 = MockColumn("name")
        col2 = MockColumn("age")
        inner_op = MockColumnOperation("+", col1, col2)

        # Should not raise exception
        ColumnValidator.validate_expression_columns(self.schema, inner_op, "select")

    def test_validate_expression_columns_missing_column(self):
        """Test expression column validation with missing column."""
        col = MockColumn("nonexistent")

        with pytest.raises(MockSparkColumnNotFoundError):
            ColumnValidator.validate_expression_columns(self.schema, col, "select")

    def test_validate_expression_columns_nested_missing_column(self):
        """Test expression column validation with nested missing column."""
        col1 = MockColumn("name")
        col2 = MockColumn("nonexistent")
        nested_op = MockColumnOperation("+", col1, col2)

        with pytest.raises(MockSparkColumnNotFoundError):
            ColumnValidator.validate_expression_columns(
                self.schema, nested_op, "select"
            )
