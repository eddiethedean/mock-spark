"""
Column validation for DataFrame operations.

This module provides centralized column validation logic that was previously
scattered throughout MockDataFrame, ensuring consistent validation across
all operations.
"""

from typing import Any, List
from ...spark_types import MockStructType
from ...functions import MockColumn, MockColumnOperation
from ...core.exceptions.operation import MockSparkColumnNotFoundError


class ColumnValidator:
    """Validates column existence and expressions for DataFrame operations.
    
    This class centralizes all column validation logic that was previously
    scattered throughout MockDataFrame, ensuring consistent validation
    across all operations.
    """
    
    @staticmethod
    def validate_column_exists(
        schema: MockStructType, 
        column_name: str, 
        operation: str
    ) -> None:
        """Validate that a single column exists in schema.
        
        Args:
            schema: The DataFrame schema to validate against.
            column_name: Name of the column to validate.
            operation: Name of the operation being performed (for error messages).
            
        Raises:
            MockSparkColumnNotFoundError: If column doesn't exist in schema.
        """
        # Skip validation for wildcard selector
        if column_name == "*":
            return
            
        column_names = [field.name for field in schema.fields]
        if column_name not in column_names:
            raise MockSparkColumnNotFoundError(column_name, column_names)
    
    @staticmethod
    def validate_columns_exist(
        schema: MockStructType, 
        column_names: List[str], 
        operation: str
    ) -> None:
        """Validate that multiple columns exist in schema.
        
        Args:
            schema: The DataFrame schema to validate against.
            column_names: List of column names to validate.
            operation: Name of the operation being performed (for error messages).
            
        Raises:
            MockSparkColumnNotFoundError: If any column doesn't exist in schema.
        """
        available_columns = [field.name for field in schema.fields]
        missing_columns = [col for col in column_names if col not in available_columns]
        if missing_columns:
            raise MockSparkColumnNotFoundError(missing_columns[0], available_columns)
    
    @staticmethod
    def validate_filter_expression(
        schema: MockStructType, 
        condition: Any, 
        operation: str,
        has_pending_joins: bool = False
    ) -> None:
        """Validate filter expressions before execution.
        
        Args:
            schema: The DataFrame schema to validate against.
            condition: The filter condition to validate.
            operation: Name of the operation being performed.
            has_pending_joins: Whether there are pending join operations.
        """
        # Skip validation for empty dataframes - they can filter on any column
        if len(schema.fields) == 0:
            return

        # Skip validation for complex expressions - let SQL generation handle them
        # Only validate simple column references

        # Skip validation for operations that create complex expressions
        if hasattr(condition, "operation") and condition.operation in [
            "between",
            "and",
            "or",
            "&",
            "|",
            "isin",
            "not_in",
            "!",
        ]:
            # These are complex expressions that may combine multiple columns, skip validation
            return

        if hasattr(condition, "column") and hasattr(condition.column, "name"):
            # Check if this is a complex operation before validating
            if hasattr(condition, "operation") and condition.operation in [
                "between",
                "and",
                "or",
                "&",
                "|",
                "isin",
                "not_in",
                "!",
            ]:
                return
            # Simple column reference
            ColumnValidator.validate_column_exists(schema, condition.column.name, operation)
        elif (
            hasattr(condition, "name")
            and not hasattr(condition, "operation")
            and not hasattr(condition, "value")
            and not hasattr(condition, "data_type")
        ):
            # Simple column reference without operation, value, or data_type (not a literal)
            ColumnValidator.validate_column_exists(schema, condition.name, operation)
        # For complex expressions (with operations, literals, etc.), skip validation
        # as they will be handled by SQL generation
    
    @staticmethod
    def validate_expression_columns(
        schema: MockStructType, 
        expression: Any, 
        operation: str,
        in_lazy_materialization: bool = False
    ) -> None:
        """Recursively validate column references in complex expressions.
        
        Args:
            schema: The DataFrame schema to validate against.
            expression: The expression to validate.
            operation: Name of the operation being performed.
            in_lazy_materialization: Whether we're in lazy materialization context.
        """
        if isinstance(expression, MockColumnOperation):
            # Check if this is a column reference
            if hasattr(expression, "column"):
                # Check if it's a MockDataFrame (has 'data' attribute) - skip validation
                if hasattr(expression.column, "data") and hasattr(
                    expression.column, "schema"
                ):
                    pass  # Skip MockDataFrame objects
                elif isinstance(expression.column, MockColumn):
                    if not in_lazy_materialization:
                        # Skip validation for wildcard selector
                        if expression.column.name != "*":
                            ColumnValidator.validate_column_exists(
                                schema, expression.column.name, operation
                            )

            # Recursively validate nested expressions
            if hasattr(expression, "column") and isinstance(
                expression.column, MockColumnOperation
            ):
                ColumnValidator.validate_expression_columns(
                    schema, expression.column, operation, in_lazy_materialization
                )
            if hasattr(expression, "value") and isinstance(
                expression.value, MockColumnOperation
            ):
                ColumnValidator.validate_expression_columns(
                    schema, expression.value, operation, in_lazy_materialization
                )
            elif hasattr(expression, "value") and isinstance(
                expression.value, MockColumn
            ):
                # Direct column reference in value
                if not in_lazy_materialization:
                    # Skip validation for wildcard selector
                    if expression.value.name != "*":
                        ColumnValidator.validate_column_exists(
                            schema, expression.value.name, operation
                        )
        elif isinstance(expression, MockColumn):
            # Check if this is an aliased column with an original column reference
            if (
                hasattr(expression, "_original_column")
                and expression._original_column is not None
            ):
                # This is an aliased column - validate the original column
                # Check if it's a MockDataFrame first
                if hasattr(expression._original_column, "data") and hasattr(
                    expression._original_column, "schema"
                ):
                    pass  # Skip MockDataFrame objects
                elif isinstance(expression._original_column, MockColumn):
                    if not in_lazy_materialization:
                        # Skip validation for wildcard selector
                        if expression._original_column.name != "*":
                            ColumnValidator.validate_column_exists(
                                schema, expression._original_column.name, operation
                            )
                elif isinstance(expression._original_column, MockColumnOperation):
                    ColumnValidator.validate_expression_columns(
                        schema, expression._original_column, operation, in_lazy_materialization
                    )
            elif hasattr(expression, "column") and isinstance(
                expression.column, MockColumn
            ):
                # This is a column operation - validate the column reference
                if not in_lazy_materialization:
                    # Skip validation for wildcard selector
                    if expression.column.name != "*":
                        ColumnValidator.validate_column_exists(schema, expression.column.name, operation)
            else:
                # Simple column reference - validate directly
                if not in_lazy_materialization:
                    # Skip validation for wildcard selector
                    if expression.name != "*":
                        ColumnValidator.validate_column_exists(schema, expression.name, operation)
