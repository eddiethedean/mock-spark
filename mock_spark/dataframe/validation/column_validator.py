"""
Column validation for DataFrame operations.

This module provides centralized column validation logic that was previously
scattered throughout DataFrame, ensuring consistent validation across
all operations.
"""

from typing import Any
from ...spark_types import StructType
from ...functions import Column, ColumnOperation
from ...core.exceptions.operation import SparkColumnNotFoundError


def is_literal(expression: Any) -> bool:
    """Check if expression is a literal value that doesn't need column validation.

    Args:
        expression: The expression to check

    Returns:
        True if expression is a literal value (Literal, str, int, etc)
    """
    from ...functions.core.literals import Literal

    # Check if it's a Literal
    if isinstance(expression, Literal):
        return True

    # Check if it's a ColumnOperation with a Literal
    if isinstance(expression, ColumnOperation):
        if hasattr(expression, "value") and isinstance(expression.value, Literal):
            return True
        if hasattr(expression, "column") and isinstance(expression.column, Literal):
            return True

    # Check if it's a string representation of a Literal
    return bool(
        isinstance(expression, str)
        and "<mock_spark.functions.core.literals.Literal" in expression
    )


class ColumnValidator:
    """Validates column existence and expressions for DataFrame operations.

    This class centralizes all column validation logic that was previously
    scattered throughout DataFrame, ensuring consistent validation
    across all operations.
    """

    @staticmethod
    def validate_column_exists(
        schema: StructType, column_name: str, operation: str
    ) -> None:
        """Validate that a single column exists in schema.

        Args:
            schema: The DataFrame schema to validate against.
            column_name: Name of the column to validate.
            operation: Name of the operation being performed (for error messages).

        Raises:
            SparkColumnNotFoundError: If column doesn't exist in schema.
        """
        # Skip validation for wildcard selector
        if column_name == "*":
            return

        column_names = [field.name for field in schema.fields]
        if column_name not in column_names:
            raise SparkColumnNotFoundError(column_name, column_names)

    @staticmethod
    def validate_columns_exist(
        schema: StructType, column_names: list[str], operation: str
    ) -> None:
        """Validate that multiple columns exist in schema.

        Args:
            schema: The DataFrame schema to validate against.
            column_names: List of column names to validate.
            operation: Name of the operation being performed (for error messages).

        Raises:
            SparkColumnNotFoundError: If any column doesn't exist in schema.
        """
        available_columns = [field.name for field in schema.fields]
        missing_columns = [col for col in column_names if col not in available_columns]
        if missing_columns:
            raise SparkColumnNotFoundError(missing_columns[0], available_columns)

    @staticmethod
    def validate_filter_expression(
        schema: StructType,
        condition: Any,
        operation: str,
        has_pending_joins: bool = False,
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

        # Import ColumnOperation for type checking
        from mock_spark.functions.base import ColumnOperation

        # If condition is a ColumnOperation, validate its column references
        if isinstance(condition, ColumnOperation):
            # Validate operations that reference columns
            if hasattr(condition, "column"):
                # For filter operations, use lazy materialization mode to allow
                # column references from original DataFrame context (PySpark behavior)
                is_lazy = (operation == "filter" and has_pending_joins) or (
                    operation == "filter"
                )  # Always allow lazy mode for filters
                # Recursively validate the column references in the expression
                ColumnValidator.validate_expression_columns(
                    schema, condition, operation, in_lazy_materialization=is_lazy
                )
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
                ">",
                "<",
                ">=",
                "<=",
                "==",
                "!=",
                "*",
                "+",
                "-",
                "/",
            ]:
                # Validate column references in the expression
                # For filter operations, allow lazy materialization mode
                is_lazy = operation == "filter"
                ColumnValidator.validate_expression_columns(
                    schema, condition, operation, in_lazy_materialization=is_lazy
                )
                return
            # Simple column reference
            ColumnValidator.validate_column_exists(
                schema, condition.column.name, operation
            )
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
        schema: StructType,
        expression: Any,
        operation: str,
        in_lazy_materialization: bool = False,
    ) -> None:
        """Recursively validate column references in complex expressions.

        Args:
            schema: The DataFrame schema to validate against.
            expression: The expression to validate.
            operation: Name of the operation being performed.
            in_lazy_materialization: Whether we're in lazy materialization context.
        """
        # Skip validation for literal values
        if is_literal(expression):
            return

        if isinstance(expression, ColumnOperation):
            # Skip validation for expr operations - they don't reference actual columns
            if hasattr(expression, "operation") and expression.operation == "expr":
                return

            # Check if this is a column reference
            if hasattr(expression, "column"):
                # Check if it's a Literal - skip validation
                if is_literal(expression.column):
                    pass  # Skip literals
                # Check if it's a DataFrame (has 'data' attribute) - skip validation
                elif hasattr(expression.column, "data") and hasattr(
                    expression.column, "schema"
                ):
                    pass  # Skip DataFrame objects
                elif isinstance(expression.column, ColumnOperation):
                    # The column itself is a ColumnOperation (e.g., struct, array) - validate it recursively
                    ColumnValidator.validate_expression_columns(
                        schema, expression.column, operation, in_lazy_materialization
                    )
                elif isinstance(expression.column, Column):
                    # Skip validation for dummy columns created by F.expr() and F.struct()
                    if expression.column.name in (
                        "__expr__",
                        "__struct_dummy__",
                        "__create_map_base__",
                        "__create_map_dummy__",
                    ):
                        return

                    # Check if the column name is actually a Literal (string representation)
                    col_name = expression.column.name
                    if (
                        isinstance(col_name, str)
                        and "<mock_spark.functions.core.literals.Literal" in col_name
                    ):
                        # This is a Literal used as a column - skip validation
                        pass
                    elif col_name != "*" and (
                        not in_lazy_materialization or operation != "filter"
                    ):
                        # Skip validation for wildcard selector
                        # In lazy materialization mode (filter after select), allow column references
                        # that might be from original DataFrame context
                        ColumnValidator.validate_column_exists(
                            schema, col_name, operation
                        )
                        # For filter operations in lazy context, allow column references
                        # that don't exist in current schema - they'll be resolved during materialization

            # Recursively validate nested expressions
            if hasattr(expression, "column"):
                if is_literal(expression.column):
                    # Skip validation for literals used as columns
                    pass
                elif isinstance(expression.column, ColumnOperation):
                    ColumnValidator.validate_expression_columns(
                        schema, expression.column, operation, in_lazy_materialization
                    )
            if hasattr(expression, "value") and isinstance(
                expression.value, ColumnOperation
            ):
                ColumnValidator.validate_expression_columns(
                    schema, expression.value, operation, in_lazy_materialization
                )
            elif hasattr(expression, "value") and is_literal(expression.value):
                # Skip validation for literals
                pass
            elif (
                hasattr(expression, "value")
                and isinstance(expression.value, Column)
                and not in_lazy_materialization
                and expression.value.name != "*"
            ):
                # Direct column reference in value
                # Skip validation for wildcard selector
                ColumnValidator.validate_column_exists(
                    schema, expression.value.name, operation
                )
            # Handle list/tuple of values (e.g., create_map with multiple args, array with literals)
            elif hasattr(expression, "value") and isinstance(
                expression.value, (list, tuple)
            ):
                for item in expression.value:
                    if is_literal(item):
                        continue  # Skip literals
                    elif isinstance(item, ColumnOperation):
                        # Recursively validate nested ColumnOperations (e.g., struct inside array)
                        ColumnValidator.validate_expression_columns(
                            schema, item, operation, in_lazy_materialization
                        )
                    elif (
                        isinstance(item, Column)
                        and not in_lazy_materialization
                        and item.name != "*"
                    ):
                        ColumnValidator.validate_column_exists(
                            schema, item.name, operation
                        )
                    # Skip other non-column types
        elif isinstance(expression, Column):
            # Check if this is an aliased column with an original column reference
            if (
                hasattr(expression, "_original_column")
                and expression._original_column is not None
            ):
                # This is an aliased column - validate the original column
                # Check if it's a DataFrame first
                if hasattr(expression._original_column, "data") and hasattr(
                    expression._original_column, "schema"
                ):
                    pass  # Skip DataFrame objects
                elif isinstance(expression._original_column, Column):
                    if (
                        not in_lazy_materialization
                        and expression._original_column.name != "*"
                    ):
                        # Skip validation for wildcard selector
                        ColumnValidator.validate_column_exists(
                            schema, expression._original_column.name, operation
                        )
                elif isinstance(expression._original_column, ColumnOperation):  # type: ignore[unreachable]
                    ColumnValidator.validate_expression_columns(
                        schema,
                        expression._original_column,
                        operation,
                        in_lazy_materialization,
                    )
            elif hasattr(expression, "column") and isinstance(
                expression.column, Column
            ):
                # This is a column operation - validate the column reference
                if not in_lazy_materialization and expression.column.name != "*":
                    # Skip validation for wildcard selector
                    ColumnValidator.validate_column_exists(
                        schema, expression.column.name, operation
                    )
            else:
                # Simple column reference - validate directly
                if not in_lazy_materialization and expression.name != "*":
                    # Skip validation for wildcard selector
                    ColumnValidator.validate_column_exists(
                        schema, expression.name, operation
                    )
