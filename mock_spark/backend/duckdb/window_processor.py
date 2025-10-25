"""
Window function processor for Mock Spark.

This module provides processing of window functions and specifications.
"""

from typing import Any

from .sql_expression_translator import SQLExpressionTranslator
from ...functions import MockColumnOperation


class WindowFunctionProcessor:
    """Processes window functions and specifications."""

    def __init__(self, expression_translator: SQLExpressionTranslator):
        """Initialize window function processor.

        Args:
            expression_translator: SQL expression translator for converting expressions
        """
        self.expression_translator = expression_translator

    def apply_window_function(self, table_class: Any, window_func: Any) -> Any:
        """Apply a window function to a table class.

        Args:
            table_class: SQLAlchemy table class
            window_func: Window function to apply

        Returns:
            SQLAlchemy expression for the window function
        """
        function_name = getattr(window_func, "function_name", "window_function")

        # Get window specification
        window_spec = window_func.window_spec

        # Build partition_by and order_by
        partition_by = []
        order_by = []

        if hasattr(window_spec, "_partition_by") and window_spec._partition_by:
            for col in window_spec._partition_by:
                if isinstance(col, str):
                    partition_by.append(getattr(table_class, col))
                elif hasattr(col, "name"):
                    partition_by.append(getattr(table_class, col.name))

        if hasattr(window_spec, "_order_by") and window_spec._order_by:
            for col in window_spec._order_by:
                if isinstance(col, str):
                    order_by.append(getattr(table_class, col))
                elif hasattr(col, "name"):
                    order_by.append(getattr(table_class, col.name))
                elif isinstance(col, MockColumnOperation):
                    if hasattr(col, "operation") and col.operation == "desc":
                        order_by.append(getattr(table_class, col.column.name).desc())
                    else:
                        order_by.append(getattr(table_class, col.column.name))

        # Build window function expression
        if function_name.upper() in [
            "ROW_NUMBER",
            "RANK",
            "DENSE_RANK",
            "CUME_DIST",
            "PERCENT_RANK",
        ]:
            # These functions don't take parameters
            from sqlalchemy import func

            return func.__getattr__(function_name.lower())().over(
                partition_by=partition_by, order_by=order_by
            )
        else:
            # Get column from the original function if it exists
            original_function = getattr(window_func, "function", None)
            if (
                original_function
                and hasattr(original_function, "column")
                and original_function.column
            ):
                column_name = getattr(original_function.column, "name", "unknown")
                column_expr = getattr(table_class, column_name)
                from sqlalchemy import func

                return func.__getattr__(function_name.lower())(column_expr).over(
                    partition_by=partition_by, order_by=order_by
                )
            else:
                from sqlalchemy import func

                return func.__getattr__(function_name.lower())().over(
                    partition_by=partition_by, order_by=order_by
                )
