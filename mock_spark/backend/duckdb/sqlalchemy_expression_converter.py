"""
SQLAlchemy Expression Converter for Mock Spark.

This module provides conversion of Mock Spark expressions to SQLAlchemy expressions,
handling conditions, columns, values, and window functions.
"""

from typing import Any, Optional
from sqlalchemy import and_, or_, func, literal_column, text
from sqlalchemy.sql import ColumnElement

from ...functions.core.column import MockColumn, MockColumnOperation
from ...functions.core.literals import MockLiteral
from ...functions.window_execution import MockWindowFunction


class SQLAlchemyExpressionConverter:
    """Converts Mock Spark expressions to SQLAlchemy expressions."""

    def __init__(self):
        """Initialize the SQLAlchemy expression converter."""
        pass

    def condition_to_sqlalchemy(self, table_obj: Any, condition: Any) -> Any:
        """Convert a condition to SQLAlchemy expression."""
        if isinstance(condition, MockColumnOperation):
            if hasattr(condition, "operation") and hasattr(condition, "column"):
                left = self.column_to_sqlalchemy(table_obj, condition.column)
                right = self.value_to_sqlalchemy(condition.value)

                if condition.operation == "==":
                    return left == right
                elif condition.operation == "!=":
                    return left != right
                elif condition.operation == ">":
                    return left > right
                elif condition.operation == "<":
                    return left < right
                elif condition.operation == ">=":
                    return left >= right
                elif condition.operation == "<=":
                    return left <= right
                elif condition.operation == "&":
                    # Logical AND operation
                    left_expr = self.condition_to_sqlalchemy(
                        table_obj, condition.column
                    )
                    right_expr = self.condition_to_sqlalchemy(
                        table_obj, condition.value
                    )
                    return and_(left_expr, right_expr)
                elif condition.operation == "|":
                    # Logical OR operation
                    left_expr = self.condition_to_sqlalchemy(
                        table_obj, condition.column
                    )
                    right_expr = self.condition_to_sqlalchemy(
                        table_obj, condition.value
                    )
                    return or_(left_expr, right_expr)
                elif condition.operation == "!":
                    # Logical NOT operation
                    expr = self.condition_to_sqlalchemy(table_obj, condition.column)
                    if expr is not None:
                        return ~expr
                    else:
                        # Handle case where the inner expression is not supported
                        return None
                elif condition.operation == "isnull":
                    # IS NULL operation
                    left = self.column_to_sqlalchemy(table_obj, condition.column)
                    return left.is_(None)
                elif condition.operation == "isnotnull":
                    # IS NOT NULL operation
                    left = self.column_to_sqlalchemy(table_obj, condition.column)
                    return left.isnot(None)
                elif condition.operation == "contains":
                    # String contains operation
                    left = self.column_to_sqlalchemy(table_obj, condition.column)
                    return left.like(f"%{condition.value}%")
                elif condition.operation == "startswith":
                    # String starts with operation
                    left = self.column_to_sqlalchemy(table_obj, condition.column)
                    return left.like(f"{condition.value}%")
                elif condition.operation == "endswith":
                    # String ends with operation
                    left = self.column_to_sqlalchemy(table_obj, condition.column)
                    return left.like(f"%{condition.value}")
                elif condition.operation == "regex":
                    # Regular expression operation - use DuckDB's regexp_matches function
                    left = self.column_to_sqlalchemy(table_obj, condition.column)
                    return func.regexp_matches(left, condition.value)
                elif condition.operation == "rlike":
                    # Regular expression operation (alias for regex) - use DuckDB's regexp_matches function
                    left = self.column_to_sqlalchemy(table_obj, condition.column)
                    return func.regexp_matches(left, condition.value)
                elif condition.operation == "isin":
                    # IN operation
                    left = self.column_to_sqlalchemy(table_obj, condition.column)
                    if isinstance(condition.value, list):
                        return left.in_(condition.value)
                    else:
                        return left.in_([condition.value])
                elif condition.operation == "between":
                    # BETWEEN operation
                    left = self.column_to_sqlalchemy(table_obj, condition.column)
                    if isinstance(condition.value, tuple) and len(condition.value) == 2:
                        lower, upper = condition.value
                        return left.between(lower, upper)
                    else:
                        return None
                else:
                    # Unsupported operation
                    return None
            else:
                # Handle case where condition doesn't have expected attributes
                return None
        elif isinstance(condition, MockColumn):
            # Direct column reference
            return self.column_to_sqlalchemy(table_obj, condition)
        else:
            # Handle other types of conditions
            return None

        return None  # Fallback

    def column_to_sqlalchemy(self, table_obj: Any, column: Any) -> Any:
        """Convert a MockColumn to SQLAlchemy expression."""
        if isinstance(column, MockColumn):
            # Get column name
            column_name = column.name
            # Return the column from the table
            return table_obj.c[column_name]
        elif isinstance(column, str):
            # Direct column name
            return table_obj.c[column]
        else:
            # Handle other column types
            return table_obj.c[str(column)]

    def expression_to_sqlalchemy(self, expr: Any, table_obj: Any) -> Any:
        """Convert a complex expression (including AND/OR) to SQLAlchemy."""
        if isinstance(expr, MockColumnOperation):
            # Recursively process left and right sides
            if hasattr(expr, "column"):
                left = self.expression_to_sqlalchemy(expr.column, table_obj)
            else:
                left = None

            if hasattr(expr, "value") and expr.value is not None:
                if isinstance(expr.value, (MockColumn, MockColumnOperation)):
                    right = self.expression_to_sqlalchemy(expr.value, table_obj)
                elif isinstance(expr.value, MockLiteral):
                    right = expr.value.value
                else:
                    right = expr.value
            else:
                right = None

            # Apply the operation
            if expr.operation == "+":
                return left + right
            elif expr.operation == "-":
                return left - right
            elif expr.operation == "*":
                return left * right
            elif expr.operation == "/":
                return left / right
            elif expr.operation == "%":
                return left % right
            elif expr.operation == "==":
                return left == right
            elif expr.operation == "!=":
                return left != right
            elif expr.operation == ">":
                return left > right
            elif expr.operation == "<":
                return left < right
            elif expr.operation == ">=":
                return left >= right
            elif expr.operation == "<=":
                return left <= right
            elif expr.operation == "&":
                return and_(left, right)
            elif expr.operation == "|":
                return or_(left, right)
            elif expr.operation == "!":
                return ~left
            else:
                # Unsupported operation
                return None
        elif isinstance(expr, MockColumn):
            # Direct column reference
            return self.column_to_sqlalchemy(table_obj, expr)
        elif isinstance(expr, MockLiteral):
            # Literal value
            return expr.value
        else:
            # Handle other expression types
            return expr

    def value_to_sqlalchemy(self, value: Any) -> Any:
        """Convert a value to SQLAlchemy expression."""
        if isinstance(value, MockLiteral):
            return value.value
        elif isinstance(value, MockColumn):
            # This shouldn't happen in normal flow, but handle it
            return value.name
        elif isinstance(value, MockColumnOperation):
            # This shouldn't happen in normal flow, but handle it
            return str(value)
        else:
            return value

    def column_to_orm(self, table_class: Any, column: Any) -> Any:
        """Convert a MockColumn to SQLAlchemy ORM expression."""
        if isinstance(column, MockColumn):
            return getattr(table_class, column.name)
        elif isinstance(column, str):
            return getattr(table_class, column)
        else:
            return getattr(table_class, str(column))

    def value_to_orm(self, value: Any) -> Any:
        """Convert a value to SQLAlchemy ORM expression."""
        if isinstance(value, MockLiteral):
            return value.value
        else:
            return value

    def window_function_to_orm(self, table_class: Any, window_func: Any) -> Any:
        """Convert a window function to SQLAlchemy ORM expression."""
        function_name = getattr(window_func, "function_name", "window_function")
        
        # Get the column from the window function
        if hasattr(window_func, "column"):
            column = self.column_to_orm(table_class, window_func.column)
        else:
            column = None

        # Apply the window function
        if function_name.upper() == "ROW_NUMBER":
            return func.row_number().over()
        elif function_name.upper() == "RANK":
            return func.rank().over()
        elif function_name.upper() == "DENSE_RANK":
            return func.dense_rank().over()
        elif function_name.upper() == "LAG":
            offset = getattr(window_func, "offset", 1)
            default = getattr(window_func, "default", None)
            if column is not None:
                return func.lag(column, offset, default).over()
            else:
                return None
        elif function_name.upper() == "LEAD":
            offset = getattr(window_func, "offset", 1)
            default = getattr(window_func, "default", None)
            if column is not None:
                return func.lead(column, offset, default).over()
            else:
                return None
        elif function_name.upper() == "FIRST_VALUE":
            if column is not None:
                return func.first_value(column).over()
            else:
                return None
        elif function_name.upper() == "LAST_VALUE":
            if column is not None:
                return func.last_value(column).over()
            else:
                return None
        elif function_name.upper() == "SUM":
            if column is not None:
                return func.sum(column).over()
            else:
                return None
        elif function_name.upper() == "AVG":
            if column is not None:
                return func.avg(column).over()
            else:
                return None
        elif function_name.upper() == "COUNT":
            if column is not None:
                return func.count(column).over()
            else:
                return func.count().over()
        elif function_name.upper() == "MIN":
            if column is not None:
                return func.min(column).over()
            else:
                return None
        elif function_name.upper() == "MAX":
            if column is not None:
                return func.max(column).over()
            else:
                return None
        else:
            # Unsupported window function
            return None
