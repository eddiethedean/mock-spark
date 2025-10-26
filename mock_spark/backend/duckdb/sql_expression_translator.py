"""
SQL expression translation utilities for Mock Spark.

This module provides translation of MockColumn expressions to SQL and SQLAlchemy.
"""

from typing import Any, Optional, Dict
from sqlalchemy import (
    and_,
    or_,
    literal,
    func,
)

from ...functions import MockColumn, MockColumnOperation, MockLiteral
from .table_manager import DuckDBTableManager
from .date_format_converter import DateFormatConverter
from .datetime_operations_handler import DatetimeOperationsHandler


class SQLExpressionTranslator:
    """Translates MockColumn expressions to SQL and SQLAlchemy expressions."""

    def __init__(self, table_manager: DuckDBTableManager):
        """Initialize expression translator.

        Args:
            table_manager: Table manager instance for table operations
        """
        self.table_manager = table_manager
        self.format_converter = DateFormatConverter()
        self.datetime_handler = DatetimeOperationsHandler()

    def column_to_sql(self, expr: Any, source_table: Optional[str] = None) -> str:
        """Convert a column reference to SQL with quotes for expressions.

        Args:
            expr: Column expression to convert
            source_table: Optional source table name for qualification

        Returns:
            SQL string representation
        """
        if isinstance(expr, str):
            # Check if this is a date/timestamp literal
            import re

            if re.match(r"^\d{4}-\d{2}-\d{2}$", expr):
                # Date literal - don't quote it, but wrap in DATE cast
                return f"DATE '{expr}'"
            elif re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$", expr):
                # Timestamp literal - don't quote it, but wrap in TIMESTAMP cast
                return f"TIMESTAMP '{expr}'"

            # Check if this is an SQL expression rather than a simple column name
            # SQL expressions contain keywords like CAST, TRY_CAST, EXTRACT, etc.
            sql_keywords = [
                "CAST(",
                "TRY_CAST(",
                "EXTRACT(",
                "STRFTIME(",
                "STRPTIME(",
                "TO_TIMESTAMP(",
                "MAKE_DATE(",
                "DATE_PART(",
            ]
            is_sql_expression = any(keyword in expr.upper() for keyword in sql_keywords)

            if is_sql_expression:
                # This is already an SQL expression, return as is
                return expr
            elif source_table:
                return f'{source_table}."{expr}"'
            else:
                return f'"{expr}"'
        elif isinstance(expr, MockLiteral):
            # Handle MockLiteral objects by extracting their value
            return self.value_to_sql(expr.value)
        elif hasattr(expr, "name"):
            # Check if this is referencing an aliased expression
            if source_table:
                return f'{source_table}."{expr.name}"'
            else:
                return f'"{expr.name}"'
        else:
            return str(expr)

    def expression_to_sql(self, expr: Any, source_table: Optional[str] = None) -> str:
        """Convert an expression to SQL.

        Args:
            expr: Expression to convert
            source_table: Optional source table name

        Returns:
            SQL string representation
        """
        if isinstance(expr, str):
            # If it's already SQL (contains function calls), return as-is
            if any(
                func in expr.upper()
                for func in [
                    "STRPTIME",
                    "STRFTIME",
                    "EXTRACT",
                    "CAST",
                    "TRY_CAST",
                    "TO_TIMESTAMP",
                    "TO_DATE",
                ]
            ):
                return expr
            return f'"{expr}"'
        elif hasattr(expr, "conditions") and hasattr(expr, "default_value"):
            # Handle MockCaseWhen objects
            return self.build_case_when_sql(expr, None)
        elif (
            hasattr(expr, "operation")
            and hasattr(expr, "column")
            and hasattr(expr, "value")
        ):
            # Check if this is a datetime operation first
            if self.datetime_handler.is_datetime_operation(expr):
                return self.datetime_handler.convert_datetime_operation_to_sql(
                    expr, source_table
                )

            # Handle string/math functions like upper, lower, abs, etc.
            if expr.operation in [
                "upper",
                "lower",
                "length",
                "trim",
                "abs",
                "round",
                "md5",
                "sha1",
                "crc32",
            ]:
                column_name = self.column_to_sql(expr.column, source_table)
                return f"{expr.operation.upper()}({column_name})"

            # Handle unary operations (value is None)
            if expr.value is None:
                # Handle functions that don't need a column input
                if expr.operation == "current_date":
                    return "CURRENT_DATE"
                elif expr.operation == "current_timestamp":
                    return "CURRENT_TIMESTAMP"

                # Handle operations that need a column input
                if expr.column is None:
                    raise ValueError(
                        f"Operation {expr.operation} requires a column input"
                    )

                left = self.column_to_sql(expr.column, source_table)
                if expr.operation == "-":
                    return f"(-{left})"
                elif expr.operation == "+":
                    return f"(+{left})"
                # Handle datetime functions
                elif expr.operation in ["to_date", "to_timestamp"]:
                    # Handle format strings for to_date and to_timestamp
                    if hasattr(expr, "value") and expr.value is not None:
                        # Has format string - use STRPTIME
                        format_str = expr.value
                        # Convert Java format to DuckDB format
                        duckdb_format = (
                            self.format_converter.convert_java_to_duckdb_format(
                                format_str
                            )
                        )
                        return f"STRPTIME({left}, '{duckdb_format}')"
                    else:
                        # No format - use TRY_CAST for safer conversion
                        target_type = (
                            "DATE" if expr.operation == "to_date" else "TIMESTAMP"
                        )
                        return f"TRY_CAST({left} AS {target_type})"
                elif expr.operation == "current_date":
                    # Handle current_date() function - no column input needed
                    return "CURRENT_DATE"
                elif expr.operation == "current_timestamp":
                    # Handle current_timestamp() function - no column input needed
                    return "CURRENT_TIMESTAMP"
                elif expr.operation == "from_unixtime":
                    # Handle from_unixtime(column, format) function
                    if expr.value is not None:
                        # Convert Java format to DuckDB format
                        format_str = (
                            self.format_converter.convert_java_to_duckdb_format(
                                expr.value
                            )
                        )
                        return f"STRFTIME(CAST({left} AS TIMESTAMP), '{format_str}')"
                    else:
                        # Default format
                        return (
                            f"STRFTIME(CAST({left} AS TIMESTAMP), '%Y-%m-%d %H:%M:%S')"
                        )
                elif expr.operation in ["hour", "minute", "second"]:
                    # DuckDB: extract(part from timestamp) - TRY_CAST handles both strings and timestamps
                    # Cast to integer to ensure proper type
                    return f"CAST(extract({expr.operation} from TRY_CAST({left} AS TIMESTAMP)) AS INTEGER)"
                elif expr.operation in ["year", "month", "day", "dayofmonth"]:
                    # DuckDB: extract(part from date) - TRY_CAST handles both strings and dates
                    # Cast to integer to ensure proper type
                    part = "day" if expr.operation == "dayofmonth" else expr.operation
                    return f"CAST(extract({part} from TRY_CAST({left} AS DATE)) AS INTEGER)"
                elif expr.operation in [
                    "dayofweek",
                    "dayofyear",
                    "weekofyear",
                    "quarter",
                ]:
                    # DuckDB date part extraction - TRY_CAST handles both strings and dates
                    # Cast to integer to ensure proper type
                    part_map = {
                        "dayofweek": "dow",
                        "dayofyear": "doy",
                        "weekofyear": "week",
                        "quarter": "quarter",
                    }
                    part = part_map.get(expr.operation, expr.operation)
                    return f"CAST(extract({part} from TRY_CAST({left} AS DATE)) AS INTEGER)"
                elif expr.operation == "log":
                    # DuckDB uses log10 for base-10 logarithm, but PySpark uses natural log
                    # For compatibility with PySpark, we need to use ln (natural log)
                    return f"ln({left})"
                elif expr.operation == "date_format":
                    # DuckDB: strftime function for date formatting
                    if hasattr(expr, "value") and expr.value is not None:
                        format_str = expr.value
                        # Convert Java format to DuckDB format
                        duckdb_format = (
                            self.format_converter.convert_java_to_duckdb_format(
                                format_str
                            )
                        )
                        return f"strftime(TRY_CAST({left} AS TIMESTAMP), '{duckdb_format}')"
                    else:
                        return f"strftime(TRY_CAST({left} AS TIMESTAMP), '%Y-%m-%d')"
                elif expr.operation == "to_timestamp":
                    # DuckDB: to_timestamp function - use STRPTIME for parsing
                    if hasattr(expr, "value") and expr.value is not None:
                        format_str = expr.value
                        # Convert Java format to DuckDB format
                        duckdb_format = (
                            self.format_converter.convert_java_to_duckdb_format(
                                format_str
                            )
                        )
                        return f"STRPTIME({left}, '{duckdb_format}')"
                    else:
                        return f"TRY_CAST({left} AS TIMESTAMP)"
                elif expr.operation == "to_date":
                    # DuckDB: to_date function - use STRPTIME for parsing
                    if hasattr(expr, "value") and expr.value is not None:
                        format_str = expr.value
                        # Convert Java format to DuckDB format
                        duckdb_format = (
                            self.format_converter.convert_java_to_duckdb_format(
                                format_str
                            )
                        )
                        return f"STRPTIME({left}, '{duckdb_format}')::DATE"
                    else:
                        return f"TRY_CAST({left} AS DATE)"
                # Handle array operations
                elif expr.operation in [
                    "array_sort",
                    "array_reverse",
                    "array_size",
                    "array_max",
                    "array_min",
                ]:
                    if expr.operation == "array_sort":
                        # array_sort(array, asc) -> LIST_SORT or LIST_REVERSE_SORT
                        asc = getattr(expr, "value", True)
                        if asc:
                            return f"LIST_SORT({left})"
                        else:
                            return f"LIST_REVERSE_SORT({left})"
                    elif expr.operation == "array_reverse":
                        return f"LIST_REVERSE({left})"
                    elif expr.operation == "array_size":
                        return f"LEN({left})"
                    elif expr.operation == "array_max":
                        return f"LIST_MAX({left})"
                    elif expr.operation == "array_min":
                        return f"LIST_MIN({left})"
                elif expr.operation == "isnull":
                    # IS NULL operation
                    return f"({left} IS NULL)"
                elif expr.operation == "isnotnull":
                    # PySpark's isnotnull is implemented as ~isnull, generates (NOT (column IS NULL))
                    return f"(NOT ({left} IS NULL))"
                else:
                    # For other unary operations, treat as function
                    return f"{expr.operation.upper()}({left})"

            # Handle arithmetic operations like MockColumnOperation
            # For column references in expressions, don't quote them
            # Check if the left side is a MockColumnOperation to avoid recursion
            if isinstance(expr.column, MockColumnOperation):
                left = self.expression_to_sql(expr.column, source_table)
            elif isinstance(expr.column, MockLiteral):
                # Handle literals - use value_to_sql to avoid quoting numeric values
                left = self.value_to_sql(expr.column.value)
            else:
                left = self.column_to_sql(expr.column, source_table)

            # Check if the right side is also a MockColumnOperation (e.g., cast of literal)
            if isinstance(expr.value, MockColumnOperation):
                right = self.expression_to_sql(expr.value, source_table)
            else:
                right = self.value_to_sql(expr.value)

            # Handle datetime operations with values
            if expr.operation == "from_unixtime":
                # Handle from_unixtime(column, format) function
                # Convert epoch seconds to timestamp, then format as string
                if expr.value is not None:
                    # Convert Java format to DuckDB format
                    format_str = self.format_converter.convert_java_to_duckdb_format(
                        expr.value
                    )
                    return f"STRFTIME(TO_TIMESTAMP({left}), '{format_str}')"
                else:
                    # Default format
                    return f"STRFTIME(TO_TIMESTAMP({left}), '%Y-%m-%d %H:%M:%S')"
            # Handle string operations
            elif expr.operation == "contains":
                return f"({left} LIKE '%{right[1:-1]}%')"  # Remove quotes from right
            elif expr.operation == "startswith":
                return f"({left} LIKE '{right[1:-1]}%')"  # Remove quotes from right
            elif expr.operation == "endswith":
                return f"({left} LIKE '%{right[1:-1]}')"  # Remove quotes from right
            elif expr.operation == "split":
                # DuckDB uses string_split function
                return f"string_split({left}, {right})"
            elif expr.operation == "concat":
                # DuckDB uses || operator for string concatenation
                # Handle multiple arguments by chaining || operators
                if isinstance(expr.value, (list, tuple)) and len(expr.value) > 0:
                    # Multiple arguments: concat(col1, col2, col3) -> col1 || col2 || col3
                    right_parts = []
                    for val in expr.value:
                        # Check for MockLiteral first (has both name and value)
                        if hasattr(val, 'value') and hasattr(val, '_name'):
                            # Handle MockLiteral objects
                            right_parts.append(f"'{val.value}'")
                        elif hasattr(val, 'name'):
                            # Handle MockColumn objects
                            right_parts.append(val.name)
                        else:
                            right_parts.append(str(val))
                    return f"({left} || {' || '.join(right_parts)})"
                else:
                    # Single argument: concat(col1, col2) -> col1 || col2
                    return f"({left} || {right})"
            elif expr.operation == "regexp_extract":
                # DuckDB supports regexp_extract function
                if isinstance(expr.value, tuple) and len(expr.value) >= 2:
                    pattern, group = expr.value[0], expr.value[1]
                    return f"regexp_extract({left}, '{pattern}', {group})"
                else:
                    return f"regexp_extract({left}, {right})"
            elif expr.operation == "between":
                # Handle BETWEEN operation: column BETWEEN lower AND upper
                if isinstance(expr.value, tuple) and len(expr.value) == 2:
                    lower, upper = expr.value
                    return f"({left} BETWEEN {lower} AND {upper})"
                else:
                    raise ValueError(f"Invalid between operation: {expr}")
            # Handle comparison operations
            elif expr.operation == "==":
                # Handle NULL comparisons specially
                if right == "NULL":
                    return f"({left} IS NULL)"
                return f"({left} = {right})"
            elif expr.operation == "!=":
                # Handle NULL comparisons specially
                if right == "NULL":
                    return f"({left} IS NOT NULL)"
                return f"({left} <> {right})"
            elif expr.operation == ">":
                return f"({left} > {right})"
            elif expr.operation == "<":
                return f"({left} < {right})"
            elif expr.operation == ">=":
                return f"({left} >= {right})"
            elif expr.operation == "<=":
                return f"({left} <= {right})"
            # Handle datetime functions with format strings
            elif expr.operation == "to_timestamp":
                # DuckDB: to_timestamp function - use STRPTIME for parsing
                if hasattr(expr, "value") and expr.value is not None:
                    format_str = expr.value
                    # Convert Java format to DuckDB format
                    duckdb_format = self.format_converter.convert_java_to_duckdb_format(
                        format_str
                    )
                    return f"STRPTIME({left}, '{duckdb_format}')"
                else:
                    return f"TRY_CAST({left} AS TIMESTAMP)"
            elif expr.operation == "to_date":
                # DuckDB: to_date function - use STRPTIME for parsing
                if hasattr(expr, "value") and expr.value is not None:
                    format_str = expr.value
                    # Convert Java format to DuckDB format
                    duckdb_format = self.format_converter.convert_java_to_duckdb_format(
                        format_str
                    )
                    return f"STRPTIME({left}, '{duckdb_format}')::DATE"
                else:
                    return f"TRY_CAST({left} AS DATE)"
            elif expr.operation == "date_format":
                # DuckDB: strftime function for date formatting
                if hasattr(expr, "value") and expr.value is not None:
                    format_str = expr.value
                    # Convert Java format to DuckDB format
                    duckdb_format = self.format_converter.convert_java_to_duckdb_format(
                        format_str
                    )
                    return f"strftime(TRY_CAST({left} AS TIMESTAMP), '{duckdb_format}')"
                else:
                    return f"strftime(TRY_CAST({left} AS TIMESTAMP), '%Y-%m-%d')"
            # Handle arithmetic operations
            elif expr.operation == "*":
                return f"({left} * {right})"
            elif expr.operation == "+":
                return f"({left} + {right})"
            elif expr.operation == "-":
                return f"({left} - {right})"
            elif expr.operation == "/":
                return f"({left} / {right})"
            elif expr.operation == "cast":
                # Handle cast operation with proper SQL syntax using TRY_CAST for safety
                return f"TRY_CAST({left} AS {right})"
            # Handle math functions
            elif expr.operation == "log":
                # DuckDB uses log10 for base-10 logarithm, but PySpark uses natural log
                # For compatibility with PySpark, we need to use ln (natural log)
                return f"ln({left})"
            elif expr.operation == "exp":
                # DuckDB uses exp for exponential function
                return f"exp({left})"
            elif expr.operation == "pow":
                # DuckDB uses power function
                return f"power({left}, {right})"
            elif expr.operation == "sqrt":
                # DuckDB uses sqrt function
                return f"sqrt({left})"
            elif expr.operation == "coalesce":
                # Handle coalesce with multiple columns
                if isinstance(expr.value, (list, tuple)):
                    # Multiple columns: coalesce(col1, col2, col3)
                    column_list = []
                    column_list.append(left)
                    for col in expr.value:
                        if isinstance(col, MockColumn):
                            column_list.append(self.column_to_sql(col, source_table))
                        elif isinstance(col, str):
                            column_list.append(self.column_to_sql(col, source_table))
                        elif isinstance(col, MockLiteral):
                            # Handle MockLiteral
                            column_list.append(self.value_to_sql(col.value))
                        else:
                            column_list.append(str(col))
                    return f"coalesce({', '.join(column_list)})"
                else:
                    # Single column: coalesce(col1, col2)
                    # Check if right is a MockLiteral
                    if isinstance(expr.value, MockLiteral):
                        return f"coalesce({left}, {self.value_to_sql(expr.value.value)})"
                    return f"coalesce({left}, {right})"
            else:
                return f"({left} {expr.operation} {right})"
        elif hasattr(expr, "name"):
            return f'"{expr.name}"'
        elif hasattr(expr, "value"):
            # Handle literals
            if isinstance(expr.value, str):
                return f"'{expr.value}'"
            else:
                return str(expr.value)
        else:
            return str(expr)

    def condition_to_sql(self, condition: Any, source_table_obj: Any) -> str:
        """Convert a condition to SQL string.

        Args:
            condition: Condition to convert
            source_table_obj: Source table object for column references

        Returns:
            SQL string representation
        """
        if isinstance(condition, MockColumnOperation):
            if hasattr(condition, "operation") and hasattr(condition, "column"):
                left = self.column_to_sql(
                    condition.column,
                    source_table_obj.name
                    if hasattr(source_table_obj, "name")
                    else None,
                )
                right = self.value_to_sql(condition.value)

                if condition.operation == "==":
                    return f"({left} = {right})"
                elif condition.operation == "!=":
                    return f"({left} <> {right})"
                elif condition.operation == ">":
                    return f"({left} > {right})"
                elif condition.operation == "<":
                    return f"({left} < {right})"
                elif condition.operation == ">=":
                    return f"({left} >= {right})"
                elif condition.operation == "<=":
                    return f"({left} <= {right})"
                elif condition.operation == "&":
                    # Logical AND operation
                    left_expr = self.condition_to_sql(
                        condition.column, source_table_obj
                    )
                    right_expr = self.condition_to_sql(
                        condition.value, source_table_obj
                    )
                    return f"({left_expr} AND {right_expr})"
                elif condition.operation == "|":
                    # Logical OR operation
                    left_expr = self.condition_to_sql(
                        condition.column, source_table_obj
                    )
                    right_expr = self.condition_to_sql(
                        condition.value, source_table_obj
                    )
                    return f"({left_expr} OR {right_expr})"
                elif condition.operation == "!":
                    # Logical NOT operation
                    expr = self.condition_to_sql(condition.column, source_table_obj)
                    if expr is not None:
                        return f"(NOT {expr})"
                    else:
                        # Handle case where the inner expression is not supported
                        return None
                elif condition.operation == "isnull":
                    # IS NULL operation
                    left = self.column_to_sql(
                        condition.column,
                        source_table_obj.name
                        if hasattr(source_table_obj, "name")
                        else None,
                    )
                    return f"({left} IS NULL)"
                elif condition.operation == "isnotnull":
                    # IS NOT NULL operation
                    left = self.column_to_sql(
                        condition.column,
                        source_table_obj.name
                        if hasattr(source_table_obj, "name")
                        else None,
                    )
                    return f"({left} IS NOT NULL)"
                elif condition.operation == "contains":
                    # String contains operation
                    left = self.column_to_sql(
                        condition.column,
                        source_table_obj.name
                        if hasattr(source_table_obj, "name")
                        else None,
                    )
                    return f"({left} LIKE '%{condition.value}%')"
                elif condition.operation == "startswith":
                    # String starts with operation
                    left = self.column_to_sql(
                        condition.column,
                        source_table_obj.name
                        if hasattr(source_table_obj, "name")
                        else None,
                    )
                    return f"({left} LIKE '{condition.value}%')"
                elif condition.operation == "endswith":
                    # String ends with operation
                    left = self.column_to_sql(
                        condition.column,
                        source_table_obj.name
                        if hasattr(source_table_obj, "name")
                        else None,
                    )
                    return f"({left} LIKE '%{condition.value}')"
                elif condition.operation == "regex":
                    # Regular expression operation - use DuckDB's regexp_matches function
                    left = self.column_to_sql(
                        condition.column,
                        source_table_obj.name
                        if hasattr(source_table_obj, "name")
                        else None,
                    )
                    return f"regexp_matches({left}, '{condition.value}')"
                elif condition.operation == "rlike":
                    # Regular expression operation (alias for regex) - use DuckDB's regexp_matches function
                    left = self.column_to_sql(
                        condition.column,
                        source_table_obj.name
                        if hasattr(source_table_obj, "name")
                        else None,
                    )
                    return f"regexp_matches({left}, '{condition.value}')"
                elif condition.operation == "isin":
                    # IN operation
                    left = self.column_to_sql(
                        condition.column,
                        source_table_obj.name
                        if hasattr(source_table_obj, "name")
                        else None,
                    )
                    if isinstance(condition.value, list):
                        values = ", ".join(
                            [self.value_to_sql(v) for v in condition.value]
                        )
                        return f"({left} IN ({values}))"
                    else:
                        raise NotImplementedError(
                            f"Unsupported condition value type: {type(condition.value)}"
                        )
                elif condition.operation == "between":
                    # Handle BETWEEN operation: column BETWEEN lower AND upper
                    left = self.column_to_sql(
                        condition.column,
                        source_table_obj.name
                        if hasattr(source_table_obj, "name")
                        else None,
                    )
                    if isinstance(condition.value, tuple) and len(condition.value) == 2:
                        lower, upper = condition.value
                        return f"({left} BETWEEN {lower} AND {upper})"
                    else:
                        raise ValueError(f"Invalid between operation: {condition}")
        elif isinstance(condition, MockColumn):
            return f'"{condition.name}"'

        raise NotImplementedError(f"Unsupported condition type: {type(condition)}")

    def column_to_sqlalchemy(self, table_obj: Any, column: Any) -> Any:
        """Convert a MockColumn to SQLAlchemy expression.

        Args:
            table_obj: SQLAlchemy table object
            column: Column to convert

        Returns:
            SQLAlchemy expression
        """
        if isinstance(column, MockColumn):
            column_name = column.name
        elif isinstance(column, str):
            column_name = column
        else:
            return column

        # Validate column exists
        if column_name not in table_obj.c:
            # Only raise errors if we're in strict validation mode (e.g., filters)
            # Window functions and other operations handle missing columns differently
            if getattr(self, "_strict_column_validation", False):
                from ...core.exceptions import AnalysisException

                available_columns = list(table_obj.c.keys())
                raise AnalysisException(
                    f"Column '{column_name}' not found. Available columns: {available_columns}"
                )
            else:
                # For window functions and other contexts, return literal False
                return literal(False)

        return table_obj.c[column_name]

    def expression_to_sqlalchemy(self, expr: Any, table_obj: Any) -> Any:
        """Convert a complex expression (including AND/OR) to SQLAlchemy.

        Args:
            expr: Expression to convert
            table_obj: SQLAlchemy table object

        Returns:
            SQLAlchemy expression
        """
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

            # Apply operation
            if expr.operation == ">":
                return left > right
            elif expr.operation == "<":
                return left < right
            elif expr.operation == ">=":
                return left >= right
            elif expr.operation == "<=":
                return left <= right
            elif expr.operation == "==":
                return left == right
            elif expr.operation == "!=":
                return left != right
            elif expr.operation == "&":
                return and_(left, right)
            elif expr.operation == "|":
                return or_(left, right)
            elif expr.operation == "!":
                return ~left
            else:
                # Fallback
                return table_obj.c[str(expr)]
        elif isinstance(expr, MockColumn):
            return table_obj.c[expr.name]
        elif isinstance(expr, MockLiteral):
            return expr.value
        else:
            # Literal value
            return expr

    def condition_to_sqlalchemy(self, table_obj: Any, condition: Any) -> Any:
        """Convert a condition to SQLAlchemy expression.

        Args:
            table_obj: SQLAlchemy table object
            condition: Condition to convert

        Returns:
            SQLAlchemy expression
        """
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
                        return None
        elif isinstance(condition, MockColumn):
            return table_obj.c[condition.name]

        return None  # Fallback

    def value_to_sqlalchemy(self, value: Any) -> Any:
        """Convert a value to SQLAlchemy expression.

        Args:
            value: Value to convert

        Returns:
            SQLAlchemy expression
        """
        if isinstance(value, MockLiteral):
            return value.value
        elif isinstance(value, MockColumn):
            # This would need the table context, but for now return the name
            return value.name
        return value

    def value_to_sql(self, value: Any) -> str:
        """Convert a value to SQL string.

        Args:
            value: Value to convert

        Returns:
            SQL string representation
        """
        if isinstance(value, MockLiteral):
            # Handle MockLiteral objects by extracting their value
            return self.value_to_sql(value.value)
        elif isinstance(value, str):
            return f"'{value}'"
        elif value is None:
            return "NULL"
        else:
            return str(value)

    def build_case_when_sql(self, case_when_obj: Any, source_table_obj: Any) -> str:
        """Build CASE WHEN SQL expression.

        Args:
            case_when_obj: MockCaseWhen object
            source_table_obj: Source table object

        Returns:
            SQL string representation
        """
        if not hasattr(case_when_obj, "conditions") or not hasattr(
            case_when_obj, "default_value"
        ):
            return "NULL"

        sql_parts = ["CASE"]

        for condition, value in case_when_obj.conditions:
            condition_sql = self.condition_to_sql(condition, source_table_obj)
            value_sql = self.value_to_sql(value)
            sql_parts.append(f"WHEN {condition_sql} THEN {value_sql}")

        default_sql = self.value_to_sql(case_when_obj.default_value)
        sql_parts.append(f"ELSE {default_sql}")
        sql_parts.append("END")

        return " ".join(sql_parts)

    def window_spec_to_sql(
        self,
        window_spec: Any,
        table_obj: Any = None,
        alias_mapping: Optional[Dict[Any, Any]] = None,
    ) -> str:
        """Convert window specification to SQL.

        Args:
            window_spec: Window specification object
            table_obj: Optional table object for column validation

        Returns:
            SQL string representation of window specification
        """
        parts = []

        # Get available columns if table_obj provided
        available_columns = set(table_obj.c.keys()) if table_obj is not None else None

        # Handle PARTITION BY
        if hasattr(window_spec, "_partition_by") and window_spec._partition_by:
            partition_cols = []
            for col in window_spec._partition_by:
                col_name = None
                if isinstance(col, str):
                    col_name = col
                elif hasattr(col, "name"):
                    col_name = col.name

                # Don't apply alias mapping in window specs - they reference source table columns

                # Validate column exists if available_columns is set
                if (
                    available_columns is not None
                    and col_name
                    and col_name not in available_columns
                ):
                    continue  # Skip non-existent columns

                if col_name:
                    partition_cols.append(f'"{col_name}"')

            if partition_cols:
                parts.append(f"PARTITION BY {', '.join(partition_cols)}")

        # Handle ORDER BY
        if hasattr(window_spec, "_order_by") and window_spec._order_by:
            order_cols = []
            for col in window_spec._order_by:
                col_name = None
                is_desc = False

                if isinstance(col, str):
                    col_name = col
                elif isinstance(col, MockColumnOperation):
                    if hasattr(col, "operation") and col.operation == "desc":
                        col_name = col.column.name
                        is_desc = True
                    else:
                        col_name = col.column.name
                elif hasattr(col, "name"):
                    col_name = col.name

                # Don't apply alias mapping in window specs - they reference source table columns

                # Validate column exists if available_columns is set
                if (
                    available_columns is not None
                    and col_name
                    and col_name not in available_columns
                ):
                    continue  # Skip non-existent columns

                if col_name:
                    if is_desc:
                        order_cols.append(f'"{col_name}" DESC')
                    else:
                        order_cols.append(f'"{col_name}"')

            if order_cols:
                parts.append(f"ORDER BY {', '.join(order_cols)}")

        # Handle ROWS BETWEEN
        if hasattr(window_spec, "_rows_between") and window_spec._rows_between:
            start, end = window_spec._rows_between
            # Convert to SQL ROWS BETWEEN syntax
            # Negative values are PRECEDING, positive are FOLLOWING
            if start == 0:
                start_clause = "CURRENT ROW"
            elif start < 0:
                start_clause = f"{abs(start)} PRECEDING"
            else:
                start_clause = f"{start} FOLLOWING"

            if end == 0:
                end_clause = "CURRENT ROW"
            elif end < 0:
                end_clause = f"{abs(end)} PRECEDING"
            else:
                end_clause = f"{end} FOLLOWING"

            parts.append(f"ROWS BETWEEN {start_clause} AND {end_clause}")

        # Handle RANGE BETWEEN
        if hasattr(window_spec, "_range_between") and window_spec._range_between:
            start, end = window_spec._range_between
            # Convert to SQL RANGE BETWEEN syntax
            if start == 0:
                start_clause = "CURRENT ROW"
            elif start < 0:
                start_clause = f"{abs(start)} PRECEDING"
            else:
                start_clause = f"{start} FOLLOWING"

            if end == 0:
                end_clause = "CURRENT ROW"
            elif end < 0:
                end_clause = f"{abs(end)} PRECEDING"
            else:
                end_clause = f"{end} FOLLOWING"

            parts.append(f"RANGE BETWEEN {start_clause} AND {end_clause}")

        return " ".join(parts)

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
