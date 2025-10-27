"""
Datetime Operations Handler for Mock Spark.

This module provides centralized handling of datetime operations,
consolidating logic from datetime functions, SQL conversion, and column operations.
"""

from typing import Any, Optional
from ...functions.core.column import MockColumn
from ...functions.core.literals import MockLiteral
from .date_format_converter import DateFormatConverter


class DatetimeOperationsHandler:
    """Centralized handler for datetime operations."""

    def __init__(self) -> None:
        """Initialize the datetime operations handler."""
        self.format_converter = DateFormatConverter()

    def convert_datetime_operation_to_sql(
        self, expr: Any, source_table: Optional[str] = None
    ) -> str:
        """Convert a datetime operation to SQL.

        Args:
            expr: The datetime operation expression
            source_table: Optional source table name

        Returns:
            SQL string representation of the datetime operation
        """
        if not hasattr(expr, "operation"):
            return str(expr)

        operation = expr.operation
        column_name = self._get_column_name(expr, source_table)

        # Handle functions that don't need a column input
        if operation == "current_date":
            return "CURRENT_DATE"
        elif operation == "current_timestamp":
            return "CURRENT_TIMESTAMP"

        # Handle make_date function specifically
        if operation == "make_date":
            return self._handle_make_date(expr, column_name)

        # Handle datetime conversion functions
        if operation in ["to_date", "to_timestamp"]:
            return self._handle_datetime_conversion(expr, column_name)

        # Handle datetime extraction functions
        if operation in ["hour", "minute", "second"]:
            return f"CAST(extract({operation} from TRY_CAST({column_name} AS TIMESTAMP)) AS INTEGER)"
        elif operation in ["year", "month", "day", "dayofmonth"]:
            part = "day" if operation == "dayofmonth" else operation
            return (
                f"CAST(extract({part} from TRY_CAST({column_name} AS DATE)) AS INTEGER)"
            )
        elif operation in ["dayofweek", "dayofyear", "weekofyear", "quarter"]:
            part_map = {
                "dayofweek": "dow",
                "dayofyear": "doy",
                "weekofyear": "week",
                "quarter": "quarter",
            }
            part = part_map.get(operation, operation)
            # PySpark dayofweek returns 1-7 (Sunday=1, Saturday=7)
            # DuckDB DOW returns 0-6 (Sunday=0, Saturday=6)
            # Add 1 to dayofweek to match PySpark
            if operation == "dayofweek":
                return f"CAST(extract({part} from TRY_CAST({column_name} AS DATE)) + 1 AS INTEGER)"
            else:
                return f"CAST(extract({part} from TRY_CAST({column_name} AS DATE)) AS INTEGER)"

        # Handle date formatting
        if operation == "date_format":
            return self._handle_date_format(expr, column_name)

        # Handle from_unixtime
        if operation == "from_unixtime":
            return self._handle_from_unixtime(expr, column_name)

        # Handle other datetime operations
        return self._handle_other_datetime_operations(expr, column_name)

    def _get_column_name(self, expr: Any, source_table: Optional[str] = None) -> str:
        """Get the column name from an expression."""
        if hasattr(expr, "column") and expr.column is not None:
            if isinstance(expr.column, MockColumn):
                return f'"{expr.column.name}"'
            elif isinstance(expr.column, str):
                return f'"{expr.column}"'
            else:
                return f'"{str(expr.column)}"'
        else:
            return "NULL"

    def _handle_make_date(self, expr: Any, column_name: str) -> str:
        """Handle make_date function."""
        if hasattr(expr, "value") and expr.value is not None:
            if isinstance(expr.value, tuple) and len(expr.value) == 2:
                month, day = expr.value
                month_sql = self._format_value(month)
                day_sql = self._format_value(day)
                return f"make_date({column_name}, {month_sql}, {day_sql})"
            else:
                return f"make_date({column_name})"
        else:
            return f"make_date({column_name})"

    def _handle_datetime_conversion(self, expr: Any, column_name: str) -> str:
        """Handle to_date and to_timestamp conversions."""
        if hasattr(expr, "value") and expr.value is not None:
            format_str = expr.value
            duckdb_format = self.format_converter.convert_java_to_duckdb_format(
                format_str
            )
            return f"STRPTIME({column_name}, '{duckdb_format}')"
        else:
            target_type = "DATE" if expr.operation == "to_date" else "TIMESTAMP"
            return f"TRY_CAST({column_name} AS {target_type})"

    def _handle_date_format(self, expr: Any, column_name: str) -> str:
        """Handle date formatting operations."""
        if hasattr(expr, "value") and expr.value is not None:
            format_str = expr.value
            duckdb_format = self.format_converter.convert_java_to_duckdb_format(
                format_str
            )
            return f"strftime(TRY_CAST({column_name} AS TIMESTAMP), '{duckdb_format}')"
        else:
            return f"strftime(TRY_CAST({column_name} AS TIMESTAMP), '%Y-%m-%d')"

    def _handle_from_unixtime(self, expr: Any, column_name: str) -> str:
        """Handle from_unixtime operations."""
        if hasattr(expr, "value") and expr.value is not None:
            format_str = expr.value
            duckdb_format = self.format_converter.convert_java_to_duckdb_format(
                format_str
            )
            return f"strftime(to_timestamp({column_name}), '{duckdb_format}')"
        else:
            return f"strftime(to_timestamp({column_name}), '%Y-%m-%d %H:%M:%S')"

    def _handle_other_datetime_operations(self, expr: Any, column_name: str) -> str:
        """Handle other datetime operations."""
        # Handle arithmetic operations on datetime columns
        if hasattr(expr, "value") and expr.value is not None:
            right = self._format_value(expr.value)
            if expr.operation == "+":
                return f"({column_name} + {right})"
            elif expr.operation == "-":
                return f"({column_name} - {right})"
            elif expr.operation == "*":
                return f"({column_name} * {right})"
            elif expr.operation == "/":
                return f"({column_name} / {right})"

        # Handle comparison operations
        if expr.operation in ["==", "!=", "<", ">", "<=", ">="]:
            right = self._format_value(expr.value) if hasattr(expr, "value") else "NULL"
            return f"({column_name} {expr.operation} {right})"

        # Default fallback
        return f"{expr.operation}({column_name})"

    def _format_value(self, value: Any) -> str:
        """Format a value for SQL."""
        if isinstance(value, MockLiteral):
            return self._format_value(value.value)
        elif isinstance(value, MockColumn):
            return f'"{value.name}"'
        elif isinstance(value, str):
            return f"'{value}'"
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        elif value is None:
            return "NULL"
        else:
            return f"'{str(value)}'"

    def is_datetime_operation(self, expr: Any) -> bool:
        """Check if an expression is a datetime operation."""
        if not hasattr(expr, "operation"):
            return False

        datetime_operations = {
            "current_date",
            "current_timestamp",
            "to_date",
            "to_timestamp",
            "hour",
            "minute",
            "second",
            "year",
            "month",
            "day",
            "dayofmonth",
            "dayofweek",
            "dayofyear",
            "weekofyear",
            "quarter",
            "date_format",
            "from_unixtime",
            "add_months",
            "months_between",
            "date_add",
            "date_sub",
            "timestampadd",
            "timestampdiff",
            "convert_timezone",
            "current_timezone",
            "from_utc_timestamp",
            "to_utc_timestamp",
            "date_part",
            "dayname",
            "make_date",
            "date_trunc",
            "datediff",
            "unix_timestamp",
            "last_day",
            "next_day",
            "trunc",
            "timestamp_seconds",
            "weekday",
        }

        return expr.operation in datetime_operations

    def get_datetime_operation_sql(
        self, operation: str, column_name: str, value: Any = None
    ) -> str:
        """Get SQL for a specific datetime operation.

        Args:
            operation: The datetime operation name
            column_name: The column name
            value: Optional value for the operation

        Returns:
            SQL string for the operation
        """
        if operation == "current_date":
            return "CURRENT_DATE"
        elif operation == "current_timestamp":
            return "CURRENT_TIMESTAMP"
        elif operation in ["hour", "minute", "second"]:
            return f"CAST(extract({operation} from TRY_CAST({column_name} AS TIMESTAMP)) AS INTEGER)"
        elif operation in ["year", "month", "day", "dayofmonth"]:
            part = "day" if operation == "dayofmonth" else operation
            return (
                f"CAST(extract({part} from TRY_CAST({column_name} AS DATE)) AS INTEGER)"
            )
        elif operation in ["dayofweek", "dayofyear", "weekofyear", "quarter"]:
            part_map = {
                "dayofweek": "dow",
                "dayofyear": "doy",
                "weekofyear": "week",
                "quarter": "quarter",
            }
            part = part_map.get(operation, operation)
            # PySpark dayofweek returns 1-7 (Sunday=1, Saturday=7)
            # DuckDB DOW returns 0-6 (Sunday=0, Saturday=6)
            # Add 1 to dayofweek to match PySpark
            if operation == "dayofweek":
                return f"CAST(extract({part} from TRY_CAST({column_name} AS DATE)) + 1 AS INTEGER)"
            else:
                return f"CAST(extract({part} from TRY_CAST({column_name} AS DATE)) AS INTEGER)"
        elif operation in ["to_date", "to_timestamp"]:
            if value is not None:
                duckdb_format = self.format_converter.convert_java_to_duckdb_format(
                    value
                )
                return f"STRPTIME({column_name}, '{duckdb_format}')"
            else:
                target_type = "DATE" if operation == "to_date" else "TIMESTAMP"
                return f"TRY_CAST({column_name} AS {target_type})"
        elif operation == "date_format":
            if value is not None:
                duckdb_format = self.format_converter.convert_java_to_duckdb_format(
                    value
                )
                return (
                    f"strftime(TRY_CAST({column_name} AS TIMESTAMP), '{duckdb_format}')"
                )
            else:
                return f"strftime(TRY_CAST({column_name} AS TIMESTAMP), '%Y-%m-%d')"
        elif operation == "from_unixtime":
            if value is not None:
                duckdb_format = self.format_converter.convert_java_to_duckdb_format(
                    value
                )
                return f"strftime(to_timestamp({column_name}), '{duckdb_format}')"
            else:
                return f"strftime(to_timestamp({column_name}), '%Y-%m-%d %H:%M:%S')"
        elif operation == "make_date":
            # make_date function takes year, month, day as separate parameters
            # The value should be a tuple of (month, day)
            if value is not None:
                if isinstance(value, tuple) and len(value) == 2:
                    month, day = value
                    month_sql = self._format_value(month)
                    day_sql = self._format_value(day)
                    return f"make_date({column_name}, {month_sql}, {day_sql})"
                else:
                    return f"make_date({column_name})"
            else:
                return f"make_date({column_name})"
        else:
            # Default fallback for other operations
            return f"{operation}({column_name})"
