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
                # But if source_table is provided and the expression contains table-qualified columns,
                # replace them with the source_table
                if source_table:
                    # Replace any table-qualified column references with source_table
                    expr = re.sub(r'"[a-z0-9_]+"\.(".*?")', rf"{source_table}.\1", expr)
                    expr = re.sub(r'[a-z0-9_]+\.(".*?")', rf"{source_table}.\1", expr)
                return expr
            elif source_table:
                # If source_table is provided, always use it, even if expr already has a table qualifier
                # Extract just the column name if it's already qualified
                column_name = expr
                if "." in expr:
                    # Extract column name from qualified reference (e.g., "table.column" -> "column")
                    parts = expr.split(".", 1)
                    if len(parts) == 2:
                        column_name = parts[1].strip('"')
                return f'{source_table}."{column_name}"'
            else:
                return f'"{expr}"'
        elif isinstance(expr, MockLiteral):
            # Handle MockLiteral objects by extracting their value
            return self.value_to_sql(expr.value)
        elif hasattr(expr, "operation") and hasattr(expr, "column"):
            # Handle MockColumnOperation - delegate to expression_to_sql for proper handling
            # This ensures boolean operations, comparisons, and complex expressions are handled correctly
            return self.expression_to_sql(expr, source_table)
        elif hasattr(expr, "name"):
            # Check if this is referencing an aliased expression
            # If source_table is provided, always use it (for CTE queries)
            # Extract just the column name if it's already qualified
            column_name = expr.name
            if "." in column_name:
                # Extract column name from qualified reference
                parts = column_name.split(".", 1)
                if len(parts) == 2:
                    column_name = parts[1].strip('"')
            if source_table:
                return f'{source_table}."{column_name}"'
            else:
                return f'"{column_name}"'
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
        # Handle function-style expressions (e.g., concat_ws) that may carry a function_name attribute
        if hasattr(expr, "function_name"):
            func_name = getattr(expr, "function_name", "").lower()
            # concat_ws(sep, col1, col2, ...) -> ARRAY_TO_STRING([col1, col2, ...], sep)
            if func_name == "concat_ws":
                sep, cols = getattr(expr, "value", ("", []))
                # First argument may also be provided via expr.column
                col_exprs = []
                if hasattr(expr, "column") and expr.column is not None:
                    col_exprs.append(self.column_to_sql(expr.column, source_table))
                for c in cols or []:
                    if hasattr(c, "operation"):
                        col_sql = self.expression_to_sql(c, source_table)
                    else:
                        col_sql = self.column_to_sql(c, source_table)
                    col_exprs.append(col_sql)
                all_cols_str = ", ".join(col_exprs)
                return f"ARRAY_TO_STRING([{all_cols_str}], '{sep}')"

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
            # Handle datediff explicitly to ensure robust table-qualified casts
            if getattr(expr, "operation", None) == "datediff":
                # datediff(end, start) -> DATEDIFF('DAY', start, end)
                # Robust casting: TRY_CAST to DATE, fallback to STRPTIME('%Y-%m-%d')
                def _to_date_sql(sql_str: str) -> str:
                    return (
                        f"COALESCE(TRY_CAST({sql_str} AS DATE), "
                        f"TRY_CAST(STRPTIME({sql_str}, '%Y-%m-%d') AS DATE))"
                    )

                # Build end expression
                end_sql = self.column_to_sql(expr.column, source_table)
                end_expr = _to_date_sql(end_sql)
                # Build start expression (expr.value)
                start_raw = expr.value
                import re

                if isinstance(start_raw, str):
                    if re.match(r"^\d{4}-\d{2}-\d{2}$", start_raw):
                        start_expr = f"DATE '{start_raw}'"
                    else:
                        start_expr = f"TRY_CAST('{start_raw}' AS DATE)"
                elif hasattr(start_raw, "name"):
                    start_sql = self.column_to_sql(start_raw, source_table)
                    start_expr = f"CAST({start_sql} AS DATE)"
                elif hasattr(start_raw, "value"):
                    val_sql = self.value_to_sql(start_raw)
                    # value_to_sql may have quoted already
                    if isinstance(getattr(start_raw, "value", None), str) and re.match(
                        r"^\d{4}-\d{2}-\d{2}$", start_raw.value
                    ):
                        start_expr = f"DATE '{start_raw.value}'"
                    else:
                        start_expr = f"CAST({val_sql} AS DATE)"
                else:
                    start_expr = f"TRY_CAST({self.value_to_sql(start_raw)} AS DATE)"
                return f"DATEDIFF('DAY', {start_expr}, {end_expr})"

            # Check if this is another datetime operation
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

                    # PySpark dayofweek returns 1-7 (Sunday=1, Saturday=7)
                    # DuckDB DOW returns 0-6 (Sunday=0, Saturday=6)
                    # Add 1 to dayofweek to match PySpark
                    if expr.operation == "dayofweek":
                        return f"CAST(extract({part} from TRY_CAST({left} AS DATE)) + 1 AS INTEGER)"
                    else:
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
                        # Ensure format string has no single quotes that could break SQL
                        duckdb_format = duckdb_format.replace("'", "")
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
                        # Ensure format string has no single quotes that could break SQL
                        duckdb_format = duckdb_format.replace("'", "")
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
                    "explode",
                    "explode_outer",
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
                    elif expr.operation == "explode":
                        # explode(array) -> UNNEST(array)
                        return f"UNNEST({left})"
                    elif expr.operation == "explode_outer":
                        # explode_outer(array) -> UNNEST(COALESCE(array, [NULL]))
                        return f"UNNEST(COALESCE({left}, [NULL]))"
                elif expr.operation == "isnull":
                    # IS NULL operation
                    return f"({left} IS NULL)"
                elif expr.operation == "isnotnull":
                    # PySpark's isnotnull is implemented as ~isnull, generates (NOT (column IS NULL))
                    return f"(NOT ({left} IS NULL))"
                else:
                    # For other unary operations, treat as function
                    return f"{expr.operation.upper()}({left})"

            # FIRST: Check types BEFORE computing left/right SQL (need values for type checking)
            # Check if right operand is numeric BEFORE converting to SQL
            right_is_numeric_for_check = False
            if isinstance(expr.value, (int, float)):
                right_is_numeric_for_check = True
                import logging

                logging.debug(
                    f"[NUMERIC_DEBUG] Right operand is numeric (raw): {expr.value}"
                )
            elif isinstance(expr.value, MockLiteral):
                if isinstance(expr.value.value, (int, float)):
                    right_is_numeric_for_check = True
                    import logging

                    logging.debug(
                        f"[NUMERIC_DEBUG] Right operand is numeric (MockLiteral): {expr.value.value}"
                    )

            # Check if left is numeric - we can check the column type early if table is available
            left_is_numeric_early = False
            left_is_string_early = False
            import logging

            col_obj = None
            if (
                source_table is not None
                and hasattr(expr.column, "name")
                and right_is_numeric_for_check
            ):
                logging.debug(
                    f"[NUMERIC_DEBUG] Checking left column type: col={expr.column.name}, source_table={source_table}, right_is_numeric={right_is_numeric_for_check}"
                )
                try:
                    if hasattr(self.table_manager, "get_table"):
                        table_obj = self.table_manager.get_table(source_table)
                        if table_obj and hasattr(table_obj, "c"):
                            col_name = expr.column.name
                            try:
                                col_obj = table_obj.c[col_name]
                            except (KeyError, AttributeError):
                                try:
                                    col_obj = getattr(table_obj.c, col_name, None)
                                except (AttributeError, TypeError):
                                    pass

                            if col_obj is not None:
                                from sqlalchemy import (
                                    Integer,
                                    Float,
                                    BigInteger,
                                    String,
                                )

                                if isinstance(
                                    col_obj.type, (Integer, Float, BigInteger)
                                ):
                                    left_is_numeric_early = True
                                elif isinstance(col_obj.type, String):
                                    left_is_string_early = True
                except (AttributeError, KeyError, TypeError):
                    # Table lookup failed - if right is numeric and left is simple column, default to numeric
                    if (
                        right_is_numeric_for_check
                        and hasattr(expr.column, "name")
                        and not hasattr(expr.column, "operation")
                    ):
                        left_is_numeric_early = True

            # NOW compute left and right SQL
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

            # Check if the right side is also a MockColumnOperation (e.g., cast of literal, boolean operation, comparison)
            if isinstance(expr.value, MockColumnOperation) or (
                hasattr(expr.value, "operation") and hasattr(expr.value, "column")
            ):
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
                        if hasattr(val, "value") and hasattr(val, "_name"):
                            # Handle MockLiteral objects
                            right_parts.append(f"'{val.value}'")
                        elif hasattr(val, "name"):
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
                    # Check for optional fractional seconds pattern like [.SSSSSS]
                    fractional_info = (
                        self.format_converter.extract_optional_fractional_seconds(
                            format_str
                        )
                    )

                    # Convert Java format to DuckDB format
                    duckdb_format = self.format_converter.convert_java_to_duckdb_format(
                        format_str
                    )

                    if fractional_info:
                        # Handle optional fractional seconds by normalizing input
                        # Strip microseconds from input if present before parsing
                        # This allows both "2025-10-29T10:30:45.123456" and "2025-10-29T10:30:45" to work
                        # DuckDB uses standard string literals for regex, not Python r'' syntax
                        normalized_left = f"regexp_replace({left}, '\\.\\d+', '')"
                        return f"STRPTIME({normalized_left}, '{duckdb_format}')"
                    else:
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
                # Use DECIMAL for precise multiplication, then cast to DOUBLE for consistency
                return f"(CAST((CAST({left} AS DECIMAL(38,10)) * CAST({right} AS DECIMAL(38,10))) AS DOUBLE))"
            elif expr.operation == "+":
                # Check if this is string concatenation
                # Strategy: Check for numeric types FIRST (most restrictive), then fall back to string detection
                is_string_operation = (
                    None  # None = unknown, True = string, False = numeric
                )

                # Import StringType for type checking (MockLiteral already imported at module level)

                # Use pre-computed type checks from above if available
                right_is_numeric = right_is_numeric_for_check
                left_is_numeric = left_is_numeric_early
                left_is_string = left_is_string_early

                import logging

                logging.debug(
                    f"[NUMERIC_DEBUG] After early checks: left_is_numeric={left_is_numeric}, left_is_string={left_is_string}, right_is_numeric={right_is_numeric}"
                )

                # If we didn't compute early checks, do them now
                if not (left_is_numeric or left_is_string):
                    logging.debug(
                        "[NUMERIC_DEBUG] Early checks didn't determine type, doing fallback check"
                    )
                    # Check if left is numeric type from source table
                    if (
                        source_table is not None
                        and hasattr(expr.column, "name")
                        and right_is_numeric
                    ):
                        # Only check table if right is numeric (optimization)
                        try:
                            if hasattr(self.table_manager, "get_table"):
                                table_obj = self.table_manager.get_table(source_table)
                                if table_obj and hasattr(table_obj, "c"):
                                    col_name = expr.column.name
                                    # Try both dictionary and attribute access
                                    col_obj = None
                                    try:
                                        col_obj = table_obj.c[col_name]
                                    except (KeyError, AttributeError):
                                        try:
                                            col_obj = getattr(
                                                table_obj.c, col_name, None
                                            )
                                        except (AttributeError, TypeError):
                                            pass

                            if col_obj is not None:
                                from sqlalchemy import (
                                    Integer,
                                    Float,
                                    BigInteger,
                                    String,
                                )

                                col_type_name = type(col_obj.type).__name__
                                logging.debug(
                                    f"[NUMERIC_DEBUG] Found column in table: {expr.column.name}, type={col_type_name}"
                                )
                                if isinstance(
                                    col_obj.type, (Integer, Float, BigInteger)
                                ):
                                    left_is_numeric = True
                                    logging.debug(
                                        f"[NUMERIC_DEBUG] Left column is NUMERIC type: {col_type_name}"
                                    )
                                elif isinstance(col_obj.type, String):
                                    left_is_string = True
                                    is_string_operation = True  # Explicitly string
                                    logging.debug(
                                        f"[NUMERIC_DEBUG] Left column is STRING type: {col_type_name}"
                                    )
                        except (AttributeError, KeyError, TypeError) as e:
                            # Table lookup failed (might be CTE or table not found)
                            logging.debug(
                                f"[NUMERIC_DEBUG] Table lookup failed: {type(e).__name__}: {e}"
                            )
                            # If right is numeric and left is a simple column (not an expression), default to numeric
                            # This handles CTE cases where we can't look up the table
                            # Check if column is simple (no operation, or operation is None)
                            has_operation = (
                                hasattr(expr.column, "operation")
                                and expr.column.operation is not None
                            )
                            if (
                                right_is_numeric
                                and hasattr(expr.column, "name")
                                and not has_operation
                            ):
                                # Simple column reference with numeric right operand - default to numeric
                                # This is the key fix: if we can't look up the table (CTE case), but right is numeric
                                # and left is a simple column, assume it's numeric (common case)
                                left_is_numeric = True
                                logging.debug(
                                    f"[NUMERIC_DEBUG] Defaulting to NUMERIC (CTE fallback): col={expr.column.name}, right_is_numeric={right_is_numeric}"
                                )
                            else:
                                logging.debug(
                                    f"[NUMERIC_DEBUG] Cannot default to numeric: right_is_numeric={right_is_numeric}, has_name={hasattr(expr.column, 'name')}, has_operation={has_operation}, operation_value={getattr(expr.column, 'operation', None) if hasattr(expr.column, 'name') else None}"
                                )

                # Hard guard: if either operand is an explicit string literal, do string concatenation
                try:
                    if (
                        isinstance(expr.column, MockLiteral)
                        and isinstance(expr.column.value, str)
                    ) or (
                        isinstance(expr.value, MockLiteral)
                        and isinstance(expr.value.value, str)
                    ):
                        is_string_operation = True
                except Exception:
                    pass

                # If both are numeric, it's definitely numeric addition - skip all string checks
                if left_is_numeric and right_is_numeric:
                    is_string_operation = False
                    logging.debug(
                        "[NUMERIC_DEBUG] BOTH NUMERIC detected - using numeric addition (+)"
                    )
                    # Skip all further string checks - go directly to numeric addition
                    # Don't check any more conditions
                # Do not immediately force string when left is VARCHAR; allow numeric fallback below
                elif left_is_string and not right_is_numeric:
                    is_string_operation = True
                    logging.debug(
                        "[NUMERIC_DEBUG] Left is STRING and right not numeric - using string concatenation (||)"
                    )
                # If we haven't determined yet, check for string indicators
                elif is_string_operation is None:
                    is_string_operation = False
                    logging.debug(
                        "[NUMERIC_DEBUG] Type unknown - defaulting to False (will check string indicators)"
                    )

                    # Check if left side already uses || (string concatenation)
                    if "||" in left or (left.startswith("'") and left.endswith("'")):
                        is_string_operation = True
                        logging.debug(
                            f"[NUMERIC_DEBUG] Left SQL contains || or string literal: left={left}"
                        )

                    # Check left operand - check for column type in source table if available
                    if not is_string_operation and hasattr(expr, "column"):
                        if isinstance(expr.column, MockLiteral):
                            if isinstance(expr.column.value, str) or (
                                hasattr(expr.column, "data_type")
                                and expr.column.data_type.__class__.__name__
                                == "StringType"
                            ):
                                is_string_operation = True
                        # Check if it's a string type column (MockColumn with StringType)
                        elif (
                            hasattr(expr.column, "column_type")
                            and expr.column.column_type.__class__.__name__
                            == "StringType"
                        ):
                            is_string_operation = True
                    # Check right operand (expr.value) - before SQL conversion
                    if isinstance(expr.value, str):
                        is_string_operation = True
                    elif isinstance(expr.value, MockLiteral):
                        if isinstance(expr.value.value, str) or (
                            hasattr(expr.value, "data_type")
                            and expr.value.data_type.__class__.__name__ == "StringType"
                        ):
                            is_string_operation = True
                    elif hasattr(expr.value, "name") and (
                        not hasattr(expr.value, "operation")
                        or expr.value.operation is None
                    ):
                        # Right is a column but we may lack table metadata (CTE case)
                        # Heuristic: if left is a simple column and not explicitly string, default to NUMERIC for column+column
                        if (
                            not left_is_string
                            and hasattr(expr.column, "name")
                            and (
                                not hasattr(expr.column, "operation")
                                or expr.column.operation is None
                            )
                        ):
                            is_string_operation = False

                    # Also check if right SQL is a quoted string (backup check)
                    if (
                        not is_string_operation
                        and right.startswith("'")
                        and right.endswith("'")
                    ):
                        is_string_operation = True

                import logging

                logging.debug(
                    f"[NUMERIC_DEBUG] FINAL DECISION: is_string_operation={is_string_operation}, left={left[:50] if len(left) > 50 else left}, right={right}"
                )

                # Final safeguard: if right is numeric and no side is an explicit quoted string, prefer numeric
                try:
                    if is_string_operation and right_is_numeric:
                        left_is_quoted = left.startswith("'") or left.startswith('"')
                        right_is_quoted = right.startswith("'") or right.startswith('"')
                        if not left_is_quoted and not right_is_quoted:
                            is_string_operation = False
                            logging.debug(
                                "[NUMERIC_DEBUG] Overriding to NUMERIC: right is numeric and no quoted strings detected"
                            )
                except Exception:
                    pass

                if is_string_operation:
                    # Use DuckDB's || operator for string concatenation
                    # Enhanced type coercion: cast non-string operands to VARCHAR
                    logging.debug(
                        "[NUMERIC_DEBUG] Generating string concatenation SQL with ||"
                    )
                    left_for_concat = left
                    right_for_concat = right

                    # Check if left needs casting (non-string column or literal)
                    if hasattr(expr, "column"):
                        # Check if left operand is a non-string type that needs coercion
                        needs_left_cast = False
                        if isinstance(expr.column, MockLiteral):
                            if not isinstance(expr.column.value, str) and (
                                not hasattr(expr.column, "data_type")
                                or expr.column.data_type.__class__.__name__
                                != "StringType"
                            ):
                                needs_left_cast = True
                        elif (
                            hasattr(expr.column, "column_type")
                            and expr.column.column_type.__class__.__name__
                            != "StringType"
                        ):
                            needs_left_cast = True

                        if needs_left_cast:
                            left_for_concat = f"CAST({left} AS VARCHAR)"

                    # Check if right needs casting (non-string literal or column)
                    # BUT only if we haven't already determined this is numeric
                    needs_right_cast = False
                    if not (left_is_numeric and right_is_numeric):
                        # Only cast to VARCHAR if this is actually a string operation
                        if isinstance(expr.value, MockLiteral):
                            if not isinstance(expr.value.value, str) and (
                                not hasattr(expr.value, "data_type")
                                or expr.value.data_type.__class__.__name__
                                != "StringType"
                            ):
                                needs_right_cast = True
                        elif (
                            hasattr(expr.value, "column_type")
                            and expr.value.column_type.__class__.__name__
                            != "StringType"
                        ):
                            needs_right_cast = True
                        # Check if right SQL is numeric (not quoted) and not already an SQL expression
                        elif (
                            not right.startswith("'")
                            and not right.startswith('"')
                            and not any(
                                keyword in right.upper()
                                for keyword in [
                                    "CAST(",
                                    "TRY_CAST(",
                                    "STRPTIME(",
                                    "EXTRACT(",
                                    "CONCAT",
                                ]
                            )
                        ):
                            # Likely a numeric literal - cast it for string concatenation
                            try:
                                # Try to parse as number to confirm
                                float(right.strip().rstrip(")").lstrip("("))
                                needs_right_cast = True
                            except (ValueError, AttributeError):
                                # Not a simple numeric literal, leave as is
                                pass

                        if needs_right_cast:
                            right_for_concat = f"CAST({right} AS VARCHAR)"

                    result = f"({left_for_concat} || {right_for_concat})"
                    logging.debug(f"[NUMERIC_DEBUG] Generated STRING SQL: {result}")
                    return result
                else:
                    # Numeric addition
                    # If we detected numeric types but left might be VARCHAR in DB, add explicit cast
                    if left_is_numeric and right_is_numeric:
                        # Explicitly cast to INTEGER to ensure numeric operation
                        # This handles cases where column is stored as VARCHAR but contains numbers
                        result = f"(CAST({left} AS INTEGER) + {right})"
                        logging.debug(
                            f"[NUMERIC_DEBUG] Generated NUMERIC SQL (with CAST): {result}"
                        )
                        return result
                    else:
                        # Regular numeric addition (force FLOAT cast to avoid accidental concatenation)
                        result = f"(CAST({left} AS DOUBLE) + CAST({right} AS DOUBLE))"
                        logging.debug(
                            f"[NUMERIC_DEBUG] Generated NUMERIC SQL (no CAST): {result}"
                        )
                        return result
            elif expr.operation == "-":
                return f"({left} - {right})"
            elif expr.operation == "/":
                return f"({left} / {right})"
            elif expr.operation == "cast":
                # Handle cast operation with proper SQL syntax using TRY_CAST for safety
                # Map PySpark type names to DuckDB type names
                type_map = {
                    "string": "VARCHAR",
                    "int": "INTEGER",
                    "integer": "INTEGER",
                    "long": "BIGINT",
                    "bigint": "BIGINT",
                    "double": "DOUBLE",
                    "float": "FLOAT",
                    "boolean": "BOOLEAN",
                    "bool": "BOOLEAN",
                    "date": "DATE",
                    "timestamp": "TIMESTAMP",
                    "decimal": "DECIMAL",
                    "varchar": "VARCHAR",
                    "numeric": "DECIMAL",
                }
                # Get the raw type value before any SQL conversion
                # Handle both string values and already-converted SQL strings
                if isinstance(expr.value, str):
                    # Direct string value (e.g., "int", "date", "string")
                    target_type = expr.value
                else:
                    # Already converted to SQL (might have quotes), extract the value
                    # right is the result of value_to_sql(expr.value), which might quote strings
                    if isinstance(right, str):
                        # Remove any surrounding quotes from the SQL string
                        target_type = right.strip().strip("'\"")
                    else:
                        # Fallback: try to get string representation
                        target_type = str(right).strip().strip("'\"")

                # Normalize: remove any remaining quotes, convert to lowercase for lookup
                target_type = target_type.strip().strip("'\"")
                # Map to DuckDB type (uppercase, no quotes)
                target_type = type_map.get(target_type.lower(), target_type.upper())
                # Ensure we never output quoted types - always use uppercase DuckDB types
                return f"TRY_CAST({left} AS {target_type})"
            # Handle boolean/logical operations
            elif expr.operation == "&":
                # Logical AND operation - convert to SQL AND
                # Use already converted left and right values
                if right is None:
                    raise ValueError("AND operation requires two operands")
                return f"({left} AND {right})"
            elif expr.operation == "|":
                # Logical OR operation - convert to SQL OR
                # Use already converted left and right values
                if right is None:
                    raise ValueError("OR operation requires two operands")
                return f"({left} OR {right})"
            elif expr.operation == "!":
                # Logical NOT using CASE to avoid dialect/operator translation issues
                left_expr = self.column_to_sql(expr.column, source_table)
                return f"(CASE WHEN {left_expr} THEN FALSE ELSE TRUE END)"
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
                        return (
                            f"coalesce({left}, {self.value_to_sql(expr.value.value)})"
                        )
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
                    # Logical NOT operation using CASE to avoid operator translation
                    expr = self.condition_to_sql(condition.column, source_table_obj)
                    if expr is not None:
                        return f"(CASE WHEN {expr} THEN FALSE ELSE TRUE END)"
                    else:
                        # Fallback: return empty string if expression cannot be converted
                        return "FALSE"
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
                # Use explicit SQL NOT to avoid dialect-specific '!' rendering
                from sqlalchemy import literal_column

                # Render inner expression via SQLAlchemy then wrap with NOT
                inner_sql = None
                try:
                    inner_sql = str(left)
                except Exception:
                    inner_sql = None
                if not inner_sql:
                    # Fallback to column name if available
                    try:
                        inner_sql = str(
                            table_obj.c[getattr(expr.column, "name", str(expr.column))]
                        )
                    except Exception:
                        inner_sql = "FALSE"
                return literal_column(f"(NOT {inner_sql})")
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
                    # Logical NOT operation - render as explicit NOT
                    from sqlalchemy import literal_column

                    inner = (
                        self.condition_to_sql(condition.column, table_obj)
                        if hasattr(self, "condition_to_sql")
                        else None
                    )
                    if inner is None:
                        # Fallback to column name
                        try:
                            inner = str(
                                self.column_to_sqlalchemy(table_obj, condition.column)
                            )
                        except Exception:
                            inner = "FALSE"
                    return literal_column(f"(NOT {inner})")
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

    def _needs_string_coercion(self, expr_or_value: Any, sql_str: str) -> bool:
        """Check if an expression or value needs coercion to VARCHAR for string operations.

        Args:
            expr_or_value: The expression or value to check
            sql_str: The SQL string representation (for fallback checking)

        Returns:
            True if coercion to VARCHAR is needed, False otherwise
        """

        # Check MockLiteral
        if isinstance(expr_or_value, MockLiteral):
            if isinstance(expr_or_value.value, str):
                return False
            if (
                hasattr(expr_or_value, "data_type")
                and expr_or_value.data_type.__class__.__name__ == "StringType"
            ):
                return False
            # Non-string literal needs coercion
            return True

        # Check MockColumn or column with type
        if hasattr(expr_or_value, "column_type"):
            if expr_or_value.column_type.__class__.__name__ == "StringType":
                return False
            # Non-string column needs coercion
            return True

        # Check if SQL string is already quoted (string literal)
        if sql_str.startswith("'") or sql_str.startswith('"'):
            return False

        # Check if it's already an SQL expression with casting/formatting
        if any(
            keyword in sql_str.upper()
            for keyword in [
                "CAST(",
                "TRY_CAST(",
                "STRPTIME(",
                "EXTRACT(",
                "CONCAT",
                "STRFTIME(",
            ]
        ):
            return False

        # Try to parse as numeric - if successful, needs coercion
        try:
            float(sql_str.strip().rstrip(")").lstrip("("))
            return True
        except (ValueError, AttributeError):
            # Not a simple numeric, leave as is
            return False

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
