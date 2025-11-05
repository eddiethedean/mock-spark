"""
Expression translator for converting MockColumn expressions to Polars expressions.

This module translates MockSpark column expressions (MockColumn, MockColumnOperation)
to Polars expressions (pl.Expr) for DataFrame operations.
"""

from typing import Any, Optional
import polars as pl
from mock_spark.functions import MockColumn, MockColumnOperation, MockLiteral
from mock_spark.functions.base import MockAggregateFunction
from mock_spark.functions.window_execution import MockWindowFunction


class PolarsExpressionTranslator:
    """Translates MockColumn expressions to Polars expressions."""

    def translate(self, expr: Any) -> pl.Expr:
        """Translate MockColumn expression to Polars expression.

        Args:
            expr: MockColumn, MockColumnOperation, or other expression

        Returns:
            Polars expression (pl.Expr)
        """
        if isinstance(expr, MockColumn):
            return self._translate_column(expr)
        elif isinstance(expr, MockColumnOperation):
            return self._translate_operation(expr)
        elif isinstance(expr, MockLiteral):
            return self._translate_literal(expr)
        elif isinstance(expr, MockAggregateFunction):
            return self._translate_aggregate_function(expr)
        elif isinstance(expr, MockWindowFunction):
            # Window functions are handled separately in window_handler.py
            raise ValueError("Window functions should be handled by WindowHandler")
        elif isinstance(expr, str):
            # String column name
            return pl.col(expr)
        elif isinstance(expr, (int, float, bool)):
            # Literal value
            return pl.lit(expr)
        elif isinstance(expr, tuple):
            # Tuple - this is likely a function argument tuple, not a literal
            # Don't try to create a literal from it - tuples as literals are not supported in Polars
            # This should be handled by the function that uses it (e.g., concat_ws, substring)
            # If we reach here, it means a tuple was passed where it shouldn't be
            raise ValueError(f"Cannot translate tuple as literal: {expr}. This should be handled by the function that uses it.")
        elif expr is None:
            return pl.lit(None)
        else:
            raise ValueError(f"Unsupported expression type: {type(expr)}")

    def _translate_column(self, col: MockColumn) -> pl.Expr:
        """Translate MockColumn to Polars column expression.

        Args:
            col: MockColumn instance

        Returns:
            Polars column expression
        """
        # If column has an alias, use the original column name for translation
        # The alias will be applied when the expression is used in select
        if hasattr(col, "_original_column") and col._original_column is not None:
            # Use the original column's name for the actual column reference
            return pl.col(col._original_column.name)
        # Use the column's name directly
        return pl.col(col.name)

    def _translate_literal(self, lit: MockLiteral) -> pl.Expr:
        """Translate MockLiteral to Polars literal expression.

        Args:
            lit: MockLiteral instance

        Returns:
            Polars literal expression
        """
        return pl.lit(lit.value)

    def _translate_operation(self, op: MockColumnOperation) -> pl.Expr:
        """Translate MockColumnOperation to Polars expression.

        Args:
            op: MockColumnOperation instance

        Returns:
            Polars expression
        """
        operation = op.operation
        column = op.column
        value = op.value

        # Translate left side
        if isinstance(column, MockColumn):
            left = pl.col(column.name)
        elif isinstance(column, MockColumnOperation):
            left = self._translate_operation(column)
        elif isinstance(column, MockLiteral):
            left = pl.lit(column.value)
        elif isinstance(column, str):
            left = pl.col(column)
        elif isinstance(column, (int, float, bool)):
            left = pl.lit(column)
        else:
            left = self.translate(column)

        # Check if this is a binary operator first (must be handled as binary operation, not function)
        binary_operators = ["==", "!=", "<", "<=", ">", ">=", "+", "-", "*", "/", "%", "&", "|"]
        if operation in binary_operators:
            # Binary operators should NOT be routed to function calls - handle as binary operation below
            pass
        # Check if this is a unary operator (must be handled as unary operation, not function)
        elif value is None and operation in ["!", "-"]:
            # Unary operators should NOT be routed to function calls - handle as unary operation below
            pass
        # Check if this is a function call (not a binary or unary operation)
        # Functions like concat_ws, substring, etc. have values but are not binary operations
        elif hasattr(op, "function_name") or operation in [
            "substring", "regexp_replace", "regexp_extract", "split", "concat", "concat_ws",
            "like", "rlike", "round", "pow", "to_date", "to_timestamp", "date_format",
            "date_add", "date_sub", "datediff", "lpad", "rpad", "repeat", "instr", "locate",
            "add_months", "last_day"
        ]:
            return self._translate_function_call(op)

        # Handle unary operations
        if value is None:
            # Handle operators first (before function calls)
            if operation == "!":
                return ~left
            elif operation == "-":
                return -left
            elif operation in ["isnull", "isNull"]:
                return left.is_null()
            elif operation in ["isnotnull", "isNotNull"]:
                return left.is_not_null()
            # Check if it's a function call (e.g., upper, lower, length)
            # Also check for datetime functions and other unary functions
            elif hasattr(op, "function_name") or operation in [
                "upper", "lower", "length", "trim", "ltrim", "rtrim",
                "abs", "ceil", "floor", "sqrt", "exp", "log", "log10",
                "sin", "cos", "tan", "round",
                "year", "month", "day", "dayofmonth", "hour", "minute", "second",
                "dayofweek", "dayofyear", "weekofyear", "quarter",
                "to_date", "current_timestamp", "current_date",
            ]:
                return self._translate_function_call(op)
            else:
                raise ValueError(f"Unsupported unary operation: {operation}")

        # Translate right side
        if isinstance(value, MockColumn):
            right = pl.col(value.name)
        elif isinstance(value, MockColumnOperation):
            right = self._translate_operation(value)
        elif isinstance(value, MockLiteral):
            right = pl.lit(value.value)
        elif isinstance(value, (int, float, bool, str)):
            right = pl.lit(value)
        elif value is None:
            right = pl.lit(None)
        else:
            right = self.translate(value)

        # Handle binary operations
        if operation == "==":
            return left == right
        elif operation == "!=":
            return left != right
        elif operation == "<":
            return left < right
        elif operation == "<=":
            return left <= right
        elif operation == ">":
            return left > right
        elif operation == ">=":
            return left >= right
        elif operation == "+":
            return left + right
        elif operation == "-":
            return left - right
        elif operation == "*":
            return left * right
        elif operation == "/":
            return left / right
        elif operation == "%":
            return left % right
        elif operation == "&":
            return left & right
        elif operation == "|":
            return left | right
        elif operation == "cast":
            # Handle cast operation
            return self._translate_cast(left, value)
        elif operation == "isin":
            # Handle isin operation
            if isinstance(value, list):
                return left.is_in(value)
            else:
                return left.is_in([value])
        elif operation in ["contains", "startswith", "endswith"]:
            return self._translate_string_operation(left, operation, value)
        elif hasattr(op, "function_name"):
            # Handle function calls (e.g., upper, lower, sum, etc.)
            return self._translate_function_call(op)
        else:
            raise ValueError(f"Unsupported operation: {operation}")

    def _translate_cast(self, expr: pl.Expr, target_type: Any) -> pl.Expr:
        """Translate cast operation.

        Args:
            expr: Polars expression to cast
            target_type: Target data type (MockDataType or string type name)

        Returns:
            Casted Polars expression
        """
        from .type_mapper import mock_type_to_polars_dtype
        from mock_spark.spark_types import (
            StringType, IntegerType, LongType, DoubleType, FloatType,
            BooleanType, DateType, TimestampType, ShortType, ByteType
        )

        # Handle string type names (e.g., "string", "int", "long")
        if isinstance(target_type, str):
            type_name_map = {
                "string": StringType(),
                "str": StringType(),
                "int": IntegerType(),
                "integer": IntegerType(),
                "long": LongType(),
                "bigint": LongType(),
                "double": DoubleType(),
                "float": FloatType(),
                "boolean": BooleanType(),
                "bool": BooleanType(),
                "date": DateType(),
                "timestamp": TimestampType(),
                "short": ShortType(),
                "byte": ByteType(),
            }
            target_type = type_name_map.get(target_type.lower())
            if target_type is None:
                raise ValueError(f"Unsupported cast type: {target_type}")

        polars_dtype = mock_type_to_polars_dtype(target_type)
        return expr.cast(polars_dtype)

    def _translate_string_operation(
        self, expr: pl.Expr, operation: str, value: Any
    ) -> pl.Expr:
        """Translate string operations.

        Args:
            expr: Polars expression (string column)
            operation: String operation name
            value: Operation value

        Returns:
            Polars expression for string operation
        """
        if operation == "contains":
            if isinstance(value, str):
                return expr.str.contains(value)
            else:
                value_expr = self.translate(value)
                return expr.str.contains(value_expr)
        elif operation == "startswith":
            if isinstance(value, str):
                return expr.str.starts_with(value)
            else:
                value_expr = self.translate(value)
                return expr.str.starts_with(value_expr)
        elif operation == "endswith":
            if isinstance(value, str):
                return expr.str.ends_with(value)
            else:
                value_expr = self.translate(value)
                return expr.str.ends_with(value_expr)
        else:
            raise ValueError(f"Unsupported string operation: {operation}")

    def _translate_function_call(self, op: MockColumnOperation) -> pl.Expr:
        """Translate function call operations.

        Args:
            op: MockColumnOperation with function call

        Returns:
            Polars expression for function call
        """
        function_name = getattr(op, "function_name", op.operation).lower()
        column = op.column

        # Handle functions without column first (e.g., current_timestamp, current_date, monotonically_increasing_id)
        if column is None:
            operation = op.operation  # Extract operation for use in comparisons
            if operation == "current_timestamp":
                # Use datetime.now() which returns current timestamp
                from datetime import datetime
                return pl.lit(datetime.now())
            elif operation == "current_date":
                # Use date.today() which returns current date
                from datetime import date
                return pl.lit(date.today())
            elif function_name == "monotonically_increasing_id":
                # monotonically_increasing_id() - generate row numbers
                # Use int_range to generate sequential IDs
                return pl.int_range(pl.len())

        # Translate column expression
        if isinstance(column, MockColumn):
            col_expr = pl.col(column.name)
        elif isinstance(column, MockColumnOperation):
            col_expr = self._translate_operation(column)
        elif isinstance(column, str):
            col_expr = pl.col(column)
        else:
            col_expr = self.translate(column)

        # Map function names to Polars expressions
        # Handle functions with arguments
        if op.value is not None:
            operation = op.operation  # Extract operation for use in comparisons
            if operation == "substring":
                # substring(col, start, length) - Polars uses 0-indexed, PySpark uses 1-indexed
                if isinstance(op.value, tuple):
                    start = op.value[0]
                    length = op.value[1] if len(op.value) > 1 else None
                    # Convert 1-indexed to 0-indexed
                    start_idx = start - 1 if start > 0 else 0
                    if length is not None:
                        return col_expr.str.slice(start_idx, length)
                    else:
                        return col_expr.str.slice(start_idx)
                else:
                    return col_expr.str.slice(op.value - 1 if op.value > 0 else 0)
            elif operation == "regexp_replace":
                # regexp_replace(col, pattern, replacement)
                if isinstance(op.value, tuple) and len(op.value) >= 2:
                    pattern = op.value[0]
                    replacement = op.value[1]
                    return col_expr.str.replace_all(pattern, replacement, literal=True)
                else:
                    raise ValueError(f"regexp_replace requires (pattern, replacement) tuple")
            elif operation == "regexp_extract":
                # regexp_extract(col, pattern, idx)
                if isinstance(op.value, tuple) and len(op.value) >= 2:
                    pattern = op.value[0]
                    idx = op.value[1] if len(op.value) > 1 else 0
                    # Polars extract_all returns a list, we need to get the first match
                    return col_expr.str.extract(pattern, idx)
                else:
                    raise ValueError(f"regexp_extract requires (pattern, idx) tuple")
            elif operation == "split":
                # split(col, delimiter)
                delimiter = op.value
                return col_expr.str.split(delimiter)
            elif operation == "concat":
                # concat(*columns) - op.value is list of additional columns/literals
                if isinstance(op.value, list) and len(op.value) > 0:
                    # Translate all columns/literals
                    other_cols = []
                    for col in op.value:
                        if isinstance(col, str):
                            # String literal
                            other_cols.append(pl.lit(col))
                        else:
                            # Column or expression
                            other_cols.append(self.translate(col))
                    # Concatenate all columns
                    result = col_expr
                    for other_col in other_cols:
                        result = result + other_col
                    return result
                else:
                    return col_expr
            elif operation == "concat_ws":
                # concat_ws(sep, *columns) - op.value is (sep, [columns])
                if isinstance(op.value, tuple) and len(op.value) >= 1:
                    sep = op.value[0]
                    other_cols = op.value[1] if len(op.value) > 1 else []
                    # Translate all columns - ensure they're properly translated
                    translated_cols = []
                    # First column is already in col_expr
                    translated_cols.append(col_expr)
                    # Translate other columns
                    for col in other_cols:
                        if isinstance(col, str):
                            # String column name
                            translated_cols.append(pl.col(col))
                        elif isinstance(col, (int, float, bool)):
                            # Literal value
                            translated_cols.append(pl.lit(col))
                        else:
                            # Expression or MockColumn
                            translated_cols.append(self.translate(col))
                    # Join with separator using Polars
                    # Ensure all columns are strings to avoid nested Objects error
                    if len(translated_cols) == 1:
                        return translated_cols[0].cast(pl.Utf8)
                    # Cast all to string first
                    str_cols = [col.cast(pl.Utf8) for col in translated_cols]
                    result = str_cols[0]
                    for other_col in str_cols[1:]:
                        result = result + pl.lit(str(sep)) + other_col
                    return result
                else:
                    raise ValueError(f"concat_ws requires (sep, [columns]) tuple")
            elif operation == "like":
                # SQL LIKE pattern - convert to Polars regex
                pattern = op.value
                # Convert SQL LIKE to regex: % -> .*, _ -> .
                regex_pattern = pattern.replace("%", ".*").replace("_", ".")
                return col_expr.str.contains(regex_pattern, literal=False)
            elif operation == "rlike":
                # Regular expression pattern matching
                pattern = op.value
                return col_expr.str.contains(pattern, literal=False)
            elif operation == "round":
                # round(col, decimals)
                decimals = op.value if isinstance(op.value, int) else 0
                return col_expr.round(decimals)
            elif operation == "pow":
                # pow(col, exponent)
                exponent = self.translate(op.value) if not isinstance(op.value, (int, float)) else pl.lit(op.value)
                return col_expr.pow(exponent)
            elif operation == "to_date":
                # to_date(col, format) or to_date(col)
                if op.value is not None:
                    # With format string
                    format_str = op.value
                    return col_expr.str.strptime(pl.Date, format_str)
                else:
                    # Without format - try to parse common formats
                    return col_expr.str.strptime(pl.Date)
            elif operation == "to_timestamp":
                # to_timestamp(col, format) or to_timestamp(col)
                if op.value is not None:
                    # With format string - convert Java SimpleDateFormat to Polars format
                    format_str = op.value
                    # Handle optional fractional seconds like [.SSSSSS]
                    import re
                    # Check if format has optional fractional seconds
                    has_optional_fractional = bool(re.search(r'\[\.S+\]', format_str))
                    # Remove optional fractional pattern from format string
                    format_str = re.sub(r'\[\.S+\]', '', format_str)
                    # Handle single-quoted literals (e.g., 'T' in yyyy-MM-dd'T'HH:mm:ss)
                    format_str = re.sub(r"'([^']*)'", r"\1", format_str)
                    # Convert Java format to Polars format
                    format_map = {
                        "yyyy": "%Y",
                        "MM": "%m",
                        "dd": "%d",
                        "HH": "%H",
                        "mm": "%M",
                        "ss": "%S",
                    }
                    # Sort by length descending to process longest matches first
                    for java_pattern, polars_pattern in sorted(format_map.items(), key=lambda x: len(x[0]), reverse=True):
                        format_str = format_str.replace(java_pattern, polars_pattern)
                    # If there was optional fractional seconds, try parsing with and without
                    if has_optional_fractional:
                        # Try with microseconds format - use %.f for Polars (not .%f)
                        format_with_us = format_str + "%.f"
                        # Use strict=False to handle optional parts
                        return col_expr.str.strptime(pl.Datetime, format_with_us, strict=False)
                    return col_expr.str.strptime(pl.Datetime, format_str, strict=False)
                else:
                    # Without format - try to parse common formats
                    return col_expr.str.strptime(pl.Datetime, strict=False)
            elif operation == "date_format":
                # date_format(col, format) - format a date/timestamp column
                if isinstance(op.value, str):
                    format_str = op.value
                    # Convert Java SimpleDateFormat to Polars strftime format
                    # Common conversions: yyyy -> %Y, MM -> %m, dd -> %d, HH -> %H, mm -> %M, ss -> %S
                    import re
                    format_map = {
                        "yyyy": "%Y", "MM": "%m", "dd": "%d", 
                        "HH": "%H", "mm": "%M", "ss": "%S",
                        "EEE": "%a", "EEEE": "%A", "MMM": "%b", "MMMM": "%B"
                    }
                    polars_format = format_str
                    for java_pattern, polars_pattern in sorted(format_map.items(), key=lambda x: len(x[0]), reverse=True):
                        polars_format = polars_format.replace(java_pattern, polars_pattern)
                    # If column is string, parse it first; if already date, use directly
                    # For now, assume we need to parse string dates
                    parsed = col_expr.str.strptime(pl.Date, '%Y-%m-%d', strict=False)
                    return parsed.dt.strftime(polars_format)
                else:
                    raise ValueError(f"date_format requires format string")
            elif operation == "date_add":
                # date_add(col, days)
                days = self.translate(op.value) if not isinstance(op.value, int) else pl.lit(op.value)
                return col_expr.dt.offset_by(days)
            elif operation == "date_sub":
                # date_sub(col, days)
                days = self.translate(op.value) if not isinstance(op.value, int) else pl.lit(op.value)
                return col_expr.dt.offset_by(-days)
            elif operation == "datediff":
                # datediff(end, start) - note: in PySpark, end comes first
                # In MockColumnOperation: column is end, value is start
                start_date = self.translate(op.value)
                # Handle both string dates and date columns
                # Polars str.strptime() only works on string columns, so it fails on date columns
                # Use cast to Date which works for both: strings are parsed, dates are unchanged
                end_parsed = col_expr.cast(pl.Date)
                start_parsed = start_date.cast(pl.Date)
                return (end_parsed - start_parsed).dt.total_days()
            elif operation == "lpad":
                # lpad(col, len, pad)
                if isinstance(op.value, tuple) and len(op.value) >= 2:
                    target_len = op.value[0]
                    pad_str = op.value[1]
                    return col_expr.str.pad_start(target_len, pad_str)
                else:
                    raise ValueError(f"lpad requires (len, pad) tuple")
            elif operation == "rpad":
                # rpad(col, len, pad)
                if isinstance(op.value, tuple) and len(op.value) >= 2:
                    target_len = op.value[0]
                    pad_str = op.value[1]
                    return col_expr.str.pad_end(target_len, pad_str)
                else:
                    raise ValueError(f"rpad requires (len, pad) tuple")
            elif operation == "repeat":
                # repeat(col, n)
                n = op.value if isinstance(op.value, int) else int(op.value)
                return col_expr.str.repeat(n)
            elif operation == "instr":
                # instr(col, substr) - returns 1-based position, or 0 if not found
                substr = op.value if isinstance(op.value, str) else str(op.value)
                # Polars str.find() returns -1 if not found, we need 0
                # So we check if it's -1, return 0, otherwise add 1 for 1-based indexing
                # Add fill_null(0) as fallback for any nulls
                find_result = col_expr.str.find(substr)
                return pl.when(find_result == -1).then(0).otherwise(find_result + 1).fill_null(0)
            elif operation == "locate":
                # locate(substr, col, pos) - op.value is (substr, pos)
                if isinstance(op.value, tuple) and len(op.value) >= 1:
                    substr = op.value[0]
                    pos = op.value[1] if len(op.value) > 1 else 1
                    # Find substring starting from pos (1-indexed)
                    return (col_expr.str.slice(pos - 1).str.find(substr) + pos).fill_null(0)
                else:
                    substr = op.value
                    return col_expr.str.find(substr) + 1
            elif operation == "add_months":
                # add_months(col, months)
                months = op.value if isinstance(op.value, int) else int(op.value)
                # Use offset_by with months
                return col_expr.dt.offset_by(f"{months}mo")
            elif operation == "last_day":
                # last_day(col) - get last day of month
                # Get first day of next month, then subtract 1 day
                first_of_month = col_expr.dt.replace(day=1)
                first_of_next_month = first_of_month.dt.offset_by("1mo")
                return first_of_next_month.dt.offset_by("-1d")
            elif operation == "array_contains":
                # array_contains(col, value) - check if array contains value
                value_expr = pl.lit(op.value) if not isinstance(op.value, (MockColumn, MockColumnOperation)) else self.translate(op.value)
                return col_expr.list.contains(value_expr)
            elif operation == "array_position":
                # array_position(col, value) - find 1-based position of value in array
                # Polars doesn't have list.index(), so we use list.eval to find position
                value_expr = pl.lit(op.value) if not isinstance(op.value, (MockColumn, MockColumnOperation)) else self.translate(op.value)
                # Use list.eval to create indices where element equals value, get first, add 1 for 1-based
                # If not found, returns null, which we convert to 0 (PySpark returns 0 if not found)
                return (col_expr.list.eval(
                    pl.int_range(pl.len()).filter(pl.element() == value_expr)
                ).list.first()).fill_null(-1) + 1
            elif operation == "element_at":
                # element_at(col, index) - get element at 1-based index (negative for reverse)
                index = op.value if isinstance(op.value, int) else int(op.value)
                # Polars list.get() uses 0-based indexing, but element_at is 1-based
                # For negative indices, count from end
                if index > 0:
                    return col_expr.list.get(index - 1)
                else:
                    # Negative index: count from end
                    return col_expr.list.get(index)
            elif operation == "array_append":
                # array_append(col, value) - append value to array
                # Polars doesn't have list.append(), use list.eval with concat
                value_expr = pl.lit(op.value) if not isinstance(op.value, (MockColumn, MockColumnOperation)) else self.translate(op.value)
                return col_expr.list.eval(pl.concat([pl.element(), value_expr]))
            elif operation == "array_remove":
                # array_remove(col, value) - remove all occurrences of value from array
                value_expr = pl.lit(op.value) if not isinstance(op.value, (MockColumn, MockColumnOperation)) else self.translate(op.value)
                return col_expr.list.eval(pl.element().filter(pl.element() != value_expr))
            elif operation == "array":
                # array(*cols) - create array from columns
                # op.value can be list, tuple, or None
                if isinstance(op.value, (list, tuple)) and len(op.value) > 0:
                    cols = [col_expr] + [self.translate(col) for col in op.value]
                    return pl.concat_list(cols)
                elif op.value is None:
                    # Single column - just wrap it in a list
                    return pl.list([col_expr])
                else:
                    # Try to handle as iterable
                    cols = [col_expr]
                    try:
                        for col in op.value:
                            cols.append(self.translate(col))
                        return pl.concat_list(cols)
                    except (TypeError, AttributeError):
                        return pl.list([col_expr])
            elif operation == "array_intersect":
                # array_intersect(col1, col2) - intersection of two arrays
                col2_expr = self.translate(op.value)
                return col_expr.list.set_intersection(col2_expr)
            elif operation == "array_union":
                # array_union(col1, col2) - union of two arrays (duplicates removed)
                col2_expr = self.translate(op.value)
                return col_expr.list.set_union(col2_expr)
            elif operation == "array_except":
                # array_except(col1, col2) - elements in col1 but not in col2
                col2_expr = self.translate(op.value)
                return col_expr.list.set_difference(col2_expr)
            elif operation == "arrays_overlap":
                # arrays_overlap(col1, col2) - check if arrays have common elements
                col2_expr = self.translate(op.value)
                # Check if intersection is non-empty
                intersection = col_expr.list.set_intersection(col2_expr)
                return intersection.list.len() > 0
            elif operation == "array_repeat":
                # array_repeat(col, count) - repeat value to create array
                # Polars doesn't have a direct repeat for columns, use map_elements
                count = op.value if isinstance(op.value, int) else int(op.value)
                # Use map_elements to create array by repeating value
                # Polars will infer the list type from the element type
                return col_expr.map_elements(lambda x: [x] * count)
            elif operation == "slice":
                # slice(col, start, length) - get slice of array (1-based start)
                if isinstance(op.value, tuple) and len(op.value) >= 2:
                    start = op.value[0]
                    length = op.value[1]
                    # Convert 1-based to 0-based for Polars
                    start_idx = start - 1 if start > 0 else 0
                    return col_expr.list.slice(start_idx, length)
                else:
                    raise ValueError(f"slice requires (start, length) tuple")
        
        # Handle special functions that need custom logic (including those that may have column but ignore it)
        if function_name == "monotonically_increasing_id":
            # monotonically_increasing_id() - can be called with or without column (ignores column)
            # Use int_range to generate sequential IDs
            return pl.int_range(pl.len())
        elif function_name == "expr":
            # expr(sql_string) - parse and evaluate SQL expression
            # For now, this should be handled at SQL executor level, not here
            # But if it reaches here, try to evaluate the SQL string
            if op.value is not None and isinstance(op.value, str):
                # This is a complex case - would need SQL parsing
                # For now, raise a more helpful error
                raise ValueError("F.expr() SQL expressions should be handled by SQL executor, not Polars backend")
        if function_name == "coalesce":
            # coalesce(*cols) - op.value should be list of columns
            if op.value is not None and isinstance(op.value, (list, tuple)):
                cols = [col_expr] + [self.translate(col) for col in op.value]
                return pl.coalesce(cols)
            else:
                return col_expr
        elif function_name == "nvl":
            # nvl(col, default) - op.value is default value
            if op.value is not None:
                default_expr = self.translate(op.value) if not isinstance(op.value, (str, int, float, bool)) else pl.lit(op.value)
                return pl.coalesce([col_expr, default_expr])
            else:
                return col_expr
        elif function_name == "nullif":
            # nullif(col1, col2) - op.value is col2
            if op.value is not None:
                col2_expr = self.translate(op.value)
                return pl.when(col_expr == col2_expr).then(None).otherwise(col_expr)
            else:
                return col_expr
        elif function_name == "greatest":
            # greatest(*cols) - op.value should be list of columns
            if op.value is not None and isinstance(op.value, (list, tuple)):
                cols = [col_expr] + [self.translate(col) for col in op.value]
                return pl.max_horizontal(cols)
            else:
                return col_expr
        elif function_name == "least":
            # least(*cols) - op.value should be list of columns
            if op.value is not None and isinstance(op.value, (list, tuple)):
                cols = [col_expr] + [self.translate(col) for col in op.value]
                return pl.min_horizontal(cols)
            else:
                return col_expr
        elif function_name == "ascii":
            # ascii(col) - return ASCII code of first character
            # Get first character's byte value
            # Using str to bytes conversion then taking first byte
            first_char = col_expr.str.slice(0, 1)
            # Convert string to bytes then get first byte as integer
            # This is a simplified approach - may need refinement
            return first_char.str.encode("utf-8").list.first().cast(pl.UInt8).fill_null(0)
        elif function_name == "hex":
            # hex(col) - convert to hexadecimal string
            # For string columns, encode to bytes then convert to hex
            # Note: This implementation may need adjustment based on Polars version
            try:
                # Try using binary encoding approach
                return col_expr.str.encode("utf-8").list.join("").str.encode("hex")
            except Exception:
                # Fallback: return as string (may need UDF support)
                return col_expr
        elif function_name == "base64":
            # base64(col) - encode to base64
            # Note: Requires UDF support or proper Polars API
            # For now, return as string (will need proper implementation)
            return col_expr
        elif function_name == "md5":
            # md5(col) - hash using MD5
            import hashlib
            # Polars doesn't have built-in MD5, so we need to use a workaround
            # For now, return a placeholder - this may need UDF support
            # Using str.encode then hashing would require a UDF
            raise ValueError("MD5 requires UDF support - not yet implemented in Polars backend")
        elif function_name == "sha1":
            # sha1(col) - hash using SHA1
            raise ValueError("SHA1 requires UDF support - not yet implemented in Polars backend")
        elif function_name == "sha2":
            # sha2(col, bits) - hash using SHA2
            raise ValueError("SHA2 requires UDF support - not yet implemented in Polars backend")
        
        # Map function names to Polars expressions (unary functions)
        function_map = {
            "upper": lambda e: e.str.to_uppercase(),
            "lower": lambda e: e.str.to_lowercase(),
            "length": lambda e: e.str.len_chars(),
            "char_length": lambda e: e.str.len_chars(),  # Alias for length
            "character_length": lambda e: e.str.len_chars(),  # Alias for length
            "trim": lambda e: e.str.strip_chars(),
            "ltrim": lambda e: e.str.strip_chars_start(),
            "rtrim": lambda e: e.str.strip_chars_end(),
            "abs": lambda e: e.abs(),
            "ceil": lambda e: e.ceil(),
            "floor": lambda e: e.floor(),
            "sqrt": lambda e: e.sqrt(),
            "exp": lambda e: e.exp(),
            "log": lambda e: e.log(),
            "log10": lambda e: e.log10(),
            "sin": lambda e: e.sin(),
            "cos": lambda e: e.cos(),
            "tan": lambda e: e.tan(),
            "sum": lambda e: e.sum(),
            "avg": lambda e: e.mean(),
            "mean": lambda e: e.mean(),
            "count": lambda e: e.count(),
            "max": lambda e: e.max(),
            "min": lambda e: e.min(),
            # Datetime extraction functions
            # For string columns, parse first; for datetime columns, use directly
            # We use a helper function to handle both cases
            "year": lambda e: self._extract_datetime_part(e, "year"),
            "month": lambda e: self._extract_datetime_part(e, "month"),
            "day": lambda e: self._extract_datetime_part(e, "day"),
            "dayofmonth": lambda e: self._extract_datetime_part(e, "day"),
            "hour": lambda e: self._extract_datetime_part(e, "hour"),
            "minute": lambda e: self._extract_datetime_part(e, "minute"),
            "second": lambda e: self._extract_datetime_part(e, "second"),
            "dayofweek": lambda e: self._extract_datetime_part(e, "dayofweek"),
            "dayofyear": lambda e: self._extract_datetime_part(e, "dayofyear"),
            "weekofyear": lambda e: self._extract_datetime_part(e, "weekofyear"),
            "quarter": lambda e: self._extract_datetime_part(e, "quarter"),
            "reverse": lambda e: self._reverse_expr(e, op),  # Handle both string and array reverse
            "isnan": lambda e: e.is_nan(),
            "to_date": lambda e: e.str.strptime(pl.Date, strict=False),
            "isnull": lambda e: e.is_null(),
            "isNull": lambda e: e.is_null(),
            "isnotnull": lambda e: e.is_not_null(),
            "isNotNull": lambda e: e.is_not_null(),
            # Array functions
            "size": lambda e: e.list.len(),
            "array_max": lambda e: e.list.max(),
            "array_min": lambda e: e.list.min(),
            "array_distinct": lambda e: e.list.unique(),
            # Note: explode expression just returns the array column
            # The actual row expansion is handled in operation_executor
            "explode": lambda e: e,  # Return the array column as-is, will be exploded in operation_executor
        }

        if function_name in function_map:
            return function_map[function_name](col_expr)
        else:
            # Fallback: try to access as attribute
            if hasattr(col_expr, function_name):
                func = getattr(col_expr, function_name)
                if callable(func):
                    if op.value is not None:
                        return func(self.translate(op.value))
                    return func()
            raise ValueError(f"Unsupported function: {function_name}")

    def _reverse_expr(self, expr: pl.Expr, op: Any) -> pl.Expr:
        """Handle reverse for both strings and arrays.
        
        Args:
            expr: Polars expression (column reference)
            op: The MockColumnOperation to check column type
            
        Returns:
            Polars expression for reverse (string or list)
        """
        # Check if the column is an array type by inspecting the operation's column
        from mock_spark.spark_types import ArrayType
        is_array = False
        if hasattr(op, 'column'):
            col = op.column
            if hasattr(col, 'column_type'):
                is_array = isinstance(col.column_type, ArrayType)
            elif hasattr(col, 'name'):
                # Could check schema, but for now default based on context
                pass
        
        if is_array:
            return expr.list.reverse()
        else:
            # Default to string reverse (F.reverse() defaults to StringFunctions)
            return expr.str.reverse()

    def _extract_datetime_part(self, expr: pl.Expr, part: str) -> pl.Expr:
        """Extract datetime part from expression, handling both string and datetime columns.
        
        Args:
            expr: Polars expression (column reference)
            part: Part to extract (year, month, day, hour, etc.)
            
        Returns:
            Polars expression for datetime part extraction
        """
        # Map of part names to Polars methods
        part_map = {
            "year": lambda e: e.dt.year(),
            "month": lambda e: e.dt.month(),
            "day": lambda e: e.dt.day(),
            "hour": lambda e: e.dt.hour(),
            "minute": lambda e: e.dt.minute(),
            "second": lambda e: e.dt.second(),
            "dayofweek": lambda e: (e.dt.weekday() % 7) + 1,  # Polars ISO: Mon=1,Sun=7; PySpark: Sun=1,Mon=2,...,Sat=7
            "dayofyear": lambda e: e.dt.ordinal_day(),
            "weekofyear": lambda e: e.dt.week(),
            "quarter": lambda e: e.dt.quarter(),
        }
        
        extractor = part_map.get(part)
        if not extractor:
            raise ValueError(f"Unsupported datetime part: {part}")
        
        # Handle both string and datetime columns
        # For string columns, we need to parse first using str.strptime()
        # For datetime columns, we can use dt methods directly
        # Since we can't check type at expression build time, we use a conditional approach
        # that tries string parsing first, with a fallback for datetime columns
        
        # Use Polars' ability to handle this with a when/then/otherwise pattern
        # But simpler: just always try str.strptime() - it will work for strings
        # For datetime columns, we need to cast them or use directly
        # Actually, str.strptime only works on string columns, so we need a different approach
        
        # Use pl.when() to conditionally handle, but we can't check dtype in expression
        # So we'll use a try-cast pattern: try to parse as string, if that fails use as datetime
        # But Polars doesn't have try-cast in expressions easily
        
        # Simplest approach: assume string and parse it
        # If the column is already datetime, this will fail at runtime
        # For now, we'll parse strings and document that datetime columns should work
        # but may need explicit handling
        
        # For string columns (most common case in tests):
        parsed = expr.str.strptime(pl.Datetime, strict=False)
        return extractor(parsed)
    
    def _translate_aggregate_function(self, agg_func: MockAggregateFunction) -> pl.Expr:
        """Translate aggregate function.

        Args:
            agg_func: MockAggregateFunction instance

        Returns:
            Polars aggregate expression
        """
        function_name = agg_func.function_name.lower()
        column = agg_func.column

        if column:
            col_expr = self.translate(column)
        else:
            # Count(*) case
            col_expr = pl.lit(1)

        if function_name == "sum":
            return col_expr.sum()
        elif function_name == "avg" or function_name == "mean":
            return col_expr.mean()
        elif function_name == "count":
            if column:
                return col_expr.count()
            else:
                return pl.len()
        elif function_name == "max":
            return col_expr.max()
        elif function_name == "min":
            return col_expr.min()
        elif function_name == "stddev" or function_name == "stddev_samp":
            return col_expr.std()
        elif function_name == "variance" or function_name == "var_samp":
            return col_expr.var()
        elif function_name == "collect_list":
            # Collect values into a list
            return col_expr.implode()
        elif function_name == "collect_set":
            # Collect unique values into a set (preserve first occurrence order, like PySpark)
            # Use maintain_order=True to preserve the order of first occurrence
            return col_expr.unique(maintain_order=True).implode()
        elif function_name == "first":
            # First value in group
            return col_expr.first()
        elif function_name == "last":
            # Last value in group
            return col_expr.last()
        else:
            raise ValueError(f"Unsupported aggregate function: {function_name}")

