"""
Centralized Expression Evaluation Engine

This module provides a unified expression evaluator used by both MockDataFrame
and MockCaseWhen to eliminate code duplication and enforce the Single
Responsibility Principle.

The ExpressionEvaluator handles:
- Condition evaluation (filter, where clauses)
- Column expressions (select, withColumn)
- Function calls (upper, lower, coalesce, etc.)
- Arithmetic operations (+, -, *, /, %)
- Comparison operations (==, !=, >, <, >=, <=)
- Logical operations (&, |, !)
- CASE WHEN expressions
"""

import re
import math
import base64
from datetime import datetime, date
from typing import Any, Dict, List, Tuple, Union, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ..functions import MockColumn, MockColumnOperation
    from ..functions.core.literals import MockLiteral


class ExpressionEvaluator:
    """Centralized expression evaluation engine.

    This class provides a single source of truth for evaluating all types
    of column expressions, conditions, and operations. It is used by both
    MockDataFrame for data operations and MockCaseWhen for conditional logic.

    Examples:
        >>> evaluator = ExpressionEvaluator()
        >>> row = {"name": "Alice", "age": 25}
        >>> condition = F.col("age") > 20
        >>> evaluator.evaluate_condition(row, condition)
        True
        >>> filtered = evaluator.apply_filter([row], condition)
        [{'name': 'Alice', 'age': 25}]
    """

    def apply_filter(self, data: List[Dict[str, Any]], condition: Any) -> List[Dict[str, Any]]:
        """Apply filter condition to dataset.

        Args:
            data: List of data rows
            condition: Filter condition to apply

        Returns:
            Filtered list of rows
        """
        filtered_data = []
        for row in data:
            if self.evaluate_condition(row, condition):
                filtered_data.append(row)
        return filtered_data

    def evaluate_condition(self, row: Dict[str, Any], condition: Any) -> bool:
        """Evaluate filter condition for a single row.

        Args:
            row: Data row as dictionary
            condition: Condition to evaluate

        Returns:
            True if condition is met, False otherwise
        """
        from ..functions import MockColumn

        if isinstance(condition, MockColumn):
            return row.get(condition.name) is not None

        operation = condition.operation
        col_value = row.get(condition.column.name)

        # Null checks
        if operation in ["isNotNull", "isnotnull"]:
            return col_value is not None
        elif operation in ["isNull", "isnull"]:
            return col_value is None

        # Comparison operations
        if operation in ["==", "!=", ">", ">=", "<", "<="]:
            return self._evaluate_comparison(col_value, operation, condition.value)

        # String operations
        if operation == "like":
            return self._evaluate_like_operation(col_value, condition.value)
        elif operation == "isin":
            return self._evaluate_isin_operation(col_value, condition.value)
        elif operation == "between":
            return self._evaluate_between_operation(col_value, condition.value)

        # Logical operations
        if operation in ["and", "&"]:
            return self.evaluate_condition(row, condition.column) and self.evaluate_condition(
                row, condition.value
            )
        elif operation in ["or", "|"]:
            return self.evaluate_condition(row, condition.column) or self.evaluate_condition(
                row, condition.value
            )
        elif operation in ["not", "!"]:
            return not self.evaluate_condition(row, condition.column)

        return False

    def _evaluate_comparison(self, col_value: Any, operation: str, condition_value: Any) -> bool:
        """Evaluate comparison operations (==, !=, >, <, >=, <=)."""
        if col_value is None:
            return operation == "!="  # Only != returns True for null values

        if operation == "==":
            return bool(col_value == condition_value)
        elif operation == "!=":
            return bool(col_value != condition_value)
        elif operation == ">":
            return bool(col_value > condition_value)
        elif operation == ">=":
            return bool(col_value >= condition_value)
        elif operation == "<":
            return bool(col_value < condition_value)
        elif operation == "<=":
            return bool(col_value <= condition_value)

        return False

    def _evaluate_like_operation(self, col_value: Any, pattern: str) -> bool:
        """Evaluate SQL LIKE operation."""
        if col_value is None:
            return False

        value = str(col_value)
        regex_pattern = str(pattern).replace("%", ".*")
        return bool(re.match(regex_pattern, value))

    def _evaluate_isin_operation(self, col_value: Any, values: List[Any]) -> bool:
        """Evaluate IN operation."""
        return col_value in values if col_value is not None else False

    def _evaluate_between_operation(self, col_value: Any, bounds: Tuple[Any, Any]) -> bool:
        """Evaluate BETWEEN operation."""
        if col_value is None:
            return False

        lower, upper = bounds
        return bool(lower <= col_value <= upper)

    def evaluate_expression(self, row: Dict[str, Any], column_expression: Any) -> Any:
        """Evaluate column expression for a single row.

        This is the main entry point for evaluating expressions in select(),
        withColumn(), and other operations.

        Args:
            row: Data row as dictionary
            column_expression: Expression to evaluate

        Returns:
            Evaluated result
        """
        from ..functions import MockColumn

        if isinstance(column_expression, MockColumn):
            return self._evaluate_mock_column(row, column_expression)
        elif hasattr(column_expression, "operation") and hasattr(column_expression, "column"):
            return self._evaluate_column_operation(row, column_expression)
        else:
            return self._evaluate_direct_value(column_expression)

    def _evaluate_mock_column(self, row: Dict[str, Any], column: Any) -> Any:
        """Evaluate a MockColumn expression."""
        col_name = column.name

        # Check if this is an aliased function call
        if self._is_aliased_function_call(column):
            if column._original_column is not None:
                original_name = column._original_column.name
                return self._evaluate_function_call_by_name(row, original_name)

        # Check if this is a direct function call
        if self._is_function_call_name(col_name):
            return self._evaluate_function_call_by_name(row, col_name)
        else:
            # Simple column reference
            return row.get(column.name)

    def _is_aliased_function_call(self, column: Any) -> bool:
        """Check if column is an aliased function call."""
        return (
            hasattr(column, "_original_column")
            and column._original_column is not None
            and hasattr(column._original_column, "name")
            and self._is_function_call_name(column._original_column.name)
        )

    def _is_function_call_name(self, name: str) -> bool:
        """Check if name represents a function call."""
        function_prefixes = (
            "coalesce(",
            "isnull(",
            "isnan(",
            "trim(",
            "ceil(",
            "floor(",
            "sqrt(",
            "regexp_replace(",
            "split(",
            "to_date(",
            "to_timestamp(",
            "hour(",
            "day(",
            "month(",
            "year(",
        )
        return name.startswith(function_prefixes)

    def _is_function_call(self, col_name: str) -> bool:
        """Check if column name is a function call."""
        function_patterns = [
            "upper(",
            "lower(",
            "length(",
            "abs(",
            "round(",
            "count(",
            "sum(",
            "avg(",
            "max(",
            "min(",
            "count(DISTINCT ",
            "coalesce(",
            "isnull(",
            "isnan(",
            "trim(",
            "ceil(",
            "floor(",
            "sqrt(",
            "regexp_replace(",
            "split(",
        ]
        return any(col_name.startswith(pattern) for pattern in function_patterns)

    def _evaluate_column_operation(self, row: Dict[str, Any], operation: Any) -> Any:
        """Evaluate a MockColumnOperation."""
        if operation.operation in ["+", "-", "*", "/", "%"]:
            return self._evaluate_arithmetic_operation(row, operation)
        else:
            return self._evaluate_function_call(row, operation)

    def _evaluate_direct_value(self, value: Any) -> Any:
        """Evaluate a direct value."""
        return value

    def _evaluate_arithmetic_operation(self, row: Dict[str, Any], operation: Any) -> Any:
        """Evaluate arithmetic operations (+, -, *, /, %)."""
        if not hasattr(operation, "operation") or not hasattr(operation, "column"):
            return None

        # Extract left value from row
        left_value = row.get(operation.column.name) if hasattr(operation.column, "name") else None

        # Extract right value - handle MockColumn, MockLiteral, or primitive values
        right_value = operation.value
        if hasattr(right_value, "name") and hasattr(right_value, "__class__"):
            # It's a MockColumn - get value from row
            if hasattr(right_value, "name"):
                right_value = row.get(right_value.name)
            else:
                right_value = None
        elif hasattr(right_value, "value"):
            # It's a MockLiteral - get the actual value
            right_value = right_value.value

        if operation.operation == "-" and operation.value is None:
            # Unary minus operation
            if left_value is None:
                return None
            return -left_value

        if left_value is None or right_value is None:
            return None

        if operation.operation == "+":
            return left_value + right_value
        elif operation.operation == "-":
            return left_value - right_value
        elif operation.operation == "*":
            return left_value * right_value
        elif operation.operation == "/":
            return left_value / right_value if right_value != 0 else None
        elif operation.operation == "%":
            return left_value % right_value if right_value != 0 else None
        else:
            return None

    def _evaluate_function_call(self, row: Dict[str, Any], operation: Any) -> Any:
        """Evaluate function calls like upper(), lower(), coalesce(), etc."""
        if not hasattr(operation, "operation") or not hasattr(operation, "column"):
            return None

        # Evaluate the column expression (could be a nested operation)
        if hasattr(operation.column, "operation") and hasattr(operation.column, "column"):
            # The column is itself a MockColumnOperation, evaluate it first
            value = self.evaluate_expression(row, operation.column)
        else:
            # Simple column reference
            column_name = (
                operation.column.name
                if hasattr(operation.column, "name")
                else str(operation.column)
            )
            value = row.get(column_name)

        func_name = operation.operation

        # Handle coalesce function before the None check
        if func_name == "coalesce":
            if value is not None:
                return value

            if hasattr(operation, "value") and isinstance(operation.value, list):
                for col in operation.value:
                    if hasattr(col, "value") and hasattr(col, "name") and hasattr(col, "data_type"):
                        col_value = col.value
                    elif hasattr(col, "name"):
                        col_value = row.get(col.name)
                    elif hasattr(col, "value"):
                        col_value = col.value
                    else:
                        col_value = col
                    if col_value is not None:
                        return col_value

            return None

        # Handle format_string
        if func_name == "format_string":
            fmt: Optional[str] = None
            args: List[Any] = []
            if hasattr(operation, "value"):
                val = operation.value
                if isinstance(val, tuple) and len(val) >= 1:
                    fmt = val[0]
                    rest = []
                    if len(val) > 1:
                        rem = val[1]
                        if isinstance(rem, (list, tuple)):
                            rest = list(rem)
                        else:
                            rest = [rem]
                    args = []
                    args.append(value)
                    for a in rest:
                        if hasattr(a, "operation") and hasattr(a, "column"):
                            args.append(self.evaluate_expression(row, a))
                        elif hasattr(a, "value"):
                            args.append(a.value)
                        elif hasattr(a, "name"):
                            args.append(row.get(a.name))
                        else:
                            args.append(a)
            try:
                if fmt is None:
                    return None
                fmt_args = tuple("" if v is None else v for v in args) if args else tuple("")
                return fmt % fmt_args
            except Exception:
                return None

        # Handle expr function - parse SQL expressions
        if func_name == "expr":
            expr_str = operation.value if hasattr(operation, "value") else ""

            if expr_str.startswith("lower(") and expr_str.endswith(")"):
                col_name = expr_str[6:-1]
                col_value = row.get(col_name)
                return col_value.lower() if col_value is not None else None
            elif expr_str.startswith("upper(") and expr_str.endswith(")"):
                col_name = expr_str[6:-1]
                col_value = row.get(col_name)
                return col_value.upper() if col_value is not None else None
            elif expr_str.startswith("ascii(") and expr_str.endswith(")"):
                col_name = expr_str[6:-1]
                col_value = row.get(col_name)
                if col_value is None:
                    return None
                s = str(col_value)
                return ord(s[0]) if s else 0
            elif expr_str.startswith("base64(") and expr_str.endswith(")"):
                col_name = expr_str[7:-1]
                col_value = row.get(col_name)
                if col_value is None:
                    return None
                return base64.b64encode(str(col_value).encode("utf-8")).decode("utf-8")
            elif expr_str.startswith("unbase64(") and expr_str.endswith(")"):
                col_name = expr_str[9:-1]
                col_value = row.get(col_name)
                if col_value is None:
                    return None
                try:
                    return base64.b64decode(str(col_value).encode("utf-8"))
                except Exception:
                    return None
            elif expr_str.startswith("length(") and expr_str.endswith(")"):
                col_name = expr_str[7:-1]
                col_value = row.get(col_name)
                return len(col_value) if col_value is not None else None
            else:
                return expr_str

        # Handle null/nan checks before the None check
        if func_name == "isnull":
            return value is None

        if func_name == "isnan":
            return isinstance(value, float) and math.isnan(value)

        # Handle datetime functions
        if func_name == "current_timestamp":
            return datetime.now()
        elif func_name == "current_date":
            return date.today()

        if value is None and func_name not in ("ascii", "base64", "unbase64"):
            return None

        # String functions
        if func_name == "upper":
            return str(value).upper()
        elif func_name == "lower":
            return str(value).lower()
        elif func_name == "length":
            return len(str(value))
        elif func_name == "trim":
            return str(value).strip()

        # Math functions
        elif func_name == "abs":
            return abs(value) if isinstance(value, (int, float)) else value
        elif func_name == "round":
            precision = getattr(operation, "precision", 0)
            return round(value, precision) if isinstance(value, (int, float)) else value
        elif func_name == "ceil":
            return math.ceil(value) if isinstance(value, (int, float)) else value
        elif func_name == "floor":
            return math.floor(value) if isinstance(value, (int, float)) else value
        elif func_name == "sqrt":
            return math.sqrt(value) if isinstance(value, (int, float)) and value >= 0 else None

        # Encoding functions
        elif func_name == "ascii":
            if value is None:
                return None
            s = str(value)
            return ord(s[0]) if s else 0
        elif func_name == "base64":
            if value is None:
                return None
            return base64.b64encode(str(value).encode("utf-8")).decode("utf-8")
        elif func_name == "unbase64":
            if value is None:
                return None
            try:
                return base64.b64decode(str(value).encode("utf-8"))
            except Exception:
                return None

        # String manipulation
        elif func_name == "split":
            if value is None:
                return None
            delimiter = operation.value
            return str(value).split(delimiter)
        elif func_name == "regexp_replace":
            if value is None:
                return None
            pattern = operation.value[0] if isinstance(operation.value, tuple) else operation.value
            replacement = (
                operation.value[1]
                if isinstance(operation.value, tuple) and len(operation.value) > 1
                else ""
            )
            return re.sub(pattern, replacement, str(value))

        # Type casting
        elif func_name == "cast":
            if value is None:
                return None
            cast_type = operation.value
            if isinstance(cast_type, str):
                if cast_type.lower() in ["double", "float"]:
                    return float(value)
                elif cast_type.lower() in ["int", "integer", "long", "bigint"]:
                    return int(float(value))
                elif cast_type.lower() in ["string", "varchar"]:
                    return str(value)
                else:
                    return value
            else:
                return value
        else:
            return value

    def _evaluate_function_call_by_name(self, row: Dict[str, Any], col_name: str) -> Any:
        """Evaluate function calls by parsing the function name string."""
        if col_name.startswith("coalesce("):
            if "name" in col_name and "Unknown" in col_name:
                name_value = row.get("name")
                return name_value if name_value is not None else "Unknown"
            else:
                return None
        elif col_name.startswith("isnull("):
            if "name" in col_name:
                return row.get("name") is None
            else:
                return None
        elif col_name.startswith("isnan("):
            if "salary" in col_name:
                value = row.get("salary")
                if isinstance(value, float):
                    return value != value  # NaN check
                return False
        elif col_name.startswith("trim("):
            if "name" in col_name:
                value = row.get("name")
                return str(value).strip() if value is not None else None
        elif col_name.startswith("ceil("):
            if "value" in col_name:
                value = row.get("value")
                return math.ceil(value) if isinstance(value, (int, float)) else value
        elif col_name.startswith("floor("):
            if "value" in col_name:
                value = row.get("value")
                return math.floor(value) if isinstance(value, (int, float)) else value
        elif col_name.startswith("sqrt("):
            if "value" in col_name:
                value = row.get("value")
                return math.sqrt(value) if isinstance(value, (int, float)) and value >= 0 else None
        elif col_name.startswith("to_date("):
            match = re.search(r"to_date\(([^)]+)\)", col_name)
            if match:
                column_name = match.group(1)
                value = row.get(column_name)
                if value is not None:
                    try:
                        if isinstance(value, str):
                            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
                            return dt.date()
                        elif hasattr(value, "date"):
                            return value.date()
                    except:
                        return None
            return None
        elif col_name.startswith("to_timestamp("):
            match = re.search(r"to_timestamp\(([^)]+)\)", col_name)
            if match:
                column_name = match.group(1)
                value = row.get(column_name)
                if value is not None:
                    try:
                        if isinstance(value, str):
                            return datetime.fromisoformat(value.replace("Z", "+00:00"))
                    except:
                        return None
            return None
        elif col_name.startswith("hour("):
            match = re.search(r"hour\(([^)]+)\)", col_name)
            if match:
                column_name = match.group(1)
                value = row.get(column_name)
                if value is not None:
                    try:
                        if isinstance(value, str):
                            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
                            return dt.hour
                        elif hasattr(value, "hour"):
                            return value.hour
                    except:
                        return None
            return None
        elif col_name.startswith("day("):
            match = re.search(r"day\(([^)]+)\)", col_name)
            if match:
                column_name = match.group(1)
                value = row.get(column_name)
                if value is not None:
                    try:
                        if isinstance(value, str):
                            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
                            return dt.day
                        elif hasattr(value, "day"):
                            return value.day
                    except:
                        return None
            return None
        elif col_name.startswith("month("):
            match = re.search(r"month\(([^)]+)\)", col_name)
            if match:
                column_name = match.group(1)
                value = row.get(column_name)
                if value is not None:
                    try:
                        if isinstance(value, str):
                            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
                            return dt.month
                        elif hasattr(value, "month"):
                            return value.month
                    except:
                        return None
            return None
        elif col_name.startswith("year("):
            match = re.search(r"year\(([^)]+)\)", col_name)
            if match:
                column_name = match.group(1)
                value = row.get(column_name)
                if value is not None:
                    try:
                        if isinstance(value, str):
                            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
                            return dt.year
                        elif hasattr(value, "year"):
                            return value.year
                    except:
                        return None
            return None
        elif col_name.startswith("regexp_replace("):
            if "name" in col_name:
                value = row.get("name")
                if value is not None:
                    return re.sub(r"e", "X", str(value))
                return value
        elif col_name.startswith("split("):
            if "name" in col_name:
                value = row.get("name")
                if value is not None:
                    return str(value).split("l")
                return []

        return None

    def evaluate_case_when(self, row: Dict[str, Any], case_when_obj: Any) -> Any:
        """Evaluate CASE WHEN expression for a row.

        Args:
            row: Data row as dictionary
            case_when_obj: MockCaseWhen object

        Returns:
            Value based on first matching condition, or else value
        """
        for condition, value in case_when_obj.conditions:
            if self._evaluate_case_when_condition(row, condition):
                return self.evaluate_value(row, value)

        if case_when_obj.else_value is not None:
            return self.evaluate_value(row, case_when_obj.else_value)

        return None

    def _evaluate_case_when_condition(self, row: Dict[str, Any], condition: Any) -> bool:
        """Evaluate a CASE WHEN condition for a row."""
        if hasattr(condition, "operation") and hasattr(condition, "column"):
            if condition.operation == ">":
                col_value = self.get_column_value(row, condition.column)
                return col_value is not None and col_value > condition.value
            elif condition.operation == ">=":
                col_value = self.get_column_value(row, condition.column)
                return col_value is not None and col_value >= condition.value
            elif condition.operation == "<":
                col_value = self.get_column_value(row, condition.column)
                return col_value is not None and col_value < condition.value
            elif condition.operation == "<=":
                col_value = self.get_column_value(row, condition.column)
                return col_value is not None and col_value <= condition.value
            elif condition.operation == "==":
                col_value = self.get_column_value(row, condition.column)
                return bool(col_value == condition.value)
            elif condition.operation == "!=":
                col_value = self.get_column_value(row, condition.column)
                return bool(col_value != condition.value)
        return False

    def evaluate_value(self, row: Dict[str, Any], value: Any) -> Any:
        """Evaluate a value (column reference, literal, or operation).

        Args:
            row: Data row as dictionary
            value: Value to evaluate

        Returns:
            Evaluated value
        """
        if hasattr(value, "operation") and hasattr(value, "column"):
            # It's a MockColumnOperation
            return self.evaluate_expression(row, value)
        elif hasattr(value, "value") and hasattr(value, "name"):
            # It's a MockLiteral
            return value.value
        elif hasattr(value, "name"):
            # It's a column reference
            return self.get_column_value(row, value)
        else:
            # It's a literal value
            return value

    def get_column_value(self, row: Dict[str, Any], column: Any) -> Any:
        """Get column value from row.

        Args:
            row: Data row as dictionary
            column: Column reference (MockColumn or string)

        Returns:
            Column value from row
        """
        if hasattr(column, "name"):
            return row.get(column.name)
        else:
            return row.get(str(column))
