"""
SQL Expression Converter for Mock Spark.

This module provides conversion of Mock Spark expressions to SQL strings,
handling conditions, columns, values, and complex expressions.
"""

from typing import Any, Optional
from ...functions.core.column import MockColumn, MockColumnOperation
from ...functions.core.literals import MockLiteral


class SQLExpressionConverter:
    """Converts Mock Spark expressions to SQL strings."""

    def __init__(self):
        """Initialize the SQL expression converter."""
        pass

    def expression_to_sql(self, expr: Any, source_table: Optional[str] = None) -> str:
        """Convert an expression to SQL."""
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
            return self._build_case_when_sql(expr, None)
        elif (
            hasattr(expr, "operation")
            and hasattr(expr, "column")
            and hasattr(expr, "value")
        ):
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
                if hasattr(expr, "column") and expr.column is not None:
                    column_name = self.column_to_sql(expr.column, source_table)
                    if expr.operation == "isnull":
                        return f"{column_name} IS NULL"
                    elif expr.operation == "isnotnull":
                        return f"{column_name} IS NOT NULL"
                    elif expr.operation == "asc":
                        return f"{column_name} ASC"
                    elif expr.operation == "desc":
                        return f"{column_name} DESC"
                    elif expr.operation == "cast":
                        # This should be handled by the cast method
                        return column_name
                    else:
                        return column_name
                else:
                    return "NULL"

            # Handle binary operations
            left = self.expression_to_sql(expr.column, source_table)
            right = self.value_to_sql(expr.value)

            if expr.operation == "+":
                return f"({left} + {right})"
            elif expr.operation == "-":
                return f"({left} - {right})"
            elif expr.operation == "*":
                return f"({left} * {right})"
            elif expr.operation == "/":
                return f"({left} / {right})"
            elif expr.operation == "%":
                return f"({left} % {right})"
            elif expr.operation == "==":
                return f"({left} = {right})"
            elif expr.operation == "!=":
                return f"({left} != {right})"
            elif expr.operation == ">":
                return f"({left} > {right})"
            elif expr.operation == "<":
                return f"({left} < {right})"
            elif expr.operation == ">=":
                return f"({left} >= {right})"
            elif expr.operation == "<=":
                return f"({left} <= {right})"
            elif expr.operation == "&":
                return f"({left} AND {right})"
            elif expr.operation == "|":
                return f"({left} OR {right})"
            elif expr.operation == "like":
                return f"({left} LIKE {right})"
            elif expr.operation == "rlike":
                return f"({left} REGEXP {right})"
            elif expr.operation == "contains":
                return f"({left} LIKE CONCAT('%', {right}, '%'))"
            elif expr.operation == "startswith":
                return f"({left} LIKE CONCAT({right}, '%'))"
            elif expr.operation == "endswith":
                return f"({left} LIKE CONCAT('%', {right}))"
            elif expr.operation == "isin":
                if isinstance(expr.value, list):
                    values = ", ".join([self.value_to_sql(v) for v in expr.value])
                    return f"({left} IN ({values}))"
                else:
                    return f"({left} = {right})"
            elif expr.operation == "between":
                if isinstance(expr.value, tuple) and len(expr.value) == 2:
                    lower, upper = expr.value
                    return f"({left} BETWEEN {self.value_to_sql(lower)} AND {self.value_to_sql(upper)})"
                else:
                    return f"({left} = {right})"
            else:
                return f"({left} {expr.operation} {right})"
        elif isinstance(expr, MockColumn):
            return self.column_to_sql(expr, source_table)
        elif isinstance(expr, MockLiteral):
            return self.value_to_sql(expr)
        else:
            return str(expr)

    def column_to_sql(self, expr: Any, source_table: Optional[str] = None) -> str:
        """Convert a column reference to SQL with quotes for expressions."""
        if isinstance(expr, MockColumn):
            column_name = expr.name
        elif isinstance(expr, str):
            column_name = expr
        else:
            column_name = str(expr)

        # Quote the column name to handle special characters
        return f'"{column_name}"'

    def value_to_sql(self, value: Any) -> str:
        """Convert a value to SQL string."""
        if isinstance(value, MockLiteral):
            return self._format_value(value.value)
        elif isinstance(value, str):
            return f"'{value}'"
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        elif value is None:
            return "NULL"
        elif isinstance(value, list):
            # Handle list values (for IN clauses)
            formatted_values = [self._format_value(v) for v in value]
            return f"({', '.join(formatted_values)})"
        else:
            return self._format_value(value)

    def _format_value(self, value: Any) -> str:
        """Format a value for SQL."""
        if isinstance(value, str):
            # Escape single quotes
            escaped = value.replace("'", "''")
            return f"'{escaped}'"
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        elif value is None:
            return "NULL"
        else:
            return f"'{str(value)}'"

    def _build_case_when_sql(self, case_when_obj: Any, source_table_obj: Any) -> str:
        """Build SQL CASE WHEN expression from MockCaseWhen object."""
        sql_parts = ["CASE"]

        # Add WHEN conditions
        for condition, value in case_when_obj.conditions:
            # Convert condition to SQL - check if it's a complex expression
            if isinstance(condition, MockColumnOperation):
                # Generate raw SQL without quoting for complex expressions
                condition_sql = self.expression_to_sql(condition)
            else:
                condition_sql = self.condition_to_sql(condition, source_table_obj)

            # Convert value to SQL - handle MockLiteral with boolean values specially
            if hasattr(value, "value") and isinstance(value.value, bool):
                value_sql = "TRUE" if value.value else "FALSE"
            else:
                value_sql = self.value_to_sql(value)
            sql_parts.append(f"WHEN {condition_sql} THEN {value_sql}")

        # Add ELSE clause if default_value is set
        if case_when_obj.default_value is not None:
            # Update the ELSE clause to handle boolean MockLiterals
            if hasattr(case_when_obj.default_value, "value") and isinstance(
                case_when_obj.default_value.value, bool
            ):
                else_sql = "TRUE" if case_when_obj.default_value.value else "FALSE"
            else:
                else_sql = self.value_to_sql(case_when_obj.default_value)
            sql_parts.append(f"ELSE {else_sql}")

        sql_parts.append("END")
        return " ".join(sql_parts)

    def condition_to_sql(self, condition: Any, source_table_obj: Any) -> str:
        """Convert a condition to SQL."""
        if isinstance(condition, MockColumnOperation):
            return self.expression_to_sql(condition)
        elif isinstance(condition, MockColumn):
            return self.column_to_sql(condition)
        elif isinstance(condition, str):
            return f'"{condition}"'
        else:
            return str(condition)
