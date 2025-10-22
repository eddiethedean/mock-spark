"""
Pivot grouped data implementation for Mock Spark.

This module provides pivot grouped data functionality for pivot table
operations, maintaining compatibility with PySpark's GroupedData interface.
"""

from typing import Any, List, Dict, Union, Tuple, TYPE_CHECKING

from ...functions import MockColumn, MockColumnOperation, MockAggregateFunction

if TYPE_CHECKING:
    from ..dataframe import MockDataFrame


class MockPivotGroupedData:
    """Mock pivot grouped data for pivot table operations."""

    def __init__(
        self,
        df: "MockDataFrame",
        group_columns: List[str],
        pivot_col: str,
        pivot_values: List[Any],
    ):
        """Initialize MockPivotGroupedData.

        Args:
            df: The DataFrame being grouped.
            group_columns: List of column names to group by.
            pivot_col: Column to pivot on.
            pivot_values: List of pivot values.
        """
        self.df = df
        self.group_columns = group_columns
        self.pivot_col = pivot_col
        self.pivot_values = pivot_values

    def agg(
        self, *exprs: Union[str, MockColumn, MockColumnOperation, MockAggregateFunction]
    ) -> "MockDataFrame":
        """Aggregate pivot grouped data.

        Creates pivot table with pivot columns as separate columns.

        Args:
            *exprs: Aggregation expressions.

        Returns:
            New MockDataFrame with pivot aggregated results.
        """
        # Group data by group columns
        groups: Dict[Any, List[Dict[str, Any]]] = {}
        for row in self.df.data:
            group_key = tuple(row.get(col) for col in self.group_columns)
            if group_key not in groups:
                groups[group_key] = []
            groups[group_key].append(row)

        result_data = []

        for group_key, group_rows in groups.items():
            result_row = dict(zip(self.group_columns, group_key))

            # For each pivot value, filter rows and apply aggregation
            for pivot_value in self.pivot_values:
                pivot_rows = [
                    row for row in group_rows if row.get(self.pivot_col) == pivot_value
                ]

                for expr in exprs:
                    if isinstance(expr, str):
                        result_key, result_value = self._evaluate_string_expression(
                            expr, pivot_rows
                        )
                        # Create pivot column name
                        pivot_col_name = f"{result_key}_{pivot_value}"
                        result_row[pivot_col_name] = result_value
                    elif hasattr(expr, "function_name"):
                        from typing import cast
                        from ...functions import MockAggregateFunction

                        result_key, result_value = self._evaluate_aggregate_function(
                            cast(MockAggregateFunction, expr), pivot_rows
                        )
                        # Create pivot column name
                        pivot_col_name = f"{result_key}_{pivot_value}"
                        result_row[pivot_col_name] = result_value
                    elif hasattr(expr, "name"):
                        result_key, result_value = self._evaluate_column_expression(
                            expr, pivot_rows
                        )
                        # Create pivot column name
                        pivot_col_name = f"{result_key}_{pivot_value}"
                        result_row[pivot_col_name] = result_value

            result_data.append(result_row)

        # Create result DataFrame with proper schema
        from ..dataframe import MockDataFrame
        from ...spark_types import (
            MockStructType,
            MockStructField,
            StringType,
            LongType,
            DoubleType,
        )

        if result_data:
            fields = []
            for key, value in result_data[0].items():
                if key in self.group_columns:
                    fields.append(MockStructField(key, StringType()))
                elif isinstance(value, int):
                    fields.append(MockStructField(key, LongType()))
                elif isinstance(value, float):
                    fields.append(MockStructField(key, DoubleType()))
                else:
                    fields.append(MockStructField(key, StringType()))
            schema = MockStructType(fields)
            return MockDataFrame(result_data, schema)
        else:
            return MockDataFrame(result_data, MockStructType([]))

    def _evaluate_string_expression(
        self, expr: str, group_rows: List[Dict[str, Any]]
    ) -> Tuple[str, Any]:
        """Evaluate string aggregation expression (reused from MockGroupedData)."""
        if expr.startswith("sum("):
            col_name = expr[4:-1]
            values = [
                row.get(col_name, 0)
                for row in group_rows
                if row.get(col_name) is not None
            ]
            return expr, sum(values) if values else 0
        elif expr.startswith("avg("):
            col_name = expr[4:-1]
            values = [
                row.get(col_name, 0)
                for row in group_rows
                if row.get(col_name) is not None
            ]
            return expr, sum(values) / len(values) if values else 0
        elif expr.startswith("count("):
            return expr, len(group_rows)
        elif expr.startswith("max("):
            col_name = expr[4:-1]
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            return expr, max(values) if values else None
        elif expr.startswith("min("):
            col_name = expr[4:-1]
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            return expr, min(values) if values else None
        else:
            return expr, None

    def _evaluate_aggregate_function(
        self, expr: MockAggregateFunction, group_rows: List[Dict[str, Any]]
    ) -> Tuple[str, Any]:
        """Evaluate MockAggregateFunction (reused from MockGroupedData)."""
        func_name = expr.function_name
        col_name = (
            getattr(expr, "column_name", "") if hasattr(expr, "column_name") else ""
        )

        # Check if the function has an alias set
        has_alias = expr.name != expr._generate_name()
        alias_name = expr.name if has_alias else None

        if func_name == "sum":
            values = [
                row.get(col_name, 0)
                for row in group_rows
                if row.get(col_name) is not None
            ]
            result_key = alias_name if alias_name else f"sum({col_name})"
            return result_key, sum(values) if values else 0
        elif func_name == "avg":
            values = [
                row.get(col_name, 0)
                for row in group_rows
                if row.get(col_name) is not None
            ]
            result_key = alias_name if alias_name else f"avg({col_name})"
            return result_key, sum(values) / len(values) if values else 0
        elif func_name == "count":
            if col_name == "*" or col_name == "":
                result_key = alias_name if alias_name else expr._generate_name()
                return result_key, len(group_rows)
            else:
                result_key = alias_name if alias_name else f"count({col_name})"
                return result_key, len(group_rows)
        elif func_name == "max":
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            result_key = alias_name if alias_name else f"max({col_name})"
            return result_key, max(values) if values else None
        elif func_name == "min":
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            result_key = alias_name if alias_name else f"min({col_name})"
            return result_key, min(values) if values else None
        else:
            result_key = alias_name if alias_name else f"{func_name}({col_name})"
            return result_key, None

    def _evaluate_column_expression(
        self,
        expr: Union[MockColumn, MockColumnOperation],
        group_rows: List[Dict[str, Any]],
    ) -> Tuple[str, Any]:
        """Evaluate MockColumn or MockColumnOperation (reused from MockGroupedData)."""
        expr_name = expr.name
        if expr_name.startswith("sum("):
            col_name = expr_name[4:-1]
            values = [
                row.get(col_name, 0)
                for row in group_rows
                if row.get(col_name) is not None
            ]
            return expr_name, sum(values) if values else 0
        elif expr_name.startswith("avg("):
            col_name = expr_name[4:-1]
            values = [
                row.get(col_name, 0)
                for row in group_rows
                if row.get(col_name) is not None
            ]
            return expr_name, sum(values) / len(values) if values else 0
        elif expr_name.startswith("count("):
            return expr_name, len(group_rows)
        elif expr_name.startswith("max("):
            col_name = expr_name[4:-1]
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            return expr_name, max(values) if values else None
        elif expr_name.startswith("min("):
            col_name = expr_name[4:-1]
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            return expr_name, min(values) if values else None
        else:
            return expr_name, None
