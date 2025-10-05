"""
Base grouped data implementation for Mock Spark.

This module provides the core MockGroupedData class for DataFrame aggregation
operations, maintaining compatibility with PySpark's GroupedData interface.
"""

from typing import Any, List, Dict, Union, Tuple, TYPE_CHECKING, Optional
import statistics

from ...functions import MockColumn, MockColumnOperation, MockAggregateFunction
from ...core.exceptions.analysis import ColumnNotFoundException

if TYPE_CHECKING:
    from ..dataframe import MockDataFrame


class MockGroupedData:
    """Mock grouped data for aggregation operations.

    Provides grouped data functionality for DataFrame aggregation operations,
    maintaining compatibility with PySpark's GroupedData interface.
    """

    def __init__(self, df: "MockDataFrame", group_columns: List[str]):
        """Initialize MockGroupedData.

        Args:
            df: The DataFrame being grouped.
            group_columns: List of column names to group by.
        """
        self.df = df
        self.group_columns = group_columns

    def agg(
        self, *exprs: Union[str, MockColumn, MockColumnOperation, MockAggregateFunction]
    ) -> "MockDataFrame":
        """Aggregate grouped data.

        Args:
            *exprs: Aggregation expressions.

        Returns:
            New MockDataFrame with aggregated results.
        """
        # Group data by group columns
        groups: Dict[Any, List[Dict[str, Any]]] = {}
        for row in self.df.data:
            group_key = tuple(row.get(col) for col in self.group_columns)
            if group_key not in groups:
                groups[group_key] = []
            groups[group_key].append(row)

        # Apply aggregations
        result_data = []
        for group_key, group_rows in groups.items():
            result_row = dict(zip(self.group_columns, group_key))

            for expr in exprs:
                if isinstance(expr, str):
                    # Handle string expressions like "sum(age)"
                    result_key, result_value = self._evaluate_string_expression(expr, group_rows)
                    result_row[result_key] = result_value
                elif hasattr(expr, "function_name"):
                    # Handle MockAggregateFunction
                    from typing import cast
                    from ...functions import MockAggregateFunction

                    result_key, result_value = self._evaluate_aggregate_function(
                        cast(MockAggregateFunction, expr), group_rows
                    )
                    result_row[result_key] = result_value
                elif hasattr(expr, "name"):
                    # Handle MockColumn or MockColumnOperation
                    result_key, result_value = self._evaluate_column_expression(expr, group_rows)
                    result_row[result_key] = result_value

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

        # Create schema based on the first result row
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
            # Empty result
            return MockDataFrame(result_data, MockStructType([]))

    def _evaluate_string_expression(
        self, expr: str, group_rows: List[Dict[str, Any]]
    ) -> Tuple[str, Any]:
        """Evaluate string aggregation expression.

        Args:
            expr: String expression to evaluate.
            group_rows: Rows in the group.

        Returns:
            Tuple of (result_key, result_value).
        """
        if expr.startswith("sum("):
            col_name = expr[4:-1]
            values = [row.get(col_name, 0) for row in group_rows if row.get(col_name) is not None]
            return expr, sum(values) if values else 0
        elif expr.startswith("avg("):
            col_name = expr[4:-1]
            values = [row.get(col_name, 0) for row in group_rows if row.get(col_name) is not None]
            return expr, sum(values) / len(values) if values else 0
        elif expr.startswith("count("):
            return expr, len(group_rows)
        elif expr.startswith("max("):
            col_name = expr[4:-1]
            values = [row.get(col_name) for row in group_rows if row.get(col_name) is not None]
            return expr, max(values) if values else None
        elif expr.startswith("min("):
            col_name = expr[4:-1]
            values = [row.get(col_name) for row in group_rows if row.get(col_name) is not None]
            return expr, min(values) if values else None
        else:
            return expr, None

    def _evaluate_aggregate_function(
        self, expr: MockAggregateFunction, group_rows: List[Dict[str, Any]]
    ) -> Tuple[str, Any]:
        """Evaluate MockAggregateFunction.

        Args:
            expr: Aggregate function to evaluate.
            group_rows: Rows in the group.

        Returns:
            Tuple of (result_key, result_value).
        """
        func_name = expr.function_name
        col_name = getattr(expr, "column_name", "") if hasattr(expr, "column_name") else ""

        # Check if the function has an alias set
        has_alias = expr.name != expr._generate_name()
        alias_name = expr.name if has_alias else None

        if func_name == "sum":
            values = [row.get(col_name, 0) for row in group_rows if row.get(col_name) is not None]
            result_key = alias_name if alias_name else f"sum({col_name})"
            return result_key, sum(values) if values else 0
        elif func_name == "avg":
            values = [row.get(col_name, 0) for row in group_rows if row.get(col_name) is not None]
            result_key = alias_name if alias_name else f"avg({col_name})"
            return result_key, sum(values) / len(values) if values else 0
        elif func_name == "count":
            if col_name == "*" or col_name == "":
                # For count(*), use alias if available, otherwise use the function's generated name
                result_key = alias_name if alias_name else expr._generate_name()
                return result_key, len(group_rows)
            else:
                result_key = alias_name if alias_name else f"count({col_name})"
                return result_key, len(group_rows)
        elif func_name == "max":
            values = [row.get(col_name) for row in group_rows if row.get(col_name) is not None]
            result_key = alias_name if alias_name else f"max({col_name})"
            return result_key, max(values) if values else None
        elif func_name == "min":
            values = [row.get(col_name) for row in group_rows if row.get(col_name) is not None]
            result_key = alias_name if alias_name else f"min({col_name})"
            return result_key, min(values) if values else None
        elif func_name == "collect_list":
            values = [row.get(col_name) for row in group_rows if row.get(col_name) is not None]
            result_key = alias_name if alias_name else f"collect_list({col_name})"
            return result_key, values
        elif func_name == "collect_set":
            values = [row.get(col_name) for row in group_rows if row.get(col_name) is not None]
            result_key = alias_name if alias_name else f"collect_set({col_name})"
            return result_key, list(set(values))
        elif func_name == "first":
            values = [row.get(col_name) for row in group_rows if row.get(col_name) is not None]
            result_key = alias_name if alias_name else f"first({col_name})"
            return result_key, values[0] if values else None
        elif func_name == "last":
            values = [row.get(col_name) for row in group_rows if row.get(col_name) is not None]
            result_key = alias_name if alias_name else f"last({col_name})"
            return result_key, values[-1] if values else None
        elif func_name == "stddev":
            values = [
                row.get(col_name)
                for row in group_rows
                if row.get(col_name) is not None and isinstance(row.get(col_name), (int, float))
            ]
            result_key = alias_name if alias_name else f"stddev({col_name})"
            if values:
                return result_key, statistics.stdev(values) if len(values) > 1 else 0.0
            else:
                return result_key, None
        elif func_name == "variance":
            values = [
                row.get(col_name)
                for row in group_rows
                if row.get(col_name) is not None and isinstance(row.get(col_name), (int, float))
            ]
            result_key = alias_name if alias_name else f"variance({col_name})"
            if values:
                return result_key, (statistics.variance(values) if len(values) > 1 else 0.0)
            else:
                return result_key, None
        elif func_name == "skewness":
            values = [
                row.get(col_name)
                for row in group_rows
                if row.get(col_name) is not None and isinstance(row.get(col_name), (int, float))
            ]
            result_key = alias_name if alias_name else f"skewness({col_name})"
            if values and len(values) > 2:
                mean_val = statistics.mean(values)
                std_val = statistics.stdev(values)
                if std_val > 0:
                    skewness = sum((x - mean_val) ** 3 for x in values) / (len(values) * std_val**3)
                    return result_key, skewness
                else:
                    return result_key, 0.0
            else:
                return result_key, None
        elif func_name == "kurtosis":
            values = [
                row.get(col_name)
                for row in group_rows
                if row.get(col_name) is not None and isinstance(row.get(col_name), (int, float))
            ]
            result_key = alias_name if alias_name else f"kurtosis({col_name})"
            if values and len(values) > 3:
                mean_val = statistics.mean(values)
                std_val = statistics.stdev(values)
                if std_val > 0:
                    kurtosis = (
                        sum((x - mean_val) ** 4 for x in values) / (len(values) * std_val**4) - 3
                    )
                    return result_key, kurtosis
                else:
                    return result_key, 0.0
            else:
                return result_key, None
        else:
            result_key = alias_name if alias_name else f"{func_name}({col_name})"
            return result_key, None

    def _evaluate_column_expression(
        self,
        expr: Union[MockColumn, MockColumnOperation],
        group_rows: List[Dict[str, Any]],
    ) -> Tuple[str, Any]:
        """Evaluate MockColumn or MockColumnOperation.

        Args:
            expr: Column expression to evaluate.
            group_rows: Rows in the group.

        Returns:
            Tuple of (result_key, result_value).
        """
        expr_name = expr.name
        if expr_name.startswith("sum("):
            col_name = expr_name[4:-1]
            values = [row.get(col_name, 0) for row in group_rows if row.get(col_name) is not None]
            return expr_name, sum(values) if values else 0
        elif expr_name.startswith("avg("):
            col_name = expr_name[4:-1]
            values = [row.get(col_name, 0) for row in group_rows if row.get(col_name) is not None]
            return expr_name, sum(values) / len(values) if values else 0
        elif expr_name.startswith("count("):
            return expr_name, len(group_rows)
        elif expr_name.startswith("max("):
            col_name = expr_name[4:-1]
            values = [row.get(col_name) for row in group_rows if row.get(col_name) is not None]
            return expr_name, max(values) if values else None
        elif expr_name.startswith("min("):
            col_name = expr_name[4:-1]
            values = [row.get(col_name) for row in group_rows if row.get(col_name) is not None]
            return expr_name, min(values) if values else None
        else:
            return expr_name, None

    def sum(self, *columns: Union[str, MockColumn]) -> "MockDataFrame":
        """Sum grouped data.

        Args:
            *columns: Columns to sum.

        Returns:
            MockDataFrame with sum aggregations.
        """
        if not columns:
            return self.agg("sum(1)")

        exprs = [f"sum({col})" if isinstance(col, str) else f"sum({col.name})" for col in columns]
        return self.agg(*exprs)

    def avg(self, *columns: Union[str, MockColumn]) -> "MockDataFrame":
        """Average grouped data.

        Args:
            *columns: Columns to average.

        Returns:
            MockDataFrame with average aggregations.
        """
        if not columns:
            return self.agg("avg(1)")

        exprs = [f"avg({col})" if isinstance(col, str) else f"avg({col.name})" for col in columns]
        return self.agg(*exprs)

    def count(self, *columns: Union[str, MockColumn]) -> "MockDataFrame":
        """Count grouped data.

        Args:
            *columns: Columns to count.

        Returns:
            MockDataFrame with count aggregations.
        """
        if not columns:
            # Use MockAggregateFunction for count(*) to get proper naming
            from ...functions.aggregate import AggregateFunctions

            return self.agg(AggregateFunctions.count())

        exprs = [
            f"count({col})" if isinstance(col, str) else f"count({col.name})" for col in columns
        ]
        return self.agg(*exprs)

    def max(self, *columns: Union[str, MockColumn]) -> "MockDataFrame":
        """Max grouped data.

        Args:
            *columns: Columns to get max of.

        Returns:
            MockDataFrame with max aggregations.
        """
        if not columns:
            return self.agg("max(1)")

        exprs = [f"max({col})" if isinstance(col, str) else f"max({col.name})" for col in columns]
        return self.agg(*exprs)

    def min(self, *columns: Union[str, MockColumn]) -> "MockDataFrame":
        """Min grouped data.

        Args:
            *columns: Columns to get min of.

        Returns:
            MockDataFrame with min aggregations.
        """
        if not columns:
            return self.agg("min(1)")

        exprs = [f"min({col})" if isinstance(col, str) else f"min({col.name})" for col in columns]
        return self.agg(*exprs)

    def rollup(self, *columns: Union[str, MockColumn]) -> "MockRollupGroupedData":
        """Create rollup grouped data for hierarchical grouping.

        Args:
            *columns: Columns to rollup.

        Returns:
            MockRollupGroupedData for hierarchical grouping.
        """
        from .rollup import MockRollupGroupedData

        col_names = []
        for col in columns:
            if isinstance(col, MockColumn):
                col_names.append(col.name)
            else:
                col_names.append(col)

        # Validate that all columns exist
        for col_name in col_names:
            if col_name not in [field.name for field in self.df.schema.fields]:
                raise ColumnNotFoundException(col_name)

        return MockRollupGroupedData(self.df, col_names)

    def cube(self, *columns: Union[str, MockColumn]) -> "MockCubeGroupedData":
        """Create cube grouped data for multi-dimensional grouping.

        Args:
            *columns: Columns to cube.

        Returns:
            MockCubeGroupedData for multi-dimensional grouping.
        """
        from .cube import MockCubeGroupedData

        col_names = []
        for col in columns:
            if isinstance(col, MockColumn):
                col_names.append(col.name)
            else:
                col_names.append(col)

        # Validate that all columns exist
        for col_name in col_names:
            if col_name not in [field.name for field in self.df.schema.fields]:
                raise ColumnNotFoundException(col_name)

        return MockCubeGroupedData(self.df, col_names)

    def pivot(self, pivot_col: str, values: Optional[List[Any]] = None) -> "MockPivotGroupedData":
        """Create pivot grouped data.

        Args:
            pivot_col: Column to pivot on.
            values: Optional list of pivot values. If None, uses all unique values.

        Returns:
            MockPivotGroupedData for pivot operations.
        """
        from .pivot import MockPivotGroupedData

        # Validate that pivot column exists
        if pivot_col not in [field.name for field in self.df.schema.fields]:
            raise ColumnNotFoundException(pivot_col)

        # If values not provided, get unique values from pivot column
        if values is None:
            values = list(
                set(row.get(pivot_col) for row in self.df.data if row.get(pivot_col) is not None)
            )
            values.sort()  # Sort for consistent ordering

        return MockPivotGroupedData(self.df, self.group_columns, pivot_col, values)
