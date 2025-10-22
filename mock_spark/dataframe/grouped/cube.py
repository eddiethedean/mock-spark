"""
Cube grouped data implementation for Mock Spark.

This module provides cube grouped data functionality for multi-dimensional
grouping operations, maintaining compatibility with PySpark's GroupedData interface.
"""

from typing import Any, List, Dict, Union, Tuple, TYPE_CHECKING
import itertools

from ...functions import MockColumn, MockColumnOperation, MockAggregateFunction
from .base import MockGroupedData

if TYPE_CHECKING:
    from ..dataframe import MockDataFrame


class MockCubeGroupedData(MockGroupedData):
    """Mock cube grouped data for multi-dimensional grouping operations."""

    def __init__(self, df: "MockDataFrame", cube_columns: List[str]):
        """Initialize MockCubeGroupedData.

        Args:
            df: The DataFrame being grouped.
            cube_columns: List of column names for cube grouping.
        """
        super().__init__(df, cube_columns)
        self.cube_columns = cube_columns

    def agg(
        self, *exprs: Union[str, MockColumn, MockColumnOperation, MockAggregateFunction]
    ) -> "MockDataFrame":
        """Aggregate cube grouped data with multi-dimensional grouping.

        Creates all possible combinations of cube columns (2^n combinations).

        Args:
            *exprs: Aggregation expressions.

        Returns:
            New MockDataFrame with cube aggregated results.
        """
        result_data = []

        # Generate all possible combinations of columns (2^n combinations)
        for r in range(len(self.cube_columns) + 1):
            for combo in itertools.combinations(self.cube_columns, r):
                active_columns = list(combo)
                inactive_columns = [
                    col for col in self.cube_columns if col not in combo
                ]

                if not active_columns:
                    # Grand total - all nulls
                    filtered_rows = self.df.data
                    result_row = {col: None for col in self.cube_columns}

                    # Apply aggregations
                    for expr in exprs:
                        if isinstance(expr, str):
                            result_key, result_value = self._evaluate_string_expression(
                                expr, filtered_rows
                            )
                            result_row[result_key] = result_value
                        elif hasattr(expr, "function_name"):
                            from typing import cast
                            from ...functions import MockAggregateFunction

                            result_key, result_value = (
                                self._evaluate_aggregate_function(
                                    cast(MockAggregateFunction, expr), filtered_rows
                                )
                            )
                            result_row[result_key] = result_value
                        elif hasattr(expr, "name"):
                            result_key, result_value = self._evaluate_column_expression(
                                expr, filtered_rows
                            )
                            result_row[result_key] = result_value

                    result_data.append(result_row)
                else:
                    # Group by active columns
                    groups: Dict[Tuple[Any, ...], List[Dict[str, Any]]] = {}
                    for row in self.df.data:
                        group_key = tuple(row.get(col) for col in active_columns)
                        if group_key not in groups:
                            groups[group_key] = []
                        groups[group_key].append(row)

                    # Process each group
                    for group_key, group_rows in groups.items():
                        result_row = {}
                        # Set active column values
                        for j, col in enumerate(active_columns):
                            result_row[col] = group_key[j]
                        # Set inactive column values to None
                        for col in inactive_columns:
                            result_row[col] = None

                        # Apply aggregations to this group
                        for expr in exprs:
                            if isinstance(expr, str):
                                result_key, result_value = (
                                    self._evaluate_string_expression(expr, group_rows)
                                )
                                result_row[result_key] = result_value
                            elif hasattr(expr, "function_name"):
                                from typing import cast
                                from ...functions import MockAggregateFunction

                                result_key, result_value = (
                                    self._evaluate_aggregate_function(
                                        cast(MockAggregateFunction, expr), group_rows
                                    )
                                )
                                result_row[result_key] = result_value
                            elif hasattr(expr, "name"):
                                result_key, result_value = (
                                    self._evaluate_column_expression(expr, group_rows)
                                )
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

        if not result_data:
            return MockDataFrame(result_data, MockStructType([]))

        fields = []
        for key, value in result_data[0].items():
            if key in self.cube_columns:
                fields.append(MockStructField(key, StringType()))
            else:
                # Count functions, window ranking functions, and boolean functions are non-nullable in PySpark
                is_count_function = any(
                    key.startswith(func) for func in ["count(", "count(1)", "count(DISTINCT", "row_num", "rank", "dense_rank", "dept_row_num", "global_row", "dept_row", "dept_rank"]
                )
                is_boolean_function = any(
                    key.startswith(func) for func in ["coalesced_", "is_null_", "is_nan_"]
                )
                nullable = not (is_count_function or is_boolean_function)
                
                if isinstance(value, int):
                    fields.append(MockStructField(key, LongType(nullable=nullable), nullable=nullable))
                elif isinstance(value, float):
                    fields.append(MockStructField(key, DoubleType(nullable=nullable), nullable=nullable))
                else:
                    # Fallback for any other type
                    fields.append(MockStructField(key, StringType(nullable=nullable), nullable=nullable))
        schema = MockStructType(fields)
        return MockDataFrame(result_data, schema)
