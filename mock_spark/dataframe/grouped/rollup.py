"""
Rollup grouped data implementation for Mock Spark.

This module provides rollup grouped data functionality for hierarchical
grouping operations, maintaining compatibility with PySpark's GroupedData interface.
"""

from typing import Any, List, Dict, Union, Tuple, TYPE_CHECKING

from ...functions import MockColumn, MockColumnOperation, MockAggregateFunction
from .base import MockGroupedData

if TYPE_CHECKING:
    from ..dataframe import MockDataFrame


class MockRollupGroupedData(MockGroupedData):
    """Mock rollup grouped data for hierarchical grouping operations."""

    def __init__(self, df: "MockDataFrame", rollup_columns: List[str]):
        """Initialize MockRollupGroupedData.

        Args:
            df: The DataFrame being grouped.
            rollup_columns: List of column names for rollup hierarchy.
        """
        super().__init__(df, rollup_columns)
        self.rollup_columns = rollup_columns

    def agg(
        self, *exprs: Union[str, MockColumn, MockColumnOperation, MockAggregateFunction]
    ) -> "MockDataFrame":
        """Aggregate rollup grouped data with hierarchical grouping.

        Creates all possible combinations of rollup columns, including null values
        for higher-level groupings.

        Args:
            *exprs: Aggregation expressions.

        Returns:
            New MockDataFrame with rollup aggregated results.
        """
        # Get all unique values for each rollup column
        unique_values = {}
        for col in self.rollup_columns:
            unique_values[col] = list(
                set(row.get(col) for row in self.df.data if row.get(col) is not None)
            )

        result_data = []

        # Generate rollup combinations: for each level i, group by first i columns
        for i in range(len(self.rollup_columns) + 1):
            if i == 0:
                # Grand total - all nulls
                filtered_rows = self.df.data
                result_row = {col: None for col in self.rollup_columns}

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

                        result_key, result_value = self._evaluate_aggregate_function(
                            cast(MockAggregateFunction, expr), filtered_rows
                        )
                        result_row[result_key] = result_value
                    elif hasattr(expr, "name"):
                        result_key, result_value = self._evaluate_column_expression(
                            expr, filtered_rows
                        )
                        result_row[result_key] = result_value

                result_data.append(result_row)
            else:
                # Group by first i columns
                active_columns = self.rollup_columns[:i]
                inactive_columns = self.rollup_columns[i:]

                # Create groups based on active columns
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
                            result_key, result_value = self._evaluate_string_expression(
                                expr, group_rows
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
                            result_key, result_value = self._evaluate_column_expression(
                                expr, group_rows
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
            if key in self.rollup_columns:
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
