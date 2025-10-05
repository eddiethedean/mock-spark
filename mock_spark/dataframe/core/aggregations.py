"""
DataFrame aggregations module for Mock Spark.

This module contains DataFrame aggregation operations including groupBy,
rollup, cube, and aggregation functions for grouping and summarizing data.
"""

from typing import Any, Dict, List, Optional, Union, Tuple

from ...functions import MockColumn, MockColumnOperation
from ...core.exceptions.analysis import ColumnNotFoundException
from .dataframe import MockDataFrame


class DataFrameAggregations:
    """Mixin class providing DataFrame aggregation functionality."""

    def groupBy(self, *columns: Union[str, MockColumn]) -> "MockGroupedData":
        """Group by columns.
        
        Args:
            *columns: Column names or MockColumn objects to group by.
            
        Returns:
            MockGroupedData for performing aggregations.
        """
        from ..grouped_data import MockGroupedData
        
        col_names = []
        for col in columns:
            if isinstance(col, MockColumn):
                col_names.append(col.name)
            else:
                col_names.append(col)

        # Validate that all columns exist
        for col_name in col_names:
            if col_name not in [field.name for field in self.schema.fields]:
                raise ColumnNotFoundException(col_name)

        return MockGroupedData(self, col_names)

    def rollup(self, *columns: Union[str, MockColumn]) -> "MockRollupGroupedData":
        """Create rollup grouped data for hierarchical grouping.

        Args:
            *columns: Columns to rollup.

        Returns:
            MockRollupGroupedData for hierarchical grouping.
        """
        from ..grouped_data import MockRollupGroupedData
        
        col_names = []
        for col in columns:
            if isinstance(col, MockColumn):
                col_names.append(col.name)
            else:
                col_names.append(col)

        # Validate that all columns exist
        for col_name in col_names:
            if col_name not in [field.name for field in self.schema.fields]:
                raise ColumnNotFoundException(col_name)

        return MockRollupGroupedData(self, col_names)

    def cube(self, *columns: Union[str, MockColumn]) -> "MockCubeGroupedData":
        """Create cube grouped data for multi-dimensional grouping.

        Args:
            *columns: Columns to cube.

        Returns:
            MockCubeGroupedData for multi-dimensional grouping.
        """
        from ..grouped_data import MockCubeGroupedData
        
        col_names = []
        for col in columns:
            if isinstance(col, MockColumn):
                col_names.append(col.name)
            else:
                col_names.append(col)

        # Validate that all columns exist
        for col_name in col_names:
            if col_name not in [field.name for field in self.schema.fields]:
                raise ColumnNotFoundException(col_name)

        return MockCubeGroupedData(self, col_names)

    def agg(self, *exprs: Union[str, MockColumn, MockColumnOperation]) -> "MockDataFrame":
        """Aggregate DataFrame without grouping.
        
        Args:
            *exprs: Aggregation expressions.
            
        Returns:
            New MockDataFrame with aggregated results.
        """
        from ..grouped_data import MockGroupedData
        
        # Create a single group with all data
        grouped_data = MockGroupedData(self, [])
        return grouped_data.agg(*exprs)

    def pivot(self, pivot_col: str, values: Optional[List[Any]] = None) -> "MockPivotGroupedData":
        """Create pivot grouped data for pivoting operations.

        Args:
            pivot_col: Column to pivot on.
            values: Optional list of values to pivot on.

        Returns:
            MockPivotGroupedData for pivoting operations.
        """
        from ..grouped_data import MockPivotGroupedData
        
        # Validate that pivot column exists
        if pivot_col not in [field.name for field in self.schema.fields]:
            raise ColumnNotFoundException(pivot_col)

        return MockPivotGroupedData(self, pivot_col, values)

    def _handle_aggregation_select(self, columns: List[Any]) -> "MockDataFrame":
        """Handle aggregation operations in select.
        
        Args:
            columns: List of aggregation columns.
            
        Returns:
            New MockDataFrame with aggregated results.
        """
        from ...functions import MockAggregateFunction
        from ...spark_types import MockStructType, MockStructField, LongType, DoubleType, StringType
        
        # Create aggregated data
        aggregated_data = {}
        
        for col in columns:
            if isinstance(col, MockAggregateFunction):
                # Handle aggregate functions
                if col.function_name == "count":
                    if col.column_name == "*":
                        aggregated_data[col.name] = len(self.data)
                    else:
                        # Count non-null values in the column
                        non_null_count = sum(1 for row in self.data if row.get(col.column_name) is not None)
                        aggregated_data[col.name] = non_null_count
                elif col.function_name == "sum":
                    values = [row.get(col.column_name) for row in self.data if row.get(col.column_name) is not None]
                    aggregated_data[col.name] = sum(values) if values else None
                elif col.function_name == "avg":
                    values = [row.get(col.column_name) for row in self.data if row.get(col.column_name) is not None]
                    aggregated_data[col.name] = sum(values) / len(values) if values else None
                elif col.function_name == "max":
                    values = [row.get(col.column_name) for row in self.data if row.get(col.column_name) is not None]
                    aggregated_data[col.name] = max(values) if values else None
                elif col.function_name == "min":
                    values = [row.get(col.column_name) for row in self.data if row.get(col.column_name) is not None]
                    aggregated_data[col.name] = min(values) if values else None
                elif col.function_name == "percentile_approx":
                    values = [row.get(col.column_name) for row in self.data if row.get(col.column_name) is not None]
                    if values:
                        sorted_values = sorted(values)
                        percentile = col.percentile if hasattr(col, 'percentile') else 0.5
                        index = int(len(sorted_values) * percentile)
                        aggregated_data[col.name] = sorted_values[min(index, len(sorted_values) - 1)]
                    else:
                        aggregated_data[col.name] = None
                elif col.function_name == "corr":
                    # Simple correlation calculation
                    col1_values = [row.get(col.column_name) for row in self.data if row.get(col.column_name) is not None]
                    col2_values = [row.get(col.column2_name) for row in self.data if row.get(col.column2_name) is not None]
                    if len(col1_values) == len(col2_values) and len(col1_values) > 1:
                        # Calculate correlation coefficient
                        mean1 = sum(col1_values) / len(col1_values)
                        mean2 = sum(col2_values) / len(col2_values)
                        numerator = sum((x - mean1) * (y - mean2) for x, y in zip(col1_values, col2_values))
                        denominator = (sum((x - mean1) ** 2 for x in col1_values) * sum((y - mean2) ** 2 for y in col2_values)) ** 0.5
                        aggregated_data[col.name] = numerator / denominator if denominator != 0 else 0
                    else:
                        aggregated_data[col.name] = None
                elif col.function_name == "covar_samp":
                    # Sample covariance calculation
                    col1_values = [row.get(col.column_name) for row in self.data if row.get(col.column_name) is not None]
                    col2_values = [row.get(col.column2_name) for row in self.data if row.get(col.column2_name) is not None]
                    if len(col1_values) == len(col2_values) and len(col1_values) > 1:
                        mean1 = sum(col1_values) / len(col1_values)
                        mean2 = sum(col2_values) / len(col2_values)
                        covariance = sum((x - mean1) * (y - mean2) for x, y in zip(col1_values, col2_values)) / (len(col1_values) - 1)
                        aggregated_data[col.name] = covariance
                    else:
                        aggregated_data[col.name] = None
                else:
                    # Default to count for unknown functions
                    aggregated_data[col.name] = len(self.data)
            elif isinstance(col, MockColumn):
                # Handle column expressions
                if col.name.startswith("count("):
                    if col.name == "count(*)" or col.name == "count(1)":
                        aggregated_data[col.name] = len(self.data)
                    else:
                        # Extract column name from count(column)
                        col_name = col.name[6:-1]  # Remove "count(" and ")"
                        non_null_count = sum(1 for row in self.data if row.get(col_name) is not None)
                        aggregated_data[col.name] = non_null_count
                elif col.name.startswith("sum("):
                    col_name = col.name[4:-1]  # Remove "sum(" and ")"
                    values = [row.get(col_name) for row in self.data if row.get(col_name) is not None]
                    aggregated_data[col.name] = sum(values) if values else None
                elif col.name.startswith("avg("):
                    col_name = col.name[4:-1]  # Remove "avg(" and ")"
                    values = [row.get(col_name) for row in self.data if row.get(col_name) is not None]
                    aggregated_data[col.name] = sum(values) / len(values) if values else None
                elif col.name.startswith("max("):
                    col_name = col.name[4:-1]  # Remove "max(" and ")"
                    values = [row.get(col_name) for row in self.data if row.get(col_name) is not None]
                    aggregated_data[col.name] = max(values) if values else None
                elif col.name.startswith("min("):
                    col_name = col.name[4:-1]  # Remove "min(" and ")"
                    values = [row.get(col_name) for row in self.data if row.get(col_name) is not None]
                    aggregated_data[col.name] = min(values) if values else None
                else:
                    # Default to count for unknown expressions
                    aggregated_data[col.name] = len(self.data)
            else:
                # Default to count for unknown column types
                aggregated_data[str(col)] = len(self.data)

        # Create schema for aggregated results
        new_fields = []
        for col in columns:
            if isinstance(col, MockAggregateFunction):
                # Determine field type based on function
                if col.function_name in ["count"]:
                    new_fields.append(MockStructField(col.name, LongType()))
                elif col.function_name in ["sum", "avg", "max", "min", "percentile_approx", "corr", "covar_samp"]:
                    new_fields.append(MockStructField(col.name, DoubleType()))
                else:
                    new_fields.append(MockStructField(col.name, LongType()))
            elif isinstance(col, MockColumn):
                # Determine field type based on function name
                if col.name.startswith("count("):
                    new_fields.append(MockStructField(col.name, LongType()))
                elif col.name.startswith(("sum(", "avg(", "max(", "min(")):
                    new_fields.append(MockStructField(col.name, DoubleType()))
                else:
                    new_fields.append(MockStructField(col.name, LongType()))
            else:
                new_fields.append(MockStructField(str(col), LongType()))

        new_schema = MockStructType(new_fields)
        return MockDataFrame([aggregated_data], new_schema, self.storage)
