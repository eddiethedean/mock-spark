"""
Aggregation operations mixin for DataFrame.

This mixin provides aggregation operations that can be mixed into
the DataFrame class to add aggregation capabilities.
"""

from typing import Union, Any, TYPE_CHECKING

if TYPE_CHECKING:
    from ..dataframe import DataFrame

from ...functions import Column, ColumnOperation
from ...core.exceptions.operation import SparkColumnNotFoundError
from ..grouped import GroupedData


class AggregationOperations:
    """Mixin providing aggregation operations for DataFrame."""

    def groupBy(self: "DataFrame", *columns: Union[str, Column]) -> "GroupedData":
        """Group DataFrame by columns for aggregation operations.

        Args:
            *columns: Column names or Column objects to group by.

        Returns:
            GroupedData for aggregation operations.

        Example:
            >>> df.groupBy("category").count()
            >>> df.groupBy("dept", "year").avg("salary")
        """
        col_names = []
        for col in columns:
            if isinstance(col, Column):
                col_names.append(col.name)
            else:
                col_names.append(col)

        # Validate that all columns exist
        for col_name in col_names:
            if col_name not in [field.name for field in self.schema.fields]:
                available_columns = [field.name for field in self.schema.fields]
                raise SparkColumnNotFoundError(col_name, available_columns)

        return GroupedData(self, col_names)

    def groupby(
        self: "DataFrame", *cols: Union[str, Column], **kwargs: Any
    ) -> GroupedData:
        """Lowercase alias for groupBy() (all PySpark versions).

        Args:
            *cols: Column names or Column objects to group by
            **kwargs: Additional grouping options

        Returns:
            GroupedData object
        """
        return self.groupBy(*cols, **kwargs)

    def rollup(
        self: "DataFrame", *columns: Union[str, Column]
    ) -> Any:  # Returns RollupGroupedData
        """Create rollup grouped data for hierarchical grouping.

        Args:
            *columns: Columns to rollup.

        Returns:
            RollupGroupedData for hierarchical grouping.

        Example:
            >>> df.rollup("country", "state").sum("sales")
        """
        col_names = []
        for col in columns:
            if isinstance(col, Column):
                col_names.append(col.name)
            else:
                col_names.append(col)

        # Validate that all columns exist
        for col_name in col_names:
            if col_name not in [field.name for field in self.schema.fields]:
                available_columns = [field.name for field in self.schema.fields]
                raise SparkColumnNotFoundError(col_name, available_columns)

        from ..grouped.rollup import RollupGroupedData

        return RollupGroupedData(self, col_names)

    def cube(
        self: "DataFrame", *columns: Union[str, Column]
    ) -> Any:  # Returns CubeGroupedData
        """Create cube grouped data for multi-dimensional grouping.

        Args:
            *columns: Columns to cube.

        Returns:
            CubeGroupedData for multi-dimensional grouping.

        Example:
            >>> df.cube("year", "month").sum("revenue")
        """
        col_names = []
        for col in columns:
            if isinstance(col, Column):
                col_names.append(col.name)
            else:
                col_names.append(col)

        # Validate that all columns exist
        for col_name in col_names:
            if col_name not in [field.name for field in self.schema.fields]:
                available_columns = [field.name for field in self.schema.fields]
                raise SparkColumnNotFoundError(col_name, available_columns)

        from ..grouped.cube import CubeGroupedData

        return CubeGroupedData(self, col_names)

    def agg(
        self: "DataFrame", *exprs: Union[str, Column, ColumnOperation]
    ) -> "DataFrame":
        """Aggregate DataFrame without grouping (global aggregation).

        Args:
            *exprs: Aggregation expressions or column names.

        Returns:
            DataFrame with aggregated results.

        Example:
            >>> df.agg(F.max("age"), F.min("age"))
            >>> df.agg({"age": "max", "salary": "avg"})
        """
        # Create a grouped data object with empty group columns for global aggregation
        grouped = GroupedData(self, [])
        return grouped.agg(*exprs)
