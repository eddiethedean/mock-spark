"""
Join service for DataFrame operations.

This service provides join and set operations using composition instead of mixin inheritance.
"""

from typing import TYPE_CHECKING, Any, Union, cast

from ...spark_types import DataType, StringType, StructField, StructType
from ..protocols import SupportsDataFrameOps

if TYPE_CHECKING:
    from ...functions import ColumnOperation
    from ..dataframe import DataFrame


class JoinService:
    """Service providing join and set operations for DataFrame."""

    def __init__(self, df: "DataFrame"):
        """Initialize join service with DataFrame instance."""
        self._df = df

    def join(
        self,
        other: SupportsDataFrameOps,
        on: Union[str, list[str], "ColumnOperation"],
        how: str = "inner",
    ) -> "SupportsDataFrameOps":
        """Join with another DataFrame."""
        if isinstance(on, str):
            on = [on]

        return self._df._queue_op("join", (other, on, how))

    def crossJoin(self, other: SupportsDataFrameOps) -> "SupportsDataFrameOps":
        """Cross join (Cartesian product) with another DataFrame.

        Args:
            other: Another DataFrame to cross join with.

        Returns:
            New DataFrame with Cartesian product of rows.
        """
        # Create new schema combining both DataFrames

        # Combine field names, handling duplicates
        new_fields = []
        field_names = set()

        # Add fields from self DataFrame
        for field in self._df.schema.fields:
            new_fields.append(field)
            field_names.add(field.name)

        # Add fields from other DataFrame - keep duplicate names as in PySpark
        for field in other.schema.fields:
            new_fields.append(field)  # Keep original name even if duplicate
            field_names.add(field.name)

        new_schema = StructType(new_fields)

        # Create Cartesian product
        result_data = []

        for left_row in self._df.data:
            for right_row in other.data:
                new_row = {}

                # Add fields from left DataFrame
                for field in self._df.schema.fields:
                    new_row[field.name] = left_row.get(field.name)

                # Add fields from right DataFrame - allow duplicates
                for field in other.schema.fields:
                    # When accessing by key, duplicate columns get overwritten
                    # Use a dict which naturally handles this (last value wins)
                    new_row[field.name] = right_row.get(field.name)

                result_data.append(new_row)

        from ..dataframe import DataFrame

        return cast(
            "SupportsDataFrameOps", DataFrame(result_data, new_schema, self._df.storage)
        )

    def union(self, other: SupportsDataFrameOps) -> "SupportsDataFrameOps":
        """Union with another DataFrame."""
        return self._df._queue_op("union", other)

    def unionByName(
        self,
        other: SupportsDataFrameOps,
        allowMissingColumns: bool = False,
    ) -> "SupportsDataFrameOps":
        """Union with another DataFrame by column names.

        Args:
            other: Another DataFrame to union with.
            allowMissingColumns: If True, allows missing columns (fills with null).

        Returns:
            New DataFrame with combined data.
        """
        # Get column names from both DataFrames
        self_cols = {field.name for field in self._df.schema.fields}
        other_cols = {field.name for field in other.schema.fields}

        # Check for missing columns
        missing_in_other = self_cols - other_cols
        missing_in_self = other_cols - self_cols

        if not allowMissingColumns and (missing_in_other or missing_in_self):
            from ...core.exceptions.analysis import AnalysisException

            raise AnalysisException(
                f"Union by name failed: missing columns in one of the DataFrames. "
                f"Missing in other: {missing_in_other}, Missing in self: {missing_in_self}"
            )

        # Get all unique column names in order
        all_cols = list(self_cols.union(other_cols))

        # Create combined data with all columns
        combined_data = []

        # Add rows from self DataFrame
        for row in self._df.data:
            new_row = {}
            for col in all_cols:
                if col in row:
                    new_row[col] = row[col]
                else:
                    new_row[col] = None  # Missing column filled with null
            combined_data.append(new_row)

        # Add rows from other DataFrame
        for row in other.data:
            new_row = {}
            for col in all_cols:
                if col in row:
                    new_row[col] = row[col]
                else:
                    new_row[col] = None  # Missing column filled with null
            combined_data.append(new_row)

        # Create new schema with all columns

        new_fields = []
        for col in all_cols:
            # Try to get the data type from the original schema, default to StringType
            field_type: DataType = StringType()
            for field in self._df.schema.fields:
                if field.name == col:
                    field_type = field.dataType
                    break
            # If not found in self schema, check other schema
            if isinstance(field_type, StringType):
                for field in other.schema.fields:
                    if field.name == col:
                        field_type = field.dataType
                        break
            new_fields.append(StructField(col, field_type))

        new_schema = StructType(new_fields)
        from ..dataframe import DataFrame

        return cast(
            "SupportsDataFrameOps",
            DataFrame(combined_data, new_schema, self._df.storage),
        )

    def unionAll(self, other: SupportsDataFrameOps) -> "SupportsDataFrameOps":
        """Deprecated alias for union() - Use union() instead (all PySpark versions).

        Args:
            other: DataFrame to union with

        Returns:
            Union of both DataFrames

        Note:
            Deprecated in PySpark 2.0+, use union() instead
        """
        import warnings

        warnings.warn(
            "unionAll is deprecated. Use union instead.", FutureWarning, stacklevel=2
        )
        return self.union(other)

    def intersect(self, other: SupportsDataFrameOps) -> "SupportsDataFrameOps":
        """Intersect with another DataFrame.

        Args:
            other: Another DataFrame to intersect with.

        Returns:
            New DataFrame with common rows.
        """
        # Convert rows to tuples for comparison
        self_rows = [
            tuple(row.get(field.name) for field in self._df.schema.fields)
            for row in self._df.data
        ]
        other_rows = [
            tuple(row.get(field.name) for field in other.schema.fields)
            for row in other.data
        ]

        # Find common rows
        self_row_set = set(self_rows)
        other_row_set = set(other_rows)
        common_rows = self_row_set.intersection(other_row_set)

        # Convert back to dictionaries
        result_data = []
        for row_tuple in common_rows:
            row_dict = {}
            for i, field in enumerate(self._df.schema.fields):
                row_dict[field.name] = row_tuple[i]
            result_data.append(row_dict)

        from ..dataframe import DataFrame

        return cast(
            "SupportsDataFrameOps",
            DataFrame(result_data, self._df.schema, self._df.storage),
        )

    def intersectAll(self, other: SupportsDataFrameOps) -> "SupportsDataFrameOps":
        """Return intersection with duplicates (PySpark 3.0+).

        Args:
            other: DataFrame to intersect with

        Returns:
            DataFrame with common rows (preserving duplicates)
        """
        from collections import Counter

        def row_to_tuple(row: dict[str, Any]) -> tuple[Any, ...]:
            return tuple(row.get(field.name) for field in self._df.schema.fields)

        # Count occurrences in each DataFrame
        self_counter = Counter(row_to_tuple(row) for row in self._df.data)
        other_counter = Counter(row_to_tuple(row) for row in other.data)

        # Intersection preserves minimum count
        result_data = []
        for row_tuple, count in self_counter.items():
            min_count = min(count, other_counter.get(row_tuple, 0))
            for _ in range(min_count):
                row_dict = {
                    field.name: value
                    for field, value in zip(self._df.schema.fields, row_tuple)
                }
                result_data.append(row_dict)

        from ..dataframe import DataFrame

        return cast(
            "SupportsDataFrameOps",
            DataFrame(result_data, self._df.schema, self._df.storage),
        )

    def exceptAll(self, other: SupportsDataFrameOps) -> "SupportsDataFrameOps":
        """Except all with another DataFrame (set difference with duplicates).

        Args:
            other: Another DataFrame to except from this one.

        Returns:
            New DataFrame with rows from self not in other, preserving duplicates.
        """
        # Convert rows to tuples for comparison
        self_rows = [
            tuple(row.get(field.name) for field in self._df.schema.fields)
            for row in self._df.data
        ]
        other_rows = [
            tuple(row.get(field.name) for field in other.schema.fields)
            for row in other.data
        ]

        # Count occurrences in other DataFrame

        other_row_counts: dict[tuple[Any, ...], int] = {}
        for row_tuple in other_rows:
            other_row_counts[row_tuple] = other_row_counts.get(row_tuple, 0) + 1

        # Count occurrences in self DataFrame
        self_row_counts: dict[tuple[Any, ...], int] = {}
        for row_tuple in self_rows:
            self_row_counts[row_tuple] = self_row_counts.get(row_tuple, 0) + 1

        # Calculate the difference preserving duplicates
        result_rows: list[tuple[Any, ...]] = []
        for row_tuple in self_rows:
            # Count how many times this row appears in other
            other_count = other_row_counts.get(row_tuple, 0)
            # Count how many times this row appears in self so far
            self_count_so_far = result_rows.count(row_tuple)
            # If we haven't exceeded the difference, include this row
            if self_count_so_far < (self_row_counts[row_tuple] - other_count):
                result_rows.append(row_tuple)

        # Convert back to dictionaries
        result_data = []
        for row_tuple in result_rows:
            row_dict = {}
            for i, field in enumerate(self._df.schema.fields):
                row_dict[field.name] = row_tuple[i]
            result_data.append(row_dict)

        from ..dataframe import DataFrame

        return cast(
            "SupportsDataFrameOps",
            DataFrame(result_data, self._df.schema, self._df.storage),
        )

    def subtract(self, other: SupportsDataFrameOps) -> "SupportsDataFrameOps":
        """Return rows in this DataFrame but not in another (all PySpark versions).

        Args:
            other: DataFrame to subtract

        Returns:
            DataFrame with rows from this DataFrame that are not in other
        """

        # Convert rows to tuples for comparison
        def row_to_tuple(row: dict[str, Any]) -> tuple[Any, ...]:
            return tuple(row.get(field.name) for field in self._df.schema.fields)

        self_rows = {row_to_tuple(row) for row in self._df.data}
        other_rows = {row_to_tuple(row) for row in other.data}

        # Find rows in self but not in other
        result_tuples = self_rows - other_rows

        # Convert back to dicts
        result_data = []
        for row_tuple in result_tuples:
            row_dict = {
                field.name: value
                for field, value in zip(self._df.schema.fields, row_tuple)
            }
            result_data.append(row_dict)

        from ..dataframe import DataFrame

        return cast(
            "SupportsDataFrameOps",
            DataFrame(result_data, self._df.schema, self._df.storage),
        )
