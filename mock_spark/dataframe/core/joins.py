"""
DataFrame joins module for Mock Spark.

This module contains DataFrame join operations including union, intersect,
crossJoin, and various join types for combining DataFrames.
"""

from typing import Any, Dict, List, Optional, Union, Tuple
from collections import Counter

from ...spark_types import (
    MockStructType,
    MockStructField,
    MockDataType,
    StringType,
)
from ...core.exceptions import AnalysisException
from .dataframe import MockDataFrame


class DataFrameJoins:
    """Mixin class providing DataFrame join operations functionality."""

    def union(self, other: "MockDataFrame") -> "MockDataFrame":
        """Union with another DataFrame.
        
        Args:
            other: Another DataFrame to union with.
            
        Returns:
            New MockDataFrame with combined data.
        """
        combined_data = self.data + other.data
        return MockDataFrame(combined_data, self.schema, self.storage)

    def unionByName(
        self, other: "MockDataFrame", allowMissingColumns: bool = False
    ) -> "MockDataFrame":
        """Union with another DataFrame by column names.

        Args:
            other: Another DataFrame to union with.
            allowMissingColumns: If True, allows missing columns (fills with null).

        Returns:
            New MockDataFrame with combined data.
        """
        # Get column names from both DataFrames
        self_cols = set(field.name for field in self.schema.fields)
        other_cols = set(field.name for field in other.schema.fields)

        # Check for missing columns
        missing_in_other = self_cols - other_cols
        missing_in_self = other_cols - self_cols

        if not allowMissingColumns and (missing_in_other or missing_in_self):
            raise AnalysisException(
                f"Union by name failed: missing columns in one of the DataFrames. "
                f"Missing in other: {missing_in_other}, Missing in self: {missing_in_self}"
            )

        # Get all unique column names in order
        all_cols = list(self_cols.union(other_cols))

        # Create combined data with all columns
        combined_data = []

        # Add rows from self DataFrame
        for row in self.data:
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
            field_type: MockDataType = StringType()
            for field in self.schema.fields:
                if field.name == col:
                    field_type = field.dataType
                    break
            # If not found in self schema, check other schema
            if isinstance(field_type, StringType):
                for field in other.schema.fields:
                    if field.name == col:
                        field_type = field.dataType
                        break
            new_fields.append(MockStructField(col, field_type))

        new_schema = MockStructType(new_fields)
        return MockDataFrame(combined_data, new_schema, self.storage)

    def intersect(self, other: "MockDataFrame") -> "MockDataFrame":
        """Intersect with another DataFrame.

        Args:
            other: Another DataFrame to intersect with.

        Returns:
            New MockDataFrame with common rows.
        """
        # Convert rows to tuples for comparison
        self_rows = [
            tuple(row.get(field.name) for field in self.schema.fields) for row in self.data
        ]
        other_rows = [
            tuple(row.get(field.name) for field in other.schema.fields) for row in other.data
        ]

        # Find common rows
        self_row_set = set(self_rows)
        other_row_set = set(other_rows)
        common_rows = self_row_set.intersection(other_row_set)

        # Convert back to dictionaries
        result_data = []
        for row_tuple in common_rows:
            row_dict = {}
            for i, field in enumerate(self.schema.fields):
                row_dict[field.name] = row_tuple[i]
            result_data.append(row_dict)

        return MockDataFrame(result_data, self.schema, self.storage)

    def exceptAll(self, other: "MockDataFrame") -> "MockDataFrame":
        """Except all with another DataFrame (set difference with duplicates).

        Args:
            other: Another DataFrame to except from this one.

        Returns:
            New MockDataFrame with rows from self not in other, preserving duplicates.
        """
        # Convert rows to tuples for comparison
        self_rows = [
            tuple(row.get(field.name) for field in self.schema.fields) for row in self.data
        ]
        other_rows = [
            tuple(row.get(field.name) for field in other.schema.fields) for row in other.data
        ]

        # Count occurrences in other DataFrame
        other_row_counts: Dict[Tuple[Any, ...], int] = {}
        for row_tuple in other_rows:
            other_row_counts[row_tuple] = other_row_counts.get(row_tuple, 0) + 1

        # Count occurrences in self DataFrame
        self_row_counts: Dict[Tuple[Any, ...], int] = {}
        for row_tuple in self_rows:
            self_row_counts[row_tuple] = self_row_counts.get(row_tuple, 0) + 1

        # Calculate the difference preserving duplicates
        result_rows: List[Tuple[Any, ...]] = []
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
            for i, field in enumerate(self.schema.fields):
                row_dict[field.name] = row_tuple[i]
            result_data.append(row_dict)

        return MockDataFrame(result_data, self.schema, self.storage)

    def crossJoin(self, other: "MockDataFrame") -> "MockDataFrame":
        """Cross join (Cartesian product) with another DataFrame.

        Args:
            other: Another DataFrame to cross join with.

        Returns:
            New MockDataFrame with Cartesian product of rows.
        """
        # Create new schema combining both DataFrames
        new_fields = []
        field_names = set()

        # Add fields from self DataFrame
        for field in self.schema.fields:
            new_fields.append(field)
            field_names.add(field.name)

        # Add fields from other DataFrame, handling name conflicts
        for field in other.schema.fields:
            if field.name in field_names:
                # Create a unique name for the conflict
                new_name = f"{field.name}_right"
                counter = 1
                while new_name in field_names:
                    new_name = f"{field.name}_right_{counter}"
                    counter += 1
                new_fields.append(MockStructField(new_name, field.dataType))
                field_names.add(new_name)
            else:
                new_fields.append(field)
                field_names.add(field.name)

        new_schema = MockStructType(new_fields)

        # Create Cartesian product
        result_data = []
        for left_row in self.data:
            for right_row in other.data:
                new_row = {}

                # Add fields from left DataFrame
                for field in self.schema.fields:
                    new_row[field.name] = left_row.get(field.name)

                # Add fields from right DataFrame, handling name conflicts
                for field in other.schema.fields:
                    if field.name in [f.name for f in self.schema.fields]:
                        # Find the renamed field
                        renamed: Optional[str] = None
                        for new_field in new_fields:
                            if new_field.name.endswith("_right") and new_field.name.startswith(
                                field.name
                            ):
                                renamed = new_field.name
                                break
                        if renamed is not None:
                            new_row[renamed] = right_row.get(field.name)
                    else:
                        new_row[field.name] = right_row.get(field.name)

                result_data.append(new_row)

        return MockDataFrame(result_data, new_schema, self.storage)

    def join(
        self, other: "MockDataFrame", on: Union[str, List[str]], how: str = "inner"
    ) -> "MockDataFrame":
        """Join with another DataFrame.
        
        Args:
            other: Another DataFrame to join with.
            on: Column name(s) to join on.
            how: Join type ('inner', 'left', 'right', 'outer').
            
        Returns:
            New MockDataFrame with joined data.
        """
        if isinstance(on, str):
            on = [on]

        # Validate join columns exist in both DataFrames
        for col in on:
            if col not in [field.name for field in self.schema.fields]:
                raise AnalysisException(f"Column '{col}' not found in left DataFrame")
            if col not in [field.name for field in other.schema.fields]:
                raise AnalysisException(f"Column '{col}' not found in right DataFrame")

        # Create joined data based on join type
        if how == "inner":
            return self._inner_join(other, on)
        elif how == "left":
            return self._left_join(other, on)
        elif how == "right":
            return self._right_join(other, on)
        elif how == "outer":
            return self._outer_join(other, on)
        else:
            raise AnalysisException(f"Unsupported join type: {how}")

    def _inner_join(self, other: "MockDataFrame", on: List[str]) -> "MockDataFrame":
        """Perform inner join."""
        joined_data = []
        for left_row in self.data:
            for right_row in other.data:
                # Check if join condition matches
                if all(left_row.get(col) == right_row.get(col) for col in on):
                    joined_row = left_row.copy()
                    joined_row.update(right_row)
                    joined_data.append(joined_row)

        # Create new schema
        new_fields = self.schema.fields.copy()
        for field in other.schema.fields:
            if not any(f.name == field.name for f in new_fields):
                new_fields.append(field)

        new_schema = MockStructType(new_fields)
        return MockDataFrame(joined_data, new_schema, self.storage)

    def _left_join(self, other: "MockDataFrame", on: List[str]) -> "MockDataFrame":
        """Perform left join."""
        joined_data = []
        matched_right_rows = set()

        for left_row in self.data:
            matched = False
            for i, right_row in enumerate(other.data):
                # Check if join condition matches
                if all(left_row.get(col) == right_row.get(col) for col in on):
                    joined_row = left_row.copy()
                    joined_row.update(right_row)
                    joined_data.append(joined_row)
                    matched_right_rows.add(i)
                    matched = True

            # If no match found, add left row with null values for right columns
            if not matched:
                joined_row = left_row.copy()
                for field in other.schema.fields:
                    if field.name not in left_row:
                        joined_row[field.name] = None
                joined_data.append(joined_row)

        # Create new schema
        new_fields = self.schema.fields.copy()
        for field in other.schema.fields:
            if not any(f.name == field.name for f in new_fields):
                new_fields.append(field)

        new_schema = MockStructType(new_fields)
        return MockDataFrame(joined_data, new_schema, self.storage)

    def _right_join(self, other: "MockDataFrame", on: List[str]) -> "MockDataFrame":
        """Perform right join."""
        joined_data = []
        matched_left_rows = set()

        for right_row in other.data:
            matched = False
            for i, left_row in enumerate(self.data):
                # Check if join condition matches
                if all(left_row.get(col) == right_row.get(col) for col in on):
                    joined_row = left_row.copy()
                    joined_row.update(right_row)
                    joined_data.append(joined_row)
                    matched_left_rows.add(i)
                    matched = True

            # If no match found, add right row with null values for left columns
            if not matched:
                joined_row = {}
                for field in self.schema.fields:
                    joined_row[field.name] = None
                joined_row.update(right_row)
                joined_data.append(joined_row)

        # Create new schema
        new_fields = self.schema.fields.copy()
        for field in other.schema.fields:
            if not any(f.name == field.name for f in new_fields):
                new_fields.append(field)

        new_schema = MockStructType(new_fields)
        return MockDataFrame(joined_data, new_schema, self.storage)

    def _outer_join(self, other: "MockDataFrame", on: List[str]) -> "MockDataFrame":
        """Perform outer join."""
        joined_data = []
        matched_left_rows = set()
        matched_right_rows = set()

        # Find all matches
        for i, left_row in enumerate(self.data):
            for j, right_row in enumerate(other.data):
                if all(left_row.get(col) == right_row.get(col) for col in on):
                    joined_row = left_row.copy()
                    joined_row.update(right_row)
                    joined_data.append(joined_row)
                    matched_left_rows.add(i)
                    matched_right_rows.add(j)

        # Add unmatched left rows
        for i, left_row in enumerate(self.data):
            if i not in matched_left_rows:
                joined_row = left_row.copy()
                for field in other.schema.fields:
                    if field.name not in left_row:
                        joined_row[field.name] = None
                joined_data.append(joined_row)

        # Add unmatched right rows
        for j, right_row in enumerate(other.data):
            if j not in matched_right_rows:
                joined_row = {}
                for field in self.schema.fields:
                    joined_row[field.name] = None
                joined_row.update(right_row)
                joined_data.append(joined_row)

        # Create new schema
        new_fields = self.schema.fields.copy()
        for field in other.schema.fields:
            if not any(f.name == field.name for f in new_fields):
                new_fields.append(field)

        new_schema = MockStructType(new_fields)
        return MockDataFrame(joined_data, new_schema, self.storage)
