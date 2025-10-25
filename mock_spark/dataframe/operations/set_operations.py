"""Set operations for MockDataFrame."""

from typing import List, Any, Dict, Tuple, TYPE_CHECKING

from ...spark_types import MockRow

if TYPE_CHECKING:
    from ...spark_types import MockStructType


class SetOperations:
    """Handles set operations for MockDataFrame."""

    @staticmethod
    def distinct_rows(rows: List[MockRow]) -> List[MockRow]:
        """Remove duplicate rows from a list."""
        seen = set()
        unique_rows = []
        for row in rows:
            # Create a hashable representation of the row
            row_values = []
            for col in row.__dict__.keys():
                value = getattr(row, col)
                # Convert unhashable types to hashable representations
                if isinstance(value, dict):
                    value = tuple(sorted(value.items()))
                elif isinstance(value, list):
                    value = tuple(value)
                row_values.append(value)
            row_tuple = tuple(row_values)
            if row_tuple not in seen:
                seen.add(row_tuple)
                unique_rows.append(row)
        return unique_rows

    @staticmethod
    def union_rows(rows1: List[MockRow], rows2: List[MockRow]) -> List[MockRow]:
        """Union two lists of rows."""
        return rows1 + rows2

    @staticmethod
    def union(
        data1: List[Dict[str, Any]],
        schema1: "MockStructType",
        data2: List[Dict[str, Any]],
        schema2: "MockStructType",
        storage: Any,
    ) -> Tuple[List[Dict[str, Any]], "MockStructType"]:
        """Union two DataFrames with their data and schemas."""
        # Convert data to MockRow objects for union
        rows1 = [MockRow(row) for row in data1]
        rows2 = [MockRow(row) for row in data2]

        # Perform union
        unioned_rows = SetOperations.union_rows(rows1, rows2)

        # Convert back to dict format
        result_data = [row.__dict__ for row in unioned_rows]

        # Return the schema from the first DataFrame (assuming they're compatible)
        return result_data, schema1

    @staticmethod
    def intersect_rows(rows1: List[MockRow], rows2: List[MockRow]) -> List[MockRow]:
        """Find intersection of two lists of rows."""

        def make_hashable(row: MockRow) -> Tuple[Any, ...]:
            row_values = []
            for col in row.__dict__.keys():
                value = getattr(row, col)
                if isinstance(value, dict):
                    value = tuple(sorted(value.items()))
                elif isinstance(value, list):
                    value = tuple(value)
                row_values.append(value)
            return tuple(row_values)

        set1 = {make_hashable(row) for row in rows1}
        set2 = {make_hashable(row) for row in rows2}

        intersection = set1.intersection(set2)
        result = []
        for row in rows1:
            row_tuple = make_hashable(row)
            if row_tuple in intersection:
                result.append(row)
        return result

    @staticmethod
    def except_rows(rows1: List[MockRow], rows2: List[MockRow]) -> List[MockRow]:
        """Find rows in rows1 that are not in rows2."""

        def make_hashable(row: MockRow) -> Tuple[Any, ...]:
            row_values = []
            for col in row.__dict__.keys():
                value = getattr(row, col)
                if isinstance(value, dict):
                    value = tuple(sorted(value.items()))
                elif isinstance(value, list):
                    value = tuple(value)
                row_values.append(value)
            return tuple(row_values)

        set2 = {make_hashable(row) for row in rows2}
        result = []
        for row in rows1:
            row_tuple = make_hashable(row)
            if row_tuple not in set2:
                result.append(row)
        return result

    @staticmethod
    def rows_equal(row1: MockRow, row2: MockRow) -> bool:
        """Check if two rows are equal."""
        if row1.__dict__.keys() != row2.__dict__.keys():
            return False
        return all(
            getattr(row1, col) == getattr(row2, col) for col in row1.__dict__.keys()
        )
