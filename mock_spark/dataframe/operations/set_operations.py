"""Set operations for DataFrame."""

from typing import Any, TYPE_CHECKING

from ...spark_types import Row

if TYPE_CHECKING:
    from ...spark_types import StructType


class SetOperations:
    """Handles set operations for DataFrame."""

    @staticmethod
    def distinct_rows(rows: list[Row]) -> list[Row]:
        """Remove duplicate rows from a list."""
        seen = set()
        unique_rows = []
        for row in rows:
            # Create a hashable representation of the row
            row_values = []
            # Handle Row objects - they may have data attribute or __dict__
            row_dict = {}
            if hasattr(row, "data"):
                if isinstance(row.data, dict):
                    row_dict = row.data
                elif isinstance(row.data, list):
                    # List of (key, value) tuples
                    row_dict = dict(row.data)
                elif hasattr(row.data, "_data_dict"):
                    row_dict = row.data._data_dict
            elif hasattr(row, "__dict__"):
                row_dict = row.__dict__

            # Get all column names from the row
            if row_dict:
                for col in sorted(row_dict.keys()):  # Sort for consistent ordering
                    value = row_dict[col]
                    # Convert unhashable types to hashable representations
                    try:
                        if isinstance(value, dict):
                            # Recursively convert dict - use string for unhashable values
                            value = tuple(
                                sorted(
                                    (
                                        k,
                                        v
                                        if not isinstance(v, (dict, list))
                                        else str(v),
                                    )
                                    for k, v in value.items()
                                )
                            )
                        elif isinstance(value, list):
                            # Convert list - use string for unhashable elements
                            value = tuple(
                                v if not isinstance(v, (dict, list)) else str(v)
                                for v in value
                            )
                        row_values.append((col, value))
                    except (TypeError, ValueError):
                        # If value can't be made hashable, use string representation
                        row_values.append((col, str(value)))
            else:
                # Fallback: try to get attributes directly
                for attr in dir(row):
                    if not attr.startswith("_"):
                        try:
                            value = getattr(row, attr)
                            if not callable(value):
                                row_values.append((attr, str(value)))
                        except (AttributeError, TypeError):
                            pass

            row_tuple = tuple(row_values)
            if row_tuple not in seen:
                seen.add(row_tuple)
                unique_rows.append(row)
        return unique_rows

    @staticmethod
    def union_rows(rows1: list[Row], rows2: list[Row]) -> list[Row]:
        """Union two lists of rows."""
        return rows1 + rows2

    @staticmethod
    def union(
        data1: list[dict[str, Any]],
        schema1: "StructType",
        data2: list[dict[str, Any]],
        schema2: "StructType",
        storage: Any,
    ) -> tuple[list[dict[str, Any]], "StructType"]:
        """Union two DataFrames with their data and schemas."""
        # Convert data to Row objects for union
        rows1 = [Row(row) for row in data1]
        rows2 = [Row(row) for row in data2]

        # Perform union
        unioned_rows = SetOperations.union_rows(rows1, rows2)

        # Convert back to dict format
        result_data = []
        for row in unioned_rows:
            if hasattr(row, "data"):
                # Row object - convert data to dict
                if isinstance(row.data, dict):
                    result_data.append(row.data)
                elif isinstance(row.data, list):
                    # List of (key, value) tuples
                    result_data.append(dict(row.data))
                else:
                    result_data.append(
                        dict(row.data) if hasattr(row.data, "items") else {}
                    )
            elif hasattr(row, "__dict__"):
                result_data.append(row.__dict__)
            else:
                result_data.append(dict(row) if hasattr(row, "items") else {})

        # Return the schema from the first DataFrame (assuming they're compatible)
        return result_data, schema1

    @staticmethod
    def intersect_rows(rows1: list[Row], rows2: list[Row]) -> list[Row]:
        """Find intersection of two lists of rows."""

        def make_hashable(row: Row) -> tuple[Any, ...]:
            row_values = []
            for col in row.__dict__:
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
    def except_rows(rows1: list[Row], rows2: list[Row]) -> list[Row]:
        """Find rows in rows1 that are not in rows2."""

        def make_hashable(row: Row) -> tuple[Any, ...]:
            row_values = []
            for col in row.__dict__:
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
    def rows_equal(row1: Row, row2: Row) -> bool:
        """Check if two rows are equal."""
        if row1.__dict__.keys() != row2.__dict__.keys():
            return False
        return all(getattr(row1, col) == getattr(row2, col) for col in row1.__dict__)
