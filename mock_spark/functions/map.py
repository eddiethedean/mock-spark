"""
Map functions for Mock Spark.

This module provides comprehensive map manipulation functions that match PySpark's
map function API. Includes operations for extracting keys, values, entries, and
combining maps for working with map columns in DataFrames.

Key Features:
    - Complete PySpark map function API compatibility
    - Key/value extraction (map_keys, map_values)
    - Entry operations (map_entries)
    - Map combination (map_concat, map_from_arrays)
    - Type-safe operations with proper return types
    - Support for both column references and map literals

Example:
    >>> from mock_spark import MockSparkSession, F
    >>> spark = MockSparkSession("test")
    >>> data = [{"properties": {"key1": "val1", "key2": "val2"}}]
    >>> df = spark.createDataFrame(data)
    >>> df.select(F.map_keys(F.col("properties"))).show()
"""

from typing import Union
from mock_spark.functions.base import MockColumn, MockColumnOperation


class MapFunctions:
    """Collection of map manipulation functions."""

    @staticmethod
    def map_keys(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Return an array of all keys in the map.

        Args:
            column: The map column.

        Returns:
            MockColumnOperation representing the map_keys function.

        Example:
            >>> df.select(F.map_keys(F.col("properties")))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(column, "map_keys", name=f"map_keys({column.name})")

    @staticmethod
    def map_values(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Return an array of all values in the map.

        Args:
            column: The map column.

        Returns:
            MockColumnOperation representing the map_values function.

        Example:
            >>> df.select(F.map_values(F.col("properties")))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(column, "map_values", name=f"map_values({column.name})")

    @staticmethod
    def map_entries(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Return an array of structs with key-value pairs.

        Args:
            column: The map column.

        Returns:
            MockColumnOperation representing the map_entries function.

        Example:
            >>> df.select(F.map_entries(F.col("properties")))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(column, "map_entries", name=f"map_entries({column.name})")

    @staticmethod
    def map_concat(*columns: Union[MockColumn, str]) -> MockColumnOperation:
        """Concatenate multiple maps into a single map.

        Args:
            *columns: Map columns to concatenate.

        Returns:
            MockColumnOperation representing the map_concat function.

        Example:
            >>> df.select(F.map_concat(F.col("map1"), F.col("map2"), F.col("map3")))
        """
        if not columns:
            raise ValueError("At least one column must be provided")

        base_column = MockColumn(columns[0]) if isinstance(columns[0], str) else columns[0]
        column_names = [col.name if hasattr(col, "name") else str(col) for col in columns]

        return MockColumnOperation(
            base_column,
            "map_concat",
            columns[1:],
            name=f"map_concat({', '.join(column_names)})",
        )

    @staticmethod
    def map_from_arrays(
        keys: Union[MockColumn, str], values: Union[MockColumn, str]
    ) -> MockColumnOperation:
        """Create a map from two arrays (keys and values).

        Args:
            keys: Array column containing keys.
            values: Array column containing values.

        Returns:
            MockColumnOperation representing the map_from_arrays function.

        Example:
            >>> df.select(F.map_from_arrays(F.col("keys"), F.col("values")))
        """
        if isinstance(keys, str):
            keys = MockColumn(keys)
        if isinstance(values, str):
            values = MockColumn(values)

        return MockColumnOperation(
            keys,
            "map_from_arrays",
            values,
            name=f"map_from_arrays({keys.name}, {values.name})",
        )

