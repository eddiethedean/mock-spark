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

from typing import Union, Callable, Any
from mock_spark.functions.base import MockColumn, MockColumnOperation, MockLambdaExpression


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

    # Advanced Map Functions (PySpark 3.2+)

    @staticmethod
    def create_map(*cols: Union[MockColumn, str, Any]) -> MockColumnOperation:
        """Create a map from key-value pairs.

        Args:
            *cols: Alternating key-value columns/literals.

        Returns:
            MockColumnOperation representing the create_map function.

        Example:
            >>> df.select(F.create_map(F.col("k1"), F.col("v1"), F.col("k2"), F.col("v2")))
        """
        if len(cols) < 2 or len(cols) % 2 != 0:
            raise ValueError("create_map requires an even number of arguments (key-value pairs)")

        # Use first column as base, store rest as value
        base_col = cols[0] if isinstance(cols[0], MockColumn) else MockColumn(str(cols[0]))
        
        return MockColumnOperation(
            base_col,
            "create_map",
            value=cols[1:],
            name="create_map(...)",
        )

    @staticmethod
    def map_contains_key(column: Union[MockColumn, str], key: Any) -> MockColumnOperation:
        """Check if map contains a specific key.

        Args:
            column: The map column.
            key: The key to check for.

        Returns:
            MockColumnOperation representing the map_contains_key function.

        Example:
            >>> df.select(F.map_contains_key(F.col("map"), "key"))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(
            column, "map_contains_key", key, name=f"map_contains_key({column.name}, {key!r})"
        )

    @staticmethod
    def map_from_entries(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Convert array of key-value structs to map.

        Args:
            column: Array column containing structs with 'key' and 'value' fields.

        Returns:
            MockColumnOperation representing the map_from_entries function.

        Example:
            >>> df.select(F.map_from_entries(F.col("entries")))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(
            column, "map_from_entries", name=f"map_from_entries({column.name})"
        )

    @staticmethod
    def map_filter(
        column: Union[MockColumn, str], function: Callable[[Any, Any], bool]
    ) -> MockColumnOperation:
        """Filter map entries based on key-value predicate.

        This is a higher-order function that filters map entries using
        the provided lambda function.

        Args:
            column: The map column to filter.
            function: Lambda function (key, value) -> bool that returns True for entries to keep.

        Returns:
            MockColumnOperation representing the map_filter function.

        Example:
            >>> df.select(F.map_filter(F.col("map"), lambda k, v: v > 10))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        # Wrap the lambda function
        lambda_expr = MockLambdaExpression(function)

        return MockColumnOperation(
            column,
            "map_filter",
            value=lambda_expr,
            name=f"map_filter({column.name}, <lambda>)",
        )

    @staticmethod
    def transform_keys(
        column: Union[MockColumn, str], function: Callable[[Any, Any], Any]
    ) -> MockColumnOperation:
        """Transform map keys using a function.

        This is a higher-order function that transforms map keys using
        the provided lambda function.

        Args:
            column: The map column.
            function: Lambda function (key, value) -> new_key to transform keys.

        Returns:
            MockColumnOperation representing the transform_keys function.

        Example:
            >>> df.select(F.transform_keys(F.col("map"), lambda k, v: F.upper(k)))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        # Wrap the lambda function
        lambda_expr = MockLambdaExpression(function)

        return MockColumnOperation(
            column,
            "transform_keys",
            value=lambda_expr,
            name=f"transform_keys({column.name}, <lambda>)",
        )

    @staticmethod
    def transform_values(
        column: Union[MockColumn, str], function: Callable[[Any, Any], Any]
    ) -> MockColumnOperation:
        """Transform map values using a function.

        This is a higher-order function that transforms map values using
        the provided lambda function.

        Args:
            column: The map column.
            function: Lambda function (key, value) -> new_value to transform values.

        Returns:
            MockColumnOperation representing the transform_values function.

        Example:
            >>> df.select(F.transform_values(F.col("map"), lambda k, v: v * 2))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        # Wrap the lambda function
        lambda_expr = MockLambdaExpression(function)

        return MockColumnOperation(
            column,
            "transform_values",
            value=lambda_expr,
            name=f"transform_values({column.name}, <lambda>)",
        )

