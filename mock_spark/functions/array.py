"""
Array functions for Mock Spark.

This module provides comprehensive array manipulation functions that match PySpark's
array function API. Includes array operations like distinct, intersect, union, except,
and element operations for working with array columns in DataFrames.

Key Features:
    - Complete PySpark array function API compatibility
    - Array set operations (distinct, intersect, union, except)
    - Element operations (position, remove)
    - Type-safe operations with proper return types
    - Support for both column references and array literals

Example:
    >>> from mock_spark import MockSparkSession, F
    >>> spark = MockSparkSession("test")
    >>> data = [{"tags": ["a", "b", "c", "a"]}, {"tags": ["d", "e", "f"]}]
    >>> df = spark.createDataFrame(data)
    >>> df.select(F.array_distinct(F.col("tags"))).show()
"""

from typing import Any, Union
from mock_spark.functions.base import MockColumn, MockColumnOperation


class ArrayFunctions:
    """Collection of array manipulation functions."""

    @staticmethod
    def array_distinct(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Remove duplicate elements from an array.

        Args:
            column: The array column to process.

        Returns:
            MockColumnOperation representing the array_distinct function.

        Example:
            >>> df.select(F.array_distinct(F.col("tags")))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(
            column, "array_distinct", name=f"array_distinct({column.name})"
        )

    @staticmethod
    def array_intersect(
        column1: Union[MockColumn, str], column2: Union[MockColumn, str]
    ) -> MockColumnOperation:
        """Return the intersection of two arrays.

        Args:
            column1: First array column.
            column2: Second array column.

        Returns:
            MockColumnOperation representing the array_intersect function.

        Example:
            >>> df.select(F.array_intersect(F.col("tags1"), F.col("tags2")))
        """
        if isinstance(column1, str):
            column1 = MockColumn(column1)
        if isinstance(column2, str):
            column2 = MockColumn(column2)

        return MockColumnOperation(
            column1,
            "array_intersect",
            column2,
            name=f"array_intersect({column1.name}, {column2.name})",
        )

    @staticmethod
    def array_union(
        column1: Union[MockColumn, str], column2: Union[MockColumn, str]
    ) -> MockColumnOperation:
        """Return the union of two arrays (with duplicates removed).

        Args:
            column1: First array column.
            column2: Second array column.

        Returns:
            MockColumnOperation representing the array_union function.

        Example:
            >>> df.select(F.array_union(F.col("tags1"), F.col("tags2")))
        """
        if isinstance(column1, str):
            column1 = MockColumn(column1)
        if isinstance(column2, str):
            column2 = MockColumn(column2)

        return MockColumnOperation(
            column1,
            "array_union",
            column2,
            name=f"array_union({column1.name}, {column2.name})",
        )

    @staticmethod
    def array_except(
        column1: Union[MockColumn, str], column2: Union[MockColumn, str]
    ) -> MockColumnOperation:
        """Return elements in first array but not in second.

        Args:
            column1: First array column.
            column2: Second array column.

        Returns:
            MockColumnOperation representing the array_except function.

        Example:
            >>> df.select(F.array_except(F.col("tags1"), F.col("tags2")))
        """
        if isinstance(column1, str):
            column1 = MockColumn(column1)
        if isinstance(column2, str):
            column2 = MockColumn(column2)

        return MockColumnOperation(
            column1,
            "array_except",
            column2,
            name=f"array_except({column1.name}, {column2.name})",
        )

    @staticmethod
    def array_position(column: Union[MockColumn, str], value: Any) -> MockColumnOperation:
        """Return the (1-based) index of the first occurrence of value in the array.

        Args:
            column: The array column.
            value: The value to find.

        Returns:
            MockColumnOperation representing the array_position function.

        Example:
            >>> df.select(F.array_position(F.col("tags"), "target"))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(
            column, "array_position", value, name=f"array_position({column.name}, {value!r})"
        )

    @staticmethod
    def array_remove(column: Union[MockColumn, str], value: Any) -> MockColumnOperation:
        """Remove all occurrences of a value from the array.

        Args:
            column: The array column.
            value: The value to remove.

        Returns:
            MockColumnOperation representing the array_remove function.

        Example:
            >>> df.select(F.array_remove(F.col("tags"), "unwanted"))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(
            column, "array_remove", value, name=f"array_remove({column.name}, {value!r})"
        )

