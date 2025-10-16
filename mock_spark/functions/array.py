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

from typing import Any, Union, Callable, Optional
from mock_spark.functions.base import MockColumn, MockColumnOperation, MockLambdaExpression


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

    @staticmethod
    def transform(
        column: Union[MockColumn, str], function: Callable[[Any], Any]
    ) -> MockColumnOperation:
        """Apply a function to each element in the array.

        This is a higher-order function that transforms each element of an array
        using the provided lambda function.

        Args:
            column: The array column to transform.
            function: Lambda function to apply to each element.

        Returns:
            MockColumnOperation representing the transform function.

        Example:
            >>> df.select(F.transform(F.col("numbers"), lambda x: x * 2))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        # Wrap the lambda function
        lambda_expr = MockLambdaExpression(function)

        return MockColumnOperation(
            column,
            "transform",
            lambda_expr,
            name=f"transform({column.name}, <lambda>)",
        )

    @staticmethod
    def filter(
        column: Union[MockColumn, str], function: Callable[[Any], bool]
    ) -> MockColumnOperation:
        """Filter array elements based on a predicate function.

        This is a higher-order function that filters array elements using
        the provided lambda function.

        Args:
            column: The array column to filter.
            function: Lambda function that returns True for elements to keep.

        Returns:
            MockColumnOperation representing the filter function.

        Example:
            >>> df.select(F.filter(F.col("numbers"), lambda x: x > 10))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        # Wrap the lambda function
        lambda_expr = MockLambdaExpression(function)

        return MockColumnOperation(
            column,
            "filter",
            lambda_expr,
            name=f"filter({column.name}, <lambda>)",
        )

    @staticmethod
    def exists(
        column: Union[MockColumn, str], function: Callable[[Any], bool]
    ) -> MockColumnOperation:
        """Check if any element in the array satisfies the predicate.

        This is a higher-order function that returns True if at least one
        element matches the condition.

        Args:
            column: The array column to check.
            function: Lambda function predicate.

        Returns:
            MockColumnOperation representing the exists function.

        Example:
            >>> df.select(F.exists(F.col("numbers"), lambda x: x > 100))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        # Wrap the lambda function
        lambda_expr = MockLambdaExpression(function)

        return MockColumnOperation(
            column,
            "exists",
            lambda_expr,
            name=f"exists({column.name}, <lambda>)",
        )

    @staticmethod
    def forall(
        column: Union[MockColumn, str], function: Callable[[Any], bool]
    ) -> MockColumnOperation:
        """Check if all elements in the array satisfy the predicate.

        This is a higher-order function that returns True only if all
        elements match the condition.

        Args:
            column: The array column to check.
            function: Lambda function predicate.

        Returns:
            MockColumnOperation representing the forall function.

        Example:
            >>> df.select(F.forall(F.col("numbers"), lambda x: x > 0))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        # Wrap the lambda function
        lambda_expr = MockLambdaExpression(function)

        return MockColumnOperation(
            column,
            "forall",
            lambda_expr,
            name=f"forall({column.name}, <lambda>)",
        )

    @staticmethod
    def aggregate(
        column: Union[MockColumn, str],
        initial_value: Any,
        merge: Callable[[Any, Any], Any],
        finish: Optional[Callable[[Any], Any]] = None,
    ) -> MockColumnOperation:
        """Reduce array elements to a single value.

        This is a higher-order function that aggregates array elements
        using an accumulator pattern.

        Args:
            column: The array column to aggregate.
            initial_value: Starting value for the accumulator.
            merge: Lambda function (acc, x) -> acc that combines accumulator and element.
            finish: Optional lambda to transform final accumulator value.

        Returns:
            MockColumnOperation representing the aggregate function.

        Example:
            >>> df.select(F.aggregate(F.col("nums"), F.lit(0), lambda acc, x: acc + x))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        # Wrap the lambda function
        merge_expr = MockLambdaExpression(merge)

        # Store initial value and lambda data as tuple in value
        lambda_data = {"merge": merge_expr, "finish": finish}
        value_tuple = (initial_value, lambda_data)

        return MockColumnOperation(
            column,
            "aggregate",
            value=value_tuple,
            name=f"aggregate({column.name}, <init>, <lambda>)",
        )

    @staticmethod
    def zip_with(
        left: Union[MockColumn, str],
        right: Union[MockColumn, str],
        function: Callable[[Any, Any], Any],
    ) -> MockColumnOperation:
        """Merge two arrays element-wise using a function.

        This is a higher-order function that combines elements from two arrays
        using the provided lambda function.

        Args:
            left: First array column.
            right: Second array column.
            function: Lambda function (x, y) -> result for combining elements.

        Returns:
            MockColumnOperation representing the zip_with function.

        Example:
            >>> df.select(F.zip_with(F.col("arr1"), F.col("arr2"), lambda x, y: x + y))
        """
        if isinstance(left, str):
            left = MockColumn(left)
        if isinstance(right, str):
            right = MockColumn(right)

        # Wrap the lambda function
        lambda_expr = MockLambdaExpression(function)

        # Store right array and lambda as tuple in value
        value_tuple = (right, lambda_expr)

        return MockColumnOperation(
            left,
            "zip_with",
            value=value_tuple,
            name=f"zip_with({left.name}, {right.name}, <lambda>)",
        )

