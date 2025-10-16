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

    # Basic Array Functions (PySpark 3.2+)

    @staticmethod
    def array_compact(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Remove null values from an array.

        Args:
            column: The array column to compact.

        Returns:
            MockColumnOperation representing the array_compact function.

        Example:
            >>> df.select(F.array_compact(F.col("nums")))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(
            column, "array_compact", name=f"array_compact({column.name})"
        )

    @staticmethod
    def slice(column: Union[MockColumn, str], start: int, length: int) -> MockColumnOperation:
        """Extract array slice starting at position for given length.

        Args:
            column: The array column.
            start: Starting position (1-based).
            length: Number of elements to extract.

        Returns:
            MockColumnOperation representing the slice function.

        Example:
            >>> df.select(F.slice(F.col("nums"), 2, 3))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(
            column, "slice", (start, length), name=f"slice({column.name}, {start}, {length})"
        )

    @staticmethod
    def element_at(column: Union[MockColumn, str], index: int) -> MockColumnOperation:
        """Get element at index (1-based, negative for reverse indexing).

        Args:
            column: The array column.
            index: Position to extract (1-based, negative counts from end).

        Returns:
            MockColumnOperation representing the element_at function.

        Example:
            >>> df.select(F.element_at(F.col("nums"), 1))  # First element
            >>> df.select(F.element_at(F.col("nums"), -1))  # Last element
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(
            column, "element_at", index, name=f"element_at({column.name}, {index})"
        )

    @staticmethod
    def array_append(column: Union[MockColumn, str], element: Any) -> MockColumnOperation:
        """Append element to end of array.

        Args:
            column: The array column.
            element: Element to append.

        Returns:
            MockColumnOperation representing the array_append function.

        Example:
            >>> df.select(F.array_append(F.col("nums"), 10))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(
            column, "array_append", element, name=f"array_append({column.name}, {element})"
        )

    @staticmethod
    def array_prepend(column: Union[MockColumn, str], element: Any) -> MockColumnOperation:
        """Prepend element to start of array.

        Args:
            column: The array column.
            element: Element to prepend.

        Returns:
            MockColumnOperation representing the array_prepend function.

        Example:
            >>> df.select(F.array_prepend(F.col("nums"), 0))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(
            column, "array_prepend", element, name=f"array_prepend({column.name}, {element})"
        )

    @staticmethod
    def array_insert(
        column: Union[MockColumn, str], pos: int, value: Any
    ) -> MockColumnOperation:
        """Insert element at position in array.

        Args:
            column: The array column.
            pos: Position to insert at (1-based).
            value: Value to insert.

        Returns:
            MockColumnOperation representing the array_insert function.

        Example:
            >>> df.select(F.array_insert(F.col("nums"), 2, 99))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(
            column, "array_insert", (pos, value), name=f"array_insert({column.name}, {pos}, {value})"
        )

    @staticmethod
    def array_size(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Get array length.

        Args:
            column: The array column.

        Returns:
            MockColumnOperation representing the array_size function.

        Example:
            >>> df.select(F.array_size(F.col("nums")))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(
            column, "array_size", name=f"array_size({column.name})"
        )

    @staticmethod
    def array_sort(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Sort array elements in ascending order.

        Args:
            column: The array column to sort.

        Returns:
            MockColumnOperation representing the array_sort function.

        Example:
            >>> df.select(F.array_sort(F.col("nums")))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(
            column, "array_sort", name=f"array_sort({column.name})"
        )

    @staticmethod
    def array_contains(
        column: Union[MockColumn, str], value: Any
    ) -> MockColumnOperation:
        """Check if array contains a specific value.

        Args:
            column: The array column to search.
            value: The value to search for.

        Returns:
            MockColumnOperation representing the array_contains function.

        Example:
            >>> df.select(F.array_contains(F.col("tags"), "spark"))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(
            column, "array_contains", value=value, name=f"array_contains({column.name}, {value})"
        )

    @staticmethod
    def array_max(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Return maximum value from array.

        Args:
            column: The array column.

        Returns:
            MockColumnOperation representing the array_max function.

        Example:
            >>> df.select(F.array_max(F.col("nums")))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(
            column, "array_max", name=f"array_max({column.name})"
        )

    @staticmethod
    def array_min(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Return minimum value from array.

        Args:
            column: The array column.

        Returns:
            MockColumnOperation representing the array_min function.

        Example:
            >>> df.select(F.array_min(F.col("nums")))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(
            column, "array_min", name=f"array_min({column.name})"
        )

    @staticmethod
    def explode(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Returns a new row for each element in the given array or map.

        Args:
            column: The array or map column.

        Returns:
            MockColumnOperation representing the explode function.

        Example:
            >>> df.select(F.explode(F.col("tags")))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(
            column, "explode", name=f"explode({column.name})"
        )

    @staticmethod
    def size(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Return the size (length) of an array or map.

        Args:
            column: The array or map column.

        Returns:
            MockColumnOperation representing the size function.

        Example:
            >>> df.select(F.size(F.col("tags")))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(
            column, "size", name=f"size({column.name})"
        )

    @staticmethod
    def flatten(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Flatten array of arrays into a single array.

        Args:
            column: The array column containing nested arrays.

        Returns:
            MockColumnOperation representing the flatten function.

        Example:
            >>> df.select(F.flatten(F.col("nested_arrays")))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(
            column, "flatten", name=f"flatten({column.name})"
        )

    @staticmethod
    def reverse(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Reverse the elements of an array.

        Args:
            column: The array column.

        Returns:
            MockColumnOperation representing the reverse function.

        Example:
            >>> df.select(F.reverse(F.col("nums")))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(
            column, "reverse", name=f"reverse({column.name})"
        )

    @staticmethod
    def arrays_overlap(
        column1: Union[MockColumn, str], column2: Union[MockColumn, str]
    ) -> MockColumnOperation:
        """Check if two arrays have any common elements.

        Args:
            column1: First array column.
            column2: Second array column.

        Returns:
            MockColumnOperation representing the arrays_overlap function.

        Example:
            >>> df.select(F.arrays_overlap(F.col("arr1"), F.col("arr2")))
        """
        if isinstance(column1, str):
            column1 = MockColumn(column1)
        if isinstance(column2, str):
            column2 = MockColumn(column2)

        return MockColumnOperation(
            column1,
            "arrays_overlap",
            column2,
            name=f"arrays_overlap({column1.name}, {column2.name})",
        )

    @staticmethod
    def explode_outer(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Returns a new row for each element, including rows with null/empty arrays.

        Args:
            column: The array or map column.

        Returns:
            MockColumnOperation representing the explode_outer function.
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(
            column, "explode_outer", name=f"explode_outer({column.name})"
        )

    @staticmethod
    def posexplode(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Returns a new row for each element with position in array.

        Args:
            column: The array column.

        Returns:
            MockColumnOperation representing the posexplode function.
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(
            column, "posexplode", name=f"posexplode({column.name})"
        )

    @staticmethod
    def posexplode_outer(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Returns a new row for each element with position, including null/empty arrays.

        Args:
            column: The array column.

        Returns:
            MockColumnOperation representing the posexplode_outer function.
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(
            column, "posexplode_outer", name=f"posexplode_outer({column.name})"
        )

    @staticmethod
    def arrays_zip(*columns: Union[MockColumn, str]) -> MockColumnOperation:
        """Merge arrays into array of structs (alias for array_zip).

        Args:
            *columns: Array columns to zip together.

        Returns:
            MockColumnOperation representing the arrays_zip function.
        """
        cols = []
        for col in columns:
            if isinstance(col, str):
                cols.append(MockColumn(col))
            else:
                cols.append(col)

        return MockColumnOperation(
            cols[0] if cols else MockColumn(""),
            "arrays_zip",
            value=cols[1:] if len(cols) > 1 else [],
            name="arrays_zip(...)"
        )

    @staticmethod
    def sequence(
        start: Union[MockColumn, str, int],
        stop: Union[MockColumn, str, int],
        step: Union[MockColumn, str, int] = 1
    ) -> MockColumnOperation:
        """Generate array of integers from start to stop by step.

        Args:
            start: Starting value
            stop: Ending value
            step: Step size (default 1)

        Returns:
            MockColumnOperation representing the sequence function.
        """
        if isinstance(start, str):
            start = MockColumn(start)
        elif isinstance(start, int):
            from mock_spark.functions.core.literals import MockLiteral
            start = MockLiteral(start)  # type: ignore[assignment]

        return MockColumnOperation(
            start,  # type: ignore[arg-type]
            "sequence",
            value=(stop, step),
            name=f"sequence({start}, {stop}, {step})"
        )

    @staticmethod
    def shuffle(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Randomly shuffle array elements.

        Args:
            column: The array column.

        Returns:
            MockColumnOperation representing the shuffle function.
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(
            column, "shuffle", name=f"shuffle({column.name})"
        )

    @staticmethod
    def array(*cols: Union[MockColumn, str]) -> MockColumnOperation:
        """Create array from multiple columns (PySpark 3.0+).

        Args:
            *cols: Variable number of columns to combine into array

        Returns:
            MockColumnOperation representing the array function.

        Example:
            >>> df.select(F.array(F.col("a"), F.col("b"), F.col("c")))
        """
        if not cols:
            raise ValueError("array requires at least one column")
        
        # Convert all columns
        converted_cols = []
        for c in cols:
            if isinstance(c, str):
                converted_cols.append(MockColumn(c))
            else:
                converted_cols.append(c)
        
        # First column is the main column, rest are in value as tuple
        first_col = converted_cols[0]
        if len(converted_cols) > 1:
            rest_cols = tuple(converted_cols[1:])
        else:
            rest_cols = ()
        
        col_names = ", ".join(c.name if hasattr(c, "name") else str(c) for c in converted_cols)
        return MockColumnOperation(
            first_col,
            "array",
            value=rest_cols if rest_cols else None,
            name=f"array({col_names})"
        )

    @staticmethod
    def array_repeat(col: Union[MockColumn, str], count: int) -> MockColumnOperation:
        """Create array by repeating value N times (PySpark 3.0+).

        Args:
            col: Value to repeat
            count: Number of repetitions

        Returns:
            MockColumnOperation representing the array_repeat function.

        Example:
            >>> df.select(F.array_repeat(F.col("value"), 3))
        """
        if isinstance(col, str):
            col = MockColumn(col)
        
        return MockColumnOperation(
            col,
            "array_repeat",
            value=count,
            name=f"array_repeat({col.name}, {count})"
        )

    @staticmethod
    def sort_array(col: Union[MockColumn, str], asc: bool = True) -> MockColumnOperation:
        """Sort array elements (PySpark 3.0+).

        Args:
            col: Array column to sort
            asc: Sort ascending if True, descending if False

        Returns:
            MockColumnOperation representing the sort_array function.

        Example:
            >>> df.select(F.sort_array(F.col("values"), asc=False))
        """
        if isinstance(col, str):
            col = MockColumn(col)
        
        # Use array_sort as internal function name (reuse existing handlers)
        return MockColumnOperation(
            col,
            "array_sort",
            value=asc,
            name=f"sort_array({col.name}, {asc})"
        )

