"""
Unit tests for array functions.
"""

import pytest
from mock_spark import SparkSession, F
from mock_spark.functions.array import ArrayFunctions


@pytest.mark.unit
class TestArrayFunctions:
    """Test array manipulation functions."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        return SparkSession("test")

    @pytest.fixture
    def array_data(self):
        """Sample data with arrays for testing."""
        return [
            {"name": "Alice", "tags": ["a", "b", "c", "a"], "numbers": [1, 2, 3]},
            {"name": "Bob", "tags": ["d", "e", "f"], "numbers": [4, 5, 6]},
            {"name": "Charlie", "tags": ["a", "b"], "numbers": [7, 8]},
        ]

    @pytest.mark.skip(reason="array_distinct feature removed")
    def test_array_distinct_with_column(self, spark):
        """Test array_distinct with Column."""
        col = F.col("tags")
        result = ArrayFunctions.array_distinct(col)
        assert result.operation == "array_distinct"
        assert result.name == "array_distinct(tags)"

    @pytest.mark.skip(reason="array_distinct feature removed")
    def test_array_distinct_with_string(self):
        """Test array_distinct with string column name."""
        result = ArrayFunctions.array_distinct("tags")
        assert result.operation == "array_distinct"
        assert result.name == "array_distinct(tags)"

    def test_array_intersect_with_columns(self, spark):
        """Test array_intersect with two columns."""
        col1 = F.col("tags1")
        col2 = F.col("tags2")
        result = ArrayFunctions.array_intersect(col1, col2)
        assert result.operation == "array_intersect"

    def test_array_intersect_with_strings(self):
        """Test array_intersect with string column names."""
        result = ArrayFunctions.array_intersect("tags1", "tags2")
        assert result.operation == "array_intersect"

    def test_array_union(self, spark):
        """Test array_union function."""
        result = ArrayFunctions.array_union(F.col("arr1"), F.col("arr2"))
        assert result.operation == "array_union"

    def test_array_except(self, spark):
        """Test array_except function."""
        result = ArrayFunctions.array_except(F.col("arr1"), F.col("arr2"))
        assert result.operation == "array_except"

    def test_array_position_with_value(self, spark):
        """Test array_position function."""
        result = ArrayFunctions.array_position(F.col("tags"), "target")
        assert result.operation == "array_position"
        assert result.value == "target"

    def test_array_remove_with_value(self, spark):
        """Test array_remove function."""
        result = ArrayFunctions.array_remove(F.col("tags"), "remove_me")
        assert result.operation == "array_remove"
        assert result.value == "remove_me"

    def test_array_compact(self, spark):
        """Test array_compact function."""
        result = ArrayFunctions.array_compact(F.col("tags"))
        assert result.operation == "array_compact"

    def test_slice(self, spark):
        """Test slice function."""
        result = ArrayFunctions.slice(F.col("arr"), 2, 3)
        assert result.operation == "slice"
        assert result.value == (2, 3)

    def test_element_at(self, spark):
        """Test element_at function."""
        result = ArrayFunctions.element_at(F.col("arr"), 1)
        assert result.operation == "element_at"
        assert result.value == 1

    def test_array_append(self, spark):
        """Test array_append function."""
        result = ArrayFunctions.array_append(F.col("arr"), 10)
        assert result.operation == "array_append"
        assert result.value == 10

    def test_array_prepend(self, spark):
        """Test array_prepend function."""
        result = ArrayFunctions.array_prepend(F.col("arr"), 0)
        assert result.operation == "array_prepend"
        assert result.value == 0

    def test_array_insert(self, spark):
        """Test array_insert function."""
        result = ArrayFunctions.array_insert(F.col("arr"), 2, 99)
        assert result.operation == "array_insert"
        assert result.value == (2, 99)

    def test_array_size(self, spark):
        """Test array_size function."""
        result = ArrayFunctions.array_size(F.col("arr"))
        assert result.operation == "array_size"

    def test_array_sort(self, spark):
        """Test array_sort function."""
        result = ArrayFunctions.array_sort(F.col("arr"))
        assert result.operation == "array_sort"

    def test_array_contains(self, spark):
        """Test array_contains function."""
        result = ArrayFunctions.array_contains(F.col("tags"), "spark")
        assert result.operation == "array_contains"
        assert result.value == "spark"

    def test_array_max(self, spark):
        """Test array_max function."""
        result = ArrayFunctions.array_max(F.col("numbers"))
        assert result.operation == "array_max"

    def test_array_min(self, spark):
        """Test array_min function."""
        result = ArrayFunctions.array_min(F.col("numbers"))
        assert result.operation == "array_min"

    def test_explode(self, spark):
        """Test explode function."""
        result = ArrayFunctions.explode(F.col("tags"))
        assert result.operation == "explode"

    def test_size(self, spark):
        """Test size function."""
        result = ArrayFunctions.size(F.col("tags"))
        assert result.operation == "size"

    def test_flatten(self, spark):
        """Test flatten function."""
        result = ArrayFunctions.flatten(F.col("nested_arrays"))
        assert result.operation == "flatten"

    def test_reverse(self, spark):
        """Test reverse function."""
        result = ArrayFunctions.reverse(F.col("arr"))
        assert result.operation == "reverse"

    def test_arrays_overlap(self, spark):
        """Test arrays_overlap function."""
        result = ArrayFunctions.arrays_overlap(F.col("arr1"), F.col("arr2"))
        assert result.operation == "arrays_overlap"

    def test_posexplode(self, spark):
        """Test posexplode function."""
        result = ArrayFunctions.posexplode(F.col("arr"))
        assert result.operation == "posexplode"

    def test_posexplode_outer(self, spark):
        """Test posexplode_outer function."""
        result = ArrayFunctions.posexplode_outer(F.col("arr"))
        assert result.operation == "posexplode_outer"

    def test_arrays_zip_with_single_column(self, spark):
        """Test arrays_zip with a single column."""
        result = ArrayFunctions.arrays_zip(F.col("arr"))
        assert result.operation == "arrays_zip"

    def test_arrays_zip_with_multiple_columns(self, spark):
        """Test arrays_zip with multiple columns."""
        result = ArrayFunctions.arrays_zip(F.col("arr1"), F.col("arr2"), F.col("arr3"))
        assert result.operation == "arrays_zip"

    def test_sequence_with_literals(self):
        """Test sequence function with integer literals."""
        result = ArrayFunctions.sequence(1, 10, 2)
        assert result.operation == "sequence"

    def test_sequence_with_column(self, spark):
        """Test sequence function with column start."""
        result = ArrayFunctions.sequence(F.col("start"), 10, 1)
        assert result.operation == "sequence"

    def test_shuffle(self, spark):
        """Test shuffle function."""
        result = ArrayFunctions.shuffle(F.col("arr"))
        assert result.operation == "shuffle"

    def test_array_function_with_single_column(self, spark):
        """Test array function with single column."""
        result = ArrayFunctions.array(F.col("a"))
        assert result.operation == "array"

    def test_array_function_with_multiple_columns(self, spark):
        """Test array function with multiple columns."""
        result = ArrayFunctions.array(F.col("a"), F.col("b"), F.col("c"))
        assert result.operation == "array"

    def test_array_function_raises_with_empty(self):
        """Test array function raises error with no columns."""
        with pytest.raises(ValueError, match="array requires at least one column"):
            ArrayFunctions.array()

    def test_array_repeat(self, spark):
        """Test array_repeat function."""
        result = ArrayFunctions.array_repeat(F.col("value"), 3)
        assert result.operation == "array_repeat"
        assert result.value == 3

    def test_sort_array_ascending(self, spark):
        """Test sort_array with ascending=True."""
        result = ArrayFunctions.sort_array(F.col("values"), asc=True)
        assert result.operation == "array_sort"
        assert result.value

    def test_sort_array_descending(self, spark):
        """Test sort_array with ascending=False."""
        result = ArrayFunctions.sort_array(F.col("values"), asc=False)
        assert result.operation == "array_sort"
        assert not result.value

    def test_array_agg(self, spark):
        """Test array_agg aggregate function."""
        result = ArrayFunctions.array_agg(F.col("name"))
        assert result.function_name == "array_agg"

    def test_cardinality(self, spark):
        """Test cardinality function."""
        result = ArrayFunctions.cardinality(F.col("array_col"))
        assert result.operation == "size"

    def test_transform_with_lambda(self, spark):
        """Test transform with lambda function."""

        def func(x):
            return x * 2

        result = ArrayFunctions.transform(F.col("numbers"), func)
        assert result.operation == "transform"
        # Should be MockLambdaExpression wrapper
        from mock_spark.functions.core.lambda_parser import MockLambdaExpression

        assert isinstance(result.value, MockLambdaExpression)

    def test_filter_with_lambda(self, spark):
        """Test filter with lambda function."""

        def func(x):
            return x > 10

        result = ArrayFunctions.filter(F.col("numbers"), func)
        assert result.operation == "filter"

    def test_exists_with_lambda(self, spark):
        """Test exists with lambda function."""

        def func(x):
            return x > 100

        result = ArrayFunctions.exists(F.col("numbers"), func)
        assert result.operation == "exists"

    def test_forall_with_lambda(self, spark):
        """Test forall with lambda function."""

        def func(x):
            return x > 0

        result = ArrayFunctions.forall(F.col("numbers"), func)
        assert result.operation == "forall"

    def test_aggregate_with_lambda(self, spark):
        """Test aggregate with lambda function."""

        def merge_func(acc, x):
            return acc + x

        result = ArrayFunctions.aggregate(F.col("nums"), 0, merge_func)
        assert result.operation == "aggregate"

    def test_aggregate_with_finish(self, spark):
        """Test aggregate with finish function."""

        def merge_func(acc, x):
            return acc + x

        def finish_func(x):
            return x * 2

        result = ArrayFunctions.aggregate(F.col("nums"), 0, merge_func, finish_func)
        assert result.operation == "aggregate"

    def test_zip_with_with_lambda(self, spark):
        """Test zip_with with lambda function."""

        def merge_func(x, y):
            return x + y

        result = ArrayFunctions.zip_with(F.col("arr1"), F.col("arr2"), merge_func)
        assert result.operation == "zip_with"
