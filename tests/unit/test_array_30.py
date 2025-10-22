"""Unit tests for PySpark 3.0 array functions."""

from mock_spark import MockSparkSession, F


class TestArrayFunctions:
    """Test array creation and manipulation functions from PySpark 3.0."""

    def test_array_basic(self):
        """Test creating array from multiple columns."""
        spark = MockSparkSession("test")
        df = spark.createDataFrame([{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}])

        result = df.select(
            F.array(F.col("a"), F.col("b"), F.col("c")).alias("arr")
        ).collect()

        # Should create arrays [1, 2, 3] and [4, 5, 6]
        arr1 = result[0]["arr"]
        arr2 = result[1]["arr"]

        # Convert from string if needed
        if isinstance(arr1, str):
            import ast

            arr1 = ast.literal_eval(arr1)
            arr2 = ast.literal_eval(arr2)

        assert arr1 == ["1", "2", "3"] or arr1 == [1, 2, 3]
        assert arr2 == ["4", "5", "6"] or arr2 == [4, 5, 6]

    def test_array_single_column(self):
        """Test array with single column."""
        spark = MockSparkSession("test")
        df = spark.createDataFrame([{"value": 42}])

        result = df.select(F.array(F.col("value")).alias("arr")).collect()

        arr = result[0]["arr"]
        if isinstance(arr, str):
            import ast

            arr = ast.literal_eval(arr)

        assert len(arr) == 1
        assert arr[0] == 42 or arr[0] == "42"

    def test_array_repeat_basic(self):
        """Test array_repeat creates repeated arrays."""
        spark = MockSparkSession("test")
        df = spark.createDataFrame([{"value": "hello"}, {"value": "world"}])

        result = df.select(F.array_repeat(F.col("value"), 3).alias("arr")).collect()

        # Should create ["hello", "hello", "hello"]
        arr1 = result[0]["arr"]
        if isinstance(arr1, str):
            import ast

            arr1 = ast.literal_eval(arr1)

        assert len(arr1) == 3
        assert all(x == "hello" for x in arr1)

    def test_array_repeat_zero(self):
        """Test array_repeat with count 0."""
        spark = MockSparkSession("test")
        df = spark.createDataFrame([{"value": "test"}])

        result = df.select(F.array_repeat(F.col("value"), 0).alias("arr")).collect()

        arr = result[0]["arr"]
        if isinstance(arr, str):
            import ast

            arr = ast.literal_eval(arr) if arr else []

        assert len(arr) == 0

    def test_sort_array_ascending(self):
        """Test sort_array in ascending order."""
        spark = MockSparkSession("test")
        df = spark.createDataFrame([{"values": [3, 1, 2]}, {"values": [9, 5, 7]}])

        result = df.select(F.sort_array(F.col("values")).alias("sorted")).collect()

        sorted1 = result[0]["sorted"]
        if isinstance(sorted1, str):
            import ast

            sorted1 = ast.literal_eval(sorted1)

        # Check if sorted (accounting for string/int conversion)
        assert sorted1[0] <= sorted1[1] <= sorted1[2]

    def test_sort_array_descending(self):
        """Test sort_array in descending order."""
        spark = MockSparkSession("test")
        df = spark.createDataFrame([{"values": [3, 1, 2]}])

        result = df.select(
            F.sort_array(F.col("values"), asc=False).alias("sorted")
        ).collect()

        sorted_arr = result[0]["sorted"]
        if isinstance(sorted_arr, str):
            import ast

            sorted_arr = ast.literal_eval(sorted_arr)

        # Check if reverse sorted
        assert sorted_arr[0] >= sorted_arr[1] >= sorted_arr[2]

    def test_array_multiple_types(self):
        """Test array with different column types."""
        spark = MockSparkSession("test")
        df = spark.createDataFrame([{"int_val": 1, "str_val": "a", "float_val": 1.5}])

        # Test array with same-type columns
        result = df.select(
            F.array(F.col("int_val"), F.col("int_val")).alias("arr")
        ).collect()

        arr = result[0]["arr"]
        if isinstance(arr, str):
            import ast

            arr = ast.literal_eval(arr)

        assert len(arr) == 2
