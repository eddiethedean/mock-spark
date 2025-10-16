"""
Compatibility tests for PySpark 3.0 array functions.

Tests that array functions work identically in mock-spark and real PySpark 3.0.
"""

import pytest

# Check if PySpark is available
try:
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as F_real
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

from mock_spark import MockSparkSession
import mock_spark.functions as F_mock


@pytest.fixture(scope="module")
def real_spark():
    """Create a real PySpark session."""
    if not PYSPARK_AVAILABLE:
        pytest.skip("PySpark not available")

    spark = SparkSession.builder \
        .appName("test_array_30") \
        .master("local[1]") \
        .getOrCreate()

    yield spark
    spark.stop()


@pytest.fixture(scope="module")
def mock_spark():
    """Create a mock Spark session."""
    return MockSparkSession("test_app")


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not available")
class TestArrayCompat:
    """Test array function compatibility."""

    def test_array_from_columns(self, real_spark, mock_spark):
        """Test array() creates identical arrays from columns."""
        data = [{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}]

        real_df = real_spark.createDataFrame(data)
        real_result = real_df.select(F_real.array(F_real.col("a"), F_real.col("b"), F_real.col("c")).alias("arr")).collect()

        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.select(F_mock.array(F_mock.col("a"), F_mock.col("b"), F_mock.col("c")).alias("arr")).collect()

        for i in range(len(data)):
            real_arr = real_result[i]["arr"]
            mock_arr = mock_result[i]["arr"]

            # Convert mock result if it's a string
            if isinstance(mock_arr, str):
                import ast
                mock_arr = ast.literal_eval(mock_arr)

            assert len(real_arr) == len(mock_arr)
            for j in range(len(real_arr)):
                assert str(real_arr[j]) == str(mock_arr[j])


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not available")
class TestArrayRepeatCompat:
    """Test array_repeat function compatibility."""

    def test_array_repeat_identical(self, real_spark, mock_spark):
        """Test array_repeat produces identical arrays."""
        data = [{"value": "test"}, {"value": "hello"}]

        real_df = real_spark.createDataFrame(data)
        real_result = real_df.select(F_real.array_repeat(F_real.col("value"), 3).alias("arr")).collect()

        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.select(F_mock.array_repeat(F_mock.col("value"), 3).alias("arr")).collect()

        for i in range(len(data)):
            real_arr = real_result[i]["arr"]
            mock_arr = mock_result[i]["arr"]

            if isinstance(mock_arr, str):
                import ast
                mock_arr = ast.literal_eval(mock_arr)

            assert len(real_arr) == len(mock_arr) == 3
            assert all(str(x) == str(real_arr[0]) for x in mock_arr)


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not available")
class TestSortArrayCompat:
    """Test sort_array function compatibility."""

    def test_sort_array_ascending(self, real_spark, mock_spark):
        """Test sort_array ascending produces identical results."""
        data = [{"values": [3, 1, 2, 5, 4]}]

        real_df = real_spark.createDataFrame(data)
        real_result = real_df.select(F_real.sort_array(F_real.col("values")).alias("sorted")).collect()

        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.select(F_mock.sort_array(F_mock.col("values")).alias("sorted")).collect()

        real_arr = real_result[0]["sorted"]
        mock_arr = mock_result[0]["sorted"]

        if isinstance(mock_arr, str):
            import ast
            mock_arr = ast.literal_eval(mock_arr)

        # Both should be sorted [1, 2, 3, 4, 5]
        assert [str(x) for x in real_arr] == [str(x) for x in mock_arr]

    def test_sort_array_descending(self, real_spark, mock_spark):
        """Test sort_array descending produces identical results."""
        data = [{"values": [3, 1, 2, 5, 4]}]

        real_df = real_spark.createDataFrame(data)
        real_result = real_df.select(F_real.sort_array(F_real.col("values"), asc=False).alias("sorted")).collect()

        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.select(F_mock.sort_array(F_mock.col("values"), asc=False).alias("sorted")).collect()

        real_arr = real_result[0]["sorted"]
        mock_arr = mock_result[0]["sorted"]

        if isinstance(mock_arr, str):
            import ast
            mock_arr = ast.literal_eval(mock_arr)

        # Both should be sorted [5, 4, 3, 2, 1]
        assert [str(x) for x in real_arr] == [str(x) for x in mock_arr]

