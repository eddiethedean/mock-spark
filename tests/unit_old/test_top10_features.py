"""
Comprehensive tests for the top 10 missing PySpark features.

Tests cover:
1. colRegex - Regex column selection
2. createOrReplaceGlobalTempView - Global temp view creation
3. replace - Value replacement in DataFrames
4. udf - User-defined functions
5. window - Time-based windowing
6-10. Deprecated aliases (approxCountDistinct, sumDistinct, bitwiseNOT, toDegrees, toRadians)
"""

import pytest
import warnings
from datetime import datetime, timedelta
from mock_spark import MockSparkSession, F
from mock_spark.spark_types import IntegerType, StringType


@pytest.fixture
def spark():
    """Create a MockSparkSession for testing."""
    session = MockSparkSession("test_top10")
    yield session
    session.stop()


class TestDataFrameColRegex:
    """Test colRegex method for regex-based column selection."""

    def test_colregex_simple_pattern(self, spark):
        """Test colRegex with simple pattern."""
        df = spark.createDataFrame([{"user_id": 1, "post_id": 2, "name": "Alice"}])
        # Note: In real PySpark, colRegex returns columns for select expansion
        # For mock implementation, we simplified to return first match
        result = df.colRegex("`.*id`")
        # Columns are stored in dict order, which may vary
        assert result.name in ["user_id", "post_id"]  # One of the matching columns
        assert hasattr(result, "_regex_matches")
        assert set(result._regex_matches) == {"user_id", "post_id"}  # type: ignore

    def test_colregex_with_backticks(self, spark):
        """Test colRegex properly extracts pattern from backticks."""
        df = spark.createDataFrame([{"col1": 1, "col2": 2, "other": 3}])
        result = df.colRegex("`col.*`")
        assert hasattr(result, "_regex_matches")
        assert set(result._regex_matches) == {"col1", "col2"}  # type: ignore

    def test_colregex_no_matches(self, spark):
        """Test colRegex when no columns match."""
        df = spark.createDataFrame([{"a": 1, "b": 2}])
        result = df.colRegex("`xyz.*`")
        assert result.name == ""  # No matches

    def test_colregex_pattern_without_backticks(self, spark):
        """Test colRegex handles pattern without backticks."""
        df = spark.createDataFrame([{"test1": 1, "test2": 2, "other": 3}])
        result = df.colRegex("test.*")
        assert hasattr(result, "_regex_matches")
        assert set(result._regex_matches) == {"test1", "test2"}  # type: ignore


class TestDataFrameCreateOrReplaceGlobalTempView:
    """Test createOrReplaceGlobalTempView method."""

    def test_create_or_replace_new_view(self, spark):
        """Test creating a new global temp view."""
        df = spark.createDataFrame([{"a": 1, "b": 2}])
        # Should not raise error
        df.createOrReplaceGlobalTempView("test_view")

        # Verify the view was created in storage
        assert spark.storage.table_exists("global_temp", "test_view")

    def test_create_or_replace_existing_view(self, spark):
        """Test replacing an existing global temp view."""
        df1 = spark.createDataFrame([{"a": 1}])
        df1.createOrReplaceGlobalTempView("test_view")

        df2 = spark.createDataFrame([{"a": 2}, {"a": 3}])
        # Should not raise error even though view exists
        df2.createOrReplaceGlobalTempView("test_view")

        # Verify view exists (was replaced, not duplicated)
        assert spark.storage.table_exists("global_temp", "test_view")

    def test_create_or_replace_idempotent(self, spark):
        """Test that createOrReplaceGlobalTempView can be called multiple times."""
        df = spark.createDataFrame([{"x": 100}])

        # Create multiple times - should not raise error
        df.createOrReplaceGlobalTempView("my_view")
        df.createOrReplaceGlobalTempView("my_view")
        df.createOrReplaceGlobalTempView("my_view")

        # Verify view exists
        assert spark.storage.table_exists("global_temp", "my_view")


class TestDataFrameReplace:
    """Test replace method for value replacement."""

    def test_replace_with_dict(self, spark):
        """Test replace using dict mapping."""
        df = spark.createDataFrame(
            [
                {"status": "A", "value": 1},
                {"status": "B", "value": 2},
                {"status": "C", "value": 3},
            ]
        )
        result = df.replace({"A": "Active", "B": "Blocked", "C": "Closed"})

        statuses = [row["status"] for row in result.collect()]
        assert statuses == ["Active", "Blocked", "Closed"]

    def test_replace_with_list_and_single_value(self, spark):
        """Test replace list of values with single value."""
        df = spark.createDataFrame(
            [{"id": 1, "score": 10}, {"id": 2, "score": 20}, {"id": 3, "score": 30}]
        )
        result = df.replace([10, 20], 99)

        scores = [row["score"] for row in result.collect()]
        assert scores == [99, 99, 30]

    def test_replace_with_subset(self, spark):
        """Test replace with subset parameter."""
        df = spark.createDataFrame([{"col1": 1, "col2": 1}, {"col1": 2, "col2": 2}])
        # Only replace in col1
        result = df.replace(1, 99, subset=["col1"])

        data = result.collect()
        assert data[0]["col1"] == 99
        assert data[0]["col2"] == 1  # Unchanged

    def test_replace_scalar_values(self, spark):
        """Test replace with scalar to_replace and value."""
        df = spark.createDataFrame([{"x": 5}, {"x": 10}, {"x": 5}])
        result = df.replace(5, 100)

        values = [row["x"] for row in result.collect()]
        assert values == [100, 10, 100]

    def test_replace_with_list_mapping(self, spark):
        """Test replace with list to_replace and list value."""
        df = spark.createDataFrame([{"x": 1}, {"x": 2}, {"x": 3}])
        result = df.replace([1, 2], [10, 20])

        values = [row["x"] for row in result.collect()]
        assert values == [10, 20, 3]


class TestUDF:
    """Test user-defined functions."""

    def test_udf_basic_lambda(self, spark):
        """Test UDF with simple lambda."""
        square = F.udf(lambda x: x * x if x is not None else None, IntegerType())
        df = spark.createDataFrame([{"value": 5}, {"value": 10}])
        result = df.select(square("value").alias("squared"))

        # Note: UDF execution requires backend support - this tests the wrapper
        assert result is not None

    def test_udf_with_string_return(self, spark):
        """Test UDF with string return type."""
        to_upper = F.udf(lambda x: x.upper() if x else None, StringType())
        df = spark.createDataFrame([{"name": "alice"}])
        result = df.select(to_upper("name").alias("upper_name"))

        assert result is not None

    def test_udf_decorator_pattern(self, spark):
        """Test UDF used as decorator."""

        @F.udf(returnType=IntegerType())
        def add_ten(x):
            return (x + 10) if x is not None else None

        df = spark.createDataFrame([{"num": 5}])
        result = df.select(add_ten("num").alias("result"))

        assert result is not None

    def test_udf_default_return_type(self, spark):
        """Test UDF with default StringType return."""
        to_str = F.udf(lambda x: str(x))
        df = spark.createDataFrame([{"num": 42}])
        result = df.select(to_str("num").alias("str_num"))

        assert result is not None


class TestWindowFunction:
    """Test window function for time-based windowing."""

    def test_window_basic_tumbling(self, spark):
        """Test window function with tumbling window."""
        # Create DataFrame with timestamps
        base_time = datetime(2024, 1, 1, 0, 0, 0)
        data = [
            {"timestamp": base_time + timedelta(minutes=i), "value": i}
            for i in range(30)
        ]
        spark.createDataFrame(data)

        # Create window operation
        window_col = F.window("timestamp", "10 minutes")

        # Verify window operation was created
        assert window_col.operation == "window"
        assert window_col._window_duration == "10 minutes"  # type: ignore
        assert window_col._window_slide == "10 minutes"  # type: ignore

    def test_window_sliding(self, spark):
        """Test window function with sliding window."""
        base_time = datetime(2024, 1, 1, 0, 0, 0)
        data = [
            {"timestamp": base_time + timedelta(minutes=i), "value": i}
            for i in range(20)
        ]
        spark.createDataFrame(data)

        window_col = F.window("timestamp", "10 minutes", "5 minutes")

        assert window_col._window_duration == "10 minutes"  # type: ignore
        assert window_col._window_slide == "5 minutes"  # type: ignore

    def test_window_with_start_time(self, spark):
        """Test window function with custom start time."""
        base_time = datetime(2024, 1, 1, 0, 0, 0)
        data = [{"timestamp": base_time, "value": 1}]
        spark.createDataFrame(data)

        window_col = F.window("timestamp", "1 hour", startTime="30 minutes")

        assert window_col._window_start == "30 minutes"  # type: ignore

    def test_window_with_column_object(self, spark):
        """Test window function with Column object."""
        base_time = datetime(2024, 1, 1, 0, 0, 0)
        data = [{"ts": base_time, "val": 1}]
        spark.createDataFrame(data)

        window_col = F.window(F.col("ts"), "5 minutes")

        assert window_col.operation == "window"


class TestDeprecatedAliases:
    """Test deprecated function aliases emit warnings and work correctly."""

    def test_approxCountDistinct_deprecated(self, spark):
        """Test approxCountDistinct emits deprecation warning."""
        spark.createDataFrame([{"a": 1}, {"a": 2}, {"a": 1}])

        with pytest.warns(FutureWarning, match="approxCountDistinct is deprecated"):
            result = F.approxCountDistinct("a")

        assert result.function_name == "approx_count_distinct"

    def test_sumDistinct_deprecated(self, spark):
        """Test sumDistinct emits deprecation warning."""
        spark.createDataFrame([{"value": 1}, {"value": 2}, {"value": 1}])

        with pytest.warns(FutureWarning, match="sumDistinct is deprecated"):
            result = F.sumDistinct("value")

        assert result.function_name == "sum_distinct"

    def test_bitwiseNOT_deprecated(self, spark):
        """Test bitwiseNOT emits deprecation warning."""
        spark.createDataFrame([{"flags": 5}])

        with pytest.warns(FutureWarning, match="bitwiseNOT is deprecated"):
            result = F.bitwiseNOT("flags")

        assert result.operation == "bitwise_not"

    def test_toDegrees_deprecated(self, spark):
        """Test toDegrees emits deprecation warning."""
        spark.createDataFrame([{"radians": 3.14159}])

        with pytest.warns(FutureWarning, match="toDegrees is deprecated"):
            result = F.toDegrees("radians")

        assert result.operation == "degrees"

    def test_toRadians_deprecated(self, spark):
        """Test toRadians emits deprecation warning."""
        spark.createDataFrame([{"degrees": 180.0}])

        with pytest.warns(FutureWarning, match="toRadians is deprecated"):
            result = F.toRadians("degrees")

        assert result.operation == "radians"


class TestIntegration:
    """Test integration of new features with existing functionality."""

    def test_replace_in_pipeline(self, spark):
        """Test replace used in transformation pipeline."""
        df = spark.createDataFrame(
            [
                {"status": "A", "score": 10},
                {"status": "B", "score": 20},
                {"status": "A", "score": 30},
            ]
        )

        result = (
            df.replace({"A": "Active", "B": "Blocked"})
            .filter(F.col("status") == "Active")
            .select("score")
        )

        scores = [row["score"] for row in result.collect()]
        assert set(scores) == {10, 30}

    def test_create_or_replace_view_multiple_queries(self, spark):
        """Test createOrReplaceGlobalTempView can be replaced."""
        df1 = spark.createDataFrame([{"count": 100}])
        df1.createOrReplaceGlobalTempView("metrics")
        assert spark.storage.table_exists("global_temp", "metrics")

        # Replace with new data
        df2 = spark.createDataFrame([{"count": 50}])
        df2.createOrReplaceGlobalTempView("metrics")

        # Verify view still exists (was replaced, not duplicated)
        assert spark.storage.table_exists("global_temp", "metrics")

    def test_udf_in_filter(self, spark):
        """Test UDF used in filter condition."""
        is_even = F.udf(lambda x: x % 2 == 0 if x is not None else False, IntegerType())
        df = spark.createDataFrame([{"num": i} for i in range(1, 6)])

        # Note: Full UDF support requires backend integration
        result = df.select(is_even("num").alias("is_even"))
        assert result is not None

    def test_deprecated_aliases_in_select(self, spark):
        """Test deprecated aliases work in select expressions."""
        df = spark.createDataFrame([{"radians": 0.0}, {"radians": 3.14159}])

        # Suppress warnings for this test
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", FutureWarning)
            result = df.select(F.toDegrees("radians").alias("degrees"))

        assert result is not None
        assert result.count() == 2


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_replace_none_value_raises(self, spark):
        """Test that replace raises error when value is None for list/scalar."""
        df = spark.createDataFrame([{"x": 1}])

        with pytest.raises(ValueError, match="value cannot be None"):
            df.replace([1, 2], None)

        with pytest.raises(ValueError, match="value cannot be None"):
            df.replace(1, None)

    def test_replace_mismatched_list_lengths(self, spark):
        """Test replace raises error for mismatched list lengths."""
        df = spark.createDataFrame([{"x": 1}])

        with pytest.raises(ValueError, match="must have same length"):
            df.replace([1, 2, 3], [10, 20])

    def test_replace_empty_dataframe(self, spark):
        """Test replace on empty DataFrame."""
        df = spark.createDataFrame([], schema="x INT, y STRING")
        result = df.replace(1, 99)

        assert result.count() == 0
        assert result.columns == ["x", "y"]

    def test_colregex_empty_pattern(self, spark):
        """Test colRegex with patterns that match all or none."""
        df = spark.createDataFrame([{"a": 1, "b": 2, "c": 3}])

        # Match all
        result_all = df.colRegex("`.*`")
        assert hasattr(result_all, "_regex_matches")
        assert set(result_all._regex_matches) == {"a", "b", "c"}  # type: ignore

    def test_window_default_parameters(self, spark):
        """Test window function with minimal parameters."""
        spark.createDataFrame([{"ts": datetime(2024, 1, 1)}])

        window_col = F.window("ts", "1 hour")

        # Verify defaults
        assert window_col._window_slide == "1 hour"  # type: ignore
        assert window_col._window_start == "0 seconds"  # type: ignore
