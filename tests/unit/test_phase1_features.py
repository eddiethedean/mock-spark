"""
Tests for Phase 1 high-priority missing PySpark features.

Tests 10 critical features available in all PySpark versions (3.0-3.5):
- foreach, foreachPartition, repartitionByRange, sortWithinPartitions
- toLocalIterator, localCheckpoint, isLocal, withWatermark
- pandas_udf, UserDefinedFunction
"""

import pytest
from mock_spark import MockSparkSession, F
from mock_spark.spark_types import IntegerType, StringType
from mock_spark.functions import UserDefinedFunction


@pytest.fixture
def spark():
    """Create MockSparkSession for tests."""
    return MockSparkSession("test_phase1_features")


class TestForeach:
    """Test foreach DataFrame method."""

    def test_foreach_basic_iteration(self, spark):
        """Test foreach applies function to each row."""
        df = spark.createDataFrame(
            [
                {"name": "Alice", "age": 25},
                {"name": "Bob", "age": 30},
                {"name": "Charlie", "age": 35},
            ]
        )

        results = []
        df.foreach(lambda row: results.append(row.name))

        assert len(results) == 3
        assert "Alice" in results
        assert "Bob" in results
        assert "Charlie" in results

    def test_foreach_with_counter(self, spark):
        """Test foreach for counting rows."""
        df = spark.createDataFrame([{"id": i} for i in range(10)])

        counter = {"count": 0}

        def increment(row):
            counter["count"] += 1

        df.foreach(increment)
        assert counter["count"] == 10

    def test_foreach_with_filter(self, spark):
        """Test foreach with filtered DataFrame."""
        df = spark.createDataFrame(
            [{"status": "active"}, {"status": "inactive"}, {"status": "active"}]
        )

        active_count = {"count": 0}
        df.filter(F.col("status") == "active").foreach(
            lambda row: active_count.update({"count": active_count["count"] + 1})
        )

        assert active_count["count"] == 2


class TestForeachPartition:
    """Test foreachPartition DataFrame method."""

    def test_foreachPartition_basic(self, spark):
        """Test foreachPartition applies function to partition iterator."""
        df = spark.createDataFrame([{"id": i} for i in range(5)])

        partition_sizes = []

        def count_partition(partition):
            count = sum(1 for _ in partition)
            partition_sizes.append(count)

        df.foreachPartition(count_partition)

        # Mock implementation treats entire dataset as single partition
        assert len(partition_sizes) == 1
        assert partition_sizes[0] == 5

    def test_foreachPartition_batch_processing(self, spark):
        """Test foreachPartition for batch processing simulation."""
        df = spark.createDataFrame([{"value": i * 10} for i in range(10)])

        batch_sums = []

        def sum_partition(partition):
            total = sum(row.value for row in partition)
            batch_sums.append(total)

        df.foreachPartition(sum_partition)

        assert len(batch_sums) == 1
        assert batch_sums[0] == sum(i * 10 for i in range(10))


class TestRepartitionByRange:
    """Test repartitionByRange DataFrame method."""

    def test_repartitionByRange_with_num_partitions(self, spark):
        """Test repartitionByRange with partition count."""
        df = spark.createDataFrame(
            [{"id": 5}, {"id": 1}, {"id": 3}, {"id": 2}, {"id": 4}]
        )

        result = df.repartitionByRange(2, "id")
        data = result.collect()

        # Mock implementation returns sorted DataFrame
        assert data[0]["id"] == 1
        assert data[1]["id"] == 2
        assert data[2]["id"] == 3
        assert data[3]["id"] == 4
        assert data[4]["id"] == 5

    def test_repartitionByRange_with_column_only(self, spark):
        """Test repartitionByRange with column argument only."""
        df = spark.createDataFrame(
            [{"name": "Charlie"}, {"name": "Alice"}, {"name": "Bob"}]
        )

        result = df.repartitionByRange("name")
        data = result.collect()

        # Should be sorted by name
        assert data[0]["name"] == "Alice"
        assert data[1]["name"] == "Bob"
        assert data[2]["name"] == "Charlie"

    def test_repartitionByRange_multiple_columns(self, spark):
        """Test repartitionByRange with multiple columns."""
        df = spark.createDataFrame(
            [
                {"dept": "B", "salary": 50000},
                {"dept": "A", "salary": 60000},
                {"dept": "A", "salary": 55000},
            ]
        )

        result = df.repartitionByRange(2, "dept", "salary")
        data = result.collect()

        # Should be sorted by dept then salary
        assert data[0]["dept"] == "A"
        assert data[0]["salary"] == 55000


class TestSortWithinPartitions:
    """Test sortWithinPartitions DataFrame method."""

    def test_sortWithinPartitions_single_column(self, spark):
        """Test sortWithinPartitions with single column."""
        df = spark.createDataFrame([{"id": 3}, {"id": 1}, {"id": 2}])

        result = df.sortWithinPartitions("id")
        data = result.collect()

        assert data[0]["id"] == 1
        assert data[1]["id"] == 2
        assert data[2]["id"] == 3

    def test_sortWithinPartitions_descending(self, spark):
        """Test sortWithinPartitions with descending order."""
        df = spark.createDataFrame([{"value": 10}, {"value": 30}, {"value": 20}])

        result = df.sortWithinPartitions(F.col("value").desc())
        data = result.collect()

        assert data[0]["value"] == 30
        assert data[1]["value"] == 20
        assert data[2]["value"] == 10

    def test_sortWithinPartitions_multiple_columns(self, spark):
        """Test sortWithinPartitions with multiple columns."""
        df = spark.createDataFrame(
            [
                {"dept": "A", "score": 85},
                {"dept": "B", "score": 90},
                {"dept": "A", "score": 95},
            ]
        )

        result = df.sortWithinPartitions("dept", F.col("score").desc())
        data = result.collect()

        # Should be sorted by dept ascending, then score descending
        assert data[0]["dept"] == "A" and data[0]["score"] == 95
        assert data[1]["dept"] == "A" and data[1]["score"] == 85
        assert data[2]["dept"] == "B"


class TestToLocalIterator:
    """Test toLocalIterator DataFrame method."""

    def test_toLocalIterator_basic(self, spark):
        """Test toLocalIterator returns iterator over rows."""
        df = spark.createDataFrame([{"id": i} for i in range(5)])

        iterator = df.toLocalIterator()
        rows = list(iterator)

        assert len(rows) == 5
        assert rows[0].id == 0
        assert rows[4].id == 4

    def test_toLocalIterator_in_for_loop(self, spark):
        """Test toLocalIterator in for loop."""
        df = spark.createDataFrame(
            [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
        )

        names = []
        for row in df.toLocalIterator():
            names.append(row.name)

        assert names == ["Alice", "Bob"]

    def test_toLocalIterator_with_prefetch(self, spark):
        """Test toLocalIterator with prefetchPartitions parameter."""
        df = spark.createDataFrame([{"value": i} for i in range(3)])

        iterator = df.toLocalIterator(prefetchPartitions=True)
        rows = list(iterator)

        assert len(rows) == 3
        assert rows[0].value == 0

    @pytest.mark.skip(reason="Requires spark_ddl_parser dependency")
    def test_toLocalIterator_empty_dataframe(self, spark):
        """Test toLocalIterator with empty DataFrame."""
        df = spark.createDataFrame([], schema="id: int, name: string")

        iterator = df.toLocalIterator()
        rows = list(iterator)

        assert len(rows) == 0


class TestLocalCheckpoint:
    """Test localCheckpoint DataFrame method."""

    def test_localCheckpoint_eager(self, spark):
        """Test localCheckpoint with eager evaluation."""
        df = spark.createDataFrame([{"id": i} for i in range(5)])

        checkpointed = df.localCheckpoint(eager=True)
        data = checkpointed.collect()

        assert len(data) == 5
        assert data[0]["id"] == 0

    def test_localCheckpoint_lazy(self, spark):
        """Test localCheckpoint with lazy evaluation."""
        df = spark.createDataFrame([{"value": i * 2} for i in range(3)])

        checkpointed = df.localCheckpoint(eager=False)
        data = checkpointed.collect()

        assert len(data) == 3
        assert data[1]["value"] == 2

    def test_localCheckpoint_returns_same_data(self, spark):
        """Test localCheckpoint returns DataFrame with same data."""
        df = spark.createDataFrame(
            [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
        )

        checkpointed = df.localCheckpoint()

        original_data = df.collect()
        checkpointed_data = checkpointed.collect()

        assert len(original_data) == len(checkpointed_data)
        assert original_data[0].name == checkpointed_data[0].name

    def test_localCheckpoint_chainable(self, spark):
        """Test localCheckpoint is chainable with other operations."""
        df = spark.createDataFrame([{"id": i} for i in range(10)])

        result = df.filter(F.col("id") > 5).localCheckpoint().select("id")
        data = result.collect()

        assert len(data) == 4
        assert all(row.id > 5 for row in data)


class TestIsLocal:
    """Test isLocal DataFrame method."""

    def test_isLocal_returns_true(self, spark):
        """Test isLocal returns True for mock implementation."""
        df = spark.createDataFrame([{"id": 1}])

        assert df.isLocal() is True

    def test_isLocal_after_transformations(self, spark):
        """Test isLocal after multiple transformations."""
        df = spark.createDataFrame([{"value": i} for i in range(5)])

        transformed = df.filter(F.col("value") > 2).select("value")

        assert transformed.isLocal() is True

    def test_isLocal_with_join(self, spark):
        """Test isLocal after join operation."""
        df1 = spark.createDataFrame([{"id": 1}, {"id": 2}])
        df2 = spark.createDataFrame([{"id": 1}, {"id": 2}])

        joined = df1.join(df2, "id")

        assert joined.isLocal() is True


class TestWithWatermark:
    """Test withWatermark DataFrame method."""

    def test_withWatermark_basic(self, spark):
        """Test withWatermark with event time column."""
        df = spark.createDataFrame(
            [
                {"timestamp": "2024-01-01 10:00:00", "value": 1},
                {"timestamp": "2024-01-01 10:05:00", "value": 2},
            ]
        )

        result = df.withWatermark("timestamp", "10 minutes")
        data = result.collect()

        # Watermark doesn't change data in mock implementation
        assert len(data) == 2
        assert data[0]["value"] == 1

    def test_withWatermark_chainable(self, spark):
        """Test withWatermark is chainable."""
        df = spark.createDataFrame([{"event_time": "2024-01-01", "count": 10}])

        result = df.withWatermark("event_time", "1 hour").select("count")
        data = result.collect()

        assert len(data) == 1
        assert data[0]["count"] == 10

    def test_withWatermark_stores_metadata(self, spark):
        """Test withWatermark stores watermark metadata."""
        df = spark.createDataFrame([{"ts": "2024-01-01", "val": 1}])

        result = df.withWatermark("ts", "30 minutes")

        # Check that watermark metadata is stored
        assert hasattr(result, "_watermark_col")
        assert result._watermark_col == "ts"
        assert result._watermark_delay == "30 minutes"

    def test_withWatermark_with_groupby(self, spark):
        """Test withWatermark with groupBy operation."""
        df = spark.createDataFrame(
            [
                {"timestamp": "2024-01-01", "category": "A", "value": 10},
                {"timestamp": "2024-01-01", "category": "B", "value": 20},
            ]
        )

        result = (
            df.withWatermark("timestamp", "1 day")
            .groupBy("category")
            .agg(F.sum("value").alias("total"))
        )

        data = result.collect()
        assert len(data) == 2


class TestPandasUDF:
    """Test pandas_udf function."""

    def test_pandas_udf_decorator_pattern(self, spark):
        """Test pandas_udf with decorator pattern."""

        @F.pandas_udf(IntegerType())
        def double_value(s):
            return s * 2

        # Check that it returns a UserDefinedFunction
        assert isinstance(double_value, UserDefinedFunction)
        assert double_value._is_pandas_udf is True
        assert double_value.evalType == "PANDAS"

        # Test applying it to a column
        spark.createDataFrame([{"value": 5}])
        result_col = double_value("value")
        assert hasattr(result_col, "_udf_func")

    def test_pandas_udf_lambda_pattern(self, spark):
        """Test pandas_udf with lambda."""
        add_ten = F.pandas_udf(lambda x: x + 10, IntegerType())

        assert isinstance(add_ten, UserDefinedFunction)
        assert add_ten.returnType == IntegerType()

    def test_pandas_udf_with_string_return_type(self, spark):
        """Test pandas_udf with StringType."""

        @F.pandas_udf(StringType())
        def to_upper(s):
            return s.upper()

        assert isinstance(to_upper, UserDefinedFunction)
        assert isinstance(to_upper.returnType, StringType)

        # Test applying it to a column
        spark.createDataFrame([{"name": "test"}])
        result_col = to_upper("name")
        assert hasattr(result_col, "_udf_func")

    def test_pandas_udf_default_return_type(self, spark):
        """Test pandas_udf with default return type."""
        simple_func = F.pandas_udf(lambda x: x)

        assert isinstance(simple_func, UserDefinedFunction)
        # Default is StringType
        assert isinstance(simple_func.returnType, StringType)

    def test_pandas_udf_with_function_type(self, spark):
        """Test pandas_udf with functionType parameter."""
        from mock_spark.spark_types import DoubleType

        avg_func = F.pandas_udf(
            lambda x: x.mean(), returnType=DoubleType(), functionType="GROUPED_AGG"
        )

        assert isinstance(avg_func, UserDefinedFunction)
        assert isinstance(avg_func.returnType, DoubleType)


class TestUserDefinedFunction:
    """Test UserDefinedFunction class."""

    def test_udf_instantiation(self, spark):
        """Test UserDefinedFunction instantiation."""

        def my_func(x):
            return x + 1

        udf_obj = UserDefinedFunction(my_func, IntegerType(), name="increment")

        assert udf_obj.func == my_func
        assert udf_obj.returnType == IntegerType()
        assert udf_obj._name == "increment"
        assert udf_obj.evalType == "SQL"
        assert udf_obj._deterministic is True

    def test_udf_as_nondeterministic(self, spark):
        """Test UserDefinedFunction.asNondeterministic()."""

        def random_func(x):
            import random

            return random.randint(1, 100)

        udf_obj = UserDefinedFunction(random_func, IntegerType())

        assert udf_obj._deterministic is True

        nondeterministic_udf = udf_obj.asNondeterministic()

        assert nondeterministic_udf._deterministic is False
        assert nondeterministic_udf is udf_obj  # Returns self

    def test_udf_call_with_string_column(self, spark):
        """Test UserDefinedFunction.__call__() with string column."""

        def square(x):
            return x**2

        udf_obj = UserDefinedFunction(square, IntegerType())
        op = udf_obj("value")

        assert hasattr(op, "_udf_func")
        assert op._udf_func == square
        assert op._udf_return_type == IntegerType()

    def test_udf_call_with_column_object(self, spark):
        """Test UserDefinedFunction.__call__() with Column object."""

        def uppercase(s):
            return s.upper()

        udf_obj = UserDefinedFunction(uppercase, StringType())
        op = udf_obj(F.col("name"))

        assert hasattr(op, "_udf_func")
        assert op._udf_func == uppercase

    def test_udf_pandas_eval_type(self, spark):
        """Test UserDefinedFunction with PANDAS evalType."""

        def pandas_func(series):
            return series * 2

        udf_obj = UserDefinedFunction(pandas_func, IntegerType(), evalType="PANDAS")

        assert udf_obj.evalType == "PANDAS"
        assert udf_obj._is_pandas_udf is True

    def test_udf_with_multiple_columns(self, spark):
        """Test UserDefinedFunction with multiple column arguments."""

        def add_columns(x, y):
            return x + y

        udf_obj = UserDefinedFunction(add_columns, IntegerType())
        op = udf_obj("col1", "col2")

        assert hasattr(op, "_udf_cols")
        assert len(op._udf_cols) == 2


class TestIntegration:
    """Integration tests combining multiple features."""

    def test_foreach_with_iterator(self, spark):
        """Test foreach combined with toLocalIterator."""
        df = spark.createDataFrame([{"id": i} for i in range(5)])

        # Use toLocalIterator to get iterator, then foreach on subset
        filtered = df.filter(F.col("id") > 2)

        results = []
        filtered.foreach(lambda row: results.append(row.id))

        assert results == [3, 4]

    def test_checkpoint_and_repartition(self, spark):
        """Test localCheckpoint with repartitionByRange."""
        df = spark.createDataFrame([{"value": i} for i in range(10, 0, -1)])

        result = df.repartitionByRange(3, "value").localCheckpoint().select("value")

        data = result.collect()
        assert data[0]["value"] == 1  # Should be sorted
        assert len(data) == 10

    def test_watermark_with_aggregation(self, spark):
        """Test withWatermark with aggregation operation."""
        df = spark.createDataFrame(
            [
                {"timestamp": "2024-01-01 10:00:00", "category": "A", "value": 1},
                {"timestamp": "2024-01-01 10:05:00", "category": "B", "value": 2},
                {"timestamp": "2024-01-01 10:15:00", "category": "A", "value": 3},
            ]
        )

        result = (
            df.withWatermark("timestamp", "10 minutes")
            .groupBy("category")
            .agg(F.sum("value").alias("total"))
        )

        # Should execute without error
        data = result.collect()
        assert len(data) == 2

    def test_all_iteration_methods(self, spark):
        """Test all iteration methods work together."""
        df = spark.createDataFrame([{"id": i, "value": i * 10} for i in range(5)])

        # Test foreach
        count1 = {"n": 0}
        df.foreach(lambda row: count1.update({"n": count1["n"] + 1}))
        assert count1["n"] == 5

        # Test foreachPartition
        count2 = {"n": 0}
        df.foreachPartition(
            lambda partition: count2.update(
                {"n": count2["n"] + sum(1 for _ in partition)}
            )
        )
        assert count2["n"] == 5

        # Test toLocalIterator
        count3 = sum(1 for _ in df.toLocalIterator())
        assert count3 == 5

        # Test isLocal
        assert df.isLocal() is True
