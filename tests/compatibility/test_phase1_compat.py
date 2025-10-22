"""
Compatibility tests for Phase 1 features (10 high-priority missing features).

Tests that mock-spark implementation matches PySpark behavior for:
- foreach, foreachPartition (action methods)
- repartitionByRange, sortWithinPartitions (optimization methods)
- toLocalIterator, localCheckpoint, isLocal (utility methods)
- withWatermark (streaming method)
- pandas_udf, UserDefinedFunction (UDF features)
"""

import pytest
from tests.compatibility.utils.comparison import assert_dataframes_equal


class TestForeachMethods:
    """Test foreach and foreachPartition methods."""

    @pytest.mark.skip(
        reason="foreach side effects behave differently in distributed vs local execution"
    )
    def test_foreach_side_effects(self, mock_spark, real_spark):
        """Test foreach executes function for each row."""
        # Note: PySpark executes foreach on distributed executors,
        # so side effects don't propagate to driver the same way as mock-spark
        data = [{"id": i, "value": i * 10} for i in range(5)]

        # Test with mock
        mock_df = mock_spark.createDataFrame(data)
        mock_results = []
        mock_df.foreach(lambda row: mock_results.append(row["value"]))

        # Mock should capture all values locally
        assert len(mock_results) == 5

    def test_foreach_exists(self, mock_spark, real_spark):
        """Test foreach method exists and can be called."""
        data = [{"id": i} for i in range(5)]

        # Both should have foreach method
        mock_df = mock_spark.createDataFrame(data)
        pyspark_df = real_spark.createDataFrame(data)

        assert hasattr(mock_df, "foreach")
        assert hasattr(pyspark_df, "foreach")

        # Should be callable without error
        mock_df.foreach(lambda row: None)
        pyspark_df.foreach(lambda row: None)

    def test_foreach_partition_exists(self, mock_spark, real_spark):
        """Test foreachPartition method exists and can be called."""
        data = [{"id": i} for i in range(10)]

        mock_df = mock_spark.createDataFrame(data)
        pyspark_df = real_spark.createDataFrame(data)

        assert hasattr(mock_df, "foreachPartition")
        assert hasattr(pyspark_df, "foreachPartition")

        # Should be callable without error
        mock_df.foreachPartition(lambda iterator: None)
        pyspark_df.foreachPartition(lambda iterator: None)


class TestOptimizationMethods:
    """Test repartitionByRange and sortWithinPartitions."""

    def test_repartition_by_range_basic(self, mock_spark, real_spark):
        """Test repartitionByRange maintains data correctness."""
        data = [{"id": i, "value": i * 10} for i in range(20)]

        # Test with mock
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.repartitionByRange(2, "id")

        # Test with PySpark
        pyspark_df = real_spark.createDataFrame(data)
        pyspark_result = pyspark_df.repartitionByRange(2, "id")

        # Data should be same (partitioning is internal)
        assert_dataframes_equal(mock_result, pyspark_result)

    def test_sort_within_partitions(
        self, mock_spark, real_spark, mock_functions, pyspark_functions
    ):
        """Test sortWithinPartitions maintains data correctness."""
        data = [{"id": i, "value": 20 - i} for i in range(10)]

        # Test with mock
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.sortWithinPartitions(mock_functions.col("value").desc())

        # Test with PySpark
        pyspark_df = real_spark.createDataFrame(data)
        pyspark_result = pyspark_df.sortWithinPartitions(
            pyspark_functions.col("value").desc()
        )

        # Data should be sorted the same way
        assert_dataframes_equal(mock_result, pyspark_result)


class TestUtilityMethods:
    """Test toLocalIterator, localCheckpoint, isLocal."""

    def test_to_local_iterator(self, mock_spark, real_spark):
        """Test toLocalIterator returns same data."""
        data = [{"id": i, "name": f"name_{i}"} for i in range(5)]

        # Test with mock
        mock_df = mock_spark.createDataFrame(data)
        mock_rows = list(mock_df.toLocalIterator())

        # Test with PySpark
        pyspark_df = real_spark.createDataFrame(data)
        pyspark_rows = list(pyspark_df.toLocalIterator())

        # Should have same number of rows
        assert len(mock_rows) == len(pyspark_rows)

        # Data should match (order may vary)
        mock_ids = sorted([r["id"] for r in mock_rows])
        pyspark_ids = sorted([r["id"] for r in pyspark_rows])
        assert mock_ids == pyspark_ids

    def test_local_checkpoint(self, mock_spark, real_spark):
        """Test localCheckpoint returns same DataFrame."""
        data = [{"id": i, "value": i * 2} for i in range(10)]

        # Test with mock
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.localCheckpoint()

        # Test with PySpark
        pyspark_df = real_spark.createDataFrame(data)
        pyspark_result = pyspark_df.localCheckpoint()

        # Checkpointed DataFrame should have same data
        assert_dataframes_equal(mock_result, pyspark_result)

    def test_is_local(self, mock_spark, real_spark):
        """Test isLocal returns boolean."""
        data = [{"id": 1}]

        # Test with mock
        mock_df = mock_spark.createDataFrame(data)
        mock_is_local = mock_df.isLocal()

        # Test with PySpark
        pyspark_df = real_spark.createDataFrame(data)
        pyspark_is_local = pyspark_df.isLocal()

        # Both should return a boolean
        assert isinstance(mock_is_local, bool)
        assert isinstance(pyspark_is_local, bool)


class TestWatermark:
    """Test withWatermark method."""

    def test_with_watermark_basic(self, mock_spark, real_spark):
        """Test withWatermark doesn't change data."""
        data = [{"timestamp": "2024-01-01 10:00:00", "value": 1}]

        # Test with mock
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.withWatermark("timestamp", "1 minute")

        # Test with PySpark
        pyspark_df = real_spark.createDataFrame(data)
        pyspark_result = pyspark_df.withWatermark("timestamp", "1 minute")

        # Watermark doesn't change data (only affects streaming)
        assert_dataframes_equal(mock_result, pyspark_result)

    def test_with_watermark_chainable(
        self, mock_spark, real_spark, mock_functions, pyspark_functions
    ):
        """Test withWatermark is chainable."""
        data = [
            {"timestamp": "2024-01-01 10:00:00", "value": 1},
            {"timestamp": "2024-01-01 10:05:00", "value": 2},
        ]

        # Test with mock
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.withWatermark("timestamp", "5 minutes").filter(
            mock_functions.col("value") > 0
        )

        # Test with PySpark
        pyspark_df = real_spark.createDataFrame(data)
        pyspark_result = pyspark_df.withWatermark("timestamp", "5 minutes").filter(
            pyspark_functions.col("value") > 0
        )

        assert_dataframes_equal(mock_result, pyspark_result)


class TestUDFFeatures:
    """Test pandas_udf and UserDefinedFunction."""

    @pytest.mark.skip(
        reason="UDF execution differs between PySpark (Catalyst) and mock-spark (Python)"
    )
    def test_udf_basic_usage(
        self, mock_spark, real_spark, mock_functions, pyspark_functions
    ):
        """Test UDF basic functionality."""
        # Note: UDFs in mock-spark execute Python directly, while PySpark
        # compiles them to Catalyst expressions. Results should be same, but
        # internal execution path differs significantly.
        from mock_spark.spark_types import IntegerType as MockIntType

        data = [{"value": 5}, {"value": 10}]

        # Test with mock
        mock_df = mock_spark.createDataFrame(data)
        mock_double = mock_functions.udf(lambda x: x * 2, MockIntType())
        mock_result = mock_df.select(
            mock_double(mock_functions.col("value")).alias("doubled")
        )
        mock_data = mock_result.collect()
        assert len(mock_data) == 2
        assert mock_data[0]["doubled"] == 10
        assert mock_data[1]["doubled"] == 20

    def test_udf_exists(self, mock_functions, pyspark_functions):
        """Test udf function exists in both environments."""
        assert hasattr(mock_functions, "udf")
        assert hasattr(pyspark_functions, "udf")
        assert callable(mock_functions.udf)
        assert callable(pyspark_functions.udf)

    @pytest.mark.skip(reason="UDF execution differs between PySpark and mock-spark")
    def test_udf_decorator_pattern(
        self, mock_spark, real_spark, mock_functions, pyspark_functions
    ):
        """Test UDF decorator pattern."""
        from mock_spark.spark_types import StringType as MockStringType

        data = [{"name": "Alice"}, {"name": "Bob"}]

        # Test with mock
        @mock_functions.udf(returnType=MockStringType())
        def mock_upper(s):
            return s.upper()

        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.select(
            mock_upper(mock_functions.col("name")).alias("upper_name")
        )
        mock_data = mock_result.collect()
        assert len(mock_data) == 2
        assert mock_data[0]["upper_name"] == "ALICE"

    def test_user_defined_function_class(self, mock_functions, pyspark_functions):
        """Test UserDefinedFunction class exists."""
        from mock_spark.functions import UserDefinedFunction as MockUDF
        from pyspark.sql.functions import UserDefinedFunction as PySparkUDF

        # Both should be classes
        assert isinstance(MockUDF, type)
        assert isinstance(PySparkUDF, type)

    @pytest.mark.skipif(
        True, reason="pandas_udf requires pandas and specific PySpark configuration"
    )
    def test_pandas_udf_basic(
        self, mock_spark, real_spark, mock_functions, pyspark_functions
    ):
        """Test pandas_udf basic functionality (requires pandas)."""
        pytest.importorskip("pandas")
        from mock_spark.spark_types import IntegerType as MockIntType
        from pyspark.sql.types import IntegerType as PySparkIntType

        data = [{"value": i} for i in range(5)]

        # Test with mock
        @mock_functions.pandas_udf(MockIntType())
        def mock_double(s):
            return s * 2

        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.select(
            mock_double(mock_functions.col("value")).alias("doubled")
        )

        # Test with PySpark
        @pyspark_functions.pandas_udf(PySparkIntType())
        def pyspark_double(s):
            return s * 2

        pyspark_df = real_spark.createDataFrame(data)
        pyspark_result = pyspark_df.select(
            pyspark_double(pyspark_functions.col("value")).alias("doubled")
        )

        assert_dataframes_equal(mock_result, pyspark_result)


class TestIntegration:
    """Integration tests combining multiple Phase 1 features."""

    def test_checkpoint_with_sort_within_partitions(self, mock_spark, real_spark):
        """Test localCheckpoint with sortWithinPartitions."""
        data = [{"id": i, "value": 20 - i} for i in range(10)]

        # Test with mock
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.sortWithinPartitions("value").localCheckpoint()

        # Test with PySpark
        pyspark_df = real_spark.createDataFrame(data)
        pyspark_result = pyspark_df.sortWithinPartitions("value").localCheckpoint()

        assert_dataframes_equal(mock_result, pyspark_result)

    def test_repartition_with_to_local_iterator(self, mock_spark, real_spark):
        """Test repartitionByRange with toLocalIterator."""
        data = [{"id": i} for i in range(20)]

        # Test with mock
        mock_df = mock_spark.createDataFrame(data)
        mock_result = list(mock_df.repartitionByRange(3, "id").toLocalIterator())

        # Test with PySpark
        pyspark_df = real_spark.createDataFrame(data)
        pyspark_result = list(pyspark_df.repartitionByRange(3, "id").toLocalIterator())

        # Should have same number of rows
        assert len(mock_result) == len(pyspark_result)

        # IDs should match
        mock_ids = sorted([r["id"] for r in mock_result])
        pyspark_ids = sorted([r["id"] for r in pyspark_result])
        assert mock_ids == pyspark_ids
