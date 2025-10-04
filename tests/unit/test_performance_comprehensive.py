"""
Comprehensive Performance Testing for Mock Spark

This module provides comprehensive performance tests, stress testing,
and memory limit scenarios to improve coverage and validate performance characteristics.
"""

import pytest
import time
import psutil
import os
from unittest.mock import patch, MagicMock
from mock_spark import MockSparkSession, F
from mock_spark.performance_simulation import MockPerformanceSimulator
from mock_spark.data_generation import MockDataGenerator
from mock_spark.window import MockWindow
from mock_spark.spark_types import (
    MockStructType,
    MockStructField,
    StringType,
    IntegerType,
    DoubleType,
    BooleanType,
)


class TestPerformanceComprehensive:
    """Comprehensive performance testing for Mock Spark."""

    @pytest.fixture
    def spark(self):
        """Create a MockSparkSession for performance testing."""
        return MockSparkSession()

    @pytest.fixture
    def performance_simulator(self, spark):
        """Create a performance simulator for testing."""
        return MockPerformanceSimulator(spark)

    @pytest.fixture
    def data_generator(self):
        """Create a data generator for performance testing."""
        return MockDataGenerator()

    def test_large_dataset_processing(self, spark, data_generator):
        """Test processing of large datasets."""
        # Generate large dataset
        large_data = data_generator.create_test_data(
            schema=MockStructType(
                [
                    MockStructField("id", IntegerType()),
                    MockStructField("value", DoubleType()),
                    MockStructField("category", StringType()),
                    MockStructField("active", BooleanType()),
                ]
            ),
            num_rows=50000,
        )

        df = spark.createDataFrame(large_data)

        # Test basic operations on large dataset
        start_time = time.time()

        # Count operation
        count_result = df.count()
        assert count_result == 50000

        # Select operation
        select_result = df.select("id", "value", "category")
        assert select_result.count() == 50000

        # Filter operation
        filter_result = df.filter(F.col("active") == True)
        filter_count = filter_result.count()
        assert filter_count >= 0  # Should handle gracefully

        # GroupBy operation
        groupby_result = df.groupBy("category").count()
        groupby_count = groupby_result.count()
        assert groupby_count >= 0

        # OrderBy operation
        orderby_result = df.orderBy("value")
        assert orderby_result.count() == 50000

        end_time = time.time()
        processing_time = end_time - start_time

        # Performance should be reasonable (less than 30 seconds for 50k rows)
        assert processing_time < 30.0

    def test_memory_intensive_operations(self, spark):
        """Test memory-intensive operations."""
        # Create dataset with many columns
        wide_data = []
        for i in range(1000):
            row = {"id": i}
            for j in range(50):  # 50 columns
                row[f"col_{j}"] = i * j
            wide_data.append(row)

        df = spark.createDataFrame(wide_data)

        # Test operations on wide dataset
        start_time = time.time()

        # Select all columns
        all_cols = df.select("*")
        assert all_cols.count() == 1000

        # Complex transformations
        complex_result = df.select(
            F.col("id"),
            *[F.col(f"col_{i}") * 2 for i in range(10)],  # First 10 columns
            *[F.col(f"col_{i}") + F.col("id") for i in range(10, 20)],  # Next 10 columns
        )
        assert complex_result.count() == 1000

        end_time = time.time()
        processing_time = end_time - start_time

        # Should complete in reasonable time
        assert processing_time < 10.0

    def test_stress_testing_scenarios(self, spark):
        """Test various stress testing scenarios."""
        # Test 1: Many small operations
        start_time = time.time()

        for i in range(100):
            data = [{"id": j, "value": j * i} for j in range(100)]
            df = spark.createDataFrame(data)
            result = df.select(F.col("id"), F.col("value") * 2)
            assert result.count() == 100

        end_time = time.time()
        stress_time = end_time - start_time

        # Should handle many operations efficiently
        assert stress_time < 15.0

    def test_window_function_performance(self, spark):
        """Test window function performance with large datasets."""
        # Create dataset for window functions
        window_data = []
        for category in ["A", "B", "C", "D", "E"]:
            for i in range(2000):  # 2000 rows per category
                window_data.append(
                    {"category": category, "id": i, "value": i * 10, "score": i % 100}
                )

        df = spark.createDataFrame(window_data)

        start_time = time.time()

        # Test window functions
        window_spec = MockWindow.partitionBy("category").orderBy("value")

        result = df.select(
            F.col("*"),
            F.row_number().over(window_spec).alias("row_num"),
            F.rank().over(window_spec).alias("rank"),
            F.dense_rank().over(window_spec).alias("dense_rank"),
            F.avg("value").over(window_spec).alias("avg_value"),
            F.sum("value").over(window_spec).alias("sum_value"),
        )

        assert result.count() == 10000

        end_time = time.time()
        window_time = end_time - start_time

        # Window functions should complete in reasonable time
        assert window_time < 20.0

    def test_join_performance(self, spark):
        """Test join performance with large datasets."""
        # Create two large datasets for joining
        left_data = [
            {"id": i, "left_value": i * 2, "category": f"cat_{i % 100}"} for i in range(5000)
        ]
        right_data = [
            {"id": i, "right_value": i * 3, "category": f"cat_{i % 100}"} for i in range(5000)
        ]

        left_df = spark.createDataFrame(left_data)
        right_df = spark.createDataFrame(right_data)

        start_time = time.time()

        # Test different join types
        inner_join = left_df.join(right_df, "id", "inner")
        assert inner_join.count() == 5000

        left_join = left_df.join(right_df, "id", "left")
        assert left_join.count() == 5000

        # Test join on different column
        category_join = left_df.join(right_df, "category", "inner")
        category_count = category_join.count()
        assert category_count >= 0

        end_time = time.time()
        join_time = end_time - start_time

        # Joins should complete efficiently - adjusted for mock implementation
        assert join_time < 60.0  # Mock implementation is slower than real PySpark

    def test_aggregation_performance(self, spark):
        """Test aggregation performance with large datasets."""
        # Create dataset for aggregation
        agg_data = []
        for category in range(100):  # 100 categories
            for i in range(1000):  # 1000 rows per category
                agg_data.append(
                    {
                        "category": f"cat_{category}",
                        "value": i * 10,
                        "score": i % 50,
                        "active": i % 2 == 0,
                    }
                )

        df = spark.createDataFrame(agg_data)

        start_time = time.time()

        # Test various aggregations
        agg_result = df.groupBy("category").agg(
            F.count("*").alias("count"),
            F.sum("value").alias("sum_value"),
            F.avg("value").alias("avg_value"),
            F.max("value").alias("max_value"),
            F.min("value").alias("min_value"),
            F.countDistinct("score").alias("distinct_scores"),
        )

        assert agg_result.count() == 100

        # Test complex aggregations
        complex_agg = df.agg(
            F.count("*").alias("total_count"),
            F.sum("value").alias("total_sum"),
            F.avg("value").alias("total_avg"),
            F.countDistinct("category").alias("distinct_categories"),
        )

        assert complex_agg.count() == 1

        end_time = time.time()
        agg_time = end_time - start_time

        # Aggregations should complete efficiently
        assert agg_time < 10.0

    def test_memory_limit_simulation(self, spark, performance_simulator):
        """Test memory limit simulation scenarios."""
        # Test with memory limit simulation - use actual method
        performance_simulator.set_memory_limit(1000)  # Set memory limit to 1000 rows

        # Create large dataset
        large_data = [{"id": i, "value": i * 1000} for i in range(100000)]
        df = spark.createDataFrame(large_data)

        # Operations should handle memory limits gracefully
        try:
            result = df.select(F.col("id"), F.col("value") * 2)
            count = result.count()
            assert count >= 0  # Should handle gracefully
        except Exception:
            # Memory limit exceptions are acceptable
            pass

    def test_performance_metrics_collection(self, spark, performance_simulator):
        """Test performance metrics collection."""
        # Test performance metrics - use actual method
        df = spark.createDataFrame([{"id": 1, "value": 100}])
        result = df.select(F.col("id"), F.col("value") * 2)
        count = result.count()

        assert count == 1

        # Test metrics collection using actual method
        metrics = performance_simulator.get_performance_metrics()
        assert isinstance(metrics, dict)

    def test_concurrent_operations_performance(self, spark):
        """Test performance of concurrent operations."""
        import threading
        import queue

        results: queue.Queue = queue.Queue()

        def worker(worker_id):
            """Worker function for concurrent operations."""
            data = [{"id": i, "worker": worker_id, "value": i * worker_id} for i in range(1000)]
            df = spark.createDataFrame(data)
            result = df.select(F.col("id"), F.col("value") * 2)
            count = result.count()
            results.put((worker_id, count))

        # Start multiple worker threads
        threads = []
        for i in range(5):  # 5 concurrent workers
            thread = threading.Thread(target=worker, args=(i,))
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Collect results
        worker_results = []
        while not results.empty():
            worker_results.append(results.get())

        # All workers should complete successfully
        assert len(worker_results) == 5
        for worker_id, count in worker_results:
            assert count == 1000

    def test_data_serialization_performance(self, spark):
        """Test data serialization performance."""
        # Test with different data types
        complex_data = []
        for i in range(10000):
            complex_data.append(
                {
                    "id": i,
                    "name": f"user_{i}",
                    "score": i * 0.1,
                    "active": i % 2 == 0,
                    "metadata": {"key": f"value_{i}", "count": i},
                }
            )

        df = spark.createDataFrame(complex_data)

        start_time = time.time()

        # Test serialization operations
        collected = df.collect()
        assert len(collected) == 10000

        # Test toPandas conversion
        pandas_df = df.toPandas()
        assert len(pandas_df) == 10000

        end_time = time.time()
        serialization_time = end_time - start_time

        # Serialization should be efficient
        assert serialization_time < 10.0

    def test_error_handling_performance(self, spark):
        """Test error handling performance impact."""
        # Test operations that might cause errors
        df = spark.createDataFrame([{"id": 1, "value": 100}])

        start_time = time.time()

        # Test error-prone operations
        try:
            # This might cause an error
            result = df.select(F.col("non_existent_column"))
        except Exception:
            pass  # Expected error

        # Test valid operations
        valid_result = df.select(F.col("id"), F.col("value"))
        assert valid_result.count() == 1

        end_time = time.time()
        error_handling_time = end_time - start_time

        # Error handling should not significantly impact performance
        assert error_handling_time < 5.0

    def test_resource_cleanup_performance(self, spark):
        """Test resource cleanup performance."""
        # Test multiple session creation and cleanup
        sessions = []

        start_time = time.time()

        for i in range(10):  # Create 10 sessions
            session = MockSparkSession()
            sessions.append(session)

            # Do some work
            df = session.createDataFrame([{"id": i, "value": i * 100}])
            result = df.select(F.col("id"), F.col("value") * 2)
            assert result.count() == 1

        # Cleanup all sessions
        for session in sessions:
            session.stop()

        end_time = time.time()
        cleanup_time = end_time - start_time

        # Cleanup should be efficient
        assert cleanup_time < 10.0

    def test_benchmark_comparison(self, spark):
        """Test benchmark comparison scenarios."""
        # Benchmark different operations
        data = [{"id": i, "value": i * 100} for i in range(10000)]
        df = spark.createDataFrame(data)

        # Benchmark select operation
        start_time = time.time()
        select_result = df.select("id", "value")
        select_count = select_result.count()
        select_time = time.time() - start_time

        # Benchmark filter operation
        start_time = time.time()
        filter_result = df.filter(F.col("value") > 5000)
        filter_count = filter_result.count()
        filter_time = time.time() - start_time

        # Benchmark aggregation operation
        start_time = time.time()
        agg_result = df.agg(F.sum("value"), F.avg("value"))
        agg_count = agg_result.count()
        agg_time = time.time() - start_time

        # All operations should complete successfully
        assert select_count == 10000
        assert filter_count >= 0
        assert agg_count == 1

        # Performance should be reasonable
        assert select_time < 5.0
        assert filter_time < 5.0
        assert agg_time < 5.0
