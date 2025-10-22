"""
Tests for CTE-based query optimization.

This module verifies that the CTE (Common Table Expression) optimization
correctly chains operations into a single query instead of creating
intermediate tables for each operation.
"""

from mock_spark import MockSparkSession, functions as F


class TestCTEOptimization:
    """Test suite for CTE-based query optimization."""

    def test_cte_filter_select_withcolumn_chain(self):
        """Test that a chain of filter, select, and withColumn operations works correctly."""
        spark = MockSparkSession.builder.appName("cte_test").getOrCreate()

        # Create test data
        data = [
            {"id": 1, "name": "Alice", "age": 30, "salary": 50000},
            {"id": 2, "name": "Bob", "age": 25, "salary": 60000},
            {"id": 3, "name": "Charlie", "age": 35, "salary": 70000},
            {"id": 4, "name": "David", "age": 28, "salary": 55000},
            {"id": 5, "name": "Eve", "age": 32, "salary": 65000},
        ]

        # Create DataFrame with lazy evaluation
        df = spark.createDataFrame(data).withLazy(True)

        # Chain multiple operations
        result_df = (
            df.filter(F.col("age") > 27)
            .select("name", "age", "salary")
            .withColumn("bonus", F.col("salary") * 0.1)
            .withColumn("total_comp", F.col("salary") + F.col("bonus"))
        )

        # Collect results (materializes the CTE chain)
        results = result_df.collect()

        # Verify results
        assert len(results) == 4  # Filtered to age > 27

        # Check Alice's total compensation
        alice = [r for r in results if r["name"] == "Alice"][0]
        assert alice["salary"] == 50000
        assert alice["bonus"] == 5000.0
        assert alice["total_comp"] == 55000.0

        # Check Charlie's total compensation
        charlie = [r for r in results if r["name"] == "Charlie"][0]
        assert charlie["salary"] == 70000
        assert charlie["bonus"] == 7000.0
        assert charlie["total_comp"] == 77000.0

    def test_cte_with_orderby_and_limit(self):
        """Test CTE optimization with orderBy and limit operations."""
        spark = MockSparkSession.builder.appName("cte_test").getOrCreate()

        data = [{"id": i, "value": i * 10} for i in range(1, 11)]

        df = spark.createDataFrame(data).withLazy(True)

        # Chain operations including orderBy and limit
        result_df = (
            df.filter(F.col("value") > 30)
            .withColumn("double_value", F.col("value") * 2)
            .orderBy(F.desc("value"))
            .limit(3)
        )

        results = result_df.collect()

        # Should get top 3 by value in descending order
        assert len(results) == 3
        assert results[0]["value"] == 100
        assert results[0]["double_value"] == 200
        assert results[1]["value"] == 90
        assert results[2]["value"] == 80

    def test_cte_with_complex_expressions(self):
        """Test CTE optimization with complex column expressions."""
        spark = MockSparkSession.builder.appName("cte_test").getOrCreate()

        data = [
            {"a": 10, "b": 5, "c": 2},
            {"a": 20, "b": 10, "c": 3},
            {"a": 30, "b": 15, "c": 4},
        ]

        df = spark.createDataFrame(data).withLazy(True)

        # Chain operations with complex expressions
        result_df = (
            df.withColumn("sum_ab", F.col("a") + F.col("b"))
            .withColumn("product_ac", F.col("a") * F.col("c"))
            .filter(F.col("sum_ab") > 15)
            .select("a", "b", "c", "sum_ab", "product_ac")
        )

        results = result_df.collect()

        assert len(results) == 2  # Filtered sum_ab > 15

        # Verify first result
        assert results[0]["a"] == 20
        assert results[0]["sum_ab"] == 30
        assert results[0]["product_ac"] == 60

        # Verify second result
        assert results[1]["a"] == 30
        assert results[1]["sum_ab"] == 45
        assert results[1]["product_ac"] == 120

    def test_cte_fallback_on_unsupported_operation(self):
        """Test that CTE optimization gracefully falls back to table-per-operation for unsupported cases."""
        spark = MockSparkSession.builder.appName("cte_test").getOrCreate()

        data = [{"id": i, "value": i * 10} for i in range(1, 6)]

        df = spark.createDataFrame(data).withLazy(True)

        # Even if CTE optimization fails, operations should still work
        result_df = df.filter(F.col("value") > 20).withColumn(
            "doubled", F.col("value") * 2
        )

        results = result_df.collect()

        assert len(results) == 3
        assert results[0]["doubled"] == 60

    def test_cte_empty_dataframe(self):
        """Test CTE optimization with empty DataFrame."""
        spark = MockSparkSession.builder.appName("cte_test").getOrCreate()

        data = [{"id": 1, "value": 10}, {"id": 2, "value": 20}]

        df = spark.createDataFrame(data).withLazy(True)

        # Filter that results in empty DataFrame
        result_df = df.filter(F.col("value") > 100).select("id", "value")

        results = result_df.collect()

        assert len(results) == 0

    def test_cte_single_operation(self):
        """Test CTE optimization with just a single operation."""
        spark = MockSparkSession.builder.appName("cte_test").getOrCreate()

        data = [{"id": 1, "value": 10}, {"id": 2, "value": 20}, {"id": 3, "value": 30}]

        df = spark.createDataFrame(data).withLazy(True)

        # Single filter operation
        result_df = df.filter(F.col("value") >= 20)

        results = result_df.collect()

        assert len(results) == 2
        assert results[0]["value"] == 20
        assert results[1]["value"] == 30
