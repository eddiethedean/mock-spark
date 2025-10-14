"""
Compatibility tests for lazy evaluation semantics.

These tests verify that Mock Spark's lazy evaluation behavior matches PySpark exactly.
"""

import pytest
from mock_spark import MockSparkSession, F
from mock_spark.window import MockWindow as Window


class TestLazyEvaluationCompatibility:
    """Test that lazy evaluation behavior matches PySpark semantics."""

    @pytest.fixture
    def spark(self):
        """Create MockSparkSession with lazy evaluation enabled (default)."""
        return MockSparkSession("lazy_test", enable_lazy_evaluation=True)

    @pytest.fixture
    def sample_data(self):
        """Sample data for testing."""
        return [
            {"name": "Alice", "age": 25, "department": "Engineering", "salary": 80000},
            {"name": "Bob", "age": 30, "department": "Engineering", "salary": 90000},
            {"name": "Charlie", "age": 35, "department": "Marketing", "salary": 70000},
            {"name": "Diana", "age": 28, "department": "Engineering", "salary": 85000},
        ]

    def test_transformations_are_queued_not_executed(self, spark, sample_data):
        """Test that transformations are queued but not executed until action."""
        df = spark.createDataFrame(sample_data)

        # Chain multiple transformations
        result = (
            df.filter(F.col("age") > 25)
            .select("name", "department", "salary")
            .withColumn("salary_bonus", F.col("salary") + 5000)
            .orderBy(F.desc("salary"))
        )

        # Verify that operations are queued but not executed
        assert result.is_lazy is True
        assert len(result._operations_queue) == 4  # filter, select, withColumn, orderBy

        # Original data should be unchanged
        assert len(df.data) == 4
        assert "salary_bonus" not in df.data[0]

    def test_actions_materialize_operations(self, spark, sample_data):
        """Test that actions trigger execution of queued operations."""
        df = spark.createDataFrame(sample_data)

        # Create a transformation chain - filter first, then select
        result = df.filter(F.col("age") > 25).select("name", "salary").orderBy(F.desc("salary"))

        # Verify operations are queued
        assert len(result._operations_queue) == 3

        # Action should materialize and execute operations
        rows = result.collect()

        # Verify results are correct
        assert len(rows) == 3  # Bob, Charlie, Diana (age > 25)
        assert rows[0]["name"] == "Bob"  # Highest salary
        assert rows[1]["name"] == "Diana"
        assert rows[2]["name"] == "Charlie"

    def test_schema_projection_without_materialization(self, spark, sample_data):
        """Test that schema is projected correctly without data materialization."""
        df = spark.createDataFrame(sample_data)

        # Create transformation that changes schema
        result = (
            df.select("name", "age")
            .withColumn("age_next_year", F.col("age") + 1)
            .withColumn("is_senior", F.col("age") > 30)
        )

        # Schema should be available without materialization
        columns = result.columns
        assert "name" in columns
        assert "age" in columns
        assert "age_next_year" in columns
        assert "is_senior" in columns
        assert "department" not in columns
        assert "salary" not in columns

    def test_count_action_fast_path(self, spark, sample_data):
        """Test that count() action uses fast path without full materialization."""
        df = spark.createDataFrame(sample_data)

        # Create transformation chain
        result = df.filter(F.col("department") == "Engineering").filter(F.col("salary") > 80000)

        # Count should work without materializing all data
        count = result.count()
        assert count == 2  # Bob and Diana

    def test_multiple_actions_idempotent(self, spark, sample_data):
        """Test that multiple actions on same DataFrame are idempotent."""
        df = spark.createDataFrame(sample_data)

        # Create transformation chain
        result = df.filter(F.col("department") == "Engineering").orderBy(F.desc("salary"))

        # First action
        rows1 = result.collect()
        count1 = result.count()

        # Second action
        rows2 = result.collect()
        count2 = result.count()

        # Results should be identical
        assert len(rows1) == len(rows2) == 3
        assert count1 == count2 == 3
        assert rows1[0]["name"] == rows2[0]["name"] == "Bob"

    def test_window_functions_lazy_evaluation(self, spark, sample_data):
        """Test that window functions work correctly with lazy evaluation."""
        df = spark.createDataFrame(sample_data)

        # Create window function transformation
        window = Window.partitionBy("department").orderBy(F.desc("salary"))
        result = df.select(F.col("*"), F.row_number().over(window).alias("rank")).filter(
            F.col("rank") <= 2
        )

        # Verify operations are queued
        assert result.is_lazy is True
        assert len(result._operations_queue) == 2  # select with window, filter

        # Materialize and verify results
        rows = result.collect()
        assert len(rows) == 3  # Top 2 from Engineering + 1 from Marketing

        # Verify ranking is correct
        engineering_ranks = [r["rank"] for r in rows if r["department"] == "Engineering"]
        marketing_ranks = [r["rank"] for r in rows if r["department"] == "Marketing"]

        assert engineering_ranks == [1, 2]  # Bob (1), Diana (2)
        assert marketing_ranks == [1]  # Charlie only (no second person)

    def test_error_deferred_to_action_time(self, spark, sample_data):
        """Test that validation errors are deferred until action time."""
        df = spark.createDataFrame(sample_data)

        # This should not raise an error immediately (transformation is queued)
        result = df.filter(F.col("nonexistent_column") > 5)

        # Error should be raised only when action is called
        with pytest.raises(Exception):  # Should be ColumnNotFoundException or similar
            result.collect()

    def test_join_lazy_evaluation(self, spark):
        """Test that join operations work correctly with lazy evaluation."""
        # Create two DataFrames
        employees = spark.createDataFrame(
            [
                {"id": 1, "name": "Alice", "dept_id": 10},
                {"id": 2, "name": "Bob", "dept_id": 20},
                {"id": 3, "name": "Charlie", "dept_id": 10},
            ]
        )

        departments = spark.createDataFrame(
            [
                {"dept_id": 10, "dept_name": "Engineering"},
                {"dept_id": 20, "dept_name": "Marketing"},
            ]
        )

        # Create join transformation
        result = (
            employees.join(departments, "dept_id", "inner")
            .select("name", "dept_name")
            .orderBy("name")
        )

        # Verify operations are queued
        assert result.is_lazy is True
        assert len(result._operations_queue) == 3  # join, select, orderBy

        # Materialize and verify results
        rows = result.collect()
        assert len(rows) == 3
        assert rows[0]["name"] == "Alice"
        assert rows[0]["dept_name"] == "Engineering"

    def test_union_lazy_evaluation(self, spark):
        """Test that union operations work correctly with lazy evaluation."""
        df1 = spark.createDataFrame([{"id": 1, "value": "A"}])
        df2 = spark.createDataFrame([{"id": 2, "value": "B"}])
        df3 = spark.createDataFrame([{"id": 3, "value": "C"}])

        # Create union transformation chain
        result = df1.union(df2).union(df3).filter(F.col("id") > 1).orderBy("id")

        # Verify operations are queued
        assert result.is_lazy is True
        assert len(result._operations_queue) == 4  # union, union, filter, orderBy

        # Materialize and verify results
        rows = result.collect()
        assert len(rows) == 2
        assert rows[0]["id"] == 2
        assert rows[1]["id"] == 3

    def test_complex_transformation_chain(self, spark, sample_data):
        """Test a complex transformation chain with multiple operations."""
        df = spark.createDataFrame(sample_data)

        # Complex transformation chain - simpler approach to avoid column dependency issues
        result = (
            df.filter(F.col("department") == "Engineering")
            .withColumn("salary_rank", F.rank().over(Window.orderBy(F.desc("salary"))))
            .filter(F.col("salary_rank") <= 2)  # Filter on window result
            .select("name", "salary", "salary_rank")
            .orderBy("salary_rank")
        )

        # Verify operations are queued
        assert result.is_lazy is True
        assert len(result._operations_queue) == 5  # filter, withColumn, filter, select, orderBy

        # Materialize and verify results
        rows = result.collect()
        assert len(rows) == 2  # Top 2 engineers
        assert rows[0]["name"] == "Bob"  # Highest salary
        assert rows[0]["salary_rank"] == 1
        assert rows[1]["name"] == "Diana"
        assert rows[1]["salary_rank"] == 2

    def test_show_action_materializes(self, spark, sample_data):
        """Test that show() action materializes the DataFrame."""
        df = spark.createDataFrame(sample_data)

        # Create transformation
        result = df.filter(F.col("age") > 25).orderBy("age")

        # show() should materialize and display results
        # We can't easily test the output, but we can verify it doesn't crash
        result.show()

        # Verify the data was materialized by checking count
        count = result.count()
        assert count == 3  # Bob, Charlie, Diana

    def test_toPandas_action_materializes(self, spark, sample_data):
        """Test that toPandas() action materializes the DataFrame."""
        df = spark.createDataFrame(sample_data)

        # Create transformation
        result = df.filter(F.col("department") == "Engineering")

        # toPandas() should materialize and convert
        pandas_df = result.toPandas()

        assert len(pandas_df) == 3  # Alice, Bob, Diana
        assert "name" in pandas_df.columns
        assert "salary" in pandas_df.columns

    def test_lazy_vs_eager_behavior_difference(self):
        """Test that lazy and eager modes behave differently for transformations."""
        # Test lazy mode (default)
        spark_lazy = MockSparkSession("lazy", enable_lazy_evaluation=True)
        df_lazy = spark_lazy.createDataFrame([{"a": 1}, {"a": 2}])
        result_lazy = df_lazy.filter(F.col("a") > 1)

        assert result_lazy.is_lazy is True
        assert len(result_lazy._operations_queue) == 1

        # Test eager mode
        spark_eager = MockSparkSession("eager", enable_lazy_evaluation=False)
        df_eager = spark_eager.createDataFrame([{"a": 1}, {"a": 2}])
        result_eager = df_eager.filter(F.col("a") > 1)

        assert result_eager.is_lazy is False
        assert len(result_eager._operations_queue) == 0
        assert len(result_eager.data) == 1  # Already filtered

        # Both should produce same results when materialized
        assert len(result_lazy.collect()) == len(result_eager.data) == 1
