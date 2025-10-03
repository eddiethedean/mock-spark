"""
Unit tests for Mock-Spark window functions.

These tests verify window function functionality without real PySpark.
"""

import pytest
from mock_spark import MockSparkSession, F
from mock_spark.window import MockWindow as Window
from mock_spark.spark_types import (
    MockStructType,
    MockStructField,
    StringType,
    IntegerType,
    DoubleType,
)


@pytest.mark.fast
class TestWindowFunctions:
    """Test Mock-Spark window functions."""

    @pytest.fixture
    def spark(self):
        """Create a MockSparkSession for testing."""
        return MockSparkSession("test")

    @pytest.fixture
    def sample_data(self):
        """Sample data for testing window functions."""
        return [
            {
                "name": "Alice",
                "department": "Engineering",
                "salary": 50000.0,
                "hire_date": "2020-01-01",
            },
            {
                "name": "Bob",
                "department": "Engineering",
                "salary": 60000.0,
                "hire_date": "2019-06-01",
            },
            {
                "name": "Charlie",
                "department": "Marketing",
                "salary": 55000.0,
                "hire_date": "2021-03-01",
            },
            {
                "name": "Diana",
                "department": "Engineering",
                "salary": 70000.0,
                "hire_date": "2018-09-01",
            },
            {
                "name": "Eve",
                "department": "Marketing",
                "salary": 45000.0,
                "hire_date": "2022-01-01",
            },
        ]

    def test_row_number_function(self, spark, sample_data):
        """Test row_number() window function."""
        df = spark.createDataFrame(sample_data)

        window = Window.orderBy("salary")
        result = df.select(F.col("*"), F.row_number().over(window).alias("row_num"))

        assert result.count() == 5
        assert "row_num" in result.columns

        # Check that row numbers are sequential
        rows = result.orderBy("salary").collect()
        for i, row in enumerate(rows, 1):
            assert row["row_num"] == i

    def test_rank_function(self, spark, sample_data):
        """Test rank() window function."""
        df = spark.createDataFrame(sample_data)

        window = Window.orderBy("salary")
        result = df.select(F.col("*"), F.rank().over(window).alias("rank"))

        assert result.count() == 5
        assert "rank" in result.columns

    def test_dense_rank_function(self, spark, sample_data):
        """Test dense_rank() window function."""
        df = spark.createDataFrame(sample_data)

        window = Window.orderBy("salary")
        result = df.select(F.col("*"), F.dense_rank().over(window).alias("dense_rank"))

        assert result.count() == 5
        assert "dense_rank" in result.columns

    def test_window_partition_by(self, spark, sample_data):
        """Test window partitioning."""
        df = spark.createDataFrame(sample_data)

        window = Window.partitionBy("department").orderBy("salary")
        result = df.select(
            F.col("*"), F.row_number().over(window).alias("dept_row_num")
        )

        assert result.count() == 5
        assert "dept_row_num" in result.columns

    def test_window_rows_between(self, spark, sample_data):
        """Test window rows between."""
        df = spark.createDataFrame(sample_data)

        window = Window.orderBy("salary").rowsBetween(Window.currentRow, 1)
        result = df.select(
            F.col("*"), F.avg("salary").over(window).alias("avg_salary_window")
        )

        assert result.count() == 5
        assert "avg_salary_window" in result.columns

    def test_window_range_between(self, spark, sample_data):
        """Test window range between."""
        df = spark.createDataFrame(sample_data)

        window = Window.orderBy("salary").rangeBetween(0, 10000)
        result = df.select(
            F.col("*"), F.count("*").over(window).alias("count_in_range")
        )

        assert result.count() == 5
        assert "count_in_range" in result.columns

    def test_lag_function(self, spark, sample_data):
        """Test lag() window function."""
        df = spark.createDataFrame(sample_data)

        window = Window.orderBy("salary")
        result = df.select(
            F.col("*"), F.lag("salary", 1).over(window).alias("prev_salary")
        )

        assert result.count() == 5
        assert "prev_salary" in result.columns

    def test_lead_function(self, spark, sample_data):
        """Test lead() window function."""
        df = spark.createDataFrame(sample_data)

        window = Window.orderBy("salary")
        result = df.select(
            F.col("*"), F.lead("salary", 1).over(window).alias("next_salary")
        )

        assert result.count() == 5
        assert "next_salary" in result.columns

    def test_avg_window_function(self, spark, sample_data):
        """Test avg() window function."""
        df = spark.createDataFrame(sample_data)

        window = Window.partitionBy("department")
        result = df.select(
            F.col("*"), F.avg("salary").over(window).alias("dept_avg_salary")
        )

        assert result.count() == 5
        assert "dept_avg_salary" in result.columns

    def test_sum_window_function(self, spark, sample_data):
        """Test sum() window function."""
        df = spark.createDataFrame(sample_data)

        window = Window.partitionBy("department")
        result = df.select(
            F.col("*"), F.sum("salary").over(window).alias("dept_total_salary")
        )

        assert result.count() == 5
        assert "dept_total_salary" in result.columns

    def test_max_window_function(self, spark, sample_data):
        """Test max() window function."""
        df = spark.createDataFrame(sample_data)

        window = Window.partitionBy("department")
        result = df.select(
            F.col("*"), F.max("salary").over(window).alias("dept_max_salary")
        )

        assert result.count() == 5
        assert "dept_max_salary" in result.columns

    def test_min_window_function(self, spark, sample_data):
        """Test min() window function."""
        df = spark.createDataFrame(sample_data)

        window = Window.partitionBy("department")
        result = df.select(
            F.col("*"), F.min("salary").over(window).alias("dept_min_salary")
        )

        assert result.count() == 5
        assert "dept_min_salary" in result.columns

    def test_count_window_function(self, spark, sample_data):
        """Test count() window function."""
        df = spark.createDataFrame(sample_data)

        window = Window.partitionBy("department")
        result = df.select(F.col("*"), F.count("*").over(window).alias("dept_count"))

        assert result.count() == 5
        assert "dept_count" in result.columns

    def test_complex_window_function(self, spark, sample_data):
        """Test complex window function with multiple operations."""
        df = spark.createDataFrame(sample_data)

        window = Window.partitionBy("department").orderBy("salary")
        result = df.select(
            F.col("*"),
            F.row_number().over(window).alias("row_num"),
            F.rank().over(window).alias("rank"),
            F.avg("salary").over(window).alias("avg_salary"),
            F.lag("salary", 1).over(window).alias("prev_salary"),
        )

        assert result.count() == 5
        assert all(
            col in result.columns
            for col in ["row_num", "rank", "avg_salary", "prev_salary"]
        )

    def test_window_with_empty_dataframe(self, spark):
        """Test window functions with empty DataFrame."""
        df = spark.createDataFrame([])

        window = Window.orderBy("salary")
        result = df.select(F.col("*"), F.row_number().over(window).alias("row_num"))

        assert result.count() == 0

    def test_window_with_single_row(self, spark):
        """Test window functions with single row."""
        df = spark.createDataFrame([{"name": "Alice", "salary": 50000.0}])

        window = Window.orderBy("salary")
        result = df.select(F.col("*"), F.row_number().over(window).alias("row_num"))

        assert result.count() == 1
        assert result.collect()[0]["row_num"] == 1

    def test_window_multiple_columns(self, spark, sample_data):
        """Test window functions with multiple columns."""
        df = spark.createDataFrame(sample_data)

        window = Window.partitionBy("department", "name").orderBy("salary")
        result = df.select(F.col("*"), F.row_number().over(window).alias("row_num"))

        assert result.count() == 5
        assert "row_num" in result.columns
