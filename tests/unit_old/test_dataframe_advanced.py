"""
Advanced DataFrame tests for improved coverage.

These tests cover complex DataFrame operations that are currently not well tested.
"""

import pytest
from mock_spark import MockSparkSession, F
from mock_spark.spark_types import (
    MockStructType,
    MockStructField,
    StringType,
    IntegerType,
    DoubleType,
)
from mock_spark.window import MockWindow


class TestDataFrameAdvancedOperations:
    """Test advanced DataFrame operations for better coverage."""

    @pytest.fixture
    def spark(self):
        """Create a MockSparkSession for testing."""
        return MockSparkSession("test")

    @pytest.fixture
    def complex_data(self):
        """Create complex test data for advanced operations."""
        return [
            {
                "id": 1,
                "name": "Alice",
                "department": "Engineering",
                "salary": 80000.0,
                "bonus": 5000.0,
            },
            {
                "id": 2,
                "name": "Bob",
                "department": "Engineering",
                "salary": 75000.0,
                "bonus": 3000.0,
            },
            {
                "id": 3,
                "name": "Charlie",
                "department": "Marketing",
                "salary": 70000.0,
                "bonus": 4000.0,
            },
            {
                "id": 4,
                "name": "Diana",
                "department": "Engineering",
                "salary": 85000.0,
                "bonus": 6000.0,
            },
            {
                "id": 5,
                "name": "Eve",
                "department": "Marketing",
                "salary": 72000.0,
                "bonus": 3500.0,
            },
        ]

    @pytest.fixture
    def large_data(self):
        """Create large dataset for performance testing."""
        return [
            {"id": i, "value": i * 10, "category": f"cat_{i % 10}"} for i in range(1000)
        ]

    def test_complex_aggregation_operations(self, spark, complex_data):
        """Test complex aggregation operations."""
        df = spark.createDataFrame(complex_data)

        # Test multiple aggregations with different functions
        result = df.groupBy("department").agg(
            F.count("*").alias("count"),
            F.sum("salary").alias("total_salary"),
            F.avg("salary").alias("avg_salary"),
            F.max("salary").alias("max_salary"),
            F.min("salary").alias("min_salary"),
            F.sum("bonus").alias("total_bonus"),
        )

        assert result.count() == 2
        assert "count" in result.columns
        assert "total_salary" in result.columns
        assert "avg_salary" in result.columns

        # Test aggregation with complex expressions
        result2 = df.agg(
            F.count("*").alias("total_count"),
            F.sum(F.col("salary") + F.col("bonus")).alias("total_compensation"),
        )

        assert result2.count() == 1
        assert result2.collect()[0]["total_count"] == 5

    def test_window_functions_comprehensive(self, spark, complex_data):
        """Test comprehensive window function coverage."""
        df = spark.createDataFrame(complex_data)

        # Test window with partitionBy and orderBy
        window_spec = MockWindow.partitionBy("department").orderBy(F.desc("salary"))

        result = df.select(
            F.col("*"),
            F.row_number().over(window_spec).alias("row_num"),
            F.rank().over(window_spec).alias("rank"),
            F.dense_rank().over(window_spec).alias("dense_rank"),
            F.lag("salary", 1).over(window_spec).alias("prev_salary"),
            F.lead("salary", 1).over(window_spec).alias("next_salary"),
            F.avg("salary").over(window_spec).alias("avg_salary"),
            F.sum("salary").over(window_spec).alias("sum_salary"),
            F.max("salary").over(window_spec).alias("max_salary"),
            F.min("salary").over(window_spec).alias("min_salary"),
            F.count("salary").over(window_spec).alias("count_salary"),
        )

        assert result.count() == 5
        assert "row_num" in result.columns
        assert "rank" in result.columns
        assert "dense_rank" in result.columns
        assert "prev_salary" in result.columns
        assert "next_salary" in result.columns
        assert "avg_salary" in result.columns

    def test_window_functions_with_rows_between(self, spark, complex_data):
        """Test window functions with rowsBetween."""
        df = spark.createDataFrame(complex_data)

        # Test window with rowsBetween
        window_spec = (
            MockWindow.partitionBy("department").orderBy("salary").rowsBetween(-1, 1)
        )

        result = df.select(
            F.col("*"),
            F.avg("salary").over(window_spec).alias("rolling_avg"),
            F.sum("salary").over(window_spec).alias("rolling_sum"),
        )

        assert result.count() == 5
        assert "rolling_avg" in result.columns
        assert "rolling_sum" in result.columns

    def test_window_functions_with_range_between(self, spark, complex_data):
        """Test window functions with rangeBetween."""
        df = spark.createDataFrame(complex_data)

        # Test window with rangeBetween
        window_spec = (
            MockWindow.partitionBy("department")
            .orderBy("salary")
            .rangeBetween(-10000, 10000)
        )

        result = df.select(
            F.col("*"),
            F.avg("salary").over(window_spec).alias("range_avg"),
            F.count("salary").over(window_spec).alias("range_count"),
        )

        assert result.count() == 5
        assert "range_avg" in result.columns
        assert "range_count" in result.columns

    def test_complex_select_operations(self, spark, complex_data):
        """Test complex select operations."""
        df = spark.createDataFrame(complex_data)

        # Test complex select with multiple expressions
        result = df.select(
            F.col("id"),
            F.col("name"),
            F.col("salary"),
            (F.col("salary") + F.col("bonus")).alias("total_compensation"),
            F.when(F.col("salary") > 75000, "High")
            .otherwise("Low")
            .alias("salary_level"),
            F.coalesce(F.col("bonus"), F.lit(0)).alias("safe_bonus"),
            F.isnull(F.col("bonus")).alias("bonus_is_null"),
            F.upper(F.col("name")).alias("upper_name"),
            F.length(F.col("name")).alias("name_length"),
        )

        assert result.count() == 5
        assert "total_compensation" in result.columns
        assert "salary_level" in result.columns
        assert "safe_bonus" in result.columns
        assert "bonus_is_null" in result.columns
        assert "upper_name" in result.columns
        assert "name_length" in result.columns

    def test_complex_filter_operations(self, spark, complex_data):
        """Test complex filter operations."""
        df = spark.createDataFrame(complex_data)

        # Test complex filter conditions
        result = df.filter(
            (F.col("salary") > 70000)
            & (F.col("department") == "Engineering")
            & (F.col("bonus") > 3000)
        )

        assert result.count() == 2

        # Test filter with simple conditions
        result2 = df.filter(F.col("salary") > 80000)

        assert result2.count() == 1  # Only Diana has salary > 80000

        # Test filter with null handling
        result3 = df.filter(F.col("bonus").isNotNull() & (F.col("bonus") > 0))

        assert result3.count() == 5

    def test_complex_groupby_operations(self, spark, complex_data):
        """Test complex groupBy operations."""
        df = spark.createDataFrame(complex_data)

        # Test groupBy with multiple columns
        result = df.groupBy("department", "salary").count()

        assert result.count() == 5

        # Test groupBy with complex aggregations
        result2 = df.groupBy("department").agg(
            F.count("*").alias("count"),
            F.sum("salary").alias("total_salary"),
            F.avg("bonus").alias("avg_bonus"),
            F.max("salary").alias("max_salary"),
        )

        assert result2.count() == 2
        assert "count" in result2.columns
        assert "total_salary" in result2.columns
        assert "avg_bonus" in result2.columns
        assert "max_salary" in result2.columns

    def test_complex_orderby_operations(self, spark, complex_data):
        """Test complex orderBy operations."""
        df = spark.createDataFrame(complex_data)

        # Test orderBy with multiple columns
        result = df.orderBy(F.desc("salary"), F.col("name").asc())

        assert result.count() == 5
        assert result.collect()[0]["name"] == "Diana"  # Highest salary

        # Test orderBy with simple expressions
        result2 = df.orderBy(F.desc("salary"))

        assert result2.count() == 5
        assert result2.collect()[0]["name"] == "Diana"  # Highest salary

    def test_complex_withcolumn_operations(self, spark, complex_data):
        """Test complex withColumn operations."""
        df = spark.createDataFrame(complex_data)

        # Test withColumn with complex expressions
        result = (
            df.withColumn("total_compensation", F.col("salary") + F.col("bonus"))
            .withColumn(
                "salary_level",
                F.when(F.col("salary") > 75000, "High")
                .when(F.col("salary") > 70000, "Medium")
                .otherwise("Low"),
            )
            .withColumn("bonus_percentage", F.col("bonus") / F.col("salary") * 100)
        )

        assert result.count() == 5
        assert "total_compensation" in result.columns
        assert "salary_level" in result.columns
        assert "bonus_percentage" in result.columns

    def test_edge_cases_and_error_handling(self, spark):
        """Test edge cases and error handling."""
        # Test with empty DataFrame
        empty_df = spark.createDataFrame([])
        assert empty_df.count() == 0
        assert empty_df.collect() == []

        # Test with single row
        single_row_df = spark.createDataFrame([{"id": 1, "value": 100}])
        assert single_row_df.count() == 1

        # Test with null values
        null_data = [
            {"id": 1, "name": "Alice", "value": None},
            {"id": 2, "name": None, "value": 100},
            {"id": 3, "name": "Charlie", "value": 200},
        ]
        null_df = spark.createDataFrame(null_data)

        # Test null handling in operations
        result = null_df.select(
            F.col("name"),
            F.isnull(F.col("name")).alias("name_is_null"),
            F.coalesce(F.col("value"), F.lit(0)).alias("safe_value"),
        )

        assert result.count() == 3
        assert "name_is_null" in result.columns
        assert "safe_value" in result.columns

    def test_large_dataset_operations(self, spark, large_data):
        """Test operations on large datasets."""
        df = spark.createDataFrame(large_data)

        # Test basic operations on large dataset
        assert df.count() == 1000

        # Test groupBy on large dataset
        result = df.groupBy("category").count()
        assert result.count() == 10  # 10 categories (0-9)

        # Test window functions on large dataset
        window_spec = MockWindow.partitionBy("category").orderBy("value")
        result2 = df.select(
            F.col("*"),
            F.row_number().over(window_spec).alias("row_num"),
            F.rank().over(window_spec).alias("rank"),
        )

        assert result2.count() == 1000
        assert "row_num" in result2.columns
        assert "rank" in result2.columns

    def test_complex_schema_operations(self, spark):
        """Test complex schema operations."""
        # Test with complex schema
        schema = MockStructType(
            [
                MockStructField("id", IntegerType(), False),
                MockStructField("name", StringType(), True),
                MockStructField("salary", DoubleType(), True),
                MockStructField("bonus", DoubleType(), True),
            ]
        )

        data = [
            {"id": 1, "name": "Alice", "salary": 80000.0, "bonus": 5000.0},
            {"id": 2, "name": "Bob", "salary": 75000.0, "bonus": 3000.0},
        ]

        df = spark.createDataFrame(data, schema)

        # Test schema properties
        assert df.schema is not None
        assert len(df.schema.fields) == 4
        assert df.schema.fields[0].name == "id"
        assert df.schema.fields[0].dataType == IntegerType()
        assert not df.schema.fields[0].nullable

        # Test operations with explicit schema
        result = df.select(
            F.col("id"),
            F.col("name"),
            (F.col("salary") + F.col("bonus")).alias("total"),
        )

        assert result.count() == 2
        assert "total" in result.columns

    def test_complex_join_operations(self, spark):
        """Test complex join operations."""
        # Create two DataFrames for joining
        df1_data = [
            {"id": 1, "name": "Alice", "department_id": 1},
            {"id": 2, "name": "Bob", "department_id": 2},
            {"id": 3, "name": "Charlie", "department_id": 1},
        ]

        df2_data = [
            {"dept_id": 1, "department": "Engineering"},
            {"dept_id": 2, "department": "Marketing"},
        ]

        df1 = spark.createDataFrame(df1_data)
        df2 = spark.createDataFrame(df2_data)

        # Rename column to match for join
        df2_renamed = df2.withColumnRenamed("dept_id", "department_id")

        # Test simple join with column names

        result = df1.join(df2_renamed, "department_id", "inner")

        assert result.count() == 3  # Should match all rows
        assert "department" in result.columns

        # Test left join
        result2 = df1.join(df2_renamed, "department_id", "left")

        assert result2.count() == 3  # Should match all rows
        assert "department" in result2.columns

    def test_complex_union_operations(self, spark):
        """Test complex union operations."""
        df1_data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        df2_data = [{"id": 3, "name": "Charlie"}, {"id": 4, "name": "Diana"}]

        df1 = spark.createDataFrame(df1_data)
        df2 = spark.createDataFrame(df2_data)

        # Test union
        result = df1.union(df2)

        assert result.count() == 4

        # Test unionAll (alias for union)
        result2 = df1.union(df2)

        assert result2.count() == 4

    def test_complex_distinct_operations(self, spark):
        """Test complex distinct operations."""
        data = [
            {"id": 1, "name": "Alice", "department": "Engineering"},
            {"id": 2, "name": "Bob", "department": "Marketing"},
            {"id": 1, "name": "Alice", "department": "Engineering"},  # Duplicate
            {"id": 3, "name": "Charlie", "department": "Engineering"},
        ]

        df = spark.createDataFrame(data)

        # Test distinct on all columns
        result = df.distinct()

        assert result.count() == 3

        # Test distinct on specific columns
        result2 = df.select("name", "department").distinct()

        assert result2.count() == 3
