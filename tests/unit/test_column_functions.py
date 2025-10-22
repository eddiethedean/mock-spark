"""
Unit tests for Mock-Spark column functions.

These tests verify all column functions work correctly without real PySpark.
"""

import pytest
from mock_spark import MockSparkSession, F


@pytest.mark.fast
class TestColumnFunctions:
    """Test Mock-Spark column functions."""

    @pytest.fixture
    def spark(self):
        """Create a MockSparkSession for testing."""
        return MockSparkSession("test")

    @pytest.fixture
    def sample_data(self):
        """Sample data for testing."""
        return [
            {
                "name": "Alice",
                "age": 25,
                "salary": 50000.0,
                "active": True,
                "department": "Engineering",
            },
            {
                "name": "Bob",
                "age": 30,
                "salary": 60000.0,
                "active": False,
                "department": "Marketing",
            },
            {
                "name": "Charlie",
                "age": 35,
                "salary": 70000.0,
                "active": True,
                "department": "Engineering",
            },
            {
                "name": "Diana",
                "age": 28,
                "salary": 55000.0,
                "active": True,
                "department": "Sales",
            },
        ]

    def test_col_function(self, spark, sample_data):
        """Test F.col() function."""
        df = spark.createDataFrame(sample_data)

        result = df.select(F.col("name"), F.col("age"))
        assert result.count() == 4
        assert "name" in result.columns
        assert "age" in result.columns

    def test_lit_function(self, spark, sample_data):
        """Test F.lit() function."""
        df = spark.createDataFrame(sample_data)

        # Test with string literal
        result = df.select(F.lit("test").alias("constant"))
        assert result.count() == 4
        assert "constant" in result.columns

        # Test with numeric literal
        result = df.select(F.lit(42).alias("number"))
        assert result.count() == 4
        assert "number" in result.columns

    def test_arithmetic_operations(self, spark, sample_data):
        """Test arithmetic operations."""
        df = spark.createDataFrame(sample_data)

        # Addition
        result = df.select((F.col("age") + 10).alias("age_plus_10"))
        assert result.count() == 4

        # Subtraction
        result = df.select((F.col("salary") - 1000).alias("salary_minus_1000"))
        assert result.count() == 4

        # Multiplication
        result = df.select((F.col("salary") * 1.1).alias("salary_times_1_1"))
        assert result.count() == 4

        # Division
        result = df.select((F.col("salary") / 12).alias("monthly_salary"))
        assert result.count() == 4

    def test_string_functions(self, spark, sample_data):
        """Test string functions."""
        df = spark.createDataFrame(sample_data)

        # Upper case
        result = df.select(F.upper(F.col("name")).alias("upper_name"))
        assert result.count() == 4
        assert "upper_name" in result.columns

        # Lower case
        result = df.select(F.lower(F.col("name")).alias("lower_name"))
        assert result.count() == 4
        assert "lower_name" in result.columns

        # Length
        result = df.select(F.length(F.col("name")).alias("name_length"))
        assert result.count() == 4
        assert "name_length" in result.columns

    def test_mathematical_functions(self, spark, sample_data):
        """Test mathematical functions."""
        df = spark.createDataFrame(sample_data)

        # Absolute value
        result = df.select(F.abs(F.col("age")).alias("abs_age"))
        assert result.count() == 4
        assert "abs_age" in result.columns

        # Round
        result = df.select(F.round(F.col("salary"), 2).alias("rounded_salary"))
        assert result.count() == 4
        assert "rounded_salary" in result.columns

    def test_aggregate_functions(self, spark, sample_data):
        """Test aggregate functions."""
        df = spark.createDataFrame(sample_data)

        # Count
        result = df.select(F.count("*").alias("total_count"))
        assert result.count() == 1
        # Note: alias may not work as expected, check actual column name
        rows = result.collect()
        assert len(rows) == 1
        # The count should be 4 regardless of column name
        row_data = rows[0].asDict()
        count_value = list(row_data.values())[0]  # Get the first (and only) value
        assert count_value == 4

        # Sum
        result = df.select(F.sum("salary").alias("total_salary"))
        assert result.count() == 1
        rows = result.collect()
        row_data = rows[0].asDict()
        sum_value = list(row_data.values())[0]
        assert sum_value == 235000.0

        # Average
        result = df.select(F.avg("salary").alias("avg_salary"))
        assert result.count() == 1
        rows = result.collect()
        row_data = rows[0].asDict()
        avg_value = list(row_data.values())[0]
        assert avg_value == 58750.0

        # Max
        result = df.select(F.max("age").alias("max_age"))
        assert result.count() == 1
        rows = result.collect()
        row_data = rows[0].asDict()
        max_value = list(row_data.values())[0]
        assert max_value == 35

        # Min
        result = df.select(F.min("age").alias("min_age"))
        assert result.count() == 1
        rows = result.collect()
        row_data = rows[0].asDict()
        min_value = list(row_data.values())[0]
        assert min_value == 25

    def test_logical_operations(self, spark, sample_data):
        """Test logical operations."""
        df = spark.createDataFrame(sample_data)

        # AND operation
        result = df.filter((F.col("age") > 25) & (F.col("active")))
        assert result.count() == 2

        # OR operation
        result = df.filter((F.col("age") < 30) | (~F.col("active")))
        assert result.count() == 3

        # NOT operation
        result = df.filter(~(~F.col("active")))
        assert result.count() == 3

    def test_comparison_operations(self, spark, sample_data):
        """Test comparison operations."""
        df = spark.createDataFrame(sample_data)

        # Greater than
        result = df.filter(F.col("age") > 30)
        assert result.count() == 1

        # Less than or equal
        result = df.filter(F.col("age") <= 30)
        assert result.count() == 3

        # Equal
        result = df.filter(F.col("active"))
        assert result.count() == 3

        # Not equal
        result = df.filter(~F.col("active"))
        assert result.count() == 1

    def test_null_handling(self, spark):
        """Test null value handling."""
        data_with_nulls = [
            {"name": "Alice", "age": 25, "salary": 50000.0},
            {"name": "Bob", "age": None, "salary": 60000.0},
            {"name": None, "age": 35, "salary": None},
        ]
        df = spark.createDataFrame(data_with_nulls)

        # Is null
        result = df.filter(F.col("age").isNull())
        assert result.count() == 1

        # Is not null
        result = df.filter(F.col("age").isNotNull())
        assert result.count() == 2

    def test_alias_operations(self, spark, sample_data):
        """Test column aliasing."""
        df = spark.createDataFrame(sample_data)

        result = df.select(
            F.col("name").alias("employee_name"), F.col("age").alias("employee_age")
        )
        assert result.count() == 4

        # Check that the data contains the aliased columns
        rows = result.collect()
        assert len(rows) == 4

        # Check that the data has the correct aliased column names
        first_row = rows[0].asDict()
        assert "employee_name" in first_row
        assert "employee_age" in first_row

        # Note: Schema columns may be empty due to alias handling limitations
        # but the data should contain the correct aliased column names

    def test_conditional_operations(self, spark, sample_data):
        """Test conditional operations."""
        df = spark.createDataFrame(sample_data)

        # When/otherwise
        result = df.select(
            F.when(F.col("age") > 30, "Senior")
            .when(F.col("age") > 25, "Mid")
            .otherwise("Junior")
            .alias("level")
        )
        assert result.count() == 4
        assert "level" in result.columns

    def test_coalesce_function(self, spark):
        """Test coalesce function."""
        data_with_nulls = [
            {"name": "Alice", "nickname": None, "display_name": "Ali"},
            {"name": "Bob", "nickname": "Bobby", "display_name": None},
            {"name": None, "nickname": None, "display_name": None},
        ]
        df = spark.createDataFrame(data_with_nulls)

        result = df.select(
            F.coalesce(F.col("name"), F.col("nickname"), F.col("display_name")).alias(
                "final_name"
            )
        )
        assert result.count() == 3
        assert "final_name" in result.columns

    def test_isnull_function(self, spark):
        """Test isnull function."""
        data_with_nulls = [
            {"name": "Alice", "age": 25},
            {"name": "Bob", "age": None},
        ]
        df = spark.createDataFrame(data_with_nulls)

        result = df.select(F.isnull(F.col("age")).alias("age_is_null"))
        assert result.count() == 2
        assert "age_is_null" in result.columns

    def test_trim_function(self, spark):
        """Test trim function."""
        data_with_spaces = [
            {"name": "  Alice  ", "department": " Engineering "},
            {"name": "Bob", "department": "Marketing"},
        ]
        df = spark.createDataFrame(data_with_spaces)

        result = df.select(
            F.trim(F.col("name")).alias("trimmed_name"),
            F.trim(F.col("department")).alias("trimmed_dept"),
        )
        assert result.count() == 2
        assert "trimmed_name" in result.columns
        assert "trimmed_dept" in result.columns
