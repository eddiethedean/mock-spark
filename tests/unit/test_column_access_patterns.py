"""
Unit tests for DataFrame column access patterns.

Tests both df.column_name and F.col("column_name") access patterns
to ensure PySpark compatibility.
"""

import pytest
from mock_spark import MockSparkSession, F


@pytest.mark.fast
class TestColumnAccessPatterns:
    """Test DataFrame column access patterns for PySpark compatibility."""

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
                "timestamp": "2024-01-15 10:30:00",
            },
            {
                "name": "Bob",
                "age": 30,
                "salary": 60000.0,
                "active": False,
                "department": "Marketing",
                "timestamp": "2024-01-16 14:45:00",
            },
            {
                "name": "Charlie",
                "age": 35,
                "salary": 70000.0,
                "active": True,
                "department": "Engineering",
                "timestamp": "2024-01-17 09:15:00",
            },
        ]

    def test_df_column_access_basic(self, spark, sample_data):
        """Test basic df.column_name access."""
        df = spark.createDataFrame(sample_data)

        # Test accessing columns via df.column_name
        name_col = df.name
        age_col = df.age
        salary_col = df.salary

        # Verify they are MockColumn objects
        assert hasattr(name_col, 'name')
        assert hasattr(age_col, 'name')
        assert hasattr(salary_col, 'name')

        # Verify column names
        assert name_col.name == "name"
        assert age_col.name == "age"
        assert salary_col.name == "salary"

    def test_df_column_access_operations(self, spark, sample_data):
        """Test df.column_name with operations."""
        df = spark.createDataFrame(sample_data)

        # Test operations on df.column_name
        result = df.select(
            df.name.alias("employee_name"),
            df.age.cast("string").alias("age_str"),
            df.salary.cast("long").alias("salary_long")
        )

        assert result.count() == 3
        assert "employee_name" in result.columns
        assert "age_str" in result.columns
        assert "salary_long" in result.columns

    def test_df_column_access_filters(self, spark, sample_data):
        """Test df.column_name in filter conditions."""
        df = spark.createDataFrame(sample_data)

        # Test filter with df.column_name
        result = df.filter(df.age > 30)
        assert result.count() == 1
        assert result.collect()[0]["name"] == "Charlie"

        # Test multiple conditions
        result = df.filter((df.age > 25) & (df.active == True))
        assert result.count() == 1
        assert result.collect()[0]["name"] == "Charlie"

    def test_df_column_access_boolean_operations(self, spark, sample_data):
        """Test boolean operations with df.column_name."""
        df = spark.createDataFrame(sample_data)

        # Test isNotNull
        result = df.filter(df.name.isNotNull())
        assert result.count() == 3

        # Test isNull
        data_with_nulls = [
            {"name": "Alice", "age": 25},
            {"name": None, "age": 30},
            {"name": "Charlie", "age": 35},
        ]
        df_with_nulls = spark.createDataFrame(data_with_nulls)
        result = df_with_nulls.filter(df_with_nulls.name.isNull())
        assert result.count() == 1

    def test_df_column_access_comparison_operations(self, spark, sample_data):
        """Test comparison operations with df.column_name."""
        df = spark.createDataFrame(sample_data)

        # Test various comparison operations
        result = df.filter(df.salary >= 60000)
        assert result.count() == 2

        result = df.filter(df.salary < 60000)
        assert result.count() == 1

        result = df.filter(df.department == "Engineering")
        assert result.count() == 2

    def test_df_column_access_derived_columns(self, spark, sample_data):
        """Test df.column_name with derived columns after withColumn."""
        df = spark.createDataFrame(sample_data)

        # Add derived column
        df_with_derived = df.withColumn("age_days", df.age * 365)

        # Test accessing derived column
        result = df_with_derived.select(df_with_derived.age_days)
        assert result.count() == 3

        # Test operations on derived column
        result = df_with_derived.filter(df_with_derived.age_days > 10000)
        assert result.count() == 2  # Bob (30*365=10950) and Charlie (35*365=12775) > 10000

    def test_df_column_access_chained_operations(self, spark, sample_data):
        """Test chained operations with df.column_name."""
        df = spark.createDataFrame(sample_data)

        # Test chained operations
        result = df.select(
            df.name,
            df.age.cast("string").alias("age_str"),
            (df.salary / 1000).cast("int").alias("salary_k")
        )

        assert result.count() == 3
        assert "name" in result.columns
        assert "age_str" in result.columns
        assert "salary_k" in result.columns

    def test_df_column_access_with_functions(self, spark, sample_data):
        """Test df.column_name with F functions."""
        df = spark.createDataFrame(sample_data)

        # Test mixing df.column_name with F functions
        result = df.select(
            df.name,
            F.upper(df.department).alias("dept_upper"),
            F.when(df.age > 30, "Senior").otherwise("Junior").alias("level")
        )

        assert result.count() == 3
        assert "name" in result.columns
        assert "dept_upper" in result.columns
        assert "level" in result.columns

    def test_df_column_access_error_handling(self, spark, sample_data):
        """Test error handling for non-existent columns."""
        df = spark.createDataFrame(sample_data)

        # Test accessing non-existent column
        with pytest.raises(AttributeError) as exc_info:
            _ = df.non_existent_column
        assert "no attribute 'non_existent_column'" in str(exc_info.value)
        assert "Available columns:" in str(exc_info.value)

    def test_df_column_access_vs_f_col_equivalence(self, spark, sample_data):
        """Test that df.column_name and F.col() produce equivalent results."""
        df = spark.createDataFrame(sample_data)

        # Test that both access patterns work the same way
        result1 = df.select(df.name, df.age)
        result2 = df.select(F.col("name"), F.col("age"))

        # Both should have same structure
        assert result1.count() == result2.count()
        assert result1.columns == result2.columns

        # Test filter equivalence
        result1 = df.filter(df.age > 30)
        result2 = df.filter(F.col("age") > 30)
        assert result1.count() == result2.count()

    def test_df_column_access_with_timestamp(self, spark, sample_data):
        """Test df.column_name with timestamp columns."""
        df = spark.createDataFrame(sample_data)

        # Test timestamp operations
        result = df.select(
            df.timestamp,
            F.hour(df.timestamp).alias("hour"),
            F.day(df.timestamp).alias("day")
        )

        assert result.count() == 3
        assert "timestamp" in result.columns
        assert "hour" in result.columns
        assert "day" in result.columns

    def test_df_column_access_complex_expressions(self, spark, sample_data):
        """Test complex expressions with df.column_name."""
        df = spark.createDataFrame(sample_data)

        # Test complex expressions
        result = df.select(
            df.name,
            (df.salary * 1.1).cast("int").alias("salary_with_bonus"),
            F.when(df.age > 30, df.salary * 1.2).otherwise(df.salary).alias("adjusted_salary")
        )

        assert result.count() == 3
        assert "name" in result.columns
        assert "salary_with_bonus" in result.columns
        assert "adjusted_salary" in result.columns

    def test_df_column_access_with_alias(self, spark, sample_data):
        """Test df.column_name with aliasing."""
        df = spark.createDataFrame(sample_data)

        # Test aliasing
        result = df.select(
            df.name.alias("employee_name"),
            df.age.alias("employee_age"),
            df.salary.alias("employee_salary")
        )

        assert result.count() == 3
        assert "employee_name" in result.columns
        assert "employee_age" in result.columns
        assert "employee_salary" in result.columns

    def test_df_column_access_nested_operations(self, spark, sample_data):
        """Test nested operations with df.column_name."""
        df = spark.createDataFrame(sample_data)

        # Test nested operations
        result = df.select(
            df.name,
            F.when(df.age > 30, F.upper(df.department)).otherwise(df.department).alias("dept_upper_if_senior")
        )

        assert result.count() == 3
        assert "name" in result.columns
        assert "dept_upper_if_senior" in result.columns

    def test_df_column_access_with_aggregation(self, spark, sample_data):
        """Test df.column_name with aggregation functions."""
        df = spark.createDataFrame(sample_data)

        # Test aggregation with df.column_name
        result = df.groupBy(df.department).agg(
            F.count(df.name).alias("count"),
            F.avg(df.salary).alias("avg_salary"),
            F.max(df.age).alias("max_age")
        )

        assert result.count() == 2  # Two departments
        assert "department" in result.columns
        assert "count" in result.columns
        assert "avg_salary" in result.columns
        assert "max_age" in result.columns
