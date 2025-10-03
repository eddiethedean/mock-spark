"""
Unit tests for basic Mock-Spark operations.

These tests verify core functionality without using real PySpark.
"""

import pytest
from mock_spark import MockSparkSession, F
from mock_spark.spark_types import (
    MockStructType,
    MockStructField,
    StringType,
    IntegerType,
    DoubleType,
    BooleanType,
)


@pytest.mark.fast
class TestBasicOperations:
    """Test basic Mock-Spark operations."""

    @pytest.fixture
    def spark(self):
        """Create a MockSparkSession for testing."""
        return MockSparkSession("test")

    @pytest.fixture
    def sample_data(self):
        """Sample data for testing."""
        return [
            {"name": "Alice", "age": 25, "salary": 50000.0, "active": True},
            {"name": "Bob", "age": 30, "salary": 60000.0, "active": False},
            {"name": "Charlie", "age": 35, "salary": 70000.0, "active": True},
            {"name": "Diana", "age": 28, "salary": 55000.0, "active": True},
        ]

    def test_create_dataframe(self, spark, sample_data):
        """Test DataFrame creation."""
        df = spark.createDataFrame(sample_data)

        assert df.count() == 4
        assert len(df.columns) == 4
        assert "name" in df.columns
        assert "age" in df.columns
        assert "salary" in df.columns
        assert "active" in df.columns

    def test_create_dataframe_with_schema(self, spark, sample_data):
        """Test DataFrame creation with explicit schema."""
        schema = MockStructType(
            [
                MockStructField("name", StringType()),
                MockStructField("age", IntegerType()),
                MockStructField("salary", DoubleType()),
                MockStructField("active", BooleanType()),
            ]
        )

        df = spark.createDataFrame(sample_data, schema)

        assert df.count() == 4
        assert df.schema == schema

    def test_select_columns(self, spark, sample_data):
        """Test column selection."""
        df = spark.createDataFrame(sample_data)

        # Select specific columns
        result = df.select("name", "age")
        assert result.count() == 4
        assert len(result.columns) == 2
        assert "name" in result.columns
        assert "age" in result.columns

    def test_filter_operations(self, spark, sample_data):
        """Test filtering operations."""
        df = spark.createDataFrame(sample_data)

        # Filter by age
        result = df.filter(F.col("age") > 30)
        assert result.count() == 1

        # Filter by boolean
        result = df.filter(F.col("active") == True)
        assert result.count() == 3

    def test_with_column(self, spark, sample_data):
        """Test adding new columns."""
        df = spark.createDataFrame(sample_data)

        # Add a new column
        result = df.withColumn("bonus", F.col("salary") * 0.1)
        assert result.count() == 4
        assert "bonus" in result.columns

    def test_group_by_operations(self, spark, sample_data):
        """Test groupBy operations."""
        df = spark.createDataFrame(sample_data)

        # Group by active status
        result = df.groupBy("active").count()
        assert result.count() == 2

        # Group by active status and calculate average salary
        result = df.groupBy("active").avg("salary")
        assert result.count() == 2
        assert "avg(salary)" in result.columns

    def test_order_by(self, spark, sample_data):
        """Test ordering operations."""
        df = spark.createDataFrame(sample_data)

        # Order by age ascending
        result = df.orderBy("age")
        rows = result.collect()
        assert rows[0]["age"] == 25
        assert rows[-1]["age"] == 35

        # Order by age descending
        result = df.orderBy(F.desc("age"))
        rows = result.collect()
        assert rows[0]["age"] == 35
        assert rows[-1]["age"] == 25

    def test_limit(self, spark, sample_data):
        """Test limit operation."""
        df = spark.createDataFrame(sample_data)

        result = df.limit(2)
        assert result.count() == 2

    def test_distinct(self, spark, sample_data):
        """Test distinct operation."""
        # Add duplicate data
        duplicate_data = sample_data + sample_data
        df = spark.createDataFrame(duplicate_data)

        result = df.distinct()
        assert result.count() == 4

    def test_drop_columns(self, spark, sample_data):
        """Test dropping columns."""
        df = spark.createDataFrame(sample_data)

        result = df.drop("salary")
        assert result.count() == 4
        assert "salary" not in result.columns
        assert "name" in result.columns

    def test_rename_columns(self, spark, sample_data):
        """Test renaming columns."""
        df = spark.createDataFrame(sample_data)

        result = df.withColumnRenamed("name", "full_name")
        assert result.count() == 4
        assert "full_name" in result.columns
        assert "name" not in result.columns

    def test_show(self, spark, sample_data):
        """Test show operation (should not raise exception)."""
        df = spark.createDataFrame(sample_data)

        # This should not raise an exception
        df.show()

    def test_collect(self, spark, sample_data):
        """Test collect operation."""
        df = spark.createDataFrame(sample_data)

        rows = df.collect()
        assert len(rows) == 4
        # MockRow objects should have asDict method
        assert all(hasattr(row, "asDict") for row in rows)
        # Test that we can convert to dict
        dict_rows = [row.asDict() for row in rows]
        assert all(isinstance(row, dict) for row in dict_rows)

    def test_to_pandas(self, spark, sample_data):
        """Test toPandas operation."""
        df = spark.createDataFrame(sample_data)

        pandas_df = df.toPandas()
        assert len(pandas_df) == 4
        assert list(pandas_df.columns) == df.columns

    def test_schema_properties(self, spark, sample_data):
        """Test schema-related properties."""
        df = spark.createDataFrame(sample_data)

        # Check that all expected columns are present (order may vary)
        expected_columns = {"name", "age", "salary", "active"}
        assert set(df.columns) == expected_columns

        # Check dtypes (order may vary)
        dtypes_dict = dict(df.dtypes)
        assert dtypes_dict["name"] == "string"
        assert dtypes_dict["age"] == "bigint"
        assert dtypes_dict["salary"] == "double"
        assert dtypes_dict["active"] == "boolean"

    def test_empty_dataframe(self, spark):
        """Test operations on empty DataFrame."""
        df = spark.createDataFrame([])

        assert df.count() == 0
        assert df.collect() == []

        # Operations on empty DataFrame should not raise exceptions
        result = df.filter(F.col("nonexistent") > 0)
        assert result.count() == 0
