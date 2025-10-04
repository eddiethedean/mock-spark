"""
Unit tests for Mock Spark testing utilities.

These tests cover the testing utilities and factories that help create
test data and mock objects for Mock Spark testing.
"""

import pytest
from mock_spark import MockSparkSession
from mock_spark.testing.mocks import (
    MockDataFrameFactory,
    MockSessionFactory,
    MockFunctionFactory,
    MockStorageFactory,
    MockSchemaFactory,
)
from mock_spark.testing.generators import DataGenerator
from mock_spark.testing.factories import DataFrameTestFactory
from mock_spark.testing.simulators import ErrorSimulator, PerformanceSimulator
from mock_spark.testing.fixtures import MockSparkSessionFixture, DataFrameFixture


class TestMockDataFrameFactory:
    """Test MockDataFrameFactory functionality."""

    def test_create_simple_dataframe(self):
        """Test creating a simple DataFrame."""
        session = MockSparkSession("test")
        df = MockDataFrameFactory.create_simple_dataframe(session)

        assert df.count() == 3
        assert len(df.columns) == 3
        assert "id" in df.columns
        assert "name" in df.columns
        assert "age" in df.columns

    def test_create_empty_dataframe(self):
        """Test creating an empty DataFrame."""
        session = MockSparkSession("test")
        df = MockDataFrameFactory.create_empty_dataframe(session)

        assert df.count() == 0
        assert len(df.columns) == 2
        assert "id" in df.columns
        assert "value" in df.columns

    def test_create_dataframe_with_nulls(self):
        """Test creating a DataFrame with null values."""
        session = MockSparkSession("test")
        df = MockDataFrameFactory.create_dataframe_with_nulls(session)

        assert df.count() == 3
        # Check that nulls are present by examining the data directly
        data = df.collect()
        has_null_name = any(row.get("name") is None for row in data)
        has_null_age = any(row.get("age") is None for row in data)
        assert has_null_name or has_null_age

    def test_create_large_dataframe(self):
        """Test creating a large DataFrame."""
        session = MockSparkSession("test")
        df = MockDataFrameFactory.create_large_dataframe(session, 100)

        assert df.count() == 100
        assert len(df.columns) >= 3

    def test_create_dataframe_with_schema(self):
        """Test creating a DataFrame with custom schema."""
        from mock_spark.spark_types import MockStructType, MockStructField, StringType, IntegerType

        session = MockSparkSession("test")
        schema = MockStructType(
            [
                MockStructField("custom_id", IntegerType(), True),
                MockStructField("custom_name", StringType(), True),
            ]
        )
        df = MockDataFrameFactory.create_dataframe_with_schema(session, schema)

        assert df.count() == 0  # No data provided
        assert "custom_id" in df.columns
        assert "custom_name" in df.columns


class TestMockSessionFactory:
    """Test MockSessionFactory functionality."""

    def test_create_default_session(self):
        """Test creating a default session."""
        session = MockSessionFactory.create_default_session()

        assert session is not None
        assert hasattr(session, "createDataFrame")
        assert hasattr(session, "sql")

    def test_create_session_with_config(self):
        """Test creating a session with custom config."""
        config = {"spark.app.name": "test_app", "spark.master": "local"}
        session = MockSessionFactory.create_session_with_config(config)

        assert session is not None
        assert hasattr(session, "createDataFrame")


class TestMockFunctionFactory:
    """Test MockFunctionFactory functionality."""

    def test_create_column(self):
        """Test creating a column."""
        column = MockFunctionFactory.create_column("test_col")

        assert column is not None
        assert hasattr(column, "name")
        assert column.name == "test_col"

    def test_create_literal(self):
        """Test creating a literal."""
        literal = MockFunctionFactory.create_literal("test_value")

        assert literal is not None
        assert hasattr(literal, "value")
        assert literal.value == "test_value"


class TestMockStorageFactory:
    """Test MockStorageFactory functionality."""

    def test_create_memory_storage(self):
        """Test creating memory storage."""
        storage = MockStorageFactory.create_memory_storage()

        assert storage is not None
        # Just verify it's the right type
        assert hasattr(storage, "insert_data")


class TestMockSchemaFactory:
    """Test MockSchemaFactory functionality."""

    def test_create_simple_schema(self):
        """Test creating a simple schema."""
        schema = MockSchemaFactory.create_simple_schema()

        assert schema is not None
        assert hasattr(schema, "fields")
        assert len(schema.fields) > 0

    def test_create_complex_schema(self):
        """Test creating a complex schema."""
        schema = MockSchemaFactory.create_complex_schema()

        assert schema is not None
        assert hasattr(schema, "fields")
        assert len(schema.fields) > 0


class TestDataGenerator:
    """Test DataGenerator base functionality."""

    def test_generate_string(self):
        """Test string generation."""
        result = DataGenerator.generate_string(10)
        assert isinstance(result, str)
        assert len(result) == 10

    def test_generate_integer(self):
        """Test integer generation."""
        result = DataGenerator.generate_integer(0, 100)
        assert isinstance(result, int)
        assert 0 <= result <= 100

    def test_generate_long(self):
        """Test long generation."""
        result = DataGenerator.generate_long(0, 1000)
        assert isinstance(result, int)
        assert 0 <= result <= 1000

    def test_generate_double(self):
        """Test double generation."""
        result = DataGenerator.generate_double(0.0, 100.0)
        assert isinstance(result, float)
        assert 0.0 <= result <= 100.0

    def test_generate_boolean(self):
        """Test boolean generation."""
        result = DataGenerator.generate_boolean()
        assert isinstance(result, bool)


class TestDataFrameTestFactory:
    """Test DataFrameTestFactory functionality."""

    def test_create_performance_test_dataframe(self):
        """Test creating a performance test DataFrame."""
        session = MockSparkSession("test")
        df = DataFrameTestFactory.create_performance_test_dataframe(session, 50)

        assert df is not None
        assert hasattr(df, "count")
        assert df.count() == 50

    def test_create_stress_test_dataframe(self):
        """Test creating a stress test DataFrame."""
        session = MockSparkSession("test")
        df = DataFrameTestFactory.create_stress_test_dataframe(session)

        assert df is not None
        assert hasattr(df, "count")
        assert df.count() > 0


class TestErrorSimulator:
    """Test ErrorSimulator functionality."""

    def test_create_error_simulator(self):
        """Test creating an error simulator."""
        simulator = ErrorSimulator()

        assert simulator is not None
        assert hasattr(simulator, "add_error_rule")
        assert hasattr(simulator, "should_raise_error")


class TestPerformanceSimulator:
    """Test PerformanceSimulator functionality."""

    def test_create_performance_simulator(self):
        """Test creating a performance simulator."""
        simulator = PerformanceSimulator()

        assert simulator is not None
        assert hasattr(simulator, "add_slowdown")
        assert hasattr(simulator, "get_performance_stats")


class TestMockSparkSessionFixture:
    """Test MockSparkSessionFixture functionality."""

    def test_create_session(self):
        """Test creating a session."""
        fixture = MockSparkSessionFixture()
        session = fixture.create_session()

        assert session is not None
        assert hasattr(session, "createDataFrame")
        assert hasattr(session, "sql")

    def test_create_session_with_config(self):
        """Test creating a session with config."""
        fixture = MockSparkSessionFixture()
        config = {"spark.app.name": "test"}
        session = fixture.create_session_with_config(config)

        assert session is not None
        assert hasattr(session, "createDataFrame")


class TestDataFrameFixture:
    """Test DataFrameFixture functionality."""

    def test_create_simple_dataframe(self):
        """Test creating a simple DataFrame."""
        session = MockSparkSession("test")
        fixture = DataFrameFixture()
        df = fixture.create_simple_dataframe(session)

        assert df is not None
        assert hasattr(df, "count")
        assert df.count() > 0

    def test_create_empty_dataframe(self):
        """Test creating an empty DataFrame."""
        session = MockSparkSession("test")
        fixture = DataFrameFixture()
        df = fixture.create_empty_dataframe(session)

        assert df is not None
        assert hasattr(df, "count")
        assert df.count() == 0
