"""
Test fixtures for Mock Spark.

This module provides reusable test fixtures for creating consistent test data
and mock objects across the test suite.
"""

import pytest
from typing import List, Dict, Any, Optional, Union
from mock_spark import MockSparkSession
from mock_spark.dataframe import MockDataFrame
from mock_spark.spark_types import MockStructType, MockStructField, StringType, IntegerType, LongType, DoubleType, BooleanType


class MockSparkSessionFixture:
    """Fixture for creating MockSparkSession instances for testing."""
    
    @staticmethod
    def create_session(app_name: str = "test_app") -> MockSparkSession:
        """Create a MockSparkSession for testing."""
        return MockSparkSession.builder.appName(app_name).getOrCreate()
    
    @staticmethod
    def create_session_with_config(config: Dict[str, str]) -> MockSparkSession:
        """Create a MockSparkSession with custom configuration."""
        builder = MockSparkSession.builder.appName("test_app")
        for key, value in config.items():
            builder = builder.config(key, value)
        return builder.getOrCreate()


class DataFrameFixture:
    """Fixture for creating test DataFrames with various schemas and data."""
    
    @staticmethod
    def create_simple_dataframe(session: MockSparkSession) -> MockDataFrame:
        """Create a simple DataFrame with basic data types."""
        data = [
            {"id": 1, "name": "Alice", "age": 25, "salary": 50000.0, "active": True},
            {"id": 2, "name": "Bob", "age": 30, "salary": 60000.0, "active": False},
            {"id": 3, "name": "Charlie", "age": 35, "salary": 70000.0, "active": True},
        ]
        schema = MockStructType([
            MockStructField("id", IntegerType(), True),
            MockStructField("name", StringType(), True),
            MockStructField("age", IntegerType(), True),
            MockStructField("salary", DoubleType(), True),
            MockStructField("active", BooleanType(), True),
        ])
        return session.createDataFrame(data, schema)
    
    @staticmethod
    def create_empty_dataframe(session: MockSparkSession) -> MockDataFrame:
        """Create an empty DataFrame with a simple schema."""
        schema = MockStructType([
            MockStructField("id", IntegerType(), True),
            MockStructField("value", StringType(), True),
        ])
        return session.createDataFrame([], schema)
    
    @staticmethod
    def create_large_dataframe(session: MockSparkSession, num_rows: int = 1000) -> MockDataFrame:
        """Create a large DataFrame for performance testing."""
        data = []
        for i in range(num_rows):
            data.append({
                "id": i,
                "name": f"User_{i}",
                "age": 20 + (i % 50),
                "salary": 30000.0 + (i * 100),
                "department": f"Dept_{i % 10}",
                "active": i % 2 == 0,
            })
        
        schema = MockStructType([
            MockStructField("id", IntegerType(), True),
            MockStructField("name", StringType(), True),
            MockStructField("age", IntegerType(), True),
            MockStructField("salary", DoubleType(), True),
            MockStructField("department", StringType(), True),
            MockStructField("active", BooleanType(), True),
        ])
        return session.createDataFrame(data, schema)
    
    @staticmethod
    def create_dataframe_with_nulls(session: MockSparkSession) -> MockDataFrame:
        """Create a DataFrame with null values for testing null handling."""
        data = [
            {"id": 1, "name": "Alice", "age": 25, "salary": 50000.0},
            {"id": 2, "name": None, "age": 30, "salary": None},
            {"id": 3, "name": "Charlie", "age": None, "salary": 70000.0},
            {"id": None, "name": "David", "age": 40, "salary": 80000.0},
        ]
        schema = MockStructType([
            MockStructField("id", IntegerType(), True),
            MockStructField("name", StringType(), True),
            MockStructField("age", IntegerType(), True),
            MockStructField("salary", DoubleType(), True),
        ])
        return session.createDataFrame(data, schema)


class SchemaFixture:
    """Fixture for creating test schemas."""
    
    @staticmethod
    def create_simple_schema() -> MockStructType:
        """Create a simple schema with basic data types."""
        return MockStructType([
            MockStructField("id", IntegerType(), True),
            MockStructField("name", StringType(), True),
            MockStructField("age", IntegerType(), True),
        ])
    
    @staticmethod
    def create_complex_schema() -> MockStructType:
        """Create a complex schema with various data types."""
        return MockStructType([
            MockStructField("id", LongType(), True),
            MockStructField("name", StringType(), False),
            MockStructField("age", IntegerType(), True),
            MockStructField("salary", DoubleType(), True),
            MockStructField("active", BooleanType(), True),
            MockStructField("created_at", StringType(), True),
        ])
    
    @staticmethod
    def create_nested_schema() -> MockStructType:
        """Create a schema with nested structures (for future array/map support)."""
        return MockStructType([
            MockStructField("id", IntegerType(), True),
            MockStructField("user_info", MockStructType([
                MockStructField("name", StringType(), True),
                MockStructField("email", StringType(), True),
            ]), True),
            MockStructField("metadata", MockStructType([
                MockStructField("created_at", StringType(), True),
                MockStructField("updated_at", StringType(), True),
            ]), True),
        ])


# Pytest fixtures for easy use in tests
@pytest.fixture
def mock_spark_session():
    """Pytest fixture for MockSparkSession."""
    session = MockSparkSessionFixture.create_session()
    yield session
    session.stop()


@pytest.fixture
def simple_dataframe(mock_spark_session):
    """Pytest fixture for a simple DataFrame."""
    return DataFrameFixture.create_simple_dataframe(mock_spark_session)


@pytest.fixture
def empty_dataframe(mock_spark_session):
    """Pytest fixture for an empty DataFrame."""
    return DataFrameFixture.create_empty_dataframe(mock_spark_session)


@pytest.fixture
def large_dataframe(mock_spark_session):
    """Pytest fixture for a large DataFrame."""
    return DataFrameFixture.create_large_dataframe(mock_spark_session)


@pytest.fixture
def dataframe_with_nulls(mock_spark_session):
    """Pytest fixture for a DataFrame with null values."""
    return DataFrameFixture.create_dataframe_with_nulls(mock_spark_session)
