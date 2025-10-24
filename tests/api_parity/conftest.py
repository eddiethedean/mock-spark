"""
Configuration and fixtures for API parity tests.

This module provides fixtures for side-by-side testing of MockSpark vs PySpark
to ensure API compatibility and identical behavior.
"""

import pytest
import os
import sys

# Add mock_spark to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

try:
    from mock_spark import MockSparkSession, F as MockF  # noqa: F401

    MOCK_SPARK_AVAILABLE = True
except ImportError:
    MOCK_SPARK_AVAILABLE = False

try:
    from pyspark.sql import SparkSession, functions as PySparkF  # noqa: F401
    from pyspark.sql.types import (  # noqa: F401
        StructType,
        StructField,
        StringType,
        IntegerType,
        DoubleType,
    )

    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False


@pytest.fixture(scope="session")
def mock_spark():
    """Create MockSparkSession for testing."""
    if not MOCK_SPARK_AVAILABLE:
        pytest.skip("MockSpark not available")
    return MockSparkSession("parity_test")


@pytest.fixture(scope="session")
def pyspark_spark():
    """Create PySpark SparkSession for testing."""
    if not PYSPARK_AVAILABLE:
        pytest.skip("PySpark not available")

    try:
        # Try to create a session to check if PySpark can actually run
        session = SparkSession.builder.appName("parity_test").getOrCreate()
        # Test that the session actually works
        session.createDataFrame([{"test": 1}]).collect()
        return session
    except Exception as e:
        pytest.skip(f"PySpark session creation failed: {e}")


@pytest.fixture
def sample_data():
    """Sample data for testing basic operations."""
    return [
        {"id": 1, "name": "Alice", "age": 25, "salary": 50000.0, "department": "IT"},
        {"id": 2, "name": "Bob", "age": 30, "salary": 60000.0, "department": "HR"},
        {"id": 3, "name": "Charlie", "age": 35, "salary": 70000.0, "department": "IT"},
        {
            "id": 4,
            "name": "David",
            "age": 40,
            "salary": 80000.0,
            "department": "Finance",
        },
        {"id": 5, "name": "Eve", "age": 45, "salary": 90000.0, "department": "IT"},
    ]


@pytest.fixture
def datetime_data():
    """Sample data with datetime fields for testing."""
    return [
        {
            "id": 1,
            "name": "Alice",
            "timestamp": "2024-01-15 10:30:00",
            "date": "2024-01-15",
            "hour": 10,
            "dayofweek": 2,  # Monday
        },
        {
            "id": 2,
            "name": "Bob",
            "timestamp": "2024-01-16 14:45:00",
            "date": "2024-01-16",
            "hour": 14,
            "dayofweek": 3,  # Tuesday
        },
        {
            "id": 3,
            "name": "Charlie",
            "timestamp": "2024-01-17 09:15:00",
            "date": "2024-01-17",
            "hour": 9,
            "dayofweek": 4,  # Wednesday
        },
    ]


@pytest.fixture
def string_data():
    """Sample data with string fields for testing."""
    return [
        {"id": 1, "text": "Hello World", "email": "alice@example.com"},
        {"id": 2, "text": "Mock Spark", "email": "bob@test.com"},
        {"id": 3, "text": "API Parity", "email": "charlie@company.org"},
        {"id": 4, "text": "Testing Framework", "email": "david@corp.net"},
    ]


@pytest.fixture
def numeric_data():
    """Sample data with numeric fields for testing."""
    return [
        {"id": 1, "value": 10.5, "count": 100, "flag": True},
        {"id": 2, "value": 20.7, "count": 200, "flag": False},
        {"id": 3, "value": 30.9, "count": 300, "flag": True},
        {"id": 4, "value": 40.1, "count": 400, "flag": False},
    ]


@pytest.fixture
def complex_data():
    """Sample data with complex structures for testing."""
    return [
        {
            "id": 1,
            "name": "Alice",
            "scores": [85, 90, 78],
            "metadata": {"department": "IT", "level": "senior"},
        },
        {
            "id": 2,
            "name": "Bob",
            "scores": [92, 88, 95],
            "metadata": {"department": "HR", "level": "junior"},
        },
        {
            "id": 3,
            "name": "Charlie",
            "scores": [76, 82, 88],
            "metadata": {"department": "Finance", "level": "mid"},
        },
    ]


def compare_dataframes(mock_df, pyspark_df, tolerance: float = 1e-6):
    """Compare two DataFrames and assert they are equivalent.

    Args:
        mock_df: MockSpark DataFrame
        pyspark_df: PySpark DataFrame
        tolerance: Tolerance for floating point comparisons
    """
    # Compare row counts
    mock_count = mock_df.count()
    pyspark_count = pyspark_df.count()
    assert mock_count == pyspark_count, (
        f"Row count mismatch: {mock_count} vs {pyspark_count}"
    )

    # Compare schemas
    mock_columns = mock_df.columns
    pyspark_columns = pyspark_df.columns
    assert set(mock_columns) == set(pyspark_columns), (
        f"Column mismatch: {mock_columns} vs {pyspark_columns}"
    )

    # Compare data
    mock_data = mock_df.collect()
    pyspark_data = pyspark_df.collect()

    assert len(mock_data) == len(pyspark_data), "Data length mismatch"

    for i, (mock_row, pyspark_row) in enumerate(zip(mock_data, pyspark_data)):
        for col in mock_columns:
            mock_val = mock_row[col]
            pyspark_val = pyspark_row[col]

            if isinstance(mock_val, float) and isinstance(pyspark_val, float):
                assert abs(mock_val - pyspark_val) < tolerance, (
                    f"Row {i}, column {col}: {mock_val} vs {pyspark_val}"
                )
            else:
                assert mock_val == pyspark_val, (
                    f"Row {i}, column {col}: {mock_val} vs {pyspark_val}"
                )


def compare_aggregations(mock_result, pyspark_result, tolerance: float = 1e-6):
    """Compare aggregation results between MockSpark and PySpark.

    Args:
        mock_result: MockSpark aggregation result
        pyspark_result: PySpark aggregation result
        tolerance: Tolerance for floating point comparisons
    """
    # Compare row counts
    mock_count = mock_result.count()
    pyspark_count = pyspark_result.count()
    assert mock_count == pyspark_count, (
        f"Aggregation count mismatch: {mock_count} vs {pyspark_count}"
    )

    # Compare data - sort by all columns to ensure consistent ordering
    mock_data = sorted(
        mock_result.collect(),
        key=lambda row: tuple(str(row[col]) for col in mock_result.columns),
    )
    pyspark_data = sorted(
        pyspark_result.collect(),
        key=lambda row: tuple(str(row[col]) for col in pyspark_result.columns),
    )

    for mock_row, pyspark_row in zip(mock_data, pyspark_data):
        for col in mock_result.columns:
            mock_val = mock_row[col]
            pyspark_val = pyspark_row[col]

            if isinstance(mock_val, float) and isinstance(pyspark_val, float):
                assert abs(mock_val - pyspark_val) < tolerance, (
                    f"Column {col}: {mock_val} vs {pyspark_val}"
                )
            else:
                assert mock_val == pyspark_val, (
                    f"Column {col}: {mock_val} vs {pyspark_val}"
                )


def skip_if_pyspark_unavailable():
    """Skip test if PySpark is not available."""
    if not PYSPARK_AVAILABLE:
        pytest.skip("PySpark not available for parity testing")


def skip_if_mock_spark_unavailable():
    """Skip test if MockSpark is not available."""
    if not MOCK_SPARK_AVAILABLE:
        pytest.skip("MockSpark not available for parity testing")


class ParityTestBase:
    """Base class for API parity tests."""

    def setup_method(self):
        """Set up test method."""
        skip_if_pyspark_unavailable()
        skip_if_mock_spark_unavailable()

    def compare_dataframes(self, mock_df, pyspark_df, tolerance: float = 1e-6):
        """Compare DataFrames with tolerance."""
        compare_dataframes(mock_df, pyspark_df, tolerance)

    def compare_aggregations(
        self, mock_result, pyspark_result, tolerance: float = 1e-6
    ):
        """Compare aggregation results with tolerance."""
        compare_aggregations(mock_result, pyspark_result, tolerance)
