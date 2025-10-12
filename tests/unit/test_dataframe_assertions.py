"""Unit tests for DataFrame assertions."""

import pytest
from mock_spark import MockSparkSession
from mock_spark.spark_types import MockStructType, MockStructField, StringType, IntegerType


@pytest.fixture
def spark():
    """Create test session."""
    session = MockSparkSession("test_assertions")
    yield session
    try:
        session.stop()
    except Exception:
        pass


def test_assert_has_columns_success(spark):
    """Test assert_has_columns succeeds when columns exist."""
    data = [{"id": 1, "name": "Alice", "age": 25}]
    df = spark.createDataFrame(data)
    
    # Should not raise
    df.assert_has_columns(["id", "name"])


def test_assert_has_columns_failure(spark):
    """Test assert_has_columns fails when columns missing."""
    data = [{"id": 1, "name": "Alice"}]
    df = spark.createDataFrame(data)
    
    with pytest.raises(AssertionError) as exc_info:
        df.assert_has_columns(["id", "age"])
    
    assert "Missing columns" in str(exc_info.value)


def test_assert_row_count_success(spark):
    """Test assert_row_count succeeds with correct count."""
    data = [{"id": 1}, {"id": 2}, {"id": 3}]
    df = spark.createDataFrame(data)
    
    df.assert_row_count(3)


def test_assert_row_count_failure(spark):
    """Test assert_row_count fails with incorrect count."""
    data = [{"id": 1}, {"id": 2}]
    df = spark.createDataFrame(data)
    
    with pytest.raises(AssertionError) as exc_info:
        df.assert_row_count(5)
    
    assert "Expected 5 rows, got 2" in str(exc_info.value)


def test_assert_schema_matches_success(spark):
    """Test assert_schema_matches succeeds with matching schema."""
    schema = MockStructType([
        MockStructField("id", IntegerType()),
        MockStructField("name", StringType())
    ])
    df = spark.createDataFrame([], schema)
    
    expected_schema = MockStructType([
        MockStructField("id", IntegerType()),
        MockStructField("name", StringType())
    ])
    
    df.assert_schema_matches(expected_schema)


def test_assert_schema_matches_failure_count(spark):
    """Test assert_schema_matches fails when field count differs."""
    schema = MockStructType([
        MockStructField("id", IntegerType())
    ])
    df = spark.createDataFrame([], schema)
    
    expected_schema = MockStructType([
        MockStructField("id", IntegerType()),
        MockStructField("name", StringType())
    ])
    
    with pytest.raises(AssertionError) as exc_info:
        df.assert_schema_matches(expected_schema)
    
    assert "field count mismatch" in str(exc_info.value)


def test_assert_schema_matches_failure_type(spark):
    """Test assert_schema_matches fails when types differ."""
    schema = MockStructType([
        MockStructField("id", StringType())
    ])
    df = spark.createDataFrame([], schema)
    
    expected_schema = MockStructType([
        MockStructField("id", IntegerType())
    ])
    
    with pytest.raises(AssertionError) as exc_info:
        df.assert_schema_matches(expected_schema)
    
    assert "Schema mismatch" in str(exc_info.value)


def test_assert_data_equals_success(spark):
    """Test assert_data_equals succeeds with matching data."""
    data = [{"id": 1, "name": "Alice"}]
    df = spark.createDataFrame(data)
    
    df.assert_data_equals(data)


def test_assert_data_equals_failure(spark):
    """Test assert_data_equals fails with different data."""
    data = [{"id": 1, "name": "Alice"}]
    df = spark.createDataFrame(data)
    
    expected = [{"id": 2, "name": "Bob"}]
    
    with pytest.raises(AssertionError) as exc_info:
        df.assert_data_equals(expected)
    
    assert "Data mismatch" in str(exc_info.value)

