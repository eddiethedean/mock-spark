"""Unit tests for GroupedData pivot operations."""

import pytest
from mock_spark import MockSparkSession, F

# Skip all pivot tests - pivot functionality not fully implemented
pytestmark = pytest.mark.skip(reason="Pivot functionality not fully implemented")


@pytest.fixture
def spark():
    """Create test session."""
    session = MockSparkSession("test_pivot")
    yield session
    try:
        session.stop()
    except Exception:
        pass


def test_pivot_basic(spark):
    """Test basic pivot operation."""
    data = [
        {"category": "A", "year": 2020, "value": 10},
        {"category": "A", "year": 2021, "value": 20},
        {"category": "B", "year": 2020, "value": 30}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("category").pivot("year").sum("value")
    assert result.count() == 2


def test_pivot_with_values(spark):
    """Test pivot with specified values."""
    data = [
        {"category": "A", "type": "X", "value": 10},
        {"category": "A", "type": "Y", "value": 20},
        {"category": "B", "type": "X", "value": 30}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("category").pivot("type", ["X", "Y"]).sum("value")
    assert result.count() == 2


def test_pivot_multiple_aggregations(spark):
    """Test pivot with multiple aggregation functions."""
    data = [
        {"category": "A", "type": "X", "value": 10},
        {"category": "A", "type": "X", "value": 15},
        {"category": "A", "type": "Y", "value": 20}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("category").pivot("type").agg(
        F.sum("value").alias("total"),
        F.avg("value").alias("average")
    )
    assert result.count() == 1


def test_pivot_with_null_values(spark):
    """Test pivot handling null values."""
    data = [
        {"category": "A", "type": "X", "value": 10},
        {"category": "A", "type": None, "value": 20}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("category").pivot("type").sum("value")
    assert result.count() == 1


def test_pivot_count(spark):
    """Test pivot with count aggregation."""
    data = [
        {"category": "A", "type": "X"},
        {"category": "A", "type": "X"},
        {"category": "A", "type": "Y"}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("category").pivot("type").count()
    assert result.count() == 1


def test_pivot_avg(spark):
    """Test pivot with average aggregation."""
    data = [
        {"category": "A", "type": "X", "value": 10},
        {"category": "A", "type": "X", "value": 20},
        {"category": "A", "type": "Y", "value": 30}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("category").pivot("type").avg("value")
    rows = result.collect()
    assert len(rows) == 1


def test_pivot_max(spark):
    """Test pivot with max aggregation."""
    data = [
        {"category": "A", "type": "X", "value": 10},
        {"category": "A", "type": "X", "value": 20}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("category").pivot("type").max("value")
    assert result.count() == 1


def test_pivot_min(spark):
    """Test pivot with min aggregation."""
    data = [
        {"category": "A", "type": "X", "value": 10},
        {"category": "A", "type": "X", "value": 20}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("category").pivot("type").min("value")
    assert result.count() == 1


def test_pivot_multiple_group_columns(spark):
    """Test pivot with multiple groupBy columns."""
    data = [
        {"region": "US", "category": "A", "type": "X", "value": 10},
        {"region": "US", "category": "A", "type": "Y", "value": 20},
        {"region": "EU", "category": "A", "type": "X", "value": 30}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("region", "category").pivot("type").sum("value")
    assert result.count() == 2


def test_pivot_with_literals(spark):
    """Test pivot with literal values."""
    data = [
        {"category": "A", "type": 1, "value": 10},
        {"category": "A", "type": 2, "value": 20}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("category").pivot("type", [1, 2]).sum("value")
    assert result.count() == 1


def test_pivot_preserves_nulls(spark):
    """Test that pivot preserves null aggregation results."""
    data = [
        {"category": "A", "type": "X", "value": 10},
        {"category": "B", "type": "Y", "value": 20}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("category").pivot("type", ["X", "Y"]).sum("value")
    # Category A should have null for Y, Category B should have null for X
    assert result.count() == 2


def test_pivot_empty_dataframe(spark):
    """Test pivot on empty DataFrame."""
    data = []
    schema_data = [{"category": "A", "type": "X", "value": 10}]
    temp_df = spark.createDataFrame(schema_data)
    df = spark.createDataFrame(data, temp_df.schema)
    
    result = df.groupBy("category").pivot("type").sum("value")
    assert result.count() == 0


def test_pivot_single_value(spark):
    """Test pivot with only one pivot value."""
    data = [
        {"category": "A", "type": "X", "value": 10},
        {"category": "B", "type": "X", "value": 20}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("category").pivot("type").sum("value")
    assert result.count() == 2


def test_pivot_many_values(spark):
    """Test pivot with many distinct values."""
    data = []
    for i in range(10):
        data.append({"category": "A", "type": f"type_{i}", "value": i * 10})
    df = spark.createDataFrame(data)
    
    result = df.groupBy("category").pivot("type").sum("value")
    assert result.count() == 1
    # Should have many columns
    assert len(result.columns) > 5

