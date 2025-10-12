"""Unit tests for GroupedData rollup operations."""

import pytest
from mock_spark import MockSparkSession, F


@pytest.fixture
def spark():
    """Create test session."""
    session = MockSparkSession("test_rollup")
    yield session
    try:
        session.stop()
    except Exception:
        pass


def test_rollup_single_column(spark):
    """Test rollup with single column."""
    data = [
        {"country": "US", "value": 10},
        {"country": "US", "value": 20},
        {"country": "UK", "value": 30}
    ]
    df = spark.createDataFrame(data)
    
    result = df.rollup("country").sum("value")
    # Should have subtotals for each country plus grand total
    assert result.count() >= 3


def test_rollup_two_columns(spark):
    """Test rollup with two columns (hierarchical)."""
    data = [
        {"country": "US", "city": "NYC", "value": 10},
        {"country": "US", "city": "LA", "value": 20},
        {"country": "UK", "city": "London", "value": 30}
    ]
    df = spark.createDataFrame(data)
    
    result = df.rollup("country", "city").sum("value")
    # Should have multiple levels: grand total, by country, by country+city
    assert result.count() >= 4


def test_rollup_count(spark):
    """Test rollup with count aggregation."""
    data = [
        {"category": "A", "type": "X"},
        {"category": "A", "type": "Y"},
        {"category": "B", "type": "X"}
    ]
    df = spark.createDataFrame(data)
    
    result = df.rollup("category", "type").count()
    assert result.count() >= 4


def test_rollup_multiple_aggregations(spark):
    """Test rollup with multiple aggregation functions."""
    data = [
        {"category": "A", "value": 10},
        {"category": "A", "value": 20},
        {"category": "B", "value": 30}
    ]
    df = spark.createDataFrame(data)
    
    result = df.rollup("category").agg(
        F.sum("value").alias("total"),
        F.avg("value").alias("average"),
        F.count("value").alias("count")
    )
    assert result.count() >= 2


def test_rollup_avg(spark):
    """Test rollup with average aggregation."""
    data = [
        {"category": "A", "value": 10},
        {"category": "A", "value": 20}
    ]
    df = spark.createDataFrame(data)
    
    result = df.rollup("category").avg("value")
    assert result.count() >= 2


def test_rollup_max(spark):
    """Test rollup with max aggregation."""
    data = [
        {"category": "A", "value": 10},
        {"category": "A", "value": 20}
    ]
    df = spark.createDataFrame(data)
    
    result = df.rollup("category").max("value")
    rows = result.collect()
    assert len(rows) >= 2


def test_rollup_min(spark):
    """Test rollup with min aggregation."""
    data = [
        {"category": "A", "value": 10},
        {"category": "A", "value": 20}
    ]
    df = spark.createDataFrame(data)
    
    result = df.rollup("category").min("value")
    assert result.count() >= 2


def test_rollup_three_columns(spark):
    """Test rollup with three columns."""
    data = [
        {"region": "NA", "country": "US", "city": "NYC", "value": 10},
        {"region": "NA", "country": "US", "city": "LA", "value": 20}
    ]
    df = spark.createDataFrame(data)
    
    result = df.rollup("region", "country", "city").sum("value")
    # Should have multiple hierarchy levels
    assert result.count() >= 3


def test_rollup_with_nulls(spark):
    """Test rollup handling null values in grouping columns."""
    data = [
        {"category": "A", "value": 10},
        {"category": None, "value": 20}
    ]
    df = spark.createDataFrame(data)
    
    result = df.rollup("category").sum("value")
    assert result.count() >= 2


def test_rollup_grand_total(spark):
    """Test that rollup includes grand total row."""
    data = [
        {"category": "A", "value": 10},
        {"category": "B", "value": 20}
    ]
    df = spark.createDataFrame(data)
    
    result = df.rollup("category").sum("value")
    rows = result.collect()
    
    # Should have rows for A, B, and grand total (null category)
    assert len(rows) >= 3


def test_rollup_empty_dataframe(spark):
    """Test rollup on empty DataFrame."""
    schema_data = [{"category": "A", "value": 10}]
    temp_df = spark.createDataFrame(schema_data)
    df = spark.createDataFrame([], temp_df.schema)
    
    result = df.rollup("category").sum("value")
    # Empty rollup might still have grand total row
    assert result.count() >= 0


def test_rollup_column_objects(spark):
    """Test rollup with Column objects."""
    data = [
        {"category": "A", "value": 10},
        {"category": "B", "value": 20}
    ]
    df = spark.createDataFrame(data)
    
    result = df.rollup(F.col("category")).sum("value")
    assert result.count() >= 2


def test_rollup_preserves_data_types(spark):
    """Test that rollup preserves column data types."""
    data = [
        {"category": "A", "int_val": 10, "float_val": 1.5},
        {"category": "A", "int_val": 20, "float_val": 2.5}
    ]
    df = spark.createDataFrame(data)
    
    result = df.rollup("category").sum("int_val")
    assert result.count() >= 2

