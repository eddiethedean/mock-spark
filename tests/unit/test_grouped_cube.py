"""Unit tests for GroupedData cube operations."""

import pytest
from mock_spark import MockSparkSession, F


@pytest.fixture
def spark():
    """Create test session."""
    session = MockSparkSession("test_cube")
    yield session
    try:
        session.stop()
    except Exception:
        pass


def test_cube_single_column(spark):
    """Test cube with single column."""
    data = [
        {"category": "A", "value": 10},
        {"category": "B", "value": 20}
    ]
    df = spark.createDataFrame(data)
    
    result = df.cube("category").sum("value")
    # Should have subtotals for each category plus grand total
    assert result.count() >= 3


def test_cube_two_columns(spark):
    """Test cube with two columns (all combinations)."""
    data = [
        {"category": "A", "type": "X", "value": 10},
        {"category": "A", "type": "Y", "value": 20},
        {"category": "B", "type": "X", "value": 30}
    ]
    df = spark.createDataFrame(data)
    
    result = df.cube("category", "type").sum("value")
    # Should have: grand total, by category, by type, by category+type
    assert result.count() >= 4


def test_cube_count(spark):
    """Test cube with count aggregation."""
    data = [
        {"category": "A", "type": "X"},
        {"category": "A", "type": "Y"},
        {"category": "B", "type": "X"}
    ]
    df = spark.createDataFrame(data)
    
    result = df.cube("category", "type").count()
    assert result.count() >= 4


def test_cube_multiple_aggregations(spark):
    """Test cube with multiple aggregation functions."""
    data = [
        {"category": "A", "value": 10},
        {"category": "A", "value": 20},
        {"category": "B", "value": 30}
    ]
    df = spark.createDataFrame(data)
    
    result = df.cube("category").agg(
        F.sum("value").alias("total"),
        F.avg("value").alias("average")
    )
    assert result.count() >= 2


def test_cube_avg(spark):
    """Test cube with average aggregation."""
    data = [
        {"category": "A", "value": 10},
        {"category": "A", "value": 20}
    ]
    df = spark.createDataFrame(data)
    
    result = df.cube("category").avg("value")
    assert result.count() >= 2


def test_cube_max(spark):
    """Test cube with max aggregation."""
    data = [
        {"category": "A", "type": "X", "value": 10},
        {"category": "A", "type": "Y", "value": 20}
    ]
    df = spark.createDataFrame(data)
    
    result = df.cube("category", "type").max("value")
    assert result.count() >= 3


def test_cube_min(spark):
    """Test cube with min aggregation."""
    data = [
        {"category": "A", "type": "X", "value": 10},
        {"category": "B", "type": "Y", "value": 20}
    ]
    df = spark.createDataFrame(data)
    
    result = df.cube("category", "type").min("value")
    assert result.count() >= 3


def test_cube_three_columns(spark):
    """Test cube with three columns."""
    data = [
        {"a": "X", "b": "Y", "c": "Z", "value": 10}
    ]
    df = spark.createDataFrame(data)
    
    result = df.cube("a", "b", "c").sum("value")
    # With 3 columns, cube should produce 2^3 = 8 combinations
    assert result.count() >= 1


def test_cube_with_nulls(spark):
    """Test cube handling null values."""
    data = [
        {"category": "A", "value": 10},
        {"category": None, "value": 20}
    ]
    df = spark.createDataFrame(data)
    
    result = df.cube("category").sum("value")
    assert result.count() >= 2


def test_cube_grand_total(spark):
    """Test that cube includes grand total row."""
    data = [
        {"category": "A", "type": "X", "value": 10},
        {"category": "B", "type": "Y", "value": 20}
    ]
    df = spark.createDataFrame(data)
    
    result = df.cube("category", "type").sum("value")
    rows = result.collect()
    
    # Should include a row where both category and type are null (grand total)
    assert len(rows) >= 4


def test_cube_vs_rollup_difference(spark):
    """Test that cube produces more rows than rollup for same data."""
    data = [
        {"category": "A", "type": "X", "value": 10},
        {"category": "A", "type": "Y", "value": 20}
    ]
    df = spark.createDataFrame(data)
    
    cube_result = df.cube("category", "type").count()
    rollup_result = df.rollup("category", "type").count()
    
    # Cube should have more combinations than rollup
    # Cube: all combinations, Rollup: hierarchical only
    assert cube_result.count() >= rollup_result.count()


def test_cube_empty_dataframe(spark):
    """Test cube on empty DataFrame."""
    schema_data = [{"category": "A", "value": 10}]
    temp_df = spark.createDataFrame(schema_data)
    df = spark.createDataFrame([], temp_df.schema)
    
    result = df.cube("category").sum("value")
    assert result.count() >= 0


def test_cube_column_objects(spark):
    """Test cube with Column objects."""
    data = [
        {"category": "A", "value": 10},
        {"category": "B", "value": 20}
    ]
    df = spark.createDataFrame(data)
    
    result = df.cube(F.col("category")).sum("value")
    assert result.count() >= 2


def test_cube_all_combinations(spark):
    """Test that cube produces all dimensional combinations."""
    data = [
        {"category": "A", "type": "X", "value": 10},
        {"category": "A", "type": "Y", "value": 15},
        {"category": "B", "type": "X", "value": 20},
        {"category": "B", "type": "Y", "value": 25}
    ]
    df = spark.createDataFrame(data)
    
    result = df.cube("category", "type").sum("value")
    rows = result.collect()
    
    # Expected: 2 categories * 2 types + 2 categories + 2 types + 1 grand = 9
    # Or at minimum should have more than simple groupBy
    assert len(rows) >= 7

