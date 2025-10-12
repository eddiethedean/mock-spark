"""Unit tests for extended GroupedData operations."""

import pytest
from mock_spark import MockSparkSession, F


@pytest.fixture
def spark():
    """Create test session."""
    session = MockSparkSession("test_grouped_extended")
    yield session
    try:
        session.stop()
    except Exception:
        pass


def test_groupby_single_column(spark):
    """Test groupBy with single column."""
    data = [
        {"category": "A", "value": 10},
        {"category": "A", "value": 20},
        {"category": "B", "value": 30}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("category").count()
    assert result.count() == 2


def test_groupby_multiple_columns(spark):
    """Test groupBy with multiple columns."""
    data = [
        {"cat1": "A", "cat2": "X", "value": 10},
        {"cat1": "A", "cat2": "X", "value": 20},
        {"cat1": "A", "cat2": "Y", "value": 30}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("cat1", "cat2").count()
    assert result.count() == 2


def test_groupby_count(spark):
    """Test groupBy().count()."""
    data = [{"group": "A"}, {"group": "A"}, {"group": "B"}]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("group").count()
    rows = result.collect()
    assert len(rows) == 2


def test_groupby_sum(spark):
    """Test groupBy().sum()."""
    data = [
        {"group": "A", "value": 10},
        {"group": "A", "value": 20},
        {"group": "B", "value": 30}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("group").sum("value")
    assert result.count() == 2


def test_groupby_avg(spark):
    """Test groupBy().avg()."""
    data = [
        {"group": "A", "value": 10},
        {"group": "A", "value": 20},
        {"group": "B", "value": 30}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("group").avg("value")
    assert result.count() == 2


def test_groupby_mean_alias(spark):
    """Test groupBy().mean() as alias for avg()."""
    data = [
        {"group": "A", "value": 10},
        {"group": "A", "value": 20}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("group").mean("value")
    assert result.count() == 1


def test_groupby_min(spark):
    """Test groupBy().min()."""
    data = [
        {"group": "A", "value": 10},
        {"group": "A", "value": 20},
        {"group": "B", "value": 5}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("group").min("value")
    assert result.count() == 2


def test_groupby_max(spark):
    """Test groupBy().max()."""
    data = [
        {"group": "A", "value": 10},
        {"group": "A", "value": 20},
        {"group": "B", "value": 30}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("group").max("value")
    assert result.count() == 2


def test_groupby_agg_with_dict(spark):
    """Test groupBy().agg() with dictionary."""
    data = [
        {"group": "A", "value": 10},
        {"group": "A", "value": 20}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("group").agg({"value": "sum"})
    assert result.count() == 1


def test_groupby_agg_with_functions(spark):
    """Test groupBy().agg() with function objects."""
    data = [
        {"group": "A", "value": 10},
        {"group": "A", "value": 20}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("group").agg(F.sum("value"))
    assert result.count() == 1


def test_groupby_agg_multiple_aggregations(spark):
    """Test groupBy().agg() with multiple aggregations."""
    data = [
        {"group": "A", "value": 10},
        {"group": "A", "value": 20}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("group").agg(
        F.sum("value").alias("total"),
        F.avg("value").alias("average")
    )
    assert result.count() == 1


def test_groupby_with_column_objects(spark):
    """Test groupBy() with Column objects."""
    data = [{"group": "A", "value": 10}]
    df = spark.createDataFrame(data)
    
    result = df.groupBy(F.col("group")).count()
    assert result.count() == 1


def test_groupby_multiple_aggregations_on_different_columns(spark):
    """Test aggregating different columns."""
    data = [
        {"group": "A", "value1": 10, "value2": 100},
        {"group": "A", "value1": 20, "value2": 200}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("group").agg(
        F.sum("value1"),
        F.sum("value2")
    )
    assert result.count() == 1


def test_groupby_empty_dataframe(spark):
    """Test groupBy on empty DataFrame."""
    data = []
    df = spark.createDataFrame(data)
    
    # Should not raise an error
    result = df.groupBy().count()
    assert result.count() == 0  # Empty result for empty DataFrame


def test_rollup_basic(spark):
    """Test rollup aggregation."""
    data = [
        {"cat1": "A", "cat2": "X", "value": 10},
        {"cat1": "A", "cat2": "Y", "value": 20}
    ]
    df = spark.createDataFrame(data)
    
    result = df.rollup("cat1", "cat2").count()
    # Rollup creates subtotals
    assert result.count() >= 2


def test_cube_basic(spark):
    """Test cube aggregation."""
    data = [
        {"cat1": "A", "cat2": "X", "value": 10},
        {"cat1": "A", "cat2": "Y", "value": 20}
    ]
    df = spark.createDataFrame(data)
    
    result = df.cube("cat1", "cat2").count()
    # Cube creates all combinations
    assert result.count() >= 2

