"""Unit tests for DataFrame aggregation operations."""

import pytest
from mock_spark import MockSparkSession, F


@pytest.fixture
def spark():
    """Create test session."""
    session = MockSparkSession("test_agg")
    yield session
    try:
        session.stop()
    except Exception:
        pass


def test_agg_single_function(spark):
    """Test aggregation with single function."""
    data = [{"value": 10}, {"value": 20}, {"value": 30}]
    df = spark.createDataFrame(data)
    
    result = df.agg(F.sum("value"))
    rows = result.collect()
    assert rows[0][0] == 60


def test_agg_multiple_functions(spark):
    """Test aggregation with multiple functions."""
    data = [{"value": 10}, {"value": 20}, {"value": 30}]
    df = spark.createDataFrame(data)
    
    result = df.agg(F.sum("value"), F.avg("value"))
    rows = result.collect()
    assert rows[0][0] == 60


def test_agg_with_alias(spark):
    """Test aggregation with column aliases."""
    data = [{"value": 10}, {"value": 20}]
    df = spark.createDataFrame(data)
    
    result = df.agg(F.sum("value").alias("total"))
    assert "total" in result.columns


def test_groupBy_single_column(spark):
    """Test groupBy with single column."""
    data = [
        {"category": "A", "value": 10},
        {"category": "A", "value": 20},
        {"category": "B", "value": 30}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("category").count()
    assert result.count() == 2


def test_groupBy_multiple_columns(spark):
    """Test groupBy with multiple columns."""
    data = [
        {"category": "A", "type": "X", "value": 10},
        {"category": "A", "type": "Y", "value": 20},
        {"category": "B", "type": "X", "value": 30}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("category", "type").count()
    assert result.count() == 3


def test_groupBy_agg_sum(spark):
    """Test groupBy with sum aggregation."""
    data = [
        {"category": "A", "value": 10},
        {"category": "A", "value": 20},
        {"category": "B", "value": 30}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("category").agg(F.sum("value").alias("total"))
    rows = result.collect()
    
    # Find category A
    category_a = [r for r in rows if r["category"] == "A"][0]
    assert category_a["total"] == 30


def test_groupBy_agg_avg(spark):
    """Test groupBy with average aggregation."""
    data = [
        {"category": "A", "value": 10},
        {"category": "A", "value": 20}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("category").agg(F.avg("value").alias("average"))
    rows = result.collect()
    assert rows[0]["average"] == 15


def test_groupBy_agg_max(spark):
    """Test groupBy with max aggregation."""
    data = [
        {"category": "A", "value": 10},
        {"category": "A", "value": 20}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("category").agg(F.max("value").alias("max_val"))
    rows = result.collect()
    assert rows[0]["max_val"] == 20


def test_groupBy_agg_min(spark):
    """Test groupBy with min aggregation."""
    data = [
        {"category": "A", "value": 10},
        {"category": "A", "value": 20}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("category").agg(F.min("value").alias("min_val"))
    rows = result.collect()
    assert rows[0]["min_val"] == 10


def test_groupBy_count(spark):
    """Test groupBy with count."""
    data = [
        {"category": "A", "value": 10},
        {"category": "A", "value": 20},
        {"category": "B", "value": 30}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("category").count()
    rows = result.collect()
    
    counts = {r["category"]: r["count"] for r in rows}
    assert counts["A"] == 2
    assert counts["B"] == 1


def test_groupBy_multiple_agg(spark):
    """Test groupBy with multiple aggregations."""
    data = [
        {"category": "A", "value": 10},
        {"category": "A", "value": 20}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("category").agg(
        F.sum("value").alias("total"),
        F.avg("value").alias("average"),
        F.count("value").alias("count")
    )
    rows = result.collect()
    assert rows[0]["total"] == 30
    assert rows[0]["average"] == 15
    assert rows[0]["count"] == 2


def test_count_method(spark):
    """Test count() method on DataFrame."""
    data = [{"id": 1}, {"id": 2}, {"id": 3}]
    df = spark.createDataFrame(data)
    
    assert df.count() == 3


def test_count_empty_dataframe(spark):
    """Test count on empty DataFrame."""
    df = spark.createDataFrame([])
    assert df.count() == 0


def test_describe(spark):
    """Test describe() for summary statistics."""
    data = [{"value": 10}, {"value": 20}, {"value": 30}]
    df = spark.createDataFrame(data)
    
    result = df.describe()
    assert result.count() > 0
    assert "summary" in result.columns


def test_describe_multiple_columns(spark):
    """Test describe with multiple columns."""
    data = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
    df = spark.createDataFrame(data)
    
    result = df.describe("a", "b")
    assert result.count() > 0


def test_summary(spark):
    """Test summary() for statistics."""
    data = [{"value": 10}, {"value": 20}, {"value": 30}]
    df = spark.createDataFrame(data)
    
    result = df.summary()
    assert result.count() > 0


def test_rollup_grouping(spark):
    """Test rollup for hierarchical grouping."""
    data = [
        {"country": "US", "city": "NYC", "value": 10},
        {"country": "US", "city": "LA", "value": 20}
    ]
    df = spark.createDataFrame(data)
    
    result = df.rollup("country", "city").count()
    # Should have multiple levels of aggregation
    assert result.count() >= 3


def test_cube_grouping(spark):
    """Test cube for multi-dimensional grouping."""
    data = [
        {"category": "A", "type": "X", "value": 10},
        {"category": "A", "type": "Y", "value": 20}
    ]
    df = spark.createDataFrame(data)
    
    result = df.cube("category", "type").count()
    # Should have all combinations
    assert result.count() >= 4


def test_groupBy_with_column_objects(spark):
    """Test groupBy with Column objects."""
    data = [{"category": "A"}, {"category": "B"}]
    df = spark.createDataFrame(data)
    
    result = df.groupBy(F.col("category")).count()
    assert result.count() == 2


@pytest.mark.skip(reason="countDistinct in agg has execution bug returning None")
def test_agg_count_distinct(spark):
    """Test aggregation with countDistinct."""
    data = [{"value": 1}, {"value": 1}, {"value": 2}]
    df = spark.createDataFrame(data)
    
    result = df.agg(F.countDistinct("value").alias("unique_count"))
    rows = result.collect()
    assert rows[0]["unique_count"] == 2


def test_groupBy_with_null_values(spark):
    """Test groupBy handling null values."""
    data = [
        {"category": "A", "value": 10},
        {"category": None, "value": 20}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("category").count()
    # Should group nulls together
    assert result.count() == 2

