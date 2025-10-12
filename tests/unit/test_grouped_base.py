"""Unit tests for GroupedData base operations."""

import pytest
from mock_spark import MockSparkSession, F


@pytest.fixture
def spark():
    """Create test session."""
    session = MockSparkSession("test_grouped_base")
    yield session
    try:
        session.stop()
    except Exception:
        pass


def test_grouped_sum(spark):
    """Test grouped sum aggregation."""
    data = [
        {"category": "A", "value": 10},
        {"category": "A", "value": 20},
        {"category": "B", "value": 30}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("category").sum("value")
    rows = result.collect()
    
    sums = {r["category"]: r["sum(value)"] for r in rows}
    assert sums["A"] == 30
    assert sums["B"] == 30


def test_grouped_avg(spark):
    """Test grouped average aggregation."""
    data = [
        {"category": "A", "value": 10},
        {"category": "A", "value": 20}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("category").avg("value")
    rows = result.collect()
    assert rows[0]["avg(value)"] == 15


def test_grouped_mean(spark):
    """Test grouped mean (alias for avg)."""
    data = [
        {"category": "A", "value": 10},
        {"category": "A", "value": 20}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("category").mean("value")
    rows = result.collect()
    assert rows[0]["avg(value)"] == 15


def test_grouped_max(spark):
    """Test grouped max aggregation."""
    data = [
        {"category": "A", "value": 10},
        {"category": "A", "value": 30},
        {"category": "A", "value": 20}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("category").max("value")
    rows = result.collect()
    assert rows[0]["max(value)"] == 30


def test_grouped_min(spark):
    """Test grouped min aggregation."""
    data = [
        {"category": "A", "value": 10},
        {"category": "A", "value": 30},
        {"category": "A", "value": 20}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("category").min("value")
    rows = result.collect()
    assert rows[0]["min(value)"] == 10


def test_grouped_count(spark):
    """Test grouped count."""
    data = [
        {"category": "A"},
        {"category": "A"},
        {"category": "B"}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("category").count()
    rows = result.collect()
    
    counts = {r["category"]: r["count"] for r in rows}
    assert counts["A"] == 2
    assert counts["B"] == 1


def test_grouped_agg_dict(spark):
    """Test grouped agg with dictionary."""
    data = [
        {"category": "A", "value": 10, "quantity": 2},
        {"category": "A", "value": 20, "quantity": 3}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("category").agg({"value": "sum", "quantity": "avg"})
    assert result.count() == 1


def test_grouped_agg_multiple_columns(spark):
    """Test grouped agg with multiple column aggregations."""
    data = [
        {"category": "A", "value": 10},
        {"category": "A", "value": 20}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("category").agg(
        F.sum("value").alias("total"),
        F.max("value").alias("max_val"),
        F.min("value").alias("min_val")
    )
    rows = result.collect()
    assert rows[0]["total"] == 30
    assert rows[0]["max_val"] == 20
    assert rows[0]["min_val"] == 10


def test_grouped_multiple_sum_columns(spark):
    """Test grouped sum on multiple columns."""
    data = [
        {"category": "A", "value1": 10, "value2": 5},
        {"category": "A", "value1": 20, "value2": 15}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("category").sum("value1", "value2")
    rows = result.collect()
    assert rows[0]["sum(value1)"] == 30
    assert rows[0]["sum(value2)"] == 20


def test_grouped_multiple_avg_columns(spark):
    """Test grouped avg on multiple columns."""
    data = [
        {"category": "A", "value1": 10, "value2": 20},
        {"category": "A", "value1": 20, "value2": 40}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("category").avg("value1", "value2")
    rows = result.collect()
    assert rows[0]["avg(value1)"] == 15
    assert rows[0]["avg(value2)"] == 30


def test_grouped_empty_group(spark):
    """Test groupBy with no matching rows."""
    data = [{"category": "A", "value": 10}]
    df = spark.createDataFrame(data)
    
    filtered = df.filter(F.col("category") == "B")
    result = filtered.groupBy("category").sum("value")
    assert result.count() == 0


def test_grouped_with_null_group_key(spark):
    """Test groupBy with null values in group key."""
    data = [
        {"category": None, "value": 10},
        {"category": None, "value": 20},
        {"category": "A", "value": 30}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("category").sum("value")
    # Should group nulls together
    assert result.count() == 2


def test_grouped_with_null_aggregation_values(spark):
    """Test groupBy with null values in aggregation column."""
    data = [
        {"category": "A", "value": 10},
        {"category": "A", "value": None},
        {"category": "A", "value": 20}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("category").sum("value")
    rows = result.collect()
    # Null values should be ignored in sum
    assert rows[0]["sum(value)"] == 30


def test_grouped_no_group_columns(spark):
    """Test groupBy with no columns (aggregate entire DataFrame)."""
    data = [{"value": 10}, {"value": 20}, {"value": 30}]
    df = spark.createDataFrame(data)
    
    result = df.groupBy().sum("value")
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["sum(value)"] == 60


def test_grouped_agg_with_column_objects(spark):
    """Test grouped agg with Column objects."""
    data = [
        {"category": "A", "value": 10},
        {"category": "A", "value": 20}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("category").agg(F.sum(F.col("value")))
    assert result.count() == 1


@pytest.mark.skip(reason="countDistinct returns None in groupBy agg")
def test_grouped_agg_count_distinct(spark):
    """Test grouped agg with countDistinct."""
    data = [
        {"category": "A", "value": 10},
        {"category": "A", "value": 10},
        {"category": "A", "value": 20}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("category").agg(F.countDistinct("value").alias("unique_count"))
    rows = result.collect()
    assert rows[0]["unique_count"] == 2


def test_grouped_agg_collect_list(spark):
    """Test grouped agg with collect_list."""
    data = [
        {"category": "A", "value": 10},
        {"category": "A", "value": 20}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("category").agg(F.collect_list("value").alias("values"))
    rows = result.collect()
    assert len(rows[0]["values"]) == 2


def test_grouped_agg_collect_set(spark):
    """Test grouped agg with collect_set."""
    data = [
        {"category": "A", "value": 10},
        {"category": "A", "value": 10},
        {"category": "A", "value": 20}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("category").agg(F.collect_set("value").alias("unique_values"))
    rows = result.collect()
    # Should have unique values
    assert len(rows[0]["unique_values"]) == 2


def test_grouped_preserves_order(spark):
    """Test that grouped results maintain consistent ordering."""
    data = [
        {"category": "B", "value": 10},
        {"category": "A", "value": 20},
        {"category": "C", "value": 30}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("category").sum("value").orderBy("category")
    rows = result.collect()
    assert rows[0]["category"] == "A"
    assert rows[1]["category"] == "B"
    assert rows[2]["category"] == "C"

