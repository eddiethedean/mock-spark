"""Unit tests for basic RDD operations."""

import pytest
from mock_spark import MockSparkSession


@pytest.fixture
def spark():
    """Create test session."""
    session = MockSparkSession("test_rdd_basic")
    yield session
    try:
        session.stop()
    except Exception:
        pass


def test_rdd_from_dataframe(spark):
    """Test creating RDD from DataFrame."""
    data = [{"id": 1, "value": 10}, {"id": 2, "value": 20}]
    df = spark.createDataFrame(data)
    
    rdd = df.rdd
    assert rdd is not None


def test_rdd_collect(spark):
    """Test RDD.collect()."""
    data = [{"id": i} for i in range(5)]
    df = spark.createDataFrame(data)
    
    rdd = df.rdd
    rows = rdd.collect()
    assert len(rows) == 5


def test_rdd_count(spark):
    """Test RDD.count()."""
    data = [{"id": i} for i in range(10)]
    df = spark.createDataFrame(data)
    
    rdd = df.rdd
    assert rdd.count() == 10


def test_rdd_take(spark):
    """Test RDD.take()."""
    data = [{"id": i} for i in range(10)]
    df = spark.createDataFrame(data)
    
    rdd = df.rdd
    taken = rdd.take(3)
    assert len(taken) == 3


def test_rdd_first(spark):
    """Test RDD.first()."""
    data = [{"id": 1}, {"id": 2}]
    df = spark.createDataFrame(data)
    
    rdd = df.rdd
    first_row = rdd.first()
    assert first_row is not None
    assert first_row["id"] == 1


def test_rdd_map(spark):
    """Test RDD.map() transformation."""
    data = [{"value": i} for i in range(5)]
    df = spark.createDataFrame(data)
    
    rdd = df.rdd
    mapped = rdd.map(lambda row: row["value"] * 2)
    result = mapped.collect()
    
    assert len(result) == 5
    assert result[0] == 0
    assert result[1] == 2


def test_rdd_filter(spark):
    """Test RDD.filter() transformation."""
    data = [{"value": i} for i in range(10)]
    df = spark.createDataFrame(data)
    
    rdd = df.rdd
    filtered = rdd.filter(lambda row: row["value"] > 5)
    result = filtered.collect()
    
    assert len(result) == 4


def test_rdd_foreach(spark):
    """Test RDD.foreach() action."""
    data = [{"id": i} for i in range(3)]
    df = spark.createDataFrame(data)
    
    rdd = df.rdd
    # Just verify it doesn't raise an error
    rdd.foreach(lambda row: None)


def test_rdd_reduce(spark):
    """Test RDD.reduce()."""
    data = [{"value": i} for i in range(1, 6)]
    df = spark.createDataFrame(data)
    
    rdd = df.rdd
    total = rdd.map(lambda row: row["value"]).reduce(lambda a, b: a + b)
    
    assert total == 15


def test_rdd_cache(spark):
    """Test RDD.cache()."""
    data = [{"id": i} for i in range(10)]
    df = spark.createDataFrame(data)
    
    rdd = df.rdd
    cached_rdd = rdd.cache()
    assert cached_rdd is not None


def test_rdd_persist(spark):
    """Test RDD.persist()."""
    data = [{"id": i} for i in range(10)]
    df = spark.createDataFrame(data)
    
    rdd = df.rdd
    persisted_rdd = rdd.persist()
    assert persisted_rdd is not None


def test_rdd_unpersist(spark):
    """Test RDD.unpersist()."""
    data = [{"id": i} for i in range(10)]
    df = spark.createDataFrame(data)
    
    rdd = df.rdd
    rdd.cache()
    unpersisted_rdd = rdd.unpersist()
    assert unpersisted_rdd is not None

