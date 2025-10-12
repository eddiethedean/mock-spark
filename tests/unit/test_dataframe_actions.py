"""Unit tests for DataFrame actions and transformations."""

import pytest
from mock_spark import MockSparkSession, F


@pytest.fixture
def spark():
    """Create test session."""
    session = MockSparkSession("test_actions")
    yield session
    try:
        session.stop()
    except Exception:
        pass


def test_take_returns_list(spark):
    """Test take() returns limited rows."""
    data = [{"id": i} for i in range(10)]
    df = spark.createDataFrame(data)
    
    result = df.take(3)
    assert len(result) == 3
    assert isinstance(result, list)


def test_head_returns_row(spark):
    """Test head() returns first row."""
    data = [{"id": 1}, {"id": 2}]
    df = spark.createDataFrame(data)
    
    row = df.head()
    assert row is not None
    assert row["id"] == 1


def test_head_with_n(spark):
    """Test head(n) returns n rows."""
    data = [{"id": i} for i in range(10)]
    df = spark.createDataFrame(data)
    
    rows = df.head(5)
    assert len(rows) == 5


def test_first_returns_first_row(spark):
    """Test first() returns first row."""
    data = [{"id": 1}, {"id": 2}]
    df = spark.createDataFrame(data)
    
    row = df.first()
    assert row is not None
    assert row["id"] == 1


def test_limit_reduces_rows(spark):
    """Test limit() reduces row count."""
    data = [{"id": i} for i in range(10)]
    df = spark.createDataFrame(data)
    
    result = df.limit(3)
    assert result.count() == 3


def test_sample_returns_dataframe(spark):
    """Test sample() returns DataFrame."""
    data = [{"id": i} for i in range(100)]
    df = spark.createDataFrame(data)
    
    result = df.sample(fraction=0.5, seed=42)
    assert result is not None
    # Sample should return fewer rows (probabilistically)
    assert result.count() <= 100


def test_distinct_removes_duplicates(spark):
    """Test distinct() removes duplicate rows."""
    data = [{"value": 1}, {"value": 1}, {"value": 2}]
    df = spark.createDataFrame(data)
    
    result = df.distinct()
    assert result.count() == 2


def test_dropDuplicates_removes_duplicates(spark):
    """Test dropDuplicates() removes duplicate rows."""
    data = [{"value": 1}, {"value": 1}, {"value": 2}]
    df = spark.createDataFrame(data)
    
    result = df.dropDuplicates()
    assert result.count() == 2


def test_drop_duplicates_alias(spark):
    """Test drop_duplicates() alias works."""
    data = [{"value": 1}, {"value": 1}, {"value": 2}]
    df = spark.createDataFrame(data)
    
    result = df.drop_duplicates()
    assert result.count() == 2


def test_dropDuplicates_subset(spark):
    """Test dropDuplicates() with subset of columns."""
    data = [
        {"id": 1, "value": "A"},
        {"id": 1, "value": "B"},
        {"id": 2, "value": "C"}
    ]
    df = spark.createDataFrame(data)
    
    result = df.dropDuplicates(["id"])
    assert result.count() == 2


def test_orderBy_sorts_dataframe(spark):
    """Test orderBy() sorts rows."""
    data = [{"value": 30}, {"value": 10}, {"value": 20}]
    df = spark.createDataFrame(data)
    
    result = df.orderBy("value")
    rows = result.collect()
    assert rows[0]["value"] <= rows[1]["value"] <= rows[2]["value"]


def test_sort_alias(spark):
    """Test sort() as alias for orderBy()."""
    data = [{"value": 30}, {"value": 10}, {"value": 20}]
    df = spark.createDataFrame(data)
    
    result = df.sort("value")
    rows = result.collect()
    assert rows[0]["value"] <= rows[1]["value"] <= rows[2]["value"]


def test_orderBy_descending(spark):
    """Test orderBy() with descending order."""
    data = [{"value": 10}, {"value": 30}, {"value": 20}]
    df = spark.createDataFrame(data)
    
    result = df.orderBy(F.col("value").desc())
    rows = result.collect()
    assert rows[0]["value"] >= rows[1]["value"] >= rows[2]["value"]


def test_orderBy_multiple_columns(spark):
    """Test orderBy() with multiple columns."""
    data = [
        {"cat": "A", "value": 20},
        {"cat": "A", "value": 10},
        {"cat": "B", "value": 30}
    ]
    df = spark.createDataFrame(data)
    
    result = df.orderBy("cat", "value")
    assert result.count() == 3


def test_alias_returns_dataframe(spark):
    """Test alias() returns DataFrame."""
    data = [{"id": 1}]
    df = spark.createDataFrame(data)
    
    result = df.alias("my_alias")
    assert result is not None
    assert result.count() == 1


def test_coalesce_reduces_partitions(spark):
    """Test coalesce() reduces partitions."""
    data = [{"id": i} for i in range(10)]
    df = spark.createDataFrame(data)
    
    result = df.coalesce(1)
    assert result is not None
    assert result.count() == 10


def test_repartition_changes_partitions(spark):
    """Test repartition() changes partitions."""
    data = [{"id": i} for i in range(10)]
    df = spark.createDataFrame(data)
    
    result = df.repartition(4)
    assert result is not None
    assert result.count() == 10


def test_isEmpty_on_empty_dataframe(spark):
    """Test isEmpty() returns True for empty DataFrame."""
    data = []
    df = spark.createDataFrame(data)
    
    assert df.isEmpty() == True


def test_isEmpty_on_nonempty_dataframe(spark):
    """Test isEmpty() returns False for non-empty DataFrame."""
    data = [{"id": 1}]
    df = spark.createDataFrame(data)
    
    assert df.isEmpty() == False


def test_intersect_returns_common_rows(spark):
    """Test intersect() returns common rows."""
    data1 = [{"value": 1}, {"value": 2}]
    data2 = [{"value": 2}, {"value": 3}]
    df1 = spark.createDataFrame(data1)
    df2 = spark.createDataFrame(data2)
    
    result = df1.intersect(df2)
    assert result.count() == 1


def test_subtract_returns_difference(spark):
    """Test subtract() returns rows in first but not second."""
    data1 = [{"value": 1}, {"value": 2}]
    data2 = [{"value": 2}, {"value": 3}]
    df1 = spark.createDataFrame(data1)
    df2 = spark.createDataFrame(data2)
    
    result = df1.subtract(df2)
    assert result.count() == 1

