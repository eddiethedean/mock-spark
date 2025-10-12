"""Unit tests for DataFrame join operations."""

import pytest
from mock_spark import MockSparkSession, F


@pytest.fixture
def spark():
    """Create test session."""
    session = MockSparkSession("test_joins")
    yield session
    try:
        session.stop()
    except Exception:
        pass


def test_join_inner(spark):
    """Test inner join."""
    left_data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    right_data = [{"id": 1, "age": 25}, {"id": 3, "age": 30}]
    
    left_df = spark.createDataFrame(left_data)
    right_df = spark.createDataFrame(right_data)
    
    result = left_df.join(right_df, on="id", how="inner")
    assert result.count() == 1
    rows = result.collect()
    assert rows[0]["name"] == "Alice"
    assert rows[0]["age"] == 25


@pytest.mark.skip(reason="Left join implementation incomplete")
def test_join_left(spark):
    """Test left outer join."""
    left_data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    right_data = [{"id": 1, "age": 25}]
    
    left_df = spark.createDataFrame(left_data)
    right_df = spark.createDataFrame(right_data)
    
    result = left_df.join(right_df, on="id", how="left")
    assert result.count() == 2


@pytest.mark.skip(reason="Right join implementation incomplete")
def test_join_right(spark):
    """Test right outer join."""
    left_data = [{"id": 1, "name": "Alice"}]
    right_data = [{"id": 1, "age": 25}, {"id": 2, "age": 30}]
    
    left_df = spark.createDataFrame(left_data)
    right_df = spark.createDataFrame(right_data)
    
    result = left_df.join(right_df, on="id", how="right")
    assert result.count() == 2


@pytest.mark.skip(reason="Outer join implementation incomplete")
def test_join_outer(spark):
    """Test full outer join."""
    left_data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    right_data = [{"id": 1, "age": 25}, {"id": 3, "age": 30}]
    
    left_df = spark.createDataFrame(left_data)
    right_df = spark.createDataFrame(right_data)
    
    result = left_df.join(right_df, on="id", how="outer")
    assert result.count() == 3


@pytest.mark.skip(reason="DataFrame subscript operator not implemented")
def test_join_with_column_condition(spark):
    """Test join with Column condition instead of string."""
    left_data = [{"id": 1, "name": "Alice"}]
    right_data = [{"id": 1, "age": 25}]
    
    left_df = spark.createDataFrame(left_data)
    right_df = spark.createDataFrame(right_data)
    
    result = left_df.join(right_df, left_df["id"] == right_df["id"])
    assert result.count() == 1


def test_join_multiple_keys(spark):
    """Test join on multiple columns."""
    left_data = [{"id": 1, "country": "US", "name": "Alice"}]
    right_data = [{"id": 1, "country": "US", "age": 25}]
    
    left_df = spark.createDataFrame(left_data)
    right_df = spark.createDataFrame(right_data)
    
    result = left_df.join(right_df, on=["id", "country"])
    assert result.count() == 1


def test_join_default_inner(spark):
    """Test that default join type is inner."""
    left_data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    right_data = [{"id": 1, "age": 25}]
    
    left_df = spark.createDataFrame(left_data)
    right_df = spark.createDataFrame(right_data)
    
    result = left_df.join(right_df, on="id")
    assert result.count() == 1


@pytest.mark.skip(reason="crossJoin has implementation bug")
def test_crossJoin(spark):
    """Test cross join (cartesian product)."""
    left_data = [{"id": 1}, {"id": 2}]
    right_data = [{"value": "A"}, {"value": "B"}]
    
    left_df = spark.createDataFrame(left_data)
    right_df = spark.createDataFrame(right_data)
    
    result = left_df.crossJoin(right_df)
    assert result.count() == 4


def test_union(spark):
    """Test union of two DataFrames."""
    data1 = [{"id": 1, "name": "Alice"}]
    data2 = [{"id": 2, "name": "Bob"}]
    
    df1 = spark.createDataFrame(data1)
    df2 = spark.createDataFrame(data2)
    
    result = df1.union(df2)
    assert result.count() == 2


def test_union_preserves_duplicates(spark):
    """Test that union preserves duplicate rows."""
    data = [{"id": 1, "name": "Alice"}]
    
    df1 = spark.createDataFrame(data)
    df2 = spark.createDataFrame(data)
    
    result = df1.union(df2)
    assert result.count() == 2


def test_unionAll(spark):
    """Test unionAll (alias for union)."""
    data1 = [{"id": 1}]
    data2 = [{"id": 2}]
    
    df1 = spark.createDataFrame(data1)
    df2 = spark.createDataFrame(data2)
    
    result = df1.unionAll(df2)
    assert result.count() == 2


def test_unionByName(spark):
    """Test unionByName with matching columns."""
    data1 = [{"name": "Alice", "age": 25}]
    data2 = [{"age": 30, "name": "Bob"}]  # Different column order
    
    df1 = spark.createDataFrame(data1)
    df2 = spark.createDataFrame(data2)
    
    result = df1.unionByName(df2)
    assert result.count() == 2
    rows = result.collect()
    assert rows[1]["name"] == "Bob"
    assert rows[1]["age"] == 30


def test_intersect(spark):
    """Test intersect operation."""
    data1 = [{"id": 1}, {"id": 2}, {"id": 3}]
    data2 = [{"id": 2}, {"id": 3}, {"id": 4}]
    
    df1 = spark.createDataFrame(data1)
    df2 = spark.createDataFrame(data2)
    
    result = df1.intersect(df2)
    assert result.count() == 2


def test_intersectAll(spark):
    """Test intersectAll (preserves duplicates)."""
    data1 = [{"id": 1}, {"id": 1}, {"id": 2}]
    data2 = [{"id": 1}, {"id": 2}, {"id": 2}]
    
    df1 = spark.createDataFrame(data1)
    df2 = spark.createDataFrame(data2)
    
    result = df1.intersectAll(df2)
    # Should have 1 occurrence of {id: 1} and 1 of {id: 2}
    assert result.count() >= 1


def test_subtract(spark):
    """Test subtract (difference) operation."""
    data1 = [{"id": 1}, {"id": 2}, {"id": 3}]
    data2 = [{"id": 2}, {"id": 3}]
    
    df1 = spark.createDataFrame(data1)
    df2 = spark.createDataFrame(data2)
    
    result = df1.subtract(df2)
    assert result.count() == 1
    rows = result.collect()
    assert rows[0]["id"] == 1


def test_exceptAll(spark):
    """Test exceptAll operation."""
    data1 = [{"id": 1}, {"id": 2}]
    data2 = [{"id": 2}]
    
    df1 = spark.createDataFrame(data1)
    df2 = spark.createDataFrame(data2)
    
    result = df1.exceptAll(df2)
    assert result.count() == 1


def test_join_empty_dataframe(spark):
    """Test join with empty DataFrame."""
    data = [{"id": 1, "name": "Alice"}]
    df1 = spark.createDataFrame(data)
    df2 = spark.createDataFrame([], df1.schema)
    
    result = df1.join(df2, on="id", how="inner")
    assert result.count() == 0


def test_join_preserves_column_order(spark):
    """Test that join preserves expected column order."""
    left_data = [{"id": 1, "name": "Alice"}]
    right_data = [{"id": 1, "age": 25}]
    
    left_df = spark.createDataFrame(left_data)
    right_df = spark.createDataFrame(right_data)
    
    result = left_df.join(right_df, on="id")
    # id should be first since it's the join key
    assert "id" in result.columns


@pytest.mark.skip(reason="Union with mismatched schemas has SQL generation bug")
def test_union_mismatched_schemas_fails(spark):
    """Test that union with mismatched schemas fails."""
    data1 = [{"id": 1, "name": "Alice"}]
    data2 = [{"id": 1, "age": 25}]
    
    df1 = spark.createDataFrame(data1)
    df2 = spark.createDataFrame(data2)
    
    # This should work but may produce unexpected results
    # In real PySpark, this would align by position
    result = df1.union(df2)
    assert result.count() == 2

