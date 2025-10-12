"""Unit tests for DataFrame filtering operations."""

import pytest
from mock_spark import MockSparkSession, F


@pytest.fixture
def spark():
    """Create test session."""
    session = MockSparkSession("test_filtering")
    yield session
    try:
        session.stop()
    except Exception:
        pass


def test_filter_simple_condition(spark):
    """Test filtering with simple condition."""
    data = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
    df = spark.createDataFrame(data)
    
    result = df.filter(F.col("age") > 26)
    assert result.count() == 1
    rows = result.collect()
    assert rows[0]["name"] == "Bob"


def test_filter_equals_condition(spark):
    """Test filtering with equals condition."""
    data = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
    df = spark.createDataFrame(data)
    
    result = df.filter(F.col("name") == "Alice")
    assert result.count() == 1


@pytest.mark.skip(reason="String expression filtering not yet fully implemented")
def test_filter_string_expression(spark):
    """Test filtering with string expression."""
    data = [{"age": 25}, {"age": 30}, {"age": 35}]
    df = spark.createDataFrame(data)
    
    result = df.filter("age > 28")
    assert result.count() == 2


def test_where_alias(spark):
    """Test that where() is an alias for filter()."""
    data = [{"age": 25}, {"age": 30}]
    df = spark.createDataFrame(data)
    
    result = df.where(F.col("age") > 26)
    assert result.count() == 1


def test_filter_multiple_conditions_and(spark):
    """Test filtering with AND conditions."""
    data = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
    df = spark.createDataFrame(data)
    
    result = df.filter((F.col("age") > 20) & (F.col("age") < 28))
    assert result.count() == 1
    rows = result.collect()
    assert rows[0]["name"] == "Alice"


def test_filter_multiple_conditions_or(spark):
    """Test filtering with OR conditions."""
    data = [{"age": 25}, {"age": 30}, {"age": 35}]
    df = spark.createDataFrame(data)
    
    result = df.filter((F.col("age") == 25) | (F.col("age") == 35))
    assert result.count() == 2


def test_filter_not_condition(spark):
    """Test filtering with NOT condition."""
    data = [{"active": True}, {"active": False}]
    df = spark.createDataFrame(data)
    
    result = df.filter(~F.col("active"))
    assert result.count() == 1


def test_filter_chained(spark):
    """Test chaining multiple filter operations."""
    data = [{"age": 25}, {"age": 30}, {"age": 35}]
    df = spark.createDataFrame(data)
    
    result = df.filter(F.col("age") > 20).filter(F.col("age") < 33)
    assert result.count() == 2


def test_filter_empty_result(spark):
    """Test filter that results in empty DataFrame."""
    data = [{"age": 25}, {"age": 30}]
    df = spark.createDataFrame(data)
    
    result = df.filter(F.col("age") > 100)
    assert result.count() == 0


def test_filter_all_rows(spark):
    """Test filter that matches all rows."""
    data = [{"age": 25}, {"age": 30}]
    df = spark.createDataFrame(data)
    
    result = df.filter(F.col("age") > 0)
    assert result.count() == 2


def test_limit_basic(spark):
    """Test limit operation."""
    data = [{"x": i} for i in range(10)]
    df = spark.createDataFrame(data)
    
    result = df.limit(5)
    assert result.count() == 5


def test_limit_zero(spark):
    """Test limit with 0."""
    data = [{"x": 1}, {"x": 2}]
    df = spark.createDataFrame(data)
    
    result = df.limit(0)
    assert result.count() == 0


def test_limit_larger_than_data(spark):
    """Test limit larger than number of rows."""
    data = [{"x": 1}, {"x": 2}]
    df = spark.createDataFrame(data)
    
    result = df.limit(100)
    assert result.count() == 2


def test_head_returns_rows(spark):
    """Test head() returns Row objects."""
    data = [{"x": 1}, {"x": 2}, {"x": 3}]
    df = spark.createDataFrame(data)
    
    rows = df.head(2)
    assert len(rows) == 2
    assert rows[0]["x"] == 1


def test_head_single_row(spark):
    """Test head() with no argument returns single row."""
    data = [{"x": 1}, {"x": 2}]
    df = spark.createDataFrame(data)
    
    row = df.head()
    assert row["x"] == 1


def test_first(spark):
    """Test first() returns first row."""
    data = [{"x": 1}, {"x": 2}]
    df = spark.createDataFrame(data)
    
    row = df.first()
    assert row["x"] == 1


def test_take(spark):
    """Test take() returns list of rows."""
    data = [{"x": i} for i in range(5)]
    df = spark.createDataFrame(data)
    
    rows = df.take(3)
    assert len(rows) == 3


def test_filter_with_null_handling(spark):
    """Test filtering with null values."""
    data = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": None}]
    df = spark.createDataFrame(data)
    
    result = df.filter(F.col("age").isNotNull())
    assert result.count() == 1


@pytest.mark.skip(reason="Column.startswith() method not yet implemented")
def test_filter_string_functions(spark):
    """Test filtering with string functions."""
    data = [{"name": "Alice"}, {"name": "Bob"}]
    df = spark.createDataFrame(data)
    
    result = df.filter(F.col("name").startswith("A"))
    assert result.count() == 1


def test_filter_preserves_schema(spark):
    """Test that filter preserves DataFrame schema."""
    data = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
    df = spark.createDataFrame(data)
    
    result = df.filter(F.col("age") > 26)
    assert result.schema == df.schema


def test_limit_chained_with_filter(spark):
    """Test limit chained with filter."""
    data = [{"x": i} for i in range(20)]
    df = spark.createDataFrame(data)
    
    result = df.filter(F.col("x") > 5).limit(3)
    assert result.count() == 3


def test_tail(spark):
    """Test tail() returns last N rows."""
    data = [{"x": i} for i in range(5)]
    df = spark.createDataFrame(data)
    
    rows = df.tail(2)
    assert len(rows) == 2
    # Tail should return last 2 rows
    assert rows[0]["x"] in [3, 4]

