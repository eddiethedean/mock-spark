"""Unit tests for DataFrame select operations."""

import pytest
from mock_spark import MockSparkSession, F
from mock_spark.spark_types import MockStructType, MockStructField, StringType, IntegerType, DoubleType


@pytest.fixture
def spark():
    """Create test session."""
    session = MockSparkSession("test_select")
    yield session
    try:
        session.stop()
    except Exception:
        pass


def test_select_single_column(spark):
    """Test selecting a single column by name."""
    data = [{"name": "Alice", "age": 25, "city": "NYC"}]
    df = spark.createDataFrame(data)
    
    result = df.select("name")
    assert result.columns == ["name"]
    assert result.count() == 1


def test_select_multiple_columns(spark):
    """Test selecting multiple columns by name."""
    data = [{"name": "Alice", "age": 25, "city": "NYC"}]
    df = spark.createDataFrame(data)
    
    result = df.select("name", "age")
    assert result.columns == ["name", "age"]
    assert "city" not in result.columns


def test_select_with_column_objects(spark):
    """Test selecting columns using Column objects."""
    data = [{"name": "Alice", "age": 25}]
    df = spark.createDataFrame(data)
    
    result = df.select(F.col("name"), F.col("age"))
    assert result.columns == ["name", "age"]


def test_select_with_expressions(spark):
    """Test selecting columns with expressions."""
    data = [{"name": "Alice", "age": 25}]
    df = spark.createDataFrame(data)
    
    result = df.select(F.col("age") + 1)
    assert result.count() == 1


def test_select_star(spark):
    """Test selecting all columns with *."""
    data = [{"name": "Alice", "age": 25, "city": "NYC"}]
    df = spark.createDataFrame(data)
    
    result = df.select("*")
    assert len(result.columns) == 3


def test_select_with_alias(spark):
    """Test selecting columns with aliases."""
    data = [{"name": "Alice", "age": 25}]
    df = spark.createDataFrame(data)
    
    result = df.select(F.col("name").alias("person_name"))
    assert "person_name" in result.columns


def test_selectExpr_simple(spark):
    """Test selectExpr with simple expressions."""
    data = [{"name": "Alice", "age": 25}]
    df = spark.createDataFrame(data)
    
    result = df.selectExpr("name", "age + 1 as age_plus_one")
    assert "age_plus_one" in result.columns


def test_selectExpr_multiple(spark):
    """Test selectExpr with multiple expressions."""
    data = [{"x": 10, "y": 20}]
    df = spark.createDataFrame(data)
    
    result = df.selectExpr("x", "y", "x + y as sum")
    assert result.columns == ["x", "y", "sum"]


def test_drop_single_column(spark):
    """Test dropping a single column."""
    data = [{"name": "Alice", "age": 25, "city": "NYC"}]
    df = spark.createDataFrame(data)
    
    result = df.drop("city")
    assert "city" not in result.columns
    assert "name" in result.columns
    assert "age" in result.columns


def test_drop_multiple_columns(spark):
    """Test dropping multiple columns."""
    data = [{"name": "Alice", "age": 25, "city": "NYC"}]
    df = spark.createDataFrame(data)
    
    result = df.drop("age", "city")
    assert result.columns == ["name"]


def test_drop_with_column_object(spark):
    """Test dropping columns with Column objects."""
    data = [{"name": "Alice", "age": 25}]
    df = spark.createDataFrame(data)
    
    result = df.drop(F.col("age"))
    assert "age" not in result.columns


def test_withColumn_add_new(spark):
    """Test adding a new column with withColumn."""
    data = [{"name": "Alice", "age": 25}]
    df = spark.createDataFrame(data)
    
    result = df.withColumn("age_plus_one", F.col("age") + 1)
    assert "age_plus_one" in result.columns
    assert len(result.columns) == 3


def test_withColumn_replace_existing(spark):
    """Test replacing an existing column with withColumn."""
    data = [{"name": "Alice", "age": 25}]
    df = spark.createDataFrame(data)
    
    result = df.withColumn("age", F.col("age") + 1)
    assert len(result.columns) == 2
    rows = result.collect()
    assert rows[0]["age"] == 26


def test_withColumn_literal(spark):
    """Test withColumn with literal values."""
    data = [{"name": "Alice"}]
    df = spark.createDataFrame(data)
    
    result = df.withColumn("constant", F.lit(42))
    rows = result.collect()
    assert rows[0]["constant"] == 42


def test_withColumn_multiple_operations(spark):
    """Test chaining multiple withColumn operations."""
    data = [{"x": 10}]
    df = spark.createDataFrame(data)
    
    result = df.withColumn("y", F.col("x") * 2).withColumn("z", F.col("y") + 1)
    rows = result.collect()
    assert rows[0]["y"] == 20
    assert rows[0]["z"] == 21


def test_withColumnRenamed(spark):
    """Test renaming a column."""
    data = [{"old_name": "Alice", "age": 25}]
    df = spark.createDataFrame(data)
    
    result = df.withColumnRenamed("old_name", "new_name")
    assert "new_name" in result.columns
    assert "old_name" not in result.columns


def test_withColumnRenamed_nonexistent(spark):
    """Test renaming a nonexistent column (should keep original)."""
    data = [{"name": "Alice"}]
    df = spark.createDataFrame(data)
    
    result = df.withColumnRenamed("nonexistent", "new_name")
    assert "name" in result.columns


def test_select_empty_dataframe(spark):
    """Test selecting from empty DataFrame."""
    schema = MockStructType([
        MockStructField("name", StringType()),
        MockStructField("age", IntegerType())
    ])
    df = spark.createDataFrame([], schema)
    
    result = df.select("name")
    assert result.count() == 0
    assert result.columns == ["name"]


def test_drop_all_columns_except_one(spark):
    """Test dropping all columns except one."""
    data = [{"a": 1, "b": 2, "c": 3, "d": 4}]
    df = spark.createDataFrame(data)
    
    result = df.drop("b", "c", "d")
    assert result.columns == ["a"]


def test_select_preserves_row_count(spark):
    """Test that select preserves row count."""
    data = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
    df = spark.createDataFrame(data)
    
    result = df.select("name")
    assert result.count() == 2


def test_withColumn_complex_expression(spark):
    """Test withColumn with complex expressions."""
    data = [{"x": 10, "y": 5}]
    df = spark.createDataFrame(data)
    
    result = df.withColumn("result", (F.col("x") + F.col("y")) * 2)
    rows = result.collect()
    assert rows[0]["result"] == 30


def test_select_duplicate_columns(spark):
    """Test selecting the same column multiple times."""
    data = [{"name": "Alice", "age": 25}]
    df = spark.createDataFrame(data)
    
    # Selecting same column twice
    result = df.select("name", "name")
    # Should keep both occurrences
    assert len(result.columns) == 2

