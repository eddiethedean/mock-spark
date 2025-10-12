"""Unit tests for basic math operations."""

import pytest
from mock_spark import MockSparkSession, F


@pytest.fixture
def spark():
    """Create test session."""
    session = MockSparkSession("test_math")
    yield session
    try:
        session.stop()
    except Exception:
        pass


def test_column_addition(spark):
    """Test adding two columns."""
    data = [{"a": 10, "b": 5}]
    df = spark.createDataFrame(data)
    
    result = df.withColumn("sum", F.col("a") + F.col("b"))
    rows = result.collect()
    assert rows[0]["sum"] == 15


def test_column_subtraction(spark):
    """Test subtracting two columns."""
    data = [{"a": 10, "b": 3}]
    df = spark.createDataFrame(data)
    
    result = df.withColumn("diff", F.col("a") - F.col("b"))
    rows = result.collect()
    assert rows[0]["diff"] == 7


def test_column_multiplication(spark):
    """Test multiplying two columns."""
    data = [{"a": 4, "b": 5}]
    df = spark.createDataFrame(data)
    
    result = df.withColumn("product", F.col("a") * F.col("b"))
    rows = result.collect()
    assert rows[0]["product"] == 20


def test_column_division(spark):
    """Test dividing two columns."""
    data = [{"a": 20, "b": 4}]
    df = spark.createDataFrame(data)
    
    result = df.withColumn("quotient", F.col("a") / F.col("b"))
    rows = result.collect()
    assert rows[0]["quotient"] == 5


def test_literal_addition(spark):
    """Test adding literal to column."""
    data = [{"value": 10}]
    df = spark.createDataFrame(data)
    
    result = df.withColumn("result", F.col("value") + F.lit(5))
    rows = result.collect()
    assert rows[0]["result"] == 15


def test_literal_multiplication(spark):
    """Test multiplying column by literal."""
    data = [{"value": 10}]
    df = spark.createDataFrame(data)
    
    result = df.withColumn("result", F.col("value") * F.lit(2))
    rows = result.collect()
    assert rows[0]["result"] == 20


def test_comparison_greater_than(spark):
    """Test > comparison."""
    data = [{"value": 10}]
    df = spark.createDataFrame(data)
    
    result = df.filter(F.col("value") > 5)
    assert result.count() == 1


def test_comparison_less_than(spark):
    """Test < comparison."""
    data = [{"value": 10}]
    df = spark.createDataFrame(data)
    
    result = df.filter(F.col("value") < 15)
    assert result.count() == 1


def test_comparison_equals(spark):
    """Test == comparison."""
    data = [{"value": 10}, {"value": 20}]
    df = spark.createDataFrame(data)
    
    result = df.filter(F.col("value") == 10)
    assert result.count() == 1


def test_comparison_not_equals(spark):
    """Test != comparison."""
    data = [{"value": 10}, {"value": 20}]
    df = spark.createDataFrame(data)
    
    result = df.filter(F.col("value") != 10)
    assert result.count() == 1


def test_comparison_greater_equal(spark):
    """Test >= comparison."""
    data = [{"value": 10}, {"value": 20}]
    df = spark.createDataFrame(data)
    
    result = df.filter(F.col("value") >= 10)
    assert result.count() == 2


def test_comparison_less_equal(spark):
    """Test <= comparison."""
    data = [{"value": 10}, {"value": 20}]
    df = spark.createDataFrame(data)
    
    result = df.filter(F.col("value") <= 15)
    assert result.count() == 1


def test_multiple_arithmetic_operations(spark):
    """Test combining multiple arithmetic operations."""
    data = [{"a": 10, "b": 5, "c": 2}]
    df = spark.createDataFrame(data)
    
    result = df.withColumn("result", (F.col("a") + F.col("b")) * F.col("c"))
    rows = result.collect()
    assert rows[0]["result"] == 30


def test_arithmetic_with_multiple_rows(spark):
    """Test arithmetic on multiple rows."""
    data = [{"a": i, "b": i * 2} for i in range(1, 6)]
    df = spark.createDataFrame(data)
    
    result = df.withColumn("sum", F.col("a") + F.col("b"))
    rows = result.collect()
    assert rows[0]["sum"] == 3  # 1 + 2
    assert rows[4]["sum"] == 15  # 5 + 10

