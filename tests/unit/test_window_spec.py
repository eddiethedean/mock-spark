"""Unit tests for Window specification."""

import pytest
from mock_spark import MockSparkSession, F
from mock_spark.window import Window


@pytest.fixture
def spark():
    """Create test session."""
    session = MockSparkSession("test_window_spec")
    yield session
    try:
        session.stop()
    except Exception:
        pass


def test_window_partition_by_single_column(spark):
    """Test Window.partitionBy() with single column."""
    window_spec = Window.partitionBy("category")
    assert window_spec is not None


def test_window_partition_by_multiple_columns(spark):
    """Test Window.partitionBy() with multiple columns."""
    window_spec = Window.partitionBy("category", "type")
    assert window_spec is not None


def test_window_order_by_single_column(spark):
    """Test Window.orderBy() with single column."""
    window_spec = Window.orderBy("value")
    assert window_spec is not None


def test_window_order_by_multiple_columns(spark):
    """Test Window.orderBy() with multiple columns."""
    window_spec = Window.orderBy("value", "id")
    assert window_spec is not None


def test_window_partition_and_order(spark):
    """Test Window with both partition and order."""
    window_spec = Window.partitionBy("category").orderBy("value")
    assert window_spec is not None


def test_window_rows_between(spark):
    """Test Window.rowsBetween()."""
    window_spec = Window.rowsBetween(-1, 1)
    assert window_spec is not None


def test_window_range_between(spark):
    """Test Window.rangeBetween()."""
    window_spec = Window.rangeBetween(-10, 10)
    assert window_spec is not None


def test_window_unbounded_preceding(spark):
    """Test Window.unboundedPreceding constant."""
    assert hasattr(Window, "unboundedPreceding")


def test_window_unbounded_following(spark):
    """Test Window.unboundedFollowing constant."""
    assert hasattr(Window, "unboundedFollowing")


def test_window_current_row(spark):
    """Test Window.currentRow constant."""
    assert hasattr(Window, "currentRow")


def test_window_full_spec(spark):
    """Test complete window specification."""
    window_spec = Window.partitionBy("category") \
                        .orderBy("value") \
                        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    assert window_spec is not None


def test_window_with_column_objects(spark):
    """Test Window with Column objects."""
    window_spec = Window.partitionBy(F.col("category")).orderBy(F.col("value"))
    assert window_spec is not None


def test_row_number_over_window(spark):
    """Test row_number() with window spec."""
    data = [
        {"category": "A", "value": 10},
        {"category": "A", "value": 20},
        {"category": "B", "value": 30}
    ]
    df = spark.createDataFrame(data)
    
    window_spec = Window.partitionBy("category").orderBy("value")
    result = df.withColumn("row_num", F.row_number().over(window_spec))
    
    rows = result.collect()
    assert len(rows) == 3


def test_rank_over_window(spark):
    """Test rank() with window spec."""
    data = [{"value": i} for i in [1, 1, 2, 3]]
    df = spark.createDataFrame(data)
    
    window_spec = Window.orderBy("value")
    result = df.withColumn("rank", F.rank().over(window_spec))
    
    assert result.count() == 4


def test_dense_rank_over_window(spark):
    """Test dense_rank() with window spec."""
    data = [{"value": i} for i in [1, 1, 2, 3]]
    df = spark.createDataFrame(data)
    
    window_spec = Window.orderBy("value")
    result = df.withColumn("dense_rank", F.dense_rank().over(window_spec))
    
    assert result.count() == 4

