"""Unit tests for window functions and operations."""

import pytest
from mock_spark import MockSparkSession, F
from mock_spark.window import Window


    @pytest.fixture
def spark():
    """Create test session."""
    session = MockSparkSession("test_window")
    yield session
    try:
        session.stop()
    except Exception:
        pass


def test_window_partition_by(spark):
    """Test window partitioning."""
    data = [
        {"category": "A", "value": 10},
        {"category": "A", "value": 20},
        {"category": "B", "value": 30}
    ]
    df = spark.createDataFrame(data)
    
    window_spec = Window.partitionBy("category")
    result = df.withColumn("sum_by_category", F.sum("value").over(window_spec))
    
    rows = result.collect()
    assert len(rows) == 3


def test_window_order_by(spark):
    """Test window ordering."""
    data = [
        {"id": 1, "value": 30},
        {"id": 2, "value": 10},
        {"id": 3, "value": 20}
    ]
    df = spark.createDataFrame(data)
    
    window_spec = Window.orderBy("value")
    result = df.withColumn("rank", F.row_number().over(window_spec))
    
    rows = result.collect()
    assert len(rows) == 3


def test_window_partition_and_order(spark):
    """Test window with both partition and order."""
    data = [
        {"category": "A", "value": 10},
        {"category": "A", "value": 20},
        {"category": "B", "value": 15}
    ]
    df = spark.createDataFrame(data)
    
    window_spec = Window.partitionBy("category").orderBy("value")
    result = df.withColumn("rank", F.row_number().over(window_spec))
    
    assert result.count() == 3


def test_window_rows_between(spark):
    """Test window frame with rows between."""
    data = [{"id": i, "value": i * 10} for i in range(1, 6)]
    df = spark.createDataFrame(data)
    
    window_spec = Window.orderBy("id").rowsBetween(-1, 1)
    result = df.withColumn("moving_avg", F.avg("value").over(window_spec))

        assert result.count() == 5


def test_window_range_between(spark):
    """Test window frame with range between."""
    data = [{"id": i, "value": i * 10} for i in range(1, 6)]
    df = spark.createDataFrame(data)
    
    window_spec = Window.orderBy("id").rangeBetween(-1, 1)
    result = df.withColumn("range_sum", F.sum("value").over(window_spec))

        assert result.count() == 5


def test_window_row_number(spark):
    """Test row_number window function."""
    data = [{"value": i} for i in [3, 1, 2]]
    df = spark.createDataFrame(data)
    
    window_spec = Window.orderBy("value")
    result = df.withColumn("row_num", F.row_number().over(window_spec))
    
    rows = result.orderBy("value").collect()
    assert rows[0]["row_num"] == 1
    assert rows[1]["row_num"] == 2
    assert rows[2]["row_num"] == 3


def test_window_rank(spark):
    """Test rank window function."""
    data = [{"value": 1}, {"value": 1}, {"value": 2}]
    df = spark.createDataFrame(data)
    
    window_spec = Window.orderBy("value")
    result = df.withColumn("rank", F.rank().over(window_spec))
    
    rows = result.collect()
    assert len(rows) == 3


def test_window_dense_rank(spark):
    """Test dense_rank window function."""
    data = [{"value": 1}, {"value": 1}, {"value": 2}]
    df = spark.createDataFrame(data)
    
    window_spec = Window.orderBy("value")
    result = df.withColumn("dense_rank", F.dense_rank().over(window_spec))
    
    rows = result.collect()
    assert len(rows) == 3


def test_window_lag(spark):
    """Test lag window function."""
    data = [{"id": i, "value": i * 10} for i in range(1, 5)]
    df = spark.createDataFrame(data)
    
    window_spec = Window.orderBy("id")
    result = df.withColumn("prev_value", F.lag("value", 1).over(window_spec))
    
    rows = result.orderBy("id").collect()
    assert rows[0]["prev_value"] is None
    assert rows[1]["prev_value"] == 10


def test_window_lead(spark):
    """Test lead window function."""
    data = [{"id": i, "value": i * 10} for i in range(1, 5)]
    df = spark.createDataFrame(data)
    
    window_spec = Window.orderBy("id")
    result = df.withColumn("next_value", F.lead("value", 1).over(window_spec))
    
    rows = result.orderBy("id").collect()
    assert rows[-1]["next_value"] is None
    assert rows[0]["next_value"] == 20


def test_window_cumulative_sum(spark):
    """Test cumulative sum with window."""
    data = [{"id": i, "value": 10} for i in range(1, 6)]
    df = spark.createDataFrame(data)
    
    window_spec = Window.orderBy("id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    result = df.withColumn("cumulative_sum", F.sum("value").over(window_spec))
    
    rows = result.orderBy("id").collect()
    assert rows[0]["cumulative_sum"] == 10
    assert rows[4]["cumulative_sum"] == 50


def test_window_partition_multiple_columns(spark):
    """Test window partitioning by multiple columns."""
    data = [
        {"region": "US", "state": "CA", "value": 10},
        {"region": "US", "state": "CA", "value": 20},
        {"region": "US", "state": "NY", "value": 30}
    ]
    df = spark.createDataFrame(data)
    
    window_spec = Window.partitionBy("region", "state")
    result = df.withColumn("total", F.sum("value").over(window_spec))
    
    assert result.count() == 3


def test_window_unbounded_following(spark):
    """Test window with unbounded following."""
    data = [{"id": i, "value": i * 10} for i in range(1, 5)]
    df = spark.createDataFrame(data)
    
    window_spec = Window.orderBy("id").rowsBetween(Window.currentRow, Window.unboundedFollowing)
    result = df.withColumn("remaining_sum", F.sum("value").over(window_spec))
    
    rows = result.orderBy("id").collect()
    assert rows[0]["remaining_sum"] == 100  # 10+20+30+40


def test_window_ntile(spark):
    """Test ntile window function."""
    data = [{"id": i} for i in range(1, 11)]
    df = spark.createDataFrame(data)
    
    window_spec = Window.orderBy("id")
    result = df.withColumn("quartile", F.ntile(4).over(window_spec))
    
    assert result.count() == 10


def test_window_percent_rank(spark):
    """Test percent_rank window function."""
    data = [{"value": i} for i in [1, 2, 3, 4, 5]]
    df = spark.createDataFrame(data)
    
    window_spec = Window.orderBy("value")
    result = df.withColumn("pct_rank", F.percent_rank().over(window_spec))
    
    rows = result.collect()
    assert len(rows) == 5


def test_window_cume_dist(spark):
    """Test cume_dist window function."""
    data = [{"value": i} for i in [1, 2, 3]]
    df = spark.createDataFrame(data)
    
    window_spec = Window.orderBy("value")
    result = df.withColumn("cume_dist", F.cume_dist().over(window_spec))
    
    rows = result.collect()
    assert len(rows) == 3


def test_window_first_value(spark):
    """Test first_value window function."""
    data = [{"id": i, "value": i * 10} for i in range(1, 5)]
    df = spark.createDataFrame(data)
    
    window_spec = Window.orderBy("id")
    result = df.withColumn("first_val", F.first("value").over(window_spec))
    
    rows = result.collect()
    assert len(rows) == 4


def test_window_last_value(spark):
    """Test last_value window function."""
    data = [{"id": i, "value": i * 10} for i in range(1, 5)]
    df = spark.createDataFrame(data)
    
    window_spec = Window.orderBy("id")
    result = df.withColumn("last_val", F.last("value").over(window_spec))
    
    rows = result.collect()
    assert len(rows) == 4
