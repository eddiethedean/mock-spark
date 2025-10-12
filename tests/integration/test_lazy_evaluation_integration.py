"""Integration tests for lazy evaluation with storage."""

import pytest
from mock_spark import MockSparkSession, F


@pytest.fixture
def spark():
    """Create test session."""
    session = MockSparkSession("integration_lazy")
    yield session
    try:
        session.stop()
    except Exception:
        pass


def test_lazy_with_storage_persistence(spark):
    """Test lazy evaluation persists correctly to storage."""
    data = [{"id": i, "value": i * 10} for i in range(100)]
    df = spark.createDataFrame(data)
    
    # Chain transformations
    result = df.filter(F.col("id") > 50) \
               .withColumn("doubled", F.col("value") * 2) \
               .select("id", "doubled")
    
    # Materialize and check
    rows = result.collect()
    assert len(rows) == 49
    assert rows[0]["doubled"] == 1020  # 51 * 10 * 2


def test_lazy_aggregation_with_storage(spark):
    """Test lazy aggregation execution with storage."""
    data = [
        {"category": "A", "value": i} for i in range(10)
    ] + [
        {"category": "B", "value": i} for i in range(10, 20)
    ]
    df = spark.createDataFrame(data)
    
    result = df.filter(F.col("value") > 5) \
               .groupBy("category") \
               .agg(F.sum("value").alias("total"))
    
    rows = result.collect()
    assert len(rows) == 2


def test_lazy_join_optimization_storage(spark):
    """Test lazy join optimization with storage backend."""
    left_data = [{"id": i, "left_val": i} for i in range(100)]
    right_data = [{"id": i, "right_val": i * 2} for i in range(50)]
    
    left_df = spark.createDataFrame(left_data)
    right_df = spark.createDataFrame(right_data)
    
    # Filter before join
    result = left_df.filter(F.col("id") < 30) \
                   .join(right_df, "id") \
                   .select("id", "left_val", "right_val")
    
    assert result.count() == 30


def test_lazy_with_cache_storage(spark):
    """Test lazy evaluation with caching and storage."""
    data = [{"id": i, "value": i * 10} for i in range(100)]
    df = spark.createDataFrame(data)
    
    # Cache intermediate result
    filtered = df.filter(F.col("id") > 25).cache()
    
    # Multiple operations on cached data
    count1 = filtered.count()
    sum_result = filtered.agg(F.sum("value")).collect()
    count2 = filtered.count()
    
    assert count1 == count2
    assert count1 == 74


def test_lazy_window_with_storage(spark):
    """Test lazy window functions with storage."""
    from mock_spark.window import Window
    
    data = [
        {"category": "A", "value": i} for i in range(10)
    ] + [
        {"category": "B", "value": i} for i in range(10, 20)
    ]
    df = spark.createDataFrame(data)
    
    window_spec = Window.partitionBy("category").orderBy("value")
    result = df.withColumn("rank", F.row_number().over(window_spec)) \
               .filter(F.col("rank") <= 3)
    
    assert result.count() == 6


def test_lazy_union_with_storage(spark):
    """Test lazy union with storage backend."""
    data1 = [{"id": i} for i in range(50)]
    data2 = [{"id": i} for i in range(50, 100)]
    data3 = [{"id": i} for i in range(100, 150)]
    
    df1 = spark.createDataFrame(data1)
    df2 = spark.createDataFrame(data2)
    df3 = spark.createDataFrame(data3)
    
    result = df1.union(df2).union(df3).filter(F.col("id") > 75)
    
    assert result.count() == 74


def test_lazy_distinct_with_storage(spark):
    """Test lazy distinct operation with storage."""
    data = [{"value": i % 10} for i in range(100)]
    df = spark.createDataFrame(data)
    
    result = df.select("value").distinct().orderBy("value")
    
    rows = result.collect()
    assert len(rows) == 10


def test_lazy_complex_pipeline_storage(spark):
    """Test complex lazy pipeline with storage."""
    data = [
        {"region": "US", "category": "A", "value": i}
        for i in range(50)
    ] + [
        {"region": "EU", "category": "B", "value": i}
        for i in range(50, 100)
    ]
    df = spark.createDataFrame(data)
    
    result = df.filter(F.col("value") > 25) \
               .withColumn("doubled", F.col("value") * 2) \
               .groupBy("region") \
               .agg(F.sum("doubled").alias("total")) \
               .orderBy("region")
    
    rows = result.collect()
    assert len(rows) == 2


def test_lazy_sql_with_storage(spark):
    """Test lazy SQL execution with storage."""
    data = [{"id": i, "value": i * 10} for i in range(100)]
    df = spark.createDataFrame(data)
    df.createOrReplaceTempView("data")
    
    result = spark.sql("""
        SELECT id, value * 2 as doubled
        FROM data
        WHERE id > 50
        ORDER BY id
        LIMIT 10
    """)
    
    rows = result.collect()
    assert len(rows) == 10
    assert rows[0]["id"] == 51


def test_lazy_self_join_storage(spark):
    """Test lazy self-join with storage."""
    data = [{"id": i, "parent_id": i - 1 if i > 0 else None} for i in range(20)]
    df = spark.createDataFrame(data)
    
    parent_df = df.alias("parent")
    child_df = df.alias("child")
    
    result = child_df.join(
        parent_df,
        child_df["parent_id"] == parent_df["id"],
        "left"
    ).select(
        child_df["id"].alias("child_id"),
        parent_df["id"].alias("parent_id")
    )
    
    assert result.count() == 20

