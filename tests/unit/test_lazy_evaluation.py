"""Unit tests for lazy evaluation functionality."""

import pytest
from mock_spark import MockSparkSession, F


@pytest.fixture
def spark():
    """Create test session."""
    session = MockSparkSession("test_lazy")
    yield session
    try:
        session.stop()
    except Exception:
        pass


def test_lazy_transformation_chaining(spark):
    """Test chaining multiple transformations lazily."""
    data = [{"value": i} for i in range(10)]
    df = spark.createDataFrame(data)
    
    # Chain transformations - should not execute yet
    result = df.filter(F.col("value") > 5) \
               .withColumn("doubled", F.col("value") * 2) \
               .select("value", "doubled")
    
    # Only execute on action
    count = result.count()
    assert count == 4


def test_lazy_filter_pushdown(spark):
    """Test that filters are pushed down in lazy evaluation."""
    data = [{"id": i, "value": i * 10} for i in range(100)]
    df = spark.createDataFrame(data)
    
    # Multiple filters should be combined
    result = df.filter(F.col("id") > 10) \
               .filter(F.col("id") < 50) \
               .filter(F.col("value") > 200)
    
    assert result.count() < 100


def test_lazy_column_pruning(spark):
    """Test column pruning in lazy evaluation."""
    data = [{"a": 1, "b": 2, "c": 3, "d": 4}]
    df = spark.createDataFrame(data)
    
    # Select only needed columns
    result = df.select("a", "b")
    
    assert len(result.columns) == 2


def test_lazy_join_optimization(spark):
    """Test join optimization in lazy evaluation."""
    left_data = [{"id": i} for i in range(10)]
    right_data = [{"id": i, "value": i * 10} for i in range(5)]
    
    left_df = spark.createDataFrame(left_data)
    right_df = spark.createDataFrame(right_data)
    
    # Join with subsequent filter
    result = left_df.join(right_df, "id") \
                   .filter(F.col("value") > 20)
    
    assert result.count() < 10


def test_lazy_aggregation(spark):
    """Test lazy evaluation with aggregations."""
    data = [{"category": "A", "value": 10}, {"category": "A", "value": 20}]
    df = spark.createDataFrame(data)
    
    # Aggregation should be lazy
    result = df.groupBy("category").agg(F.sum("value").alias("total"))
    
    # Execute on action
    rows = result.collect()
    assert rows[0]["total"] == 30


def test_lazy_distinct(spark):
    """Test lazy distinct operation."""
    data = [{"value": 1}, {"value": 1}, {"value": 2}]
    df = spark.createDataFrame(data)
    
    result = df.select("value").distinct()
    
    assert result.count() == 2


def test_lazy_order_by(spark):
    """Test lazy orderBy operation."""
    data = [{"id": 3}, {"id": 1}, {"id": 2}]
    df = spark.createDataFrame(data)
    
    result = df.orderBy("id")
    
    rows = result.collect()
    assert rows[0]["id"] == 1


def test_lazy_union(spark):
    """Test lazy union operation."""
    data1 = [{"id": 1}]
    data2 = [{"id": 2}]
    
    df1 = spark.createDataFrame(data1)
    df2 = spark.createDataFrame(data2)
    
    result = df1.union(df2)
    
    assert result.count() == 2


def test_lazy_limit(spark):
    """Test lazy limit operation."""
    data = [{"id": i} for i in range(100)]
    df = spark.createDataFrame(data)
    
    result = df.limit(10)
    
    assert result.count() == 10


def test_lazy_sample(spark):
    """Test lazy sample operation."""
    data = [{"id": i} for i in range(100)]
    df = spark.createDataFrame(data)
    
    result = df.sample(fraction=0.1, seed=42)
    
    assert result.count() < 100


def test_lazy_repartition(spark):
    """Test lazy repartition operation."""
    data = [{"id": i} for i in range(50)]
    df = spark.createDataFrame(data)
    
    result = df.repartition(4)
    
    assert result.count() == 50


def test_lazy_cache(spark):
    """Test lazy cache operation."""
    data = [{"id": i} for i in range(10)]
    df = spark.createDataFrame(data)
    
    cached = df.filter(F.col("id") > 5).cache()
    
    # Multiple actions should reuse cached data
    count1 = cached.count()
    count2 = cached.count()
    assert count1 == count2


def test_lazy_window_function(spark):
    """Test lazy window function evaluation."""
    from mock_spark.window import Window
    
    data = [{"category": "A", "value": 10}, {"category": "A", "value": 20}]
    df = spark.createDataFrame(data)
    
    window_spec = Window.partitionBy("category").orderBy("value")
    result = df.withColumn("rank", F.row_number().over(window_spec))
    
    assert result.count() == 2


def test_lazy_explain(spark, capsys):
    """Test explain shows execution plan without executing."""
    data = [{"id": i} for i in range(10)]
    df = spark.createDataFrame(data)
    
    result = df.filter(F.col("id") > 5).select("id")
    result.explain()
    
    captured = capsys.readouterr()
    assert len(captured.out) > 0


def test_lazy_multiple_actions(spark):
    """Test that multiple actions don't recompute."""
    data = [{"id": i} for i in range(10)]
    df = spark.createDataFrame(data)
    
    filtered = df.filter(F.col("id") > 5).cache()
    
    count = filtered.count()
    rows = filtered.collect()
    
    assert count == len(rows)


def test_lazy_with_sql(spark):
    """Test lazy evaluation with SQL queries."""
    data = [{"id": i, "value": i * 10} for i in range(10)]
    df = spark.createDataFrame(data)
    df.createOrReplaceTempView("data")
    
    result = spark.sql("SELECT * FROM data WHERE id > 5")
    
    assert result.count() == 4


def test_lazy_transformation_lineage(spark):
    """Test transformation lineage is tracked."""
    data = [{"value": i} for i in range(5)]
    df = spark.createDataFrame(data)
    
    result = df.filter(F.col("value") > 2) \
               .withColumn("doubled", F.col("value") * 2) \
               .select("doubled")
    
    # Should maintain lineage
    assert result.count() == 2

