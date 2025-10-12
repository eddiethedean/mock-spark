"""Unit tests for DataFrame core methods."""

import pytest
from mock_spark import MockSparkSession, F


@pytest.fixture
def spark():
    """Create test session."""
    session = MockSparkSession("test_core_methods")
    yield session
    try:
        session.stop()
    except Exception:
        pass


def test_collect_returns_rows(spark):
    """Test collect() returns list of Row objects."""
    data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    df = spark.createDataFrame(data)
    
    rows = df.collect()
    assert len(rows) == 2
    assert rows[0]["id"] == 1
    assert rows[0]["name"] == "Alice"


def test_show_prints_output(spark, capsys):
    """Test show() prints DataFrame."""
    data = [{"id": 1, "name": "Alice"}]
    df = spark.createDataFrame(data)
    
    df.show()
    captured = capsys.readouterr()
    assert "Alice" in captured.out or "id" in captured.out


def test_show_with_num_rows(spark, capsys):
    """Test show() with specific number of rows."""
    data = [{"id": i} for i in range(10)]
    df = spark.createDataFrame(data)
    
    df.show(3)
    captured = capsys.readouterr()
    # Should show something
    assert len(captured.out) > 0


def test_show_truncate_false(spark, capsys):
    """Test show() with truncate=False."""
    data = [{"text": "a" * 100}]
    df = spark.createDataFrame(data)
    
    df.show(truncate=False)
    captured = capsys.readouterr()
    assert len(captured.out) > 0


def test_count_returns_integer(spark):
    """Test count() returns integer."""
    data = [{"id": i} for i in range(5)]
    df = spark.createDataFrame(data)
    
    count = df.count()
    assert isinstance(count, int)
    assert count == 5


def test_columns_returns_list(spark):
    """Test columns returns list of column names."""
    data = [{"id": 1, "name": "Alice", "age": 25}]
    df = spark.createDataFrame(data)
    
    cols = df.columns
    assert isinstance(cols, list)
    assert len(cols) == 3
    assert all(isinstance(c, str) for c in cols)


def test_schema_returns_struct_type(spark):
    """Test schema returns MockStructType."""
    data = [{"id": 1, "name": "Alice"}]
    df = spark.createDataFrame(data)
    
    from mock_spark.spark_types import MockStructType
    assert isinstance(df.schema, MockStructType)
    assert len(df.schema.fields) == 2


def test_toLocalIterator(spark):
    """Test toLocalIterator() returns iterator."""
    data = [{"id": i} for i in range(5)]
    df = spark.createDataFrame(data)
    
    iterator = df.toLocalIterator()
    rows = list(iterator)
    assert len(rows) == 5


def test_foreach_executes(spark):
    """Test foreach() executes function on each row."""
    data = [{"id": i} for i in range(3)]
    df = spark.createDataFrame(data)
    
    # Test that foreach doesn't raise an error
    df.foreach(lambda row: None)


def test_foreachPartition_executes(spark):
    """Test foreachPartition() executes on partitions."""
    data = [{"id": i} for i in range(5)]
    df = spark.createDataFrame(data)
    
    # Test that foreachPartition doesn't raise an error
    df.foreachPartition(lambda partition: None)


def test_createTempView_registers_view(spark):
    """Test createTempView() registers a view."""
    data = [{"id": 1, "value": "test"}]
    df = spark.createDataFrame(data)
    
    df.createTempView("test_view")
    
    # Should be queryable
    result = spark.sql("SELECT * FROM test_view")
    assert result.count() == 1


def test_createOrReplaceTempView_creates_view(spark):
    """Test createOrReplaceTempView() creates view."""
    data = [{"id": 1}]
    df = spark.createDataFrame(data)
    
    df.createOrReplaceTempView("replace_view")
    result = spark.sql("SELECT * FROM replace_view")
    assert result.count() == 1


def test_createOrReplaceTempView_replaces_view(spark):
    """Test createOrReplaceTempView() replaces existing view."""
    data1 = [{"id": 1}]
    df1 = spark.createDataFrame(data1)
    df1.createOrReplaceTempView("replace_view")
    
    data2 = [{"id": 2}, {"id": 3}]
    df2 = spark.createDataFrame(data2)
    df2.createOrReplaceTempView("replace_view")
    
    result = spark.sql("SELECT * FROM replace_view")
    assert result.count() == 2


def test_dtypes_returns_list_of_tuples(spark):
    """Test dtypes returns list of (name, type) tuples."""
    data = [{"id": 1, "name": "Alice"}]
    df = spark.createDataFrame(data)
    
    dtypes = df.dtypes
    assert isinstance(dtypes, list)
    assert len(dtypes) == 2
    # Each should be a tuple
    assert all(isinstance(dt, tuple) for dt in dtypes)


def test_printSchema_shows_structure(spark, capsys):
    """Test printSchema() displays schema structure."""
    data = [{"id": 1, "name": "Alice"}]
    df = spark.createDataFrame(data)
    
    df.printSchema()
    captured = capsys.readouterr()
    
    # Should contain field info
    assert "id" in captured.out
    assert "name" in captured.out


def test_explain_shows_plan(spark, capsys):
    """Test explain() shows execution plan."""
    data = [{"id": 1}]
    df = spark.createDataFrame(data)
    
    df.explain()
    captured = capsys.readouterr()
    
    assert len(captured.out) > 0


def test_cache_returns_dataframe(spark):
    """Test cache() returns DataFrame."""
    data = [{"id": i} for i in range(10)]
    df = spark.createDataFrame(data)
    
    cached = df.cache()
    assert cached is not None
    assert cached.count() == 10


def test_persist_returns_dataframe(spark):
    """Test persist() returns DataFrame."""
    data = [{"id": i} for i in range(10)]
    df = spark.createDataFrame(data)
    
    persisted = df.persist()
    assert persisted is not None
    assert persisted.count() == 10


def test_unpersist_returns_dataframe(spark):
    """Test unpersist() returns DataFrame."""
    data = [{"id": i} for i in range(10)]
    df = spark.createDataFrame(data)
    
    df.cache()
    unpersisted = df.unpersist()
    assert unpersisted is not None


def test_checkpoint_returns_dataframe(spark):
    """Test checkpoint() returns DataFrame."""
    data = [{"id": i} for i in range(10)]
    df = spark.createDataFrame(data)
    
    checkpointed = df.checkpoint()
    assert checkpointed is not None
    assert checkpointed.count() == 10


def test_rdd_property_returns_rdd(spark):
    """Test rdd property returns MockRDD."""
    data = [{"id": 1}, {"id": 2}]
    df = spark.createDataFrame(data)
    
    rdd = df.rdd
    assert rdd is not None


def test_write_property_returns_writer(spark):
    """Test write property returns writer."""
    data = [{"id": 1}]
    df = spark.createDataFrame(data)
    
    writer = df.write
    assert writer is not None

