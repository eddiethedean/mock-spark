"""Unit tests for DataFrame metadata operations."""

import pytest
from mock_spark import MockSparkSession, F
from mock_spark.spark_types import MockStructType, MockStructField, StringType, IntegerType, DoubleType


@pytest.fixture
def spark():
    """Create test session."""
    session = MockSparkSession("test_metadata")
    yield session
    try:
        session.stop()
    except Exception:
        pass


def test_schema_property(spark):
    """Test accessing DataFrame schema."""
    data = [{"name": "Alice", "age": 25}]
    df = spark.createDataFrame(data)
    
    assert isinstance(df.schema, MockStructType)
    assert len(df.schema.fields) == 2


def test_columns_property(spark):
    """Test accessing DataFrame columns."""
    data = [{"name": "Alice", "age": 25, "city": "NYC"}]
    df = spark.createDataFrame(data)
    
    assert df.columns == ["name", "age", "city"]


def test_dtypes_property(spark):
    """Test accessing DataFrame dtypes."""
    data = [{"name": "Alice", "age": 25}]
    df = spark.createDataFrame(data)
    
    dtypes = df.dtypes
    assert isinstance(dtypes, list)
    assert len(dtypes) == 2


def test_printSchema(spark, capsys):
    """Test printSchema output."""
    data = [{"name": "Alice", "age": 25}]
    df = spark.createDataFrame(data)
    
    df.printSchema()
    captured = capsys.readouterr()
    assert "root" in captured.out
    assert "name" in captured.out
    assert "age" in captured.out


def test_explain(spark, capsys):
    """Test explain shows query plan."""
    data = [{"value": 10}]
    df = spark.createDataFrame(data)
    
    result = df.filter(F.col("value") > 5)
    result.explain()
    
    captured = capsys.readouterr()
    # Should print something about the plan
    assert len(captured.out) > 0


def test_explain_extended(spark, capsys):
    """Test explain with extended=True."""
    data = [{"value": 10}]
    df = spark.createDataFrame(data)
    
    df.explain(extended=True)
    captured = capsys.readouterr()
    assert len(captured.out) > 0


def test_isEmpty(spark):
    """Test isEmpty on empty DataFrame."""
    df = spark.createDataFrame([])
    assert df.isEmpty()


def test_isEmpty_false(spark):
    """Test isEmpty on non-empty DataFrame."""
    data = [{"id": 1}]
    df = spark.createDataFrame(data)
    assert not df.isEmpty()


def test_columns_after_select(spark):
    """Test columns property after select operation."""
    data = [{"a": 1, "b": 2, "c": 3}]
    df = spark.createDataFrame(data)
    
    result = df.select("a", "b")
    assert result.columns == ["a", "b"]


def test_schema_after_withColumn(spark):
    """Test schema after withColumn."""
    data = [{"name": "Alice"}]
    df = spark.createDataFrame(data)
    
    result = df.withColumn("age", F.lit(25))
    assert len(result.schema.fields) == 2


def test_schema_field_names(spark):
    """Test accessing field names from schema."""
    data = [{"name": "Alice", "age": 25}]
    df = spark.createDataFrame(data)
    
    field_names = [f.name for f in df.schema.fields]
    assert "name" in field_names
    assert "age" in field_names


def test_schema_field_types(spark):
    """Test accessing field types from schema."""
    schema = MockStructType([
        MockStructField("name", StringType()),
        MockStructField("age", IntegerType())
    ])
    df = spark.createDataFrame([], schema)
    
    field_types = {f.name: f.dataType for f in df.schema.fields}
    assert isinstance(field_types["name"], StringType)
    assert isinstance(field_types["age"], IntegerType)


def test_inputFiles(spark):
    """Test inputFiles returns list of files."""
    data = [{"id": 1}]
    df = spark.createDataFrame(data)
    
    files = df.inputFiles()
    assert isinstance(files, list)


def test_isLocal(spark):
    """Test isLocal indicates if DataFrame is local."""
    data = [{"id": 1}]
    df = spark.createDataFrame(data)
    
    # In mock implementation, might always be True
    result = df.isLocal()
    assert isinstance(result, bool)


def test_isStreaming(spark):
    """Test isStreaming indicates if DataFrame is streaming."""
    data = [{"id": 1}]
    df = spark.createDataFrame(data)
    
    # Should be False for regular DataFrames
    assert not df.isStreaming()


def test_storageLevel(spark):
    """Test storageLevel property."""
    data = [{"id": 1}]
    df = spark.createDataFrame(data)
    
    # Should have some storage level
    storage_level = df.storageLevel
    assert storage_level is not None


def test_rdd_property(spark):
    """Test accessing RDD property."""
    data = [{"id": 1}]
    df = spark.createDataFrame(data)
    
    rdd = df.rdd
    assert rdd is not None


def test_toJSON(spark):
    """Test toJSON returns RDD of JSON strings."""
    data = [{"name": "Alice", "age": 25}]
    df = spark.createDataFrame(data)
    
    json_rdd = df.toJSON()
    json_list = json_rdd.collect()
    assert len(json_list) == 1
    assert "Alice" in json_list[0]


def test_createTempView(spark):
    """Test creating temporary view."""
    data = [{"id": 1}]
    df = spark.createDataFrame(data)
    
    df.createTempView("test_view")
    
    # Should be able to query the view
    result = spark.sql("SELECT * FROM test_view")
    assert result.count() == 1


def test_createOrReplaceTempView(spark):
    """Test creating or replacing temporary view."""
    data = [{"id": 1}]
    df = spark.createDataFrame(data)
    
    df.createOrReplaceTempView("test_view")
    
    # Replace with new data
    data2 = [{"id": 2}, {"id": 3}]
    df2 = spark.createDataFrame(data2)
    df2.createOrReplaceTempView("test_view")
    
    result = spark.sql("SELECT * FROM test_view")
    assert result.count() == 2


def test_createGlobalTempView(spark):
    """Test creating global temporary view."""
    data = [{"id": 1}]
    df = spark.createDataFrame(data)
    
    df.createGlobalTempView("global_test_view")
    
    # Should be accessible via global_temp database
    result = spark.sql("SELECT * FROM global_temp.global_test_view")
    assert result.count() == 1


def test_createOrReplaceGlobalTempView(spark):
    """Test creating or replacing global temporary view."""
    data = [{"id": 1}]
    df = spark.createDataFrame(data)
    
    df.createOrReplaceGlobalTempView("global_test_view")
    
    # Replace
    data2 = [{"id": 2}]
    df2 = spark.createDataFrame(data2)
    df2.createOrReplaceGlobalTempView("global_test_view")
    
    result = spark.sql("SELECT * FROM global_temp.global_test_view")
    assert result.count() == 1


def test_cache(spark):
    """Test cache method."""
    data = [{"id": 1}]
    df = spark.createDataFrame(data)
    
    cached_df = df.cache()
    assert cached_df is not None


def test_persist(spark):
    """Test persist method."""
    data = [{"id": 1}]
    df = spark.createDataFrame(data)
    
    persisted_df = df.persist()
    assert persisted_df is not None


def test_unpersist(spark):
    """Test unpersist method."""
    data = [{"id": 1}]
    df = spark.createDataFrame(data)
    
    df.cache()
    unpersisted_df = df.unpersist()
    assert unpersisted_df is not None


def test_checkpoint(spark):
    """Test checkpoint method."""
    data = [{"id": 1}]
    df = spark.createDataFrame(data)
    
    checkpointed_df = df.checkpoint()
    assert checkpointed_df is not None


def test_localCheckpoint(spark):
    """Test localCheckpoint method."""
    data = [{"id": 1}]
    df = spark.createDataFrame(data)
    
    checkpointed_df = df.localCheckpoint()
    assert checkpointed_df is not None


def test_withWatermark(spark):
    """Test withWatermark for streaming."""
    data = [{"timestamp": "2023-01-01", "value": 10}]
    df = spark.createDataFrame(data)
    
    result = df.withWatermark("timestamp", "10 minutes")
    assert result is not None


def test_hint(spark):
    """Test hint for query optimization."""
    data = [{"id": 1}]
    df = spark.createDataFrame(data)
    
    result = df.hint("broadcast")
    assert result is not None


def test_alias(spark):
    """Test alias for DataFrame."""
    data = [{"id": 1}]
    df = spark.createDataFrame(data)
    
    aliased = df.alias("my_table")
    assert aliased is not None

