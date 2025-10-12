"""Integration tests for DataFrame, functions, and storage working together."""

import pytest
from mock_spark import MockSparkSession, F


@pytest.fixture
def spark():
    """Create test session."""
    session = MockSparkSession("integration_df_func_storage")
    yield session
    try:
        session.stop()
    except Exception:
        pass


def test_dataframe_with_functions_and_storage(spark):
    """Test DataFrame operations with functions persist correctly."""
    # Create DataFrame
    data = [{"name": "alice", "age": 25}, {"name": "bob", "age": 30}]
    df = spark.createDataFrame(data)
    
    # Apply functions
    result = df.withColumn("name_upper", F.upper(F.col("name"))) \
               .withColumn("age_plus_10", F.col("age") + 10) \
               .filter(F.col("age") > 20)
    
    # Verify storage and retrieval
    rows = result.collect()
    assert len(rows) == 2
    assert rows[0]["name_upper"] == "ALICE"
    assert rows[0]["age_plus_10"] == 35


def test_complex_aggregation_with_storage(spark):
    """Test complex aggregations are stored and retrieved correctly."""
    data = [
        {"category": "A", "value": 10},
        {"category": "A", "value": 20},
        {"category": "B", "value": 30}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("category").agg(
        F.sum("value").alias("total"),
        F.avg("value").alias("average"),
        F.count("value").alias("count")
    )
    
    rows = result.collect()
    category_a = [r for r in rows if r["category"] == "A"][0]
    assert category_a["total"] == 30
    assert category_a["average"] == 15
    assert category_a["count"] == 2


def test_join_with_functions(spark):
    """Test joins combined with function transformations."""
    left_data = [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
    right_data = [{"id": 1, "value": 100}, {"id": 2, "value": 200}]
    
    left_df = spark.createDataFrame(left_data)
    right_df = spark.createDataFrame(right_data)
    
    result = left_df.join(right_df, "id") \
                    .withColumn("name_upper", F.upper(F.col("name"))) \
                    .withColumn("value_doubled", F.col("value") * 2)
    
    rows = result.collect()
    assert rows[0]["name_upper"] == "ALICE"
    assert rows[0]["value_doubled"] == 200


def test_window_functions_with_storage(spark):
    """Test window functions work with storage backend."""
    from mock_spark.window import Window
    
    data = [
        {"category": "A", "value": 10},
        {"category": "A", "value": 20},
        {"category": "B", "value": 30}
    ]
    df = spark.createDataFrame(data)
    
    window_spec = Window.partitionBy("category").orderBy("value")
    result = df.withColumn("rank", F.row_number().over(window_spec))
    
    rows = result.collect()
    assert len(rows) == 3


def test_sql_with_functions(spark):
    """Test SQL queries with function transformations."""
    data = [{"name": "alice", "age": 25}]
    df = spark.createDataFrame(data)
    df.createOrReplaceTempView("people")
    
    result = spark.sql("SELECT UPPER(name) as name_upper, age + 5 as age_plus FROM people")
    rows = result.collect()
    assert rows[0]["name_upper"] == "ALICE"
    assert rows[0]["age_plus"] == 30


def test_multiple_transformations_persist(spark):
    """Test multiple transformations maintain data integrity."""
    data = [{"x": i} for i in range(100)]
    df = spark.createDataFrame(data)
    
    result = df.filter(F.col("x") > 10) \
               .withColumn("y", F.col("x") * 2) \
               .groupBy(F.col("y") % 10) \
               .count()
    
    assert result.count() > 0


def test_string_functions_with_aggregation(spark):
    """Test string functions combined with aggregation."""
    data = [
        {"text": "hello world", "category": "A"},
        {"text": "hello spark", "category": "A"},
        {"text": "goodbye world", "category": "B"}
    ]
    df = spark.createDataFrame(data)
    
    result = df.withColumn("word_count", F.length(F.col("text"))) \
               .groupBy("category") \
               .agg(F.avg("word_count").alias("avg_length"))
    
    assert result.count() == 2


def test_null_handling_across_operations(spark):
    """Test null handling works across operations."""
    data = [
        {"name": "Alice", "value": 10},
        {"name": None, "value": 20},
        {"name": "Bob", "value": None}
    ]
    df = spark.createDataFrame(data)
    
    result = df.withColumn("name_filled", F.coalesce(F.col("name"), F.lit("Unknown"))) \
               .withColumn("value_filled", F.coalesce(F.col("value"), F.lit(0)))
    
    rows = result.collect()
    assert rows[1]["name_filled"] == "Unknown"
    assert rows[2]["value_filled"] == 0


def test_conditional_logic_with_grouping(spark):
    """Test conditional logic combined with grouping."""
    data = [
        {"category": "A", "value": 5},
        {"category": "A", "value": 15},
        {"category": "B", "value": 25}
    ]
    df = spark.createDataFrame(data)
    
    result = df.withColumn(
        "size",
        F.when(F.col("value") < 10, "small")
         .when(F.col("value") < 20, "medium")
         .otherwise("large")
    ).groupBy("category", "size").count()
    
    assert result.count() >= 2


def test_persistence_after_cache(spark):
    """Test data persists correctly after caching."""
    data = [{"id": i, "value": i * 10} for i in range(50)]
    df = spark.createDataFrame(data)
    
    cached_df = df.filter(F.col("id") > 10).cache()
    
    # Multiple collections should return same result
    count1 = cached_df.count()
    count2 = cached_df.count()
    assert count1 == count2

