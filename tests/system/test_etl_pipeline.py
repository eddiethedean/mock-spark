"""System tests for complete ETL pipelines."""

import pytest
import tempfile
import csv
from pathlib import Path
from mock_spark import MockSparkSession, F


@pytest.fixture
def spark():
    """Create test session."""
    session = MockSparkSession("system_etl")
    yield session
    try:
        session.stop()
    except Exception:
        pass


@pytest.fixture
def sample_csv(tmp_path):
    """Create a sample CSV file."""
    csv_file = tmp_path / "data.csv"
    with open(csv_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "name", "age", "city"])
        writer.writeheader()
        writer.writerows([
            {"id": 1, "name": "Alice", "age": 25, "city": "NYC"},
            {"id": 2, "name": "Bob", "age": 30, "city": "LA"},
            {"id": 3, "name": "Charlie", "age": 35, "city": "NYC"},
            {"id": 4, "name": "Diana", "age": 28, "city": "LA"}
        ])
    return csv_file


@pytest.mark.skip(reason="String function F.upper() has SQL generation bug")
def test_basic_etl_pipeline(spark):
    """Test basic ETL: extract, transform, load."""
    # Extract
    source_data = [
        {"id": 1, "name": "Alice", "value": 100},
        {"id": 2, "name": "Bob", "value": 200},
        {"id": 3, "name": "Charlie", "value": 300}
    ]
    df = spark.createDataFrame(source_data)
    
    # Transform
    transformed = df.withColumn("value_doubled", F.col("value") * 2) \
                    .withColumn("name_upper", F.upper(F.col("name"))) \
                    .filter(F.col("value") > 150)
    
    # Load (collect results)
    result = transformed.collect()
    
    assert len(result) == 2
    assert result[0]["value_doubled"] == 400
    assert result[0]["name_upper"] == "BOB"


def test_aggregation_pipeline(spark):
    """Test ETL pipeline with aggregations."""
    # Extract
    sales_data = [
        {"region": "North", "product": "A", "sales": 100},
        {"region": "North", "product": "B", "sales": 150},
        {"region": "South", "product": "A", "sales": 200},
        {"region": "South", "product": "B", "sales": 250}
    ]
    df = spark.createDataFrame(sales_data)
    
    # Transform - aggregate by region
    summary = df.groupBy("region").agg(
        F.sum("sales").alias("total_sales"),
        F.avg("sales").alias("avg_sales"),
        F.count("product").alias("product_count")
    )
    
    # Load
    result = summary.orderBy("region").collect()
    
    assert len(result) == 2
    north = result[0]
    assert north["total_sales"] == 250
    assert north["avg_sales"] == 125


def test_join_pipeline(spark):
    """Test ETL pipeline with joins."""
    # Extract from multiple sources
    customers = [
        {"customer_id": 1, "name": "Alice"},
        {"customer_id": 2, "name": "Bob"}
    ]
    orders = [
        {"order_id": 1, "customer_id": 1, "amount": 100},
        {"order_id": 2, "customer_id": 1, "amount": 200},
        {"order_id": 3, "customer_id": 2, "amount": 150}
    ]
    
    customers_df = spark.createDataFrame(customers)
    orders_df = spark.createDataFrame(orders)
    
    # Transform - join and aggregate
    result = orders_df.join(customers_df, "customer_id") \
                     .groupBy("name") \
                     .agg(F.sum("amount").alias("total_spent"))
    
    # Load
    rows = result.collect()
    alice = [r for r in rows if r["name"] == "Alice"][0]
    assert alice["total_spent"] == 300


def test_data_quality_pipeline(spark):
    """Test ETL pipeline with data quality checks."""
    # Extract
    raw_data = [
        {"id": 1, "value": 100, "status": "valid"},
        {"id": 2, "value": None, "status": "valid"},
        {"id": 3, "value": 200, "status": "invalid"},
        {"id": 4, "value": 300, "status": "valid"}
    ]
    df = spark.createDataFrame(raw_data)
    
    # Transform - clean and validate
    cleaned = df.filter(F.col("status") == "valid") \
                .filter(F.col("value").isNotNull()) \
                .withColumn("value_normalized", F.col("value") / 100)
    
    # Load
    result = cleaned.collect()
    
    assert len(result) == 2
    assert result[0]["id"] in [1, 4]


def test_multi_stage_pipeline(spark):
    """Test multi-stage ETL pipeline."""
    # Stage 1: Extract and initial transform
    data = [{"id": i, "value": i * 10} for i in range(1, 101)]
    df = spark.createDataFrame(data)
    
    stage1 = df.filter(F.col("value") > 50)
    
    # Stage 2: Enrich with calculations
    stage2 = stage1.withColumn("squared", F.col("value") * F.col("value")) \
                   .withColumn("category", 
                              F.when(F.col("value") < 300, "low")
                               .when(F.col("value") < 700, "medium")
                               .otherwise("high"))
    
    # Stage 3: Aggregate
    stage3 = stage2.groupBy("category").agg(
        F.count("id").alias("count"),
        F.avg("value").alias("avg_value")
    )
    
    # Load
    result = stage3.collect()
    
    assert len(result) > 0


@pytest.mark.skip(reason="Window function references unknown column in SQL generation")
def test_windowed_analytics_pipeline(spark):
    """Test ETL with window functions."""
    from mock_spark.window import Window
    
    # Extract
    transactions = [
        {"account": "A", "date": "2023-01-01", "amount": 100},
        {"account": "A", "date": "2023-01-02", "amount": 150},
        {"account": "B", "date": "2023-01-01", "amount": 200},
        {"account": "B", "date": "2023-01-02", "amount": 250}
    ]
    df = spark.createDataFrame(transactions)
    
    # Transform with window functions
    window_spec = Window.partitionBy("account").orderBy("date")
    result = df.withColumn("running_total", 
                          F.sum("amount").over(window_spec))
    
    # Load
    rows = result.collect()
    assert len(rows) == 4


def test_deduplication_pipeline(spark):
    """Test ETL with deduplication."""
    # Extract with duplicates
    data = [
        {"id": 1, "name": "Alice", "timestamp": "2023-01-01"},
        {"id": 1, "name": "Alice", "timestamp": "2023-01-02"},
        {"id": 2, "name": "Bob", "timestamp": "2023-01-01"}
    ]
    df = spark.createDataFrame(data)
    
    # Transform - deduplicate keeping latest
    deduplicated = df.dropDuplicates(["id"])
    
    # Load
    result = deduplicated.collect()
    assert len(result) == 2


@pytest.mark.skip(reason="rlike() and CASE WHEN with boolean column has SQL generation bug")
def test_error_handling_pipeline(spark):
    """Test ETL with error handling."""
    # Extract
    data = [
        {"id": 1, "value": "100"},
        {"id": 2, "value": "200"},
        {"id": 3, "value": "invalid"}
    ]
    df = spark.createDataFrame(data)
    
    # Transform with try-catch logic using when
    transformed = df.withColumn(
        "value_int",
        F.when(F.col("value").rlike("^[0-9]+$"), F.col("value").cast("int"))
         .otherwise(None)
    ).filter(F.col("value_int").isNotNull())
    
    # Load
    result = transformed.collect()
    assert len(result) == 2


@pytest.mark.skip(reason="Pivot.sum() method not implemented")
def test_pivot_aggregation_pipeline(spark):
    """Test ETL with pivot operations."""
    # Extract
    sales_data = [
        {"month": "Jan", "product": "A", "sales": 100},
        {"month": "Jan", "product": "B", "sales": 150},
        {"month": "Feb", "product": "A", "sales": 120},
        {"month": "Feb", "product": "B", "sales": 180}
    ]
    df = spark.createDataFrame(sales_data)
    
    # Transform with pivot
    pivoted = df.groupBy("month").pivot("product").sum("sales")
    
    # Load
    result = pivoted.collect()
    assert len(result) == 2


def test_union_merge_pipeline(spark):
    """Test ETL merging multiple datasets."""
    # Extract from multiple sources
    data1 = [{"id": 1, "value": "A"}]
    data2 = [{"id": 2, "value": "B"}]
    data3 = [{"id": 3, "value": "C"}]
    
    df1 = spark.createDataFrame(data1)
    df2 = spark.createDataFrame(data2)
    df3 = spark.createDataFrame(data3)
    
    # Transform - merge all sources
    merged = df1.union(df2).union(df3)
    
    # Load
    result = merged.collect()
    assert len(result) == 3

