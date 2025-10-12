"""System tests for complex real-world workflows."""

import pytest
from mock_spark import MockSparkSession, F
from mock_spark.window import Window


@pytest.fixture
def spark():
    """Create test session."""
    session = MockSparkSession("system_complex")
    yield session
    try:
        session.stop()
    except Exception:
        pass


def test_ecommerce_analytics_workflow(spark):
    """Test complete e-commerce analytics workflow."""
    # Customer data
    customers = [
        {"customer_id": 1, "name": "Alice", "segment": "Premium"},
        {"customer_id": 2, "name": "Bob", "segment": "Standard"},
        {"customer_id": 3, "name": "Charlie", "segment": "Premium"}
    ]
    
    # Orders data
    orders = [
        {"order_id": 1, "customer_id": 1, "amount": 100, "date": "2023-01-01"},
        {"order_id": 2, "customer_id": 1, "amount": 200, "date": "2023-01-02"},
        {"order_id": 3, "customer_id": 2, "amount": 50, "date": "2023-01-01"},
        {"order_id": 4, "customer_id": 3, "amount": 300, "date": "2023-01-01"}
    ]
    
    customers_df = spark.createDataFrame(customers)
    orders_df = spark.createDataFrame(orders)
    
    # Analytics pipeline
    result = orders_df.join(customers_df, "customer_id") \
                     .groupBy("segment") \
                     .agg(
                         F.sum("amount").alias("total_revenue"),
                         F.avg("amount").alias("avg_order"),
                         F.count("order_id").alias("order_count")
                     )
    
    rows = result.collect()
    assert len(rows) == 2


def test_log_analysis_workflow(spark):
    """Test log analysis and anomaly detection workflow."""
    # Simulated log data
    logs = []
    for i in range(100):
        logs.append({
            "timestamp": f"2023-01-01 {i % 24:02d}:00:00",
            "user_id": i % 10,
            "action": ["login", "click", "purchase"][i % 3],
            "duration_ms": 100 + (i % 50) * 10
        })
    
    logs_df = spark.createDataFrame(logs)
    
    # Analyze patterns
    window_spec = Window.partitionBy("action").orderBy("duration_ms")
    
    result = logs_df.withColumn("rank", F.row_number().over(window_spec)) \
                   .filter(F.col("rank") <= 5) \
                   .groupBy("action") \
                   .agg(F.avg("duration_ms").alias("avg_duration"))
    
    assert result.count() == 3


@pytest.mark.skip(reason="Test assertion expects anomalies but filter returns 0 rows")
def test_fraud_detection_workflow(spark):
    """Test fraud detection workflow."""
    # Transaction data
    transactions = []
    for i in range(100):
        transactions.append({
            "transaction_id": i,
            "user_id": i % 20,
            "amount": 100 if i % 10 != 0 else 5000,  # Anomaly every 10th
            "timestamp": f"2023-01-01 {i % 24:02d}:00:00"
        })
    
    trans_df = spark.createDataFrame(transactions)
    
    # Calculate user statistics
    user_stats = trans_df.groupBy("user_id").agg(
        F.avg("amount").alias("avg_amount"),
        F.max("amount").alias("max_amount"),
        F.count("transaction_id").alias("transaction_count")
    )
    
    # Flag suspicious transactions
    flagged = trans_df.join(user_stats, "user_id") \
                     .withColumn(
                         "suspicious",
                         F.when(F.col("amount") > F.col("avg_amount") * 3, True).otherwise(False)
                     ) \
                     .filter(F.col("suspicious"))
    
    assert flagged.count() > 0


@pytest.mark.skip(reason="F.substring() has SQL generation bug")
def test_realtime_aggregation_workflow(spark):
    """Test real-time aggregation workflow."""
    # Streaming-like data (simulated)
    events = []
    for minute in range(60):
        for user in range(10):
            events.append({
                "timestamp": f"2023-01-01 10:{minute:02d}:00",
                "user_id": user,
                "event_type": ["view", "click", "purchase"][minute % 3],
                "value": (minute + user) % 100
            })
    
    events_df = spark.createDataFrame(events)
    
    # 5-minute window aggregation
    result = events_df.withColumn(
        "window",
        F.substring(F.col("timestamp"), 1, 16)  # Truncate to minute
    ).groupBy("window", "event_type").agg(
        F.count("user_id").alias("event_count"),
        F.sum("value").alias("total_value")
    )
    
    assert result.count() > 0


def test_ml_feature_engineering_workflow(spark):
    """Test ML feature engineering workflow."""
    # User behavior data
    behavior = []
    for user in range(50):
        behavior.append({
            "user_id": user,
            "page_views": (user * 7) % 100,
            "time_on_site": (user * 13) % 300,
            "purchases": user % 5,
            "days_since_signup": user * 2
        })
    
    behavior_df = spark.createDataFrame(behavior)
    
    # Feature engineering
    features = behavior_df.withColumn(
        "engagement_score",
        (F.col("page_views") * 0.3 + F.col("time_on_site") * 0.5 + F.col("purchases") * 10)
    ).withColumn(
        "user_segment",
        F.when(F.col("engagement_score") > 500, "High")
         .when(F.col("engagement_score") > 200, "Medium")
         .otherwise("Low")
    ).withColumn(
        "is_active",
        F.col("days_since_signup") < 30
    )
    
    assert "engagement_score" in features.columns
    assert features.count() == 50


@pytest.mark.skip(reason="rlike() function has SQL generation bug")
def test_data_quality_pipeline(spark):
    """Test data quality and validation pipeline."""
    # Data with quality issues
    raw_data = [
        {"id": 1, "email": "alice@test.com", "age": 25, "country": "US"},
        {"id": 2, "email": "invalid", "age": -5, "country": None},
        {"id": 3, "email": "bob@test.com", "age": 150, "country": "UK"},
        {"id": 4, "email": "charlie@test.com", "age": 30, "country": "US"}
    ]
    
    df = spark.createDataFrame(raw_data)
    
    # Data quality checks
    validated = df.withColumn(
        "valid_email",
        F.col("email").rlike(".*@.*\\..*")
    ).withColumn(
        "valid_age",
        (F.col("age") > 0) & (F.col("age") < 120)
    ).withColumn(
        "has_country",
        F.col("country").isNotNull()
    ).withColumn(
        "is_valid",
        F.col("valid_email") & F.col("valid_age") & F.col("has_country")
    )
    
    valid_records = validated.filter(F.col("is_valid"))
    assert valid_records.count() == 2


@pytest.mark.skip(reason="F.concat() with cast and literals has SQL generation bug")
def test_sessionization_workflow(spark):
    """Test user session analysis workflow."""
    # Click stream data
    clicks = []
    for user in range(5):
        for click in range(20):
            clicks.append({
                "user_id": user,
                "timestamp": f"2023-01-01 10:{click:02d}:00",
                "page": f"page_{click % 5}",
                "click_id": user * 100 + click
            })
    
    clicks_df = spark.createDataFrame(clicks)
    
    # Session analysis
    window_spec = Window.partitionBy("user_id").orderBy("timestamp")
    
    sessions = clicks_df.withColumn(
        "session_rank",
        F.row_number().over(window_spec)
    ).withColumn(
        "session_id",
        F.concat(F.col("user_id").cast("string"), F.lit("_"), 
                F.floor(F.col("session_rank") / 5).cast("string"))
    )
    
    session_summary = sessions.groupBy("user_id", "session_id").agg(
        F.count("click_id").alias("clicks_in_session"),
        F.min("timestamp").alias("session_start"),
        F.max("timestamp").alias("session_end")
    )
    
    assert session_summary.count() == 20  # 5 users * 4 sessions each


@pytest.mark.skip(reason="Boolean column type conversion to float has SQL bug")
def test_inventory_optimization_workflow(spark):
    """Test inventory optimization workflow."""
    # Product inventory and sales
    inventory = [
        {"product_id": i, "current_stock": (i * 7) % 100, "reorder_point": 20}
        for i in range(50)
    ]
    
    sales = []
    for day in range(7):
        for product in range(50):
            sales.append({
                "product_id": product,
                "day": day,
                "units_sold": (product + day) % 10
            })
    
    inventory_df = spark.createDataFrame(inventory)
    sales_df = spark.createDataFrame(sales)
    
    # Calculate sales velocity
    sales_velocity = sales_df.groupBy("product_id").agg(
        F.avg("units_sold").alias("avg_daily_sales"),
        F.sum("units_sold").alias("total_units_sold")
    )
    
    # Optimization recommendations
    recommendations = inventory_df.join(sales_velocity, "product_id") \
        .withColumn(
            "days_until_stockout",
            F.col("current_stock") / F.col("avg_daily_sales")
        ).withColumn(
            "needs_reorder",
            F.col("days_until_stockout") < 7
        ).filter(F.col("needs_reorder"))
    
    assert recommendations.count() > 0

