"""System tests for complex analytics workflows."""

import pytest
from mock_spark import MockSparkSession, F
from mock_spark.window import Window


@pytest.fixture
def spark():
    """Create test session."""
    session = MockSparkSession("system_analytics")
    yield session
    try:
        session.stop()
    except Exception:
        pass


def test_customer_segmentation_workflow(spark):
    """Test customer segmentation analytics workflow."""
    # Customer transaction data
    transactions = [
        {"customer_id": 1, "amount": 100, "date": "2023-01-01"},
        {"customer_id": 1, "amount": 200, "date": "2023-01-02"},
        {"customer_id": 2, "amount": 50, "date": "2023-01-01"},
        {"customer_id": 3, "amount": 300, "date": "2023-01-01"}
    ]
    df = spark.createDataFrame(transactions)
    
    # Calculate customer lifetime value
    customer_value = df.groupBy("customer_id").agg(
        F.sum("amount").alias("total_spent"),
        F.count("amount").alias("transaction_count")
    )
    
    # Segment customers
    segmented = customer_value.withColumn(
        "segment",
        F.when(F.col("total_spent") > 250, "High Value")
         .when(F.col("total_spent") > 100, "Medium Value")
         .otherwise("Low Value")
    )
    
    result = segmented.collect()
    assert len(result) == 3


def test_time_series_analysis_workflow(spark):
    """Test time series analysis workflow."""
    # Daily sales data
    sales = [
        {"date": "2023-01-01", "revenue": 1000},
        {"date": "2023-01-02", "revenue": 1200},
        {"date": "2023-01-03", "revenue": 1100},
        {"date": "2023-01-04", "revenue": 1300}
    ]
    df = spark.createDataFrame(sales)
    
    # Calculate moving average
    window_spec = Window.orderBy("date").rowsBetween(-1, 0)
    result = df.withColumn("moving_avg", F.avg("revenue").over(window_spec))
    
    assert result.count() == 4


def test_funnel_analysis_workflow(spark):
    """Test conversion funnel analysis workflow."""
    # User events
    events = [
        {"user_id": 1, "event": "page_view"},
        {"user_id": 1, "event": "add_to_cart"},
        {"user_id": 1, "event": "purchase"},
        {"user_id": 2, "event": "page_view"},
        {"user_id": 2, "event": "add_to_cart"},
        {"user_id": 3, "event": "page_view"}
    ]
    df = spark.createDataFrame(events)
    
    # Count users at each funnel stage
    funnel = df.groupBy("event").agg(
        F.countDistinct("user_id").alias("unique_users")
    )
    
    result = funnel.collect()
    assert len(result) == 3


def test_cohort_analysis_workflow(spark):
    """Test cohort analysis workflow."""
    # User cohorts
    users = [
        {"user_id": 1, "signup_month": "2023-01", "activity_month": "2023-01"},
        {"user_id": 1, "signup_month": "2023-01", "activity_month": "2023-02"},
        {"user_id": 2, "signup_month": "2023-01", "activity_month": "2023-01"},
        {"user_id": 3, "signup_month": "2023-02", "activity_month": "2023-02"}
    ]
    df = spark.createDataFrame(users)
    
    # Calculate retention by cohort
    retention = df.groupBy("signup_month", "activity_month").agg(
        F.countDistinct("user_id").alias("active_users")
    )
    
    result = retention.collect()
    assert len(result) >= 3


def test_recommendation_workflow(spark):
    """Test product recommendation workflow."""
    # Product purchases
    purchases = [
        {"user_id": 1, "product": "A"},
        {"user_id": 1, "product": "B"},
        {"user_id": 2, "product": "A"},
        {"user_id": 2, "product": "C"},
        {"user_id": 3, "product": "B"}
    ]
    df = spark.createDataFrame(purchases)
    
    # Find product co-occurrences
    user_products = df.groupBy("user_id").agg(
        F.collect_set("product").alias("products")
    )
    
    result = user_products.collect()
    assert len(result) == 3


def test_ab_test_analysis_workflow(spark):
    """Test A/B test analysis workflow."""
    # Experiment results
    results = [
        {"user_id": 1, "variant": "A", "converted": True},
        {"user_id": 2, "variant": "A", "converted": False},
        {"user_id": 3, "variant": "B", "converted": True},
        {"user_id": 4, "variant": "B", "converted": True}
    ]
    df = spark.createDataFrame(results)
    
    # Calculate conversion rates
    conversion_rates = df.groupBy("variant").agg(
        F.count("user_id").alias("total_users"),
        F.sum(F.when(F.col("converted"), 1).otherwise(0)).alias("conversions")
    ).withColumn("conversion_rate", F.col("conversions") / F.col("total_users"))
    
    result = conversion_rates.collect()
    assert len(result) == 2


def test_churn_prediction_workflow(spark):
    """Test churn prediction feature engineering workflow."""
    # User activity
    activity = [
        {"user_id": 1, "days_since_last_activity": 5, "total_sessions": 20},
        {"user_id": 2, "days_since_last_activity": 30, "total_sessions": 5},
        {"user_id": 3, "days_since_last_activity": 2, "total_sessions": 50}
    ]
    df = spark.createDataFrame(activity)
    
    # Create churn risk features
    features = df.withColumn(
        "churn_risk",
        F.when((F.col("days_since_last_activity") > 14) & (F.col("total_sessions") < 10), "High")
         .when(F.col("days_since_last_activity") > 7, "Medium")
         .otherwise("Low")
    )
    
    result = features.collect()
    assert len(result) == 3


def test_revenue_attribution_workflow(spark):
    """Test multi-touch attribution workflow."""
    # Customer touchpoints
    touchpoints = [
        {"customer_id": 1, "channel": "email", "order": 1},
        {"customer_id": 1, "channel": "social", "order": 2},
        {"customer_id": 1, "channel": "search", "order": 3, "revenue": 100}
    ]
    df = spark.createDataFrame(touchpoints)
    
    # Simple last-touch attribution
    attribution = df.filter(F.col("revenue").isNotNull()) \
                    .select("customer_id", "channel", "revenue")
    
    result = attribution.collect()
    assert len(result) == 1


def test_inventory_optimization_workflow(spark):
    """Test inventory optimization analytics workflow."""
    # Inventory data
    inventory = [
        {"product": "A", "sold": 100, "stock": 50, "reorder_point": 30},
        {"product": "B", "sold": 50, "stock": 10, "reorder_point": 20},
        {"product": "C", "sold": 200, "stock": 100, "reorder_point": 80}
    ]
    df = spark.createDataFrame(inventory)
    
    # Identify products needing reorder
    reorder = df.withColumn(
        "needs_reorder",
        F.col("stock") < F.col("reorder_point")
    ).filter(F.col("needs_reorder"))
    
    result = reorder.collect()
    assert len(result) == 1


def test_geographic_analysis_workflow(spark):
    """Test geographic sales analysis workflow."""
    # Regional sales
    sales = [
        {"region": "North", "city": "NYC", "sales": 1000},
        {"region": "North", "city": "Boston", "sales": 800},
        {"region": "South", "city": "Miami", "sales": 1200},
        {"region": "South", "city": "Atlanta", "sales": 900}
    ]
    df = spark.createDataFrame(sales)
    
    # Aggregate by region and rank cities
    window_spec = Window.partitionBy("region").orderBy(F.col("sales").desc())
    ranked = df.withColumn("city_rank", F.row_number().over(window_spec))
    
    # Get top city per region
    top_cities = ranked.filter(F.col("city_rank") == 1)
    
    result = top_cities.collect()
    assert len(result) == 2

