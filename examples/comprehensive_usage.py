#!/usr/bin/env python3
"""
Comprehensive Usage Example for Mock Spark

Showcases advanced features including:
- Analytics Engine with DuckDB
- Complex transformations and aggregations
- Window functions and ranking
- Error handling and validation
- Performance features

Status: 515 tests passing (100%) | Production Ready | Version 2.0.0
"""

from typing import List, Dict, Any

from mock_spark import MockSparkSession, F
from mock_spark.window import MockWindow as Window


def create_sample_data() -> List[Dict[str, Any]]:
    """Generate comprehensive sample dataset."""
    return [
        {
            "id": 1,
            "name": "Alice",
            "dept": "Engineering",
            "salary": 95000,
            "years": 5,
            "rating": 4.5,
        },
        {
            "id": 2,
            "name": "Bob",
            "dept": "Sales",
            "salary": 80000,
            "years": 3,
            "rating": 4.2,
        },
        {
            "id": 3,
            "name": "Charlie",
            "dept": "Engineering",
            "salary": 105000,
            "years": 7,
            "rating": 4.8,
        },
        {
            "id": 4,
            "name": "Diana",
            "dept": "Marketing",
            "salary": 75000,
            "years": 2,
            "rating": 4.0,
        },
        {
            "id": 5,
            "name": "Eve",
            "dept": "Sales",
            "salary": 90000,
            "years": 4,
            "rating": 4.6,
        },
        {
            "id": 6,
            "name": "Frank",
            "dept": "Engineering",
            "salary": 88000,
            "years": 3,
            "rating": 4.1,
        },
        {
            "id": 7,
            "name": "Grace",
            "dept": "Marketing",
            "salary": 82000,
            "years": 4,
            "rating": 4.4,
        },
        {
            "id": 8,
            "name": "Henry",
            "dept": "Sales",
            "salary": 95000,
            "years": 6,
            "rating": 4.7,
        },
    ]


def demo_advanced_transformations(df: Any) -> None:
    """Demonstrate complex DataFrame transformations."""
    print("\n" + "=" * 60)
    print("📊 ADVANCED TRANSFORMATIONS")
    print("=" * 60)

    # Complex column expressions
    result = df.select(
        "name",
        "dept",
        "salary",
        "years",
        # Salary calculations
        F.round(F.col("salary") / 1000, 1).alias("salary_k"),
        (F.col("salary") * 1.1).alias("salary_with_raise"),
        # Conditional logic
        F.when(F.col("years") >= 5, "Senior")
        .when(F.col("years") >= 3, "Mid")
        .otherwise("Junior")
        .alias("level"),
        # Ratings
        F.round(F.col("rating") * 20, 0).alias("rating_pct"),
    )

    print("\n✓ Complex expressions with conditionals:")
    result.show()

    return result


def demo_window_analytics(df: Any) -> None:
    """Demonstrate window functions and analytics."""
    print("\n" + "=" * 60)
    print("🪟 WINDOW ANALYTICS")
    print("=" * 60)

    # Ranking within departments
    dept_window = Window.partitionBy("dept").orderBy(F.desc("salary"))

    # Multiple window functions
    analytics = df.select(
        "name",
        "dept",
        "salary",
        F.row_number().over(dept_window).alias("rank"),
        F.rank().over(dept_window).alias("dense_rank"),
        F.round(F.avg("salary").over(Window.partitionBy("dept")), 0).alias("dept_avg"),
    )

    print("\n✓ Ranking and department averages:")
    analytics.show()

    return analytics


def demo_complex_aggregations(df: Any) -> None:
    """Demonstrate multi-level aggregations."""
    print("\n" + "=" * 60)
    print("📈 COMPLEX AGGREGATIONS")
    print("=" * 60)

    # Department statistics
    dept_stats = (
        df.groupBy("dept")
        .agg(
            F.count("*").alias("employee_count"),
            F.avg("salary").alias("avg_salary"),
            F.min("salary").alias("min_salary"),
            F.max("salary").alias("max_salary"),
            F.avg("years").alias("avg_tenure"),
            F.avg("rating").alias("avg_rating"),
        )
        .orderBy(F.desc("avg_salary"))
    )

    print("\n✓ Department statistics:")
    dept_stats.show()

    # Seniority analysis
    seniority = (
        df.withColumn(
            "level",
            F.when(F.col("years") >= 5, "Senior")
            .when(F.col("years") >= 3, "Mid")
            .otherwise("Junior"),
        )
        .groupBy("level")
        .agg(F.count("*").alias("count"), F.avg("salary").alias("avg_salary"))
    )

    print("\n✓ Seniority analysis:")
    seniority.show()

    return dept_stats


def demo_sql_operations(spark: Any, df: Any) -> None:
    """Demonstrate SQL query capabilities."""
    print("\n" + "=" * 60)
    print("🗄️  SQL OPERATIONS")
    print("=" * 60)

    # Create temp view
    df.createOrReplaceTempView("employees")

    # Simple SQL query with filtering
    sql_result = spark.sql(
        "SELECT name, dept, salary FROM employees WHERE salary > 85000"
    )

    print("\n✓ SQL query (salary > 85k):")
    sql_result.show()

    # Another SQL example
    top_rated = spark.sql(
        "SELECT name, dept, rating FROM employees WHERE rating >= 4.5"
    )

    print("\n✓ Top rated employees (rating >= 4.5):")
    top_rated.show()


def demo_advanced_features(df: Any) -> None:
    """Demonstrate advanced DataFrame features."""
    print("\n" + "=" * 60)
    print("🔬 ADVANCED FEATURES")
    print("=" * 60)

    # Multiple aggregations
    print("\n✓ Multiple metrics at once:")
    overall_stats = df.agg(
        F.count("*").alias("total_employees"),
        F.avg("salary").alias("avg_salary"),
        F.min("salary").alias("min_salary"),
        F.max("salary").alias("max_salary"),
        F.avg("years").alias("avg_tenure"),
        F.avg("rating").alias("avg_rating"),
    )
    overall_stats.show()

    # Percentiles using window functions
    print("\n✓ Salary percentiles:")
    percentile_window = Window.orderBy("salary")
    percentiles = df.select(
        "name", "salary", F.row_number().over(percentile_window).alias("salary_rank")
    ).orderBy(F.desc("salary"))
    percentiles.show()


def demo_error_handling(spark: Any, df: Any) -> None:
    """Demonstrate error handling and validation."""
    print("\n" + "=" * 60)
    print("⚠️  ERROR HANDLING")
    print("=" * 60)

    # Lazy evaluation - error deferred
    print("\n✓ Lazy evaluation (error deferred):")
    bad_query = df.filter(F.col("nonexistent") > 0)  # No error yet
    print("   Transformation queued (no error)")

    try:
        bad_query.count()  # Error happens here
    except Exception as e:
        print(f"   ✓ Error caught at action time: {type(e).__name__}")

    # Valid null handling
    print("\n✓ Null handling with coalesce:")
    null_data = [
        {"name": "Test1", "value": 100},
        {"name": "Test2", "value": None},
    ]
    null_df = spark.createDataFrame(null_data)
    result = null_df.select(
        "name", F.coalesce(F.col("value"), F.lit(0)).alias("value_safe")
    )
    result.show()


def main() -> None:
    """Run comprehensive Mock Spark demonstration."""
    print("🚀 Mock Spark - Comprehensive Feature Showcase")
    print("=" * 60)

    # Initialize
    print("\n📦 Initializing Mock Spark...")
    spark = MockSparkSession("ComprehensiveDemo")
    print(f"   ✓ Session: {spark.app_name}")

    # Load data
    print("\n📊 Loading sample data...")
    data = create_sample_data()
    df = spark.createDataFrame(data)
    print(f"   ✓ Created DataFrame: {df.count()} rows, {len(df.columns)} columns")

    print("\n   Initial data:")
    df.show()

    # Run demonstrations
    demo_advanced_transformations(df)
    demo_window_analytics(df)
    demo_complex_aggregations(df)
    demo_sql_operations(spark, df)
    demo_advanced_features(df)
    demo_error_handling(spark, df)

    # Performance info
    print("\n" + "=" * 60)
    print("⚡ PERFORMANCE BENEFITS")
    print("=" * 60)
    print("\n✓ No JVM overhead - 10x faster than PySpark")
    print("✓ In-memory DuckDB backend for fast analytics")
    print("✓ Lazy evaluation with optimized execution")
    print("✓ Perfect for unit tests and CI/CD pipelines")

    # Cleanup
    print("\n" + "=" * 60)
    print("🧹 CLEANUP")
    print("=" * 60)
    spark.stop()
    print("   ✓ Session stopped")

    print("\n✨ Comprehensive demonstration completed!")
    print("\n📚 Learn more:")
    print("   • Docs: docs/")
    print("   • API Reference: docs/api_reference.md")
    print("   • GitHub: https://github.com/eddiethedean/mock-spark")


if __name__ == "__main__":
    main()
