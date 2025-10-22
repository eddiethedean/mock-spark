#!/usr/bin/env python3
"""
Example usage of Mock Spark package.

Current Status: 535 tests passing (100% pass rate) | Production Ready | Version 2.4.0
"""

from mock_spark import MockSparkSession, F


def main() -> None:
    """Demonstrate Mock Spark functionality."""
    print("🚀 Mock Spark Example Usage")
    print("=" * 40)

    # Create a mock Spark session
    spark = MockSparkSession("ExampleApp")
    print("✅ Created MockSparkSession")

    # Create sample data
    data = [
        {"name": "Alice", "age": 25, "city": "New York"},
        {"name": "Bob", "age": 30, "city": "San Francisco"},
        {"name": "Charlie", "age": 35, "city": "Chicago"},
        {"name": "Diana", "age": 28, "city": "Boston"},
    ]

    # Create DataFrame
    df = spark.createDataFrame(data)
    print("✅ Created DataFrame with sample data")

    # Show the data
    print("\n📊 Original Data:")
    df.show()

    # Filter data
    filtered_df = df.filter(F.col("age") > 28)
    print("\n🔍 Filtered Data (age > 28):")
    filtered_df.show()

    # Group by city and count
    grouped_df = df.groupBy("city").count()
    print("\n📈 Grouped by City:")
    grouped_df.show()

    # Select specific columns
    selected_df = df.select("name", "age")
    print("\n📋 Selected Columns (name, age):")
    selected_df.show()

    # Sort data
    sorted_df = df.orderBy("age")
    print("\n📊 Sorted by Age:")
    sorted_df.show()

    # New 2.4.0 features
    print("\n🆕 New 2.4.0 Features:")

    # String functions
    print("\n🔤 String Functions:")
    string_ops = df.select(
        F.col("name"),
        F.upper(F.col("name")).alias("upper_name"),
        F.length(F.col("name")).alias("name_length"),
    )
    string_ops.show()

    # Mathematical functions
    print("\n🔢 Mathematical Functions:")
    math_ops = df.select(
        F.col("name"),
        F.col("age"),
        F.round(F.col("age") / 10.0, 1).alias("age_decade"),
        F.sqrt(F.col("age")).alias("age_sqrt"),
    )
    math_ops.show()

    # Window functions
    print("\n🪟 Window Functions:")
    from mock_spark.window import MockWindow as Window

    window_spec = Window.orderBy(F.desc("age"))
    window_ops = df.select(
        F.col("name"), F.col("age"), F.row_number().over(window_spec).alias("rank")
    )
    window_ops.show()

    # DataFrame enhancements
    print("\n📊 DataFrame Enhancements:")
    print(f"  - isStreaming: {df.isStreaming}")
    print(f"  - Schema fields: {len(df.schema.fields)}")

    # Session enhancements
    print("\n🔧 Session Enhancements:")
    print(f"  - getOrCreate available: {hasattr(spark.builder, 'getOrCreate')}")

    print("\n✅ All examples completed successfully!")


if __name__ == "__main__":
    main()
