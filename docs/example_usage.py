#!/usr/bin/env python3
"""
Example usage of Mock Spark package.

Current Status: 343+ tests passing (100% pass rate) | 62% code coverage | Production Ready
"""

from mock_spark import MockSparkSession, F
from mock_spark.spark_types import StringType, IntegerType


def main():
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
    
    print("\n✅ All examples completed successfully!")


if __name__ == "__main__":
    main()
