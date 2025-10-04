"""
Comprehensive usage examples for Mock Spark.

This file demonstrates all the new features and improvements made to Mock Spark
based on real-world usage feedback. Includes examples of data types, error
simulation, performance testing, data generation, and more.

Current Status: 343+ tests passing (100% pass rate) | 62% code coverage | Production Ready

Run this file to see Mock Spark in action with all its enhanced capabilities.
"""

from mock_spark import (
    MockSparkSession,
    F,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    BooleanType,
    DateType,
    TimestampType,
    DecimalType,
    ArrayType,
    MapType,
    BinaryType,
    NullType,
    FloatType,
    ShortType,
    ByteType,
    MockStructType,
    MockStructField,
    MockErrorSimulator,
    MockErrorSimulatorBuilder,
    MockPerformanceSimulator,
    MockPerformanceSimulatorBuilder,
    MockDataGenerator,
    MockDataGeneratorBuilder,
    create_test_data,
    create_corrupted_data,
    create_realistic_data,
    create_table_not_found_simulator,
    create_data_too_large_simulator,
    create_slow_simulator,
    create_memory_limited_simulator,
    AnalysisException,
    IllegalArgumentException,
    PySparkValueError,
)
from mock_spark.window import MockWindow
import pytest


def demonstrate_data_types():
    """Demonstrate all available data types."""
    print("=== Data Types Demo ===")

    spark = MockSparkSession("DataTypesDemo")

    # Create schema with all data types
    schema = MockStructType(
        [
            MockStructField("name", StringType()),
            MockStructField("age", IntegerType()),
            MockStructField("id", LongType()),
            MockStructField("salary", DoubleType()),
            MockStructField("active", BooleanType()),
            MockStructField("birth_date", DateType()),
            MockStructField("created_at", TimestampType()),
            MockStructField("balance", DecimalType(10, 2)),
            MockStructField("tags", ArrayType(StringType())),
            MockStructField("metadata", MapType(StringType(), StringType())),
            MockStructField("data", BinaryType()),
            MockStructField("null_field", NullType()),
            MockStructField("score", FloatType()),
            MockStructField("count", ShortType()),
            MockStructField("flag", ByteType()),
        ]
    )

    # Create test data
    data = [
        {
            "name": "Alice",
            "age": 25,
            "id": 123456789,
            "salary": 75000.50,
            "active": True,
            "birth_date": "1998-05-15",
            "created_at": "2024-01-15 10:30:00",
            "balance": 1234.56,
            "tags": ["engineer", "python"],
            "metadata": {"department": "engineering", "level": "senior"},
            "data": b"binary_data",
            "null_field": None,
            "score": 95.5,
            "count": 100,
            "flag": 1,
        }
    ]

    df = spark.createDataFrame(data, schema)
    print(f"Created DataFrame with schema: {df.schema}")
    df.show()
    print()


def demonstrate_error_simulation():
    """Demonstrate error simulation capabilities."""
    print("=== Error Simulation Demo ===")

    spark = MockSparkSession("ErrorSimulationDemo")

    # Create error simulator
    error_sim = MockErrorSimulator(spark)

    # Add rules for different error scenarios
    error_sim.add_rule(
        "table",
        lambda name: "nonexistent" in name,
        AnalysisException("Table or view not found: nonexistent.table"),
    )

    error_sim.add_rule(
        "createDataFrame",
        lambda data, schema=None: len(data) > 5,
        PySparkValueError("Data too large: maximum 5 rows allowed"),
    )

    # Enable error simulation
    error_sim.enable_error_simulation()

    try:
        # This should raise an error
        spark.table("nonexistent.table")
    except AnalysisException as e:
        print(f"Caught expected error: {e}")

    try:
        # This should also raise an error
        large_data = [{"id": i} for i in range(10)]
        spark.createDataFrame(large_data)
    except PySparkValueError as e:
        print(f"Caught expected error: {e}")

    # Disable error simulation
    error_sim.disable_error_simulation()

    # Now normal operations should work
    normal_data = [{"id": i} for i in range(3)]
    df = spark.createDataFrame(normal_data)
    print(f"Normal operation succeeded: {df.count()} rows")
    print()


def demonstrate_performance_simulation():
    """Demonstrate performance simulation capabilities."""
    print("=== Performance Simulation Demo ===")

    spark = MockSparkSession("PerformanceSimulationDemo")

    # Create performance simulator
    perf_sim = MockPerformanceSimulator(spark)
    perf_sim.set_slowdown(2.0)  # 2x slower
    perf_sim.set_memory_limit(100)  # Max 100 rows

    # Enable performance simulation
    perf_sim.enable_performance_simulation()

    # Create some data
    data = [{"id": i, "value": f"item_{i}"} for i in range(50)]
    df = spark.createDataFrame(data)

    print(f"Created DataFrame with {df.count()} rows")

    # Get performance metrics
    metrics = perf_sim.get_performance_metrics()
    print(f"Performance metrics: {metrics}")

    # Disable performance simulation
    perf_sim.disable_performance_simulation()
    print()


def demonstrate_data_generation():
    """Demonstrate data generation capabilities."""
    print("=== Data Generation Demo ===")

    # Create schema
    schema = MockStructType(
        [
            MockStructField("name", StringType()),
            MockStructField("age", IntegerType()),
            MockStructField("salary", DoubleType()),
            MockStructField("active", BooleanType()),
            MockStructField("tags", ArrayType(StringType())),
        ]
    )

    # Generate test data
    test_data = create_test_data(schema, num_rows=10, seed=42)
    print(f"Generated {len(test_data)} test rows")
    print(f"Sample row: {test_data[0]}")

    # Generate realistic data
    realistic_data = create_realistic_data(schema, num_rows=10, seed=42)
    print(f"Generated {len(realistic_data)} realistic rows")
    print(f"Sample row: {realistic_data[0]}")

    # Generate corrupted data
    corrupted_data = create_corrupted_data(schema, corruption_rate=0.2, num_rows=10, seed=42)
    print(f"Generated {len(corrupted_data)} corrupted rows")
    print(f"Sample row: {corrupted_data[0]}")
    print()


def demonstrate_enhanced_dataframe_writer():
    """Demonstrate enhanced DataFrameWriter capabilities."""
    print("=== Enhanced DataFrameWriter Demo ===")

    spark = MockSparkSession("DataFrameWriterDemo")

    # Create test data
    data = [
        {"name": "Alice", "age": 25, "department": "engineering"},
        {"name": "Bob", "age": 30, "department": "marketing"},
        {"name": "Charlie", "age": 35, "department": "engineering"},
    ]
    df = spark.createDataFrame(data)

    # Test different save modes
    print("Testing save modes...")

    # Overwrite mode
    df.write.mode("overwrite").saveAsTable("employees")
    print("Saved with overwrite mode")

    # Append mode
    df.write.mode("append").saveAsTable("employees")
    print("Appended to table")

    # Error mode (should fail if table exists)
    try:
        df.write.mode("error").saveAsTable("employees")
    except AnalysisException as e:
        print(f"Error mode worked as expected: {e}")

    # Ignore mode (should do nothing if table exists)
    df.write.mode("ignore").saveAsTable("employees")
    print("Ignore mode completed")

    # Test options
    df.write.format("parquet").option("compression", "snappy").save("/tmp/test.parquet")
    print("Saved with options")

    # Verify table exists
    result_df = spark.table("employees")
    print(f"Table has {result_df.count()} rows")
    print()


def demonstrate_enhanced_window_functions():
    """Demonstrate enhanced window functions."""
    print("=== Enhanced Window Functions Demo ===")

    spark = MockSparkSession("WindowFunctionsDemo")

    # Create test data
    data = [
        {"name": "Alice", "department": "engineering", "salary": 75000},
        {"name": "Bob", "department": "engineering", "salary": 80000},
        {"name": "Charlie", "department": "marketing", "salary": 65000},
        {"name": "Diana", "department": "marketing", "salary": 70000},
        {"name": "Eve", "department": "engineering", "salary": 85000},
    ]
    df = spark.createDataFrame(data)

    # Test window functions with validation
    try:
        # This should work
        window = MockWindow.partitionBy("department").orderBy("salary")
        result = df.select(
            F.col("*"),
            F.row_number().over(window).alias("row_num"),
            F.rank().over(window).alias("rank"),
            F.dense_rank().over(window).alias("dense_rank"),
        )
        result.show()

        # Test rowsBetween
        window_with_rows = MockWindow.partitionBy("department").orderBy("salary").rowsBetween(-1, 1)
        result_with_rows = df.select(
            F.col("*"),
            F.avg("salary").over(window_with_rows).alias("avg_salary_window"),
        )
        result_with_rows.show()

    except ValueError as e:
        print(f"Window function validation error: {e}")

    print()


def demonstrate_enhanced_storage_catalog():
    """Demonstrate enhanced storage and catalog operations."""
    print("=== Enhanced Storage & Catalog Demo ===")

    spark = MockSparkSession("StorageCatalogDemo")

    # Test schema creation with validation
    try:
        spark.catalog.createDatabase("test_db")
        print("Created database 'test_db'")

        # Test table creation
        data = [{"id": 1, "name": "test"}]
        df = spark.createDataFrame(data)
        df.write.mode("overwrite").saveAsTable("test_db.test_table")
        print("Created table 'test_db.test_table'")

        # Test table existence check
        exists = spark.catalog.tableExists("test_db", "test_table")
        print(f"Table exists: {exists}")

        # Test listing tables
        tables = spark.catalog.listTables("test_db")
        print(f"Tables in test_db: {tables}")

        # Test error handling
        try:
            spark.catalog.createDatabase("")  # Empty name should fail
        except IllegalArgumentException as e:
            print(f"Caught expected error for empty database name: {e}")

    except Exception as e:
        print(f"Storage operation error: {e}")

    print()


def demonstrate_mockable_methods():
    """Demonstrate mockable methods for error testing."""
    print("=== Mockable Methods Demo ===")

    spark = MockSparkSession("MockableMethodsDemo")

    # Test mocking createDataFrame
    spark.mock_createDataFrame(side_effect=PySparkValueError("Mocked error"))

    try:
        spark.createDataFrame([{"id": 1}])
    except PySparkValueError as e:
        print(f"Caught mocked error: {e}")

    # Test mocking table method
    spark.mock_table(return_value=spark.createDataFrame([{"id": 1, "name": "mocked"}]))

    # This should return the mocked DataFrame
    result = spark.table("any_table")
    print(f"Mocked table result: {result.collect()}")

    # Reset mocks
    spark.reset_mocks()

    # Now normal operations should work
    normal_df = spark.createDataFrame([{"id": 1}])
    print(f"Normal operation after reset: {normal_df.count()} rows")
    print()


def demonstrate_builder_patterns():
    """Demonstrate builder patterns for common scenarios."""
    print("=== Builder Patterns Demo ===")

    spark = MockSparkSession("BuilderPatternsDemo")

    # Error simulation builder
    error_sim = (
        MockErrorSimulatorBuilder(spark).table_not_found("nonexistent.*").data_too_large(5).build()
    )

    # Performance simulation builder
    perf_sim = (
        MockPerformanceSimulatorBuilder(spark)
        .slowdown(1.5)
        .memory_limit(50)
        .enable_monitoring()
        .build()
    )

    # Data generation builder
    schema = MockStructType(
        [
            MockStructField("name", StringType()),
            MockStructField("age", IntegerType()),
        ]
    )

    data = MockDataGeneratorBuilder(schema).num_rows(20).realistic().corruption_rate(0.1).build()

    print(f"Generated {len(data)} rows with builder pattern")
    print(f"Sample row: {data[0]}")
    print()


def main():
    """Run all demonstrations."""
    print("Mock Spark Comprehensive Usage Examples")
    print("=" * 50)
    print()

    try:
        demonstrate_data_types()
        demonstrate_error_simulation()
        demonstrate_performance_simulation()
        demonstrate_data_generation()
        demonstrate_enhanced_dataframe_writer()
        demonstrate_enhanced_window_functions()
        demonstrate_enhanced_storage_catalog()
        demonstrate_mockable_methods()
        demonstrate_builder_patterns()

        print("All demonstrations completed successfully!")

    except Exception as e:
        print(f"Error during demonstration: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
