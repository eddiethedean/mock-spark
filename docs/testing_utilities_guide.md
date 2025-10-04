# Testing Utilities Guide

This guide provides comprehensive documentation for Mock Spark's testing utilities, including factories, fixtures, generators, and simulators.

**Current Status**: 343+ tests passing (100% pass rate) | 62% code coverage | Production Ready

## Overview

Mock Spark provides a comprehensive testing framework that makes it easy to create test data, mock objects, and simulate various scenarios:

- **Factories** - Create test objects and data
- **Fixtures** - Reusable test components
- **Generators** - Generate realistic test data
- **Simulators** - Simulate errors and performance scenarios
- **Mocks** - Mock objects for testing

## Factories

### MockDataFrameFactory

The `MockDataFrameFactory` class provides methods to create test DataFrames with various configurations.

#### Basic Usage

```python
from mock_spark.testing.factories import MockDataFrameFactory
from mock_spark import MockSparkSession

# Create factory
factory = MockDataFrameFactory()

# Create simple DataFrame
df = factory.create_simple_dataframe()
print(df.count())  # 10 (default)
print(df.columns)  # ['id', 'name', 'age', 'salary']

# Create DataFrame with custom number of rows
df_large = factory.create_large_dataframe(1000)
print(df_large.count())  # 1000
```

#### Advanced DataFrame Creation

```python
from mock_spark.spark_types import MockStructType, MockStructField, StringType, IntegerType, DoubleType

# Create DataFrame with custom schema
custom_schema = MockStructType([
    MockStructField("user_id", IntegerType()),
    MockStructField("username", StringType()),
    MockStructField("score", DoubleType())
])

df_custom = factory.create_dataframe_with_schema(custom_schema)
print(df_custom.schema)

# Create DataFrame with nulls
df_with_nulls = factory.create_dataframe_with_nulls()
print(df_with_nulls.collect())

# Create empty DataFrame
df_empty = factory.create_empty_dataframe()
print(df_empty.count())  # 0
```

### MockSessionFactory

The `MockSessionFactory` class creates configured Spark sessions for testing.

#### Basic Usage

```python
from mock_spark.testing.factories import MockSessionFactory

# Create factory
session_factory = MockSessionFactory()

# Create default session
spark = session_factory.create_default_session()
print(spark.appName)  # "MockSparkTest"

# Create session with custom config
config = {
    "spark.app.name": "CustomTestApp",
    "spark.master": "local[2]",
    "spark.sql.adaptive.enabled": "true"
}
spark_custom = session_factory.create_session_with_config(config)
print(spark_custom.appName)  # "CustomTestApp"
```

### MockFunctionFactory

The `MockFunctionFactory` class creates function objects for testing.

#### Basic Usage

```python
from mock_spark.testing.factories import MockFunctionFactory

# Create factory
func_factory = MockFunctionFactory()

# Create column function
col_func = func_factory.create_column()
col_expr = col_func.col("name")

# Create literal function
lit_func = func_factory.create_literal()
lit_expr = lit_func.lit("test_value")

# Create aggregate function
agg_func = func_factory.create_aggregate()
sum_expr = agg_func.sum("salary")
```

### MockStorageFactory

The `MockStorageFactory` class creates storage backends for testing.

#### Basic Usage

```python
from mock_spark.testing.factories import MockStorageFactory

# Create factory
storage_factory = MockStorageFactory()

# Create memory storage
memory_storage = storage_factory.create_memory_storage()
memory_storage.create_table("test_table", [])

# Create SQLite storage
sqlite_storage = storage_factory.create_sqlite_storage()
sqlite_storage.create_table("test_table", [])
```

## Fixtures

### Pytest Fixtures

Mock Spark provides pytest fixtures for easy test setup.

#### Basic Fixtures

```python
import pytest
from mock_spark.testing.fixtures import mock_spark_session, mock_dataframe, mock_functions

def test_with_fixtures(mock_spark_session, mock_dataframe, mock_functions):
    """Test using fixtures."""
    # Use the provided session
    df = mock_spark_session.createDataFrame([{"id": 1, "name": "Alice"}])
    
    # Use the provided DataFrame
    result = mock_dataframe.select("name")
    
    # Use the provided functions
    col_expr = mock_functions.col("name")
```

#### Custom Fixtures

```python
@pytest.fixture
def custom_spark_session():
    """Custom Spark session fixture."""
    from mock_spark import MockSparkSession
    return MockSparkSession("CustomTest")

@pytest.fixture
def test_data():
    """Test data fixture."""
    return [
        {"id": 1, "name": "Alice", "age": 25},
        {"id": 2, "name": "Bob", "age": 30},
        {"id": 3, "name": "Charlie", "age": 35}
    ]

def test_with_custom_fixtures(custom_spark_session, test_data):
    """Test using custom fixtures."""
    df = custom_spark_session.createDataFrame(test_data)
    result = df.filter(df.age > 25)
    assert result.count() == 2
```

## Generators

### DataGenerator

The `DataGenerator` class generates realistic test data for various data types.

#### Basic Usage

```python
from mock_spark.testing.generators import DataGenerator

# Create generator
generator = DataGenerator()

# Generate string data
names = generator.generate_string(10)
print(names)  # ['Alice', 'Bob', 'Charlie', ...]

# Generate integer data
ages = generator.generate_integer(10, min_val=18, max_val=65)
print(ages)  # [25, 30, 35, ...]

# Generate double data
salaries = generator.generate_double(10, min_val=30000, max_val=150000)
print(salaries)  # [50000.0, 75000.0, ...]

# Generate boolean data
active_flags = generator.generate_boolean(10)
print(active_flags)  # [True, False, True, ...]
```

#### Schema-Based Generation

```python
from mock_spark.spark_types import MockStructType, MockStructField, StringType, IntegerType, DoubleType

# Define schema
schema = MockStructType([
    MockStructField("name", StringType()),
    MockStructField("age", IntegerType()),
    MockStructField("salary", DoubleType())
])

# Generate data with schema
data = generator.generate_data_with_schema(schema, num_rows=100)
print(len(data))  # 100
print(data[0])  # {'name': 'Alice', 'age': 25, 'salary': 50000.0}
```

#### Realistic Data Generation

```python
# Generate realistic employee data
employee_data = generator.generate_realistic_employees(100)
print(employee_data[0])
# {'employee_id': 1, 'first_name': 'Alice', 'last_name': 'Smith', 
#  'email': 'alice.smith@company.com', 'department': 'Engineering', 
#  'salary': 75000.0, 'hire_date': '2020-01-15'}

# Generate realistic product data
product_data = generator.generate_realistic_products(50)
print(product_data[0])
# {'product_id': 1, 'name': 'Widget A', 'category': 'Electronics', 
#  'price': 99.99, 'in_stock': True, 'created_date': '2023-01-01'}
```

## Simulators

### ErrorSimulator

The `ErrorSimulator` class simulates various error conditions for testing error handling.

#### Basic Usage

```python
from mock_spark.testing.simulators import ErrorSimulator
from mock_spark.core.exceptions.analysis import AnalysisException

# Create simulator
error_sim = ErrorSimulator()

# Add error rules
error_sim.add_error_rule(
    operation="table_access",
    condition=lambda table_name: "nonexistent" in table_name,
    error=AnalysisException("Table not found")
)

# Simulate error
try:
    if error_sim.should_simulate_error("table_access", "nonexistent_table"):
        raise AnalysisException("Table not found")
except AnalysisException as e:
    print(f"Simulated error: {e}")
```

#### Advanced Error Simulation

```python
# Add multiple error rules
error_sim.add_error_rule(
    operation="column_access",
    condition=lambda col_name: col_name.startswith("invalid_"),
    error=AnalysisException("Column not found")
)

error_sim.add_error_rule(
    operation="sql_execution",
    condition=lambda query: "INVALID" in query.upper(),
    error=AnalysisException("SQL syntax error")
)

# Simulate with probability
error_sim.add_error_rule(
    operation="network",
    condition=lambda: True,  # Always true
    error=ConnectionError("Network timeout"),
    probability=0.1  # 10% chance
)

# Check if error should be simulated
if error_sim.should_simulate_error("network"):
    raise ConnectionError("Network timeout")
```

### PerformanceSimulator

The `PerformanceSimulator` class simulates performance characteristics for testing.

#### Basic Usage

```python
from mock_spark.testing.simulators import PerformanceSimulator
import time

# Create simulator
perf_sim = PerformanceSimulator()

# Set slowdown factor
perf_sim.set_slowdown(2.0)  # 2x slower

# Simulate slow operation
def slow_operation():
    time.sleep(0.1)  # 100ms operation

# Execute with simulation
start_time = time.time()
perf_sim.simulate_slow_operation(slow_operation)
end_time = time.time()
print(f"Operation took: {end_time - start_time:.2f}s")  # ~0.2s
```

#### Memory Simulation

```python
# Set memory limit
perf_sim.set_memory_limit(1024 * 1024)  # 1MB limit

# Simulate memory-intensive operation
def memory_intensive_operation():
    data = [0] * 1000000  # 1M integers
    return sum(data)

# Check memory limit
if perf_sim.check_memory_limit(1000000 * 4):  # 4MB needed
    raise MemoryError("Memory limit exceeded")

# Execute operation
result = memory_intensive_operation()
```

## Mocks

### MockableMethods

The `MockableMethods` class provides methods to mock core Spark operations.

#### Basic Usage

```python
from mock_spark.testing.mocks import MockableMethods
from mock_spark import MockSparkSession

# Create session
spark = MockSparkSession("TestApp")

# Create mockable methods
mockable = MockableMethods(spark)

# Mock createDataFrame
mockable.mock_createDataFrame(side_effect=Exception("Connection failed"))

# Mock table access
mock_df = spark.createDataFrame([{"id": 1, "name": "Alice"}])
mockable.mock_table(return_value=mock_df)

# Mock SQL execution
mockable.mock_sql(side_effect=AnalysisException("SQL error"))

# Reset mocks
mockable.reset_mocks()
```

### DataFrameWriter

The `DataFrameWriter` class provides methods to mock DataFrame writing operations.

#### Basic Usage

```python
from mock_spark.testing.mocks import DataFrameWriter

# Create writer
writer = DataFrameWriter()

# Mock write modes
writer.mock_mode("overwrite")
writer.mock_mode("append")

# Mock write options
writer.mock_option("compression", "snappy")
writer.mock_option("header", "true")

# Mock save operations
writer.mock_save("/path/to/data")
writer.mock_saveAsTable("table_name")
```

## Complete Testing Example

Here's a complete example of using Mock Spark's testing utilities:

```python
import pytest
from mock_spark.testing.factories import MockDataFrameFactory, MockSessionFactory
from mock_spark.testing.generators import DataGenerator
from mock_spark.testing.simulators import ErrorSimulator, PerformanceSimulator

class TestDataProcessing:
    """Test data processing with comprehensive testing utilities."""
    
    @pytest.fixture
    def spark(self):
        """Create test Spark session."""
        factory = MockSessionFactory()
        return factory.create_session_with_config({
            "spark.app.name": "DataProcessingTest"
        })
    
    @pytest.fixture
    def test_data(self):
        """Generate test data."""
        generator = DataGenerator()
        return generator.generate_realistic_employees(100)
    
    @pytest.fixture
    def error_simulator(self):
        """Create error simulator."""
        sim = ErrorSimulator()
        sim.add_error_rule(
            operation="data_processing",
            condition=lambda data: len(data) > 1000,
            error=ValueError("Data too large")
        )
        return sim
    
    def test_employee_filtering(self, spark, test_data):
        """Test employee filtering logic."""
        df = spark.createDataFrame(test_data)
        
        # Test filtering
        senior_employees = df.filter(df.age > 30)
        assert senior_employees.count() > 0
        
        # Test aggregation
        dept_stats = df.groupBy("department").agg(
            spark.functions.count("*").alias("count"),
            spark.functions.avg("salary").alias("avg_salary")
        )
        assert dept_stats.count() > 0
    
    def test_error_handling(self, spark, test_data, error_simulator):
        """Test error handling scenarios."""
        df = spark.createDataFrame(test_data)
        
        # Test normal operation
        result = df.select("name", "salary")
        assert result.count() == len(test_data)
        
        # Test error simulation
        large_data = test_data * 20  # 2000 records
        if error_simulator.should_simulate_error("data_processing", large_data):
            with pytest.raises(ValueError, match="Data too large"):
                df_large = spark.createDataFrame(large_data)
    
    def test_performance_characteristics(self, spark, test_data):
        """Test performance characteristics."""
        perf_sim = PerformanceSimulator()
        perf_sim.set_slowdown(1.5)  # 1.5x slower
        
        df = spark.createDataFrame(test_data)
        
        # Test with performance simulation
        start_time = time.time()
        result = perf_sim.simulate_slow_operation(
            lambda: df.groupBy("department").count().collect()
        )
        end_time = time.time()
        
        # Verify performance characteristics
        assert end_time - start_time > 0
        assert len(result) > 0
```

## Best Practices

1. **Use factories for object creation** - Consistent test object creation
2. **Generate realistic test data** - Use generators for realistic scenarios
3. **Simulate error conditions** - Test error handling thoroughly
4. **Mock external dependencies** - Isolate units under test
5. **Use fixtures for setup** - Reusable test components
6. **Test performance characteristics** - Ensure acceptable performance

## Troubleshooting

### Common Issues

1. **Fixture not found** - Ensure fixtures are properly imported
2. **Mock not working** - Check mock setup and reset
3. **Data generation issues** - Verify schema compatibility
4. **Simulation not triggering** - Check condition logic

### Debug Mode

```python
# Enable debug mode for testing utilities
factory.set_debug(True)
generator.set_debug(True)
error_sim.set_debug(True)
perf_sim.set_debug(True)
```

This comprehensive testing utilities guide provides everything you need to effectively test Mock Spark applications.
