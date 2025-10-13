# Mock Spark API Reference

This document provides a comprehensive reference for Mock Spark's API, including all classes, methods, and functions.

**Current Status**: 396 tests passing (100% pass rate) | 59% code coverage | Production Ready | Version 0.3.0

## Quick Stats
- **Total Tests**: 396 (100% pass rate)
- **Unit Tests**: 151 tests
- **Compatibility Tests**: 192 tests  
- **Code Coverage**: 59% across all modules
- **PySpark Compatibility**: 100% (PySpark 3.2)
- **Version**: 0.3.0 with enhanced features

## Table of Contents

- [Core Classes](#core-classes)
- [DataFrame Operations](#dataframe-operations)
- [Column Functions](#column-functions)
- [Window Functions](#window-functions)
- [Data Types](#data-types)
- [Storage Backends](#storage-backends)
- [Error Handling](#error-handling)
- [Performance Simulation](#performance-simulation)

## Core Classes

### MockSparkSession

The main entry point for Mock Spark applications.

```python
from mock_spark import MockSparkSession

# Create session
spark = MockSparkSession("MyApp")

# Create DataFrame
df = spark.createDataFrame([{"name": "Alice", "age": 25}])

# SQL operations
result = spark.sql("SELECT * FROM users WHERE age > 18")
```

#### Methods

- `createDataFrame(data, schema=None, samplingRatio=None, verifySchema=True)`
- `sql(query)`
- `table(tableName)`
- `catalog`
- `conf`
- `stop()`

### MockDataFrame

Represents a distributed collection of data organized into named columns.

```python
# Create DataFrame
df = spark.createDataFrame([{"name": "Alice", "age": 25}])

# Basic operations
df.select("name", "age")
df.filter(df.age > 20)
df.groupBy("age").count()
df.orderBy("age")
df.limit(10)
```

#### Methods

- `select(*cols)`
- `filter(condition)`
- `where(condition)`
- `groupBy(*cols)`
- `orderBy(*cols)`
- `limit(num)`
- `distinct()`
- `drop(*cols)`
- `withColumn(colName, col)`
- `withColumnRenamed(existing, new)`
- `show(n=20, truncate=True, vertical=False)`
- `collect()`
- `count()`
- `toPandas()`

### MockColumn

Represents a column in a DataFrame.

```python
from mock_spark import F

# Column expressions
F.col("name")
F.col("age") > 25
F.col("salary") + F.col("bonus")
F.col("name").alias("full_name")
```

#### Methods

- `alias(name)`
- `cast(dataType)`
- `asc()`
- `desc()`
- `isNull()`
- `isNotNull()`
- `when(condition, value)`
- `otherwise(value)`

## Column Functions

### Core Functions

```python
from mock_spark import F

# Column references
F.col("column_name")
F.lit(value)

# Aggregation functions
F.count(col)
F.sum(col)
F.avg(col)
F.max(col)
F.min(col)
F.first(col)
F.last(col)

# String functions
F.upper(col)
F.lower(col)
F.length(col)
F.trim(col)
F.ltrim(col)
F.rtrim(col)
F.regexp_replace(col, pattern, replacement)
F.split(col, delimiter)

# Mathematical functions
F.abs(col)
F.round(col, scale)
F.ceil(col)
F.floor(col)
F.sqrt(col)
F.pow(col, exponent)

# Null handling
F.coalesce(*cols)
F.isnull(col)
F.isnan(col)
F.nanvl(col1, col2)

# Conditional functions
F.when(condition, value)
F.otherwise(value)
F.case()
```

### Date/Time Functions

```python
# Current date/time
F.current_date()
F.current_timestamp()

# Date extraction
F.year(col)
F.month(col)
F.dayofyear(col)
F.dayofweek(col)
F.weekofyear(col)
F.hour(col)
F.minute(col)
F.second(col)

# Date arithmetic
F.date_add(col, days)
F.date_sub(col, days)
F.datediff(end, start)
F.add_months(col, months)
```

### Window Functions

```python
from mock_spark.window import MockWindow

# Window specifications
window = MockWindow.partitionBy("department").orderBy("salary")
window = MockWindow.rowsBetween(start, end)
window = MockWindow.rangeBetween(start, end)

# Window functions
F.row_number().over(window)
F.rank().over(window)
F.dense_rank().over(window)
F.lag(col, offset).over(window)
F.lead(col, offset).over(window)
F.avg(col).over(window)
F.sum(col).over(window)
F.count(col).over(window)
```

## Data Types

### Basic Types

```python
from mock_spark.spark_types import (
    StringType, IntegerType, LongType, DoubleType, FloatType,
    BooleanType, ShortType, ByteType, BinaryType, NullType
)

# Type definitions
StringType()
IntegerType()
LongType()
DoubleType()
FloatType()
BooleanType()
ShortType()
ByteType()
BinaryType()
NullType()
```

### Complex Types

```python
from mock_spark.spark_types import ArrayType, MapType, StructType, StructField

# Array type
ArrayType(StringType())

# Map type
MapType(StringType(), IntegerType())

# Struct type
StructType([
    StructField("name", StringType()),
    StructField("age", IntegerType())
])
```

### Date/Time Types

```python
from mock_spark.spark_types import DateType, TimestampType

# Date types
DateType()
TimestampType()
```

## Storage Backends

### DuckDB Storage (Default)

```python
# Recommended: Use via storage module (backward compatible)
from mock_spark.storage import DuckDBStorageManager

# Or: Direct import from new backend location
from mock_spark.backend.duckdb import DuckDBStorageManager

# Create storage manager (in-memory by default)
storage = DuckDBStorageManager()

# Table operations
table = storage.create_table("users", schema)
storage.table_exists("users")
storage.list_tables()
storage.drop_table("users")

# Data operations
table.insert_data(data)
table.query_data()
table.get_schema()
```

### Memory Storage

```python
from mock_spark.storage.backends.memory import MemoryStorageManager

# Create memory storage
storage = MemoryStorageManager()

# Operations
storage.create_table("users", schema)
storage.insert_data("users", data)
storage.query_data("users")
```

### File Storage

```python
from mock_spark.storage.backends.file import FileStorageManager

# Create file storage
storage = FileStorageManager("/path/to/data")

# Operations
storage.save_data("users", data, format="csv")
storage.load_data("users", format="csv")
```

## Error Handling

### Exception Classes

```python
from mock_spark.core.exceptions import (
    AnalysisException, ParseException, QueryExecutionException,
    IllegalArgumentException, PySparkValueError, PySparkTypeError,
    PySparkRuntimeError, PySparkAttributeError
)

# Analysis exceptions
AnalysisException("Column not found")
ParseException("SQL syntax error")
QueryExecutionException("Execution failed")

# Validation exceptions
IllegalArgumentException("Invalid argument")
PySparkValueError("Invalid value")
PySparkTypeError("Type mismatch")

# Runtime exceptions
PySparkRuntimeError("Runtime error")
PySparkAttributeError("Attribute not found")
```

### Error Simulation

```python
from mock_spark.error_simulation import MockErrorSimulator

# Error simulator
error_sim = MockErrorSimulator(spark)
error_sim.add_rule('table', lambda name: 'nonexistent' in name, 
                   AnalysisException("Table not found"))

# Simulate errors
with pytest.raises(AnalysisException):
    spark.table("nonexistent.table")
```

## Performance Simulation

### Performance Simulator

```python
from mock_spark.performance_simulation import MockPerformanceSimulator

# Performance simulator
perf_sim = MockPerformanceSimulator(spark)
perf_sim.set_slowdown(2.0)  # 2x slower
perf_sim.set_memory_limit(1024 * 1024)  # 1MB limit

# Simulate slow operations
result = perf_sim.simulate_slow_operation(df.count)
```

### Performance Metrics

```python
# Get performance metrics
metrics = perf_sim.get_metrics()
print(f"Execution time: {metrics['execution_time']}ms")
print(f"Memory usage: {metrics['memory_usage']}MB")
print(f"CPU usage: {metrics['cpu_usage']}%")
```

## Configuration

### Session Configuration

```python
# Set configuration
spark.conf.set("spark.app.name", "MyApp")
spark.conf.set("spark.master", "local[2]")

# Get configuration
app_name = spark.conf.get("spark.app.name")
```

### Storage Configuration

```python
# DuckDB configuration (in-memory by default)
storage = DuckDBStorageManager()  # Uses in-memory storage

# For persistent storage:
storage = DuckDBStorageManager("my_database.duckdb")

# File storage configuration
storage = FileStorageManager(
    base_path="/data",
    default_format="parquet",
    compression="snappy"
)
```

## Best Practices

### Performance

1. **Use appropriate data types** - Choose efficient types
2. **Limit data size** - Use LIMIT for large datasets
3. **Optimize queries** - Use efficient operations
4. **Monitor memory usage** - Watch for memory leaks

### Error Handling

1. **Validate input data** - Check data before processing
2. **Handle exceptions gracefully** - Use try-catch blocks
3. **Provide meaningful error messages** - Help with debugging
4. **Test error scenarios** - Use error simulation

### Code Quality

1. **Follow naming conventions** - Use clear, descriptive names
2. **Add type hints** - Improve code clarity
3. **Write documentation** - Document complex logic
4. **Use consistent formatting** - Follow style guidelines

## Troubleshooting

### Common Issues

1. **Import errors** - Check module imports
2. **Type errors** - Verify data types
3. **Memory issues** - Monitor memory usage
4. **Performance problems** - Profile operations

### Debug Mode

```python
# Enable debug mode
spark.conf.set("spark.sql.debug", "true")
storage.set_debug(True)
error_sim.set_debug(True)
perf_sim.set_debug(True)
```

### Logging

```python
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("mock_spark")

# Use logging
logger.debug("Processing data")
logger.info("Operation completed")
logger.warning("Performance issue detected")
logger.error("Operation failed")
```

This comprehensive API reference provides everything you need to effectively use Mock Spark's capabilities.
