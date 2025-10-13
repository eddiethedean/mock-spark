# Mock Spark

<div align="center">

**🚀 Test PySpark code at lightning speed—no JVM required**

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![PyPI version](https://badge.fury.io/py/mock-spark.svg)](https://badge.fury.io/py/mock-spark)
[![Tests](https://img.shields.io/badge/tests-324%20passing%20%7C%200%20failing-brightgreen.svg)](https://github.com/eddiethedean/mock-spark)
[![Type Checked](https://img.shields.io/badge/mypy-100%25%20typed-blue.svg)](https://github.com/python/mypy)
[![Code Style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

*⚡ 10x faster tests • 🎯 Drop-in PySpark replacement • 📦 Zero JVM overhead*

</div>

---

## Why Mock Spark?

**Tired of waiting 30+ seconds for Spark to initialize in every test?**

Mock Spark is a lightweight PySpark replacement that runs your tests **10x faster** by eliminating JVM overhead. Your existing PySpark code works unchanged—just swap the import.

```python
# Before
from pyspark.sql import SparkSession

# After  
from mock_spark import MockSparkSession as SparkSession
```

### Key Benefits

| Feature | Description |
|---------|-------------|
| ⚡ **10x Faster** | No JVM startup (30s → 0.1s) |
| 🎯 **Drop-in Replacement** | Use existing PySpark code unchanged |
| 📦 **Zero Java** | Pure Python with DuckDB backend |
| 🧪 **100% Compatible** | Full PySpark 3.2 API support |
| 🔄 **Lazy Evaluation** | Mirrors PySpark's execution model |
| 🏭 **Production Ready** | 324 passing tests, 100% mypy typed, zero raw SQL |

### Perfect For

- **Unit Testing** - Fast, isolated test execution with automatic cleanup
- **CI/CD Pipelines** - Reliable tests without infrastructure or resource leaks
- **Local Development** - Prototype without Spark cluster
- **Documentation** - Runnable examples without setup
- **Learning** - Understand PySpark without complexity
- **Integration Tests** - Configurable memory limits for large dataset testing

---

## What's New in 2.1.0

### 🎯 Delta Lake Support
Full Delta Lake format compatibility for advanced testing workflows:

- **Time Travel** - Query historical versions with `versionAsOf` option
- **MERGE Operations** - Full MERGE INTO support for upsert patterns
- **Schema Evolution** - Automatic column addition with `mergeSchema` option
- **Version Tracking** - Complete version history with timestamps

```python
# Delta Lake basic usage
df.write.format("delta").mode("overwrite").saveAsTable("users")

# Time travel - read historical version
old_data = spark.read.format("delta").option("versionAsOf", 0).table("users")

# Schema evolution during append
df_new_columns.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("users")

# MERGE INTO for upserts
spark.sql("""
    MERGE INTO target USING source ON target.id = source.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

# View version history
history = spark.sql("DESCRIBE HISTORY users")
```

### ⏰ DateTime Functions
Enhanced datetime transformation capabilities:

- **Date Conversion**: `to_date()` for timestamp parsing
- **Time Extraction**: `hour()`, `minute()`, `second()`
- **Date Components**: `year()`, `month()`, `day()`, `dayofmonth()`
- **Full DuckDB Compatibility** - Generates optimized SQL

```python
# Extract datetime components
df.withColumn("event_date", F.to_date("timestamp_col")) \
  .withColumn("hour", F.hour("timestamp_col")) \
  .withColumn("year", F.year("timestamp_col"))

# Works with groupBy
hourly_stats = df.groupBy(F.hour("timestamp")).agg(F.count("*"))
```

### 🔗 Complex Column Expressions
Advanced boolean logic with proper AND/OR handling:

- **Nested Expressions** - Combine multiple conditions with `&` and `|`
- **Null Checking** - `isNull()` and `isNotNull()` in complex expressions
- **Filter & Compute** - Works in both `filter()` and `withColumn()`

```python
# Complex filtering with AND/OR
result = df.filter(
    ((F.col("value") > 100) & F.col("active")) | 
    (F.col("status") == "premium")
)

# Computed columns with complex logic
df.withColumn(
    "flag",
    (F.col("amount") > 1000) & F.col("region").isNotNull()
)
```

### 📊 Test Coverage
- **38 new tests** across Delta Lake, datetime, and complex expressions
- **319 total tests** passing (281 existing + 38 new)
- **Zero regressions** - all existing functionality preserved

---

## What's New in 2.0.0

### 🎯 Zero Raw SQL Architecture
- **100% type-safe** - All database operations use SQLAlchemy Core expressions
- **Database agnostic** - Switch between DuckDB, PostgreSQL, MySQL, SQLite with one line
- **SQL injection prevention** - Comprehensive parameter binding throughout

### 🔧 Pure SQLAlchemy Stack
- **Removed SQLModel dependency** - Simplified to pure SQLAlchemy for cleaner architecture
- **1,400+ lines of new infrastructure** - SQL translation, query building, type-safe helpers
- **100+ Spark SQL functions mapped** - Comprehensive function support via sqlglot
- **Improved performance** - Optimized query execution and bulk operations

### 🗄️ Backend Flexibility
```python
# DuckDB (default - fastest)
spark = MockSparkSession("app", backend="duckdb:///:memory:")

# PostgreSQL
spark = MockSparkSession("app", backend="postgresql://localhost/testdb")

# SQLite
spark = MockSparkSession("app", backend="sqlite:///test.db")

# MySQL
spark = MockSparkSession("app", backend="mysql://localhost/testdb")
```

---

## Quick Start

### Installation

```bash
pip install mock-spark
```

### Basic Usage

```python
from mock_spark import MockSparkSession, F

# Create session
spark = MockSparkSession("MyApp")

# Your PySpark code works as-is
data = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
df = spark.createDataFrame(data)

# All operations work
result = df.filter(F.col("age") > 25).select("name").collect()
print(result)
# Output: [Row(name='Bob')]

# Show the DataFrame
df.show()
# Output:
# name  age
# Alice  25
# Bob    30
```

### Testing Example

```python
import pytest
from mock_spark import MockSparkSession, F

def test_data_pipeline():
    """Test PySpark logic without Spark cluster."""
    spark = MockSparkSession("TestApp")
    
    # Test data
    data = [{"score": 95}, {"score": 87}, {"score": 92}]
    df = spark.createDataFrame(data)
    
    # Business logic
    high_scores = df.filter(F.col("score") > 90)
    
    # Assertions
    assert high_scores.count() == 2
    assert high_scores.agg(F.avg("score")).collect()[0][0] == 93.5
    
    # Always clean up
    spark.stop()

def test_large_dataset():
    """Test with larger dataset requiring more memory."""
    spark = MockSparkSession(
        "LargeTest",
        max_memory="4GB",
        allow_disk_spillover=True
    )
    
    # Process large dataset
    data = [{"id": i, "value": i * 10} for i in range(100000)]
    df = spark.createDataFrame(data)
    
    result = df.filter(F.col("id") > 50000).count()
    assert result < 50000
    
    spark.stop()
```

---

## Core Features

### DataFrame Operations
- **Transformations**: `select`, `filter`, `withColumn`, `drop`, `distinct`, `orderBy`
- **Aggregations**: `groupBy`, `agg`, `count`, `sum`, `avg`, `min`, `max`
- **Joins**: `inner`, `left`, `right`, `outer`, `cross`
- **Advanced**: `union`, `pivot`, `unpivot`, `explode`

### Functions (60+)
- **String**: `upper`, `lower`, `concat`, `split`, `substring`, `trim`
- **Math**: `round`, `abs`, `sqrt`, `pow`, `ceil`, `floor`
- **Date/Time**: `current_date`, `date_add`, `date_sub`, `to_date`, `year`, `month`, `day`, `hour`, `minute`, `second`
- **Conditional**: `when`, `otherwise`, `coalesce`, `isnull`, `isnan`, `isNotNull`
- **Aggregate**: `sum`, `avg`, `count`, `min`, `max`, `first`, `last`

### Window Functions
```python
from mock_spark.window import MockWindow as Window

# Ranking and analytics
df.withColumn("rank", F.row_number().over(
    Window.partitionBy("dept").orderBy(F.desc("salary"))
))
```

### SQL Support
```python
df.createOrReplaceTempView("employees")
result = spark.sql("SELECT name, salary FROM employees WHERE salary > 50000")
```

### Delta Lake Format
Full Delta Lake table format support for advanced workflows:

```python
# Write as Delta table
df.write.format("delta").mode("overwrite").saveAsTable("catalog.users")

# Time travel - query historical versions
v0_data = spark.read.format("delta").option("versionAsOf", 0).table("catalog.users")
v1_data = spark.read.format("delta").option("versionAsOf", 1).table("catalog.users")

# Schema evolution - add columns automatically
new_df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("catalog.users")

# MERGE operations for upserts
spark.sql("""
    MERGE INTO catalog.users AS target
    USING updates AS source
    ON target.id = source.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

# View version history
history = spark.sql("DESCRIBE HISTORY catalog.users")
history.show()
# Output: version | timestamp | operation
#         0       | 2024...   | WRITE
#         1       | 2024...   | APPEND
#         2       | 2024...   | OVERWRITE
```

### Lazy Evaluation
Mock Spark mirrors PySpark's lazy execution model:

```python
# Transformations are queued (not executed)
result = df.filter(F.col("age") > 25).select("name")  

# Actions trigger execution
rows = result.collect()  # ← Execution happens here
count = result.count()   # ← Or here
```

**Control evaluation mode:**
```python
# Lazy (default, recommended)
spark = MockSparkSession("App", enable_lazy_evaluation=True)

# Eager (for legacy tests)
spark = MockSparkSession("App", enable_lazy_evaluation=False)
```

---

## Advanced Features

### Storage Backends
- **Memory** (default) - Fast, ephemeral
- **DuckDB** - In-memory SQL analytics with configurable memory limits
- **File System** - Persistent storage

### Configurable Memory & Isolation

Control memory usage and test isolation:

```python
# Default: 1GB memory limit, no disk spillover (best for tests)
spark = MockSparkSession("MyApp")

# Custom memory limit
spark = MockSparkSession("MyApp", max_memory="4GB")

# Allow disk spillover for large datasets (with test isolation)
spark = MockSparkSession(
    "MyApp",
    max_memory="8GB",
    allow_disk_spillover=True  # Uses unique temp directory per session
)
```

**Key Features:**
- **Memory Limits**: Set per-session memory limits to prevent resource exhaustion
- **Test Isolation**: Each session gets unique temp directories when spillover is enabled
- **Default Behavior**: Disk spillover disabled by default for fast, isolated tests
- **Automatic Cleanup**: Temp directories automatically cleaned up when session stops

---

## Performance Comparison

Real-world test suite improvements:

| Operation | PySpark | Mock Spark | Speedup |
|-----------|---------|------------|---------|
| Session Creation | 30-45s | 0.1s | **300x** |
| Simple Query | 2-5s | 0.01s | **200x** |
| Window Functions | 5-10s | 0.05s | **100x** |
| Full Test Suite | 5-10min | 30-60s | **10x** |

---

## Documentation

### Getting Started
- 📖 [Installation & Setup](https://github.com/eddiethedean/mock-spark/blob/main/docs/getting_started.md)
- 🎯 [Quick Start Guide](https://github.com/eddiethedean/mock-spark/blob/main/docs/getting_started.md#quick-start)
- 🔄 [Migration from PySpark](https://github.com/eddiethedean/mock-spark/blob/main/docs/guides/migration.md)

### Core Concepts
- 📊 [API Reference](https://github.com/eddiethedean/mock-spark/blob/main/docs/api_reference.md)
- 🔄 [Lazy Evaluation](https://github.com/eddiethedean/mock-spark/blob/main/docs/guides/lazy_evaluation.md)
- 🗄️ [SQL Operations](https://github.com/eddiethedean/mock-spark/blob/main/docs/sql_operations_guide.md)
- 💾 [Storage & Persistence](https://github.com/eddiethedean/mock-spark/blob/main/docs/storage_serialization_guide.md)

### Advanced Topics
- ⚙️ [Configuration](https://github.com/eddiethedean/mock-spark/blob/main/docs/guides/configuration.md)
- 📈 [Benchmarking](https://github.com/eddiethedean/mock-spark/blob/main/docs/guides/benchmarking.md)
- 🔌 [Plugins & Hooks](https://github.com/eddiethedean/mock-spark/blob/main/docs/guides/plugins.md)
- 🐍 [Pytest Integration](https://github.com/eddiethedean/mock-spark/blob/main/docs/guides/pytest_integration.md)

---

## Previous Releases

### Version 1.4.0

### New Features

#### 🔺 Delta Lake Support
Mock Spark now includes basic Delta Lake API compatibility for testing Delta workflows:

```python
from mock_spark import MockSparkSession, DeltaTable

spark = MockSparkSession("app")
df = spark.createDataFrame([{"id": 1, "value": "test"}])

# Save as table
df.write.saveAsTable("my_table")

# Access as Delta table
delta_table = DeltaTable.forName(spark, "my_table")
delta_df = delta_table.toDF()

# Mock Delta operations (API compatible, no-op execution)
delta_table.delete("id < 10")
delta_table.merge(source_df, "target.id = source.id").whenMatchedUpdate({"value": "new"}).execute()
delta_table.vacuum()
history_df = delta_table.history()
```

**Features:**
- ✅ `DeltaTable.forName()` and `DeltaTable.forPath()` - Load Delta tables
- ✅ `toDF()` - Convert to DataFrame
- ✅ `delete()`, `update()`, `merge()` - Mock Delta operations (API compatible)
- ✅ `vacuum()`, `history()` - Mock maintenance operations
- ✅ `DeltaMergeBuilder` - Fluent API for merge operations

**Note:** Mock operations are no-ops for API compatibility. For real Delta features (time travel, ACID), use actual PySpark + delta-spark.

#### 🗄️ SQL DDL Enhancements
Enhanced SQL support for schema/database management:

```python
# CREATE DATABASE/SCHEMA with IF NOT EXISTS
spark.sql("CREATE DATABASE IF NOT EXISTS analytics")
spark.sql("CREATE SCHEMA bronze")

# DROP DATABASE/SCHEMA with IF EXISTS
spark.sql("DROP DATABASE IF EXISTS old_schema")

# Catalog integration - SQL and API work together
dbs = spark.catalog.listDatabases()
spark.catalog.dropDatabase("temp_db")
```

**Features:**
- ✅ `CREATE DATABASE/SCHEMA` - SQL parser recognizes both keywords
- ✅ `DROP DATABASE/SCHEMA` - With IF EXISTS support
- ✅ `catalog.dropDatabase()` - New catalog API method
- ✅ Catalog Integration - SQL DDL updates catalog automatically
- ✅ Case-insensitive keywords - `create`, `CREATE`, `CrEaTe` all work

### Test Infrastructure Improvements
- ⚡ **Parallel Testing** - Run 489 tests in parallel with pytest-xdist (8 cores)
- ☕ **Java 11 Support** - Full Java 11 compatibility with automated configuration
- 🔒 **Enhanced Test Isolation** - Delta Lake tests run serially with proper session cleanup
- 🧪 **101 New Tests** - Expanded test coverage (388 → 489 tests)
- 🎯 **Zero Test Failures** - All tests pass with parallel execution

### Developer Experience
- 🚀 **Faster CI/CD** - Tests complete in ~90 seconds with parallel execution
- 🔧 **Automated Setup** - `setup_spark_env.sh` configures Java 11 and dependencies
- 📝 **Black Formatting** - Consistent code style across entire codebase
- 🏷️ **Test Markers** - `@pytest.mark.delta` for proper test categorization

## What's New in 1.3.0

### Major Improvements
- 🔧 **Configurable Memory** - Set custom memory limits per session
- 🔒 **Test Isolation** - Each session gets unique temp directories
- 🧹 **Resource Cleanup** - Automatic cleanup prevents test leaks
- 🚀 **Performance** - Memory-only operations by default (no disk I/O)
- 🧪 **26 New Tests** - Comprehensive resource management tests

### Resource Management
- Configurable DuckDB memory limits (`max_memory="4GB"`)
- Optional disk spillover with isolation (`allow_disk_spillover=True`)
- Automatic cleanup on `session.stop()` and `__del__`
- No shared temp files between tests - complete isolation

### Previous Releases

**1.0.0**
- ✨ **DuckDB Integration** - Replaced SQLite for 30% faster operations
- 🧹 **Code Consolidation** - Removed 1,300+ lines of duplicate code
- 📦 **Optional Pandas** - Pandas now optional, reducing core dependencies
- ⚡ **Performance** - Sub-4s aggregations on large datasets
- 🧪 **Test Coverage** - Initial 388 passing tests with 100% compatibility

---

## Known Limitations & Future Features

While Mock Spark provides comprehensive PySpark compatibility, some advanced features are planned for future releases:

**Type System**: Strict runtime type validation, custom validators  
**Error Handling**: Enhanced error messages with recovery strategies  
**Functions**: Extended date/time, math, and null handling  
**Performance**: Query optimization, parallel execution, intelligent caching  
**Enterprise**: Schema evolution, data lineage, audit logging  
**Compatibility**: PySpark 3.3+, Delta Lake, Iceberg support  

**Want to contribute?** These are great opportunities for community contributions! See [Contributing](#contributing) below.

---

## Contributing

We welcome contributions! Areas of interest:

- ⚡ **Performance** - Further DuckDB optimizations
- 📚 **Documentation** - Examples, guides, tutorials
- 🐛 **Bug Fixes** - Edge cases and compatibility issues
- 🧪 **PySpark API Coverage** - Additional functions and methods
- 🧪 **Tests** - Additional test coverage and scenarios

---

## Development Setup

```bash
# Install for development
git clone https://github.com/eddiethedean/mock-spark.git
cd mock-spark
pip install -e ".[dev]"

# Setup Java 11 and Spark environment (macOS)
bash tests/setup_spark_env.sh

# Run all tests (parallel execution with 8 cores)
pytest tests/ -v -n 8 -m "not delta"  # Non-Delta tests
pytest tests/ -v -m "delta"            # Delta tests (serial)

# Run all tests with proper isolation
python3 -m pytest tests/ -v -n 8 -m "not delta" && python3 -m pytest tests/ -v -m "delta"

# Format code
black mock_spark tests --line-length 100

# Type checking
mypy mock_spark --config-file mypy.ini
```

---

## License

MIT License - see [LICENSE](LICENSE) file for details.

---

## Links

- **GitHub**: [github.com/eddiethedean/mock-spark](https://github.com/eddiethedean/mock-spark)
- **PyPI**: [pypi.org/project/mock-spark](https://pypi.org/project/mock-spark/)
- **Issues**: [github.com/eddiethedean/mock-spark/issues](https://github.com/eddiethedean/mock-spark/issues)
- **Documentation**: [Full documentation](https://github.com/eddiethedean/mock-spark/tree/main/docs)

---

<div align="center">

**Built with ❤️ for the PySpark community**

*Star ⭐ this repo if Mock Spark helps speed up your tests!*

</div>
