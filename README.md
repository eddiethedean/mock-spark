# Mock Spark

<div align="center">

**🚀 Test PySpark code at lightning speed—no JVM required**

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![PySpark 3.2-3.5](https://img.shields.io/badge/pyspark-3.2--3.5-orange.svg)](https://spark.apache.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![PyPI version](https://badge.fury.io/py/mock-spark.svg)](https://badge.fury.io/py/mock-spark)
[![Tests](https://img.shields.io/badge/tests-884+%20passing%20%7C%200%20failing-brightgreen.svg)](https://github.com/eddiethedean/mock-spark)
[![Type Checked](https://img.shields.io/badge/mypy-106%20files%20clean-blue.svg)](https://github.com/python/mypy)
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
| 📦 **Zero Java** | Pure Python with pluggable backend architecture (DuckDB default) |
| 🧪 **100% Compatible** | Full PySpark 3.2-3.5 API support |
| 🔄 **Lazy Evaluation** | Mirrors PySpark's execution model |
| 🏭 **Production Ready** | 857+ passing tests, 100% mypy typed, CTE-optimized queries |
| 🔧 **Modular Design** | DDL parsing via standalone spark-ddl-parser package |
| ✅ **Tested** | Verified on Python 3.9-3.13 + PySpark 3.2-3.5 |

### Perfect For

- **Unit Testing** - Fast, isolated test execution with automatic cleanup
- **CI/CD Pipelines** - Reliable tests without infrastructure or resource leaks
- **Local Development** - Prototype without Spark cluster
- **Documentation** - Runnable examples without setup
- **Learning** - Understand PySpark without complexity
- **Integration Tests** - Configurable memory limits for large dataset testing

---

## Recent Updates

### Latest (Version 2.17.0)

**Thread Safety & Code Quality Improvements**:
- 🔒 **Thread-Safe DuckDB Backend** - Added thread-safe locking for schema and table operations to prevent race conditions and segmentation faults
- 🛡️ **Connection Isolation Handling** - Improved handling of DuckDB's in-memory connection isolation in multi-threaded scenarios
- 🐛 **Fixed Deprecated APIs** - Replaced deprecated `datetime.utcnow()` with `datetime.now(timezone.utc)` for future Python compatibility
- ✨ **Code Quality** - Ruff formatting and type checking improvements
- ✅ **All tests passing** - 884+ tests validated
- 📦 **Production-ready** - Stable release with improved thread safety

### Version 2.16.0

**Critical Regression Fixes** - Fixed three critical regressions from version 2.15.0:
- 🔧 **Schema Conversion Failure** - Fixed empty table schemas when creating tables with `StructType` - now properly validates schema before table creation
- 📊 **Empty DataFrame Column Detection** - Fixed empty DataFrames with explicit schemas losing column information - schemas are now preserved even when data is empty
- 🔄 **Schema Preservation** - Fixed schema loss during transformations (select, filter, withColumn, groupBy) on empty DataFrames - schemas are now correctly preserved through all operations
- ✅ **Comprehensive Test Coverage** - Added 27 regression tests to prevent future issues
- ✅ **All tests passing** - 857+ tests validated including new regression tests
- 📦 **Production-ready** - Stable release with all critical issues resolved

### Version 2.15.0

**Table Persistence & CTE Optimization Improvements** - Major fixes for persistent storage and query optimization:
- 🗄️ **Table Persistence** - Fixed cross-session table persistence with proper DuckDB connection configuration
- 🔍 **Schema Discovery** - Enhanced schema and table discovery when opening persistent databases
- ⚡ **CTE Optimization** - Fixed column reference issues in CTE filter conditions
- 🔄 **Catalog Synchronization** - Improved `catalog.tableExists()` reliability with comprehensive checks
- 🐛 **SQL Syntax Compatibility** - Fixed type casting and STRPTIME format string generation for DuckDB
- ✅ **All tests passing** - 857+ tests validated including new persistence tests
- 📦 **Production-ready** - Stable release with improved reliability

### Version 2.13.1

**Version Bump** - Patch release for stability and compatibility:
- 🔧 **Version update** - Bumped to 2.13.1 for consistency
- ✅ **All tests passing** - 857+ tests validated
- ✅ **Type safety maintained** - 100% mypy compliance preserved
- 📦 **Production-ready** - Stable release ready for deployment

### Version 2.13.0

**Complete Type Safety & Mypy Compliance** - Zero typing errors across entire codebase:
- ✅ **30 mypy errors fixed** - Complete type safety with proper annotations throughout
- ✅ **100% mypy compliance** - Zero typing errors across all 7 files with type issues
- ✅ **Comprehensive type coverage** - Fixed MockRow, table manager, functions, and DataFrame types
- ✅ **All tests passing** - 857+ tests validated after type fixes
- ✅ **Production-ready** - Full type safety with enhanced developer experience

**What's New Since 2.12.0:**

**Version 2.12.1 - Pluggable Backend Architecture & SRP Refactoring:**
- 🔌 **Pluggable Backend System** - Modular architecture with DuckDB, memory, and file backends
- 🧪 **Complete Test Suite Overhaul** - PySpark-free compatibility testing with comprehensive test coverage
- 🏗️ **Single Responsibility Principle Refactoring** - Major architectural improvements:
  - **TypeConverter** - Extracted type conversion logic for better separation of concerns
  - **DataFrameFormatter** - Specialized display operations with proper column width calculation
  - **SetOperations, JoinOperations, AggregationOperations** - Dedicated operation classes
  - **CollectionHandler, ValidationHandler** - Specialized handler classes
  - **Schema Management** - Extracted schema management logic
- 🔄 **Backend Module Organization** - Improved DuckDB backend structure and SQLAlchemyMaterializer refactoring
- 🎯 **Interface Standardization** - Unified storage interfaces with proper type annotations

**Version 2.13.0 - Completes Type Safety (Current Release):**
- 🎯 **Complete Type Safety** - Fixed all 30 mypy typing errors introduced during SRP refactoring
  - **MockRow data types** - Fixed 12 errors related to Union[List[Tuple], Dict] type handling
  - **Table manager types** - Fixed 7 errors with proper List[Any] annotations
  - **Exception handling** - Replaced non-existent MockSparkNotImplementedError with proper exception
  - **Function return types** - Fixed nvl2 and other functions with proper type annotations
  - **Query executor** - Fixed type assignment issues with proper MockDataType handling
  - **CTE query builder** - Fixed missing return statement in all code paths
  - **DataFrame types** - Fixed Optional[str] and list type mismatches
- 🔧 **Import Path Fixes** - Corrected MockLiteral imports in conditional functions
- 🧪 **Test Validation** - All 857+ tests passing with type-safe implementations
- 📦 **Code Quality** - 100% ruff compliance with proper formatting

### Version 2.7.0 Highlights

**Extended PySpark API Coverage:**
- 🎯 **User-Defined Functions** - Lambda and decorator patterns with `udf()`
- ⏰ **Time Windows** - Group by time periods with `window()` function
- 📝 **Value Replacement** - Data cleaning with `df.replace()`
- 🔍 **Regex Columns** - Select columns by pattern with `df.colRegex()`
- 🔄 **Backward Compatibility** - Deprecated aliases for legacy code

### Version 2.6.0 - 2.7.0 Highlights

**Complete PySpark 3.2-3.5 API Support:**
- 🔥 **Higher-Order Functions** - Lambda support for `transform`, `filter`, `exists`, `aggregate`, `zip_with`
- 📊 **Advanced Aggregates** - `max_by`, `min_by`, `count_if`, `median`, `mode`, `percentile`
- 🗺️ **Map Operations** - `map_filter`, `transform_keys`, `transform_values` with lambda
- 📄 **XML Processing** - Complete suite: `from_xml`, `to_xml`, `xpath_*` functions
- 🐼 **Pandas Integration** - `mapInPandas`, `applyInPandas` for hybrid workflows
- 🧮 **Math Extensions** - Hyperbolic functions, bitwise aggregates, trigonometric extensions
- ⏰ **Enhanced DateTime** - Timezone handling, time windows, interval arithmetic

### Version 2.0.0 - Architecture Overhaul

**Zero Raw SQL + Type Safety:**
- 🎯 **Pure SQLAlchemy** - 100% type-safe database operations
- 🗄️ **Backend Flexibility** - DuckDB, PostgreSQL, MySQL, SQLite support
- 🔧 **Modular Design** - Standalone `spark-ddl-parser` package
- ⚡ **Delta Lake** - Time travel, MERGE operations, schema evolution

---

## Quick Start

### Installation

**Standard Installation (All Features):**
```bash
pip install mock-spark
```

**Version-Specific Installation:**

Match a specific PySpark version's API (only exposes functions/methods available in that version):

```bash
# Match PySpark 3.0 API
pip install mock-spark[pyspark-3-0]

# Match PySpark 3.1 API  
pip install mock-spark[pyspark-3-1]

# Match PySpark 3.2 API
pip install mock-spark[pyspark-3-2]

# Match PySpark 3.3 API
pip install mock-spark[pyspark-3-3]

# Match PySpark 3.4 API
pip install mock-spark[pyspark-3-4]

# Match PySpark 3.5 API
pip install mock-spark[pyspark-3-5]
```

**Environment Variable:**

You can also set PySpark compatibility mode via environment variable:

```bash
# Set version at runtime
export MOCK_SPARK_PYSPARK_VERSION=3.1

# Or inline
MOCK_SPARK_PYSPARK_VERSION=3.2 python my_tests.py
```

**Why Version-Specific Installation?**

- **Exact API matching**: Test code against a specific PySpark version's API
- **Catch compatibility issues**: Functions not available in target version raise `AttributeError`
- **Safe upgrades**: Ensure code works with older PySpark versions before upgrading
- **CI/CD flexibility**: Test against multiple PySpark versions in parallel

See [`PYSPARK_FUNCTION_MATRIX.md`](https://github.com/eddiethedean/mock-spark/blob/main/PYSPARK_FUNCTION_MATRIX.md) for complete function availability across versions.

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
# Output: [Row(name=Bob)]

# Show the DataFrame
df.show()
# Output:
# MockDataFrame[2 rows, 2 columns]
# age name 
# 25    Alice  
# 30    Bob
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

### Backend Selection

Mock Spark supports multiple backend engines for different use cases:

```python
# Default backend (DuckDB) - backward compatible
spark = MockSparkSession("MyApp")

# Explicit backend selection via config
spark = MockSparkSession.builder \
    .config("spark.mock.backend", "duckdb") \
    .config("spark.mock.backend.maxMemory", "4GB") \
    .config("spark.mock.backend.allowDiskSpillover", True) \
    .getOrCreate()

# Memory backend for lightweight testing
spark = MockSparkSession.builder \
    .config("spark.mock.backend", "memory") \
    .getOrCreate()

# File backend for persistent storage
spark = MockSparkSession.builder \
    .config("spark.mock.backend", "file") \
    .config("spark.mock.backend.basePath", "/tmp/mock_spark") \
    .getOrCreate()
```

**Available Backends:**
- **DuckDB** (default): High-performance analytical database
- **Memory**: In-memory storage for lightweight testing
- **File**: File-based storage for persistent data

**Backend Configuration:**
- `spark.mock.backend`: Backend type ("duckdb", "memory", "file")
- `spark.mock.backend.maxMemory`: Memory limit (e.g., "1GB", "4GB")
- `spark.mock.backend.allowDiskSpillover`: Allow disk usage when memory is full
- `spark.mock.backend.basePath`: Base path for file backend

---

## Core Features

### 🚀 CTE Query Optimization (New in 2.10.0)

DataFrame operation chains are now automatically optimized using Common Table Expressions:

```python
# Enable lazy evaluation for CTE optimization
df = spark.createDataFrame(data).withLazy(True)

# This entire chain executes as ONE optimized query:
result = (
    df.filter(F.col("age") > 25)           # CTE 0: WHERE clause
      .select("name", "age", "salary")     # CTE 1: Column selection
      .withColumn("bonus", F.col("salary") * 0.1)  # CTE 2: New column
      .orderBy(F.desc("salary"))           # CTE 3: ORDER BY
      .limit(10)                           # CTE 4: LIMIT
).collect()  # Single query execution here

# Performance: 5-10x faster than creating 5 intermediate tables
```

**Benefits**: Reduced I/O, lower memory usage, faster execution for operation chains.

### DataFrame Operations
- **Transformations**: `select`, `filter`, `withColumn`, `drop`, `distinct`, `orderBy`, `replace`
- **Aggregations**: `groupBy`, `agg`, `count`, `sum`, `avg`, `min`, `max`, `median`, `mode`
- **Joins**: `inner`, `left`, `right`, `outer`, `cross`
- **Advanced**: `union`, `pivot`, `unpivot`, `explode`, `transform`

### 120+ PySpark Functions

Mock Spark implements comprehensive function coverage across 10+ categories:

| Category | Functions | Examples |
|----------|-----------|----------|
| **String** (40+) | Text manipulation, regex, formatting | `upper`, `concat`, `regexp_extract`, `soundex`, `url_encode` |
| **Math** (35+) | Arithmetic, trigonometry, rounding | `abs`, `sqrt`, `sin`, `cos`, `cot`, `ln` |
| **DateTime** (30+) | Date/time operations, timezones | `date_add`, `hour`, `weekday`, `convert_timezone` |
| **Array** (25+) | Array manipulation, lambdas | `array_distinct`, `transform`, `filter`, `aggregate` |
| **Aggregate** (20+) | Statistical functions | `sum`, `avg`, `median`, `percentile`, `max_by` |
| **Map** (10+) | Dictionary operations | `map_keys`, `map_filter`, `transform_values` |
| **Conditional** (8+) | Logic and null handling | `when`, `coalesce`, `ifnull`, `nullif` |
| **Window** (8+) | Ranking and analytics | `row_number`, `rank`, `lag`, `lead` |
| **XML** (9+) | XML parsing and generation | `from_xml`, `to_xml`, `xpath_*` |
| **Bitwise** (6+) | Bit manipulation | `bit_count`, `bit_and`, `bit_xor` |

📖 **See complete function list**: [`PYSPARK_FUNCTION_MATRIX.md`](https://github.com/eddiethedean/mock-spark/blob/main/PYSPARK_FUNCTION_MATRIX.md) - Full compatibility matrix across PySpark 3.0-3.5

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
result.show()
# Output:
# MockDataFrame[2 rows, 2 columns]
# name  salary
# Alice   60000   
# Bob     40000
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
# Output:
# MockDataFrame[1 rows, 3 columns]
# operation timestamp            version
# WRITE       2024-01-15 10:30:00   0
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

### Table Persistence (New in 2.15.0)

Tables created with `saveAsTable()` can now persist across multiple sessions when using persistent storage:

```python
# First session - create table
spark1 = MockSparkSession("App1", db_path="test.db")
df = spark1.createDataFrame([{"id": 1, "name": "Alice"}])
df.write.mode("overwrite").saveAsTable("schema.my_table")
spark1.stop()

# Second session - table persists
spark2 = MockSparkSession("App2", db_path="test.db")
assert spark2.catalog.tableExists("schema", "my_table")  # ✅ True
result = spark2.table("schema.my_table").collect()  # ✅ Works!
spark2.stop()
```

**Key Features:**
- **Cross-Session Persistence**: Tables persist when using `db_path` parameter
- **Schema Discovery**: Automatically discovers existing schemas and tables when opening persistent databases
- **Catalog Synchronization**: Improved `catalog.tableExists()` reliability
- **Data Integrity**: Full support for `append` and `overwrite` modes across sessions
- **In-Memory Default**: Default behavior (no `db_path`) provides test isolation without persistence

**Note**: When `db_path` is `None` (default), tables use in-memory storage and don't persist between sessions. This provides test isolation. For tables that need to persist across pipeline runs, provide a `db_path`.

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

### Related Packages
- 🔧 [spark-ddl-parser](https://github.com/eddiethedean/spark-ddl-parser) - Zero-dependency PySpark DDL schema parser (used by mock-spark)

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

## Feature Highlights

### Complete API Coverage
See [`PYSPARK_FUNCTION_MATRIX.md`](https://github.com/eddiethedean/mock-spark/blob/main/PYSPARK_FUNCTION_MATRIX.md) for the complete compatibility matrix showing all 120 functions and 70 DataFrame methods across PySpark versions 3.0-3.5.

**Quick Examples:**

```python
# String operations
df.withColumn("upper_name", F.upper(F.col("name"))) \
  .withColumn("phonetic", F.soundex(F.col("name")))

# Array operations with lambdas
df.withColumn("doubled", F.transform(F.col("nums"), lambda x: x * 2)) \
  .withColumn("evens", F.filter(F.col("nums"), lambda x: x % 2 == 0))

# Statistical aggregates
df.groupBy("dept").agg(
    F.median("salary"),
    F.percentile("salary", 0.95),
    F.max_by("employee", "salary")
)

# User-defined functions
square = F.udf(lambda x: x * x, IntegerType())
df.select(square("value"))

# Time-based windowing
df.groupBy(F.window("timestamp", "10 minutes")).count()
```

### Previous Major Releases

**v1.0.0** - DuckDB Integration, Performance Boost  
**v1.3.0** - Configurable Memory, Test Isolation  
**v1.4.0** - Delta Lake Support, Parallel Testing  
**v2.0.0** - Zero Raw SQL Architecture, Type Safety  
**v2.3.0** - Delta Time Travel, MERGE Operations  
**v2.5.0** - Complete PySpark 3.2 API  
**v2.6.0** - Higher-Order Functions, Lambda Support  
**v2.7.0** - Extended 3.1/3.3/3.5 Compatibility  
**v2.8.0** - Iteration Methods, Streaming Support  
**v2.9.0** - CTE Query Optimization, 5-10x Performance Boost  
**v2.10.0** - Boolean Literal Support, Type Safety Improvements  
**v2.11.0** - Enhanced Error Handling, Improved Type Safety, Test Suite Robustness  
**v2.12.0** - Complete Type Safety & Test Suite Overhaul, Interface Standardization  
**v2.12.1** - Pluggable Backend Architecture, SRP Refactoring, PySpark-free Testing, Modular Design  
**v2.13.0** - Complete Mypy Type Safety - Fixed all 30 typing errors from SRP refactoring, 100% type compliance  
**v2.13.1** - Version bump for stability and compatibility  
**v2.15.0** - Table Persistence & CTE Optimization - Fixed cross-session persistence, schema discovery, column references, and catalog synchronization  
**v2.16.0** - Critical Regression Fixes - Fixed schema conversion failures, empty DataFrame column detection, and schema preservation during transformations  
**v2.16.1** - Version bump  
**v2.17.0** - Thread Safety & Code Quality - Added thread-safe DuckDB backend improvements, fixed deprecated datetime APIs, and improved code quality with ruff formatting

---

## Known Limitations & Future Features

While Mock Spark provides comprehensive PySpark compatibility, some advanced features are planned for future releases:

**Type System**: Strict runtime type validation, custom validators  
**Error Handling**: Enhanced error messages with recovery strategies  
**Functions**: Extended date/time, math, and null handling  
**Performance**: Advanced query optimization, parallel execution, intelligent caching  
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

# Linting
ruff check .
```

---

## Compatibility Testing

Mock Spark is tested against multiple Python and PySpark version combinations to ensure broad compatibility.

### Run Compatibility Matrix Tests

Test mock-spark against Python 3.9-3.13 and PySpark 3.2-3.5:

```bash
# Run all compatibility tests (requires Docker)
./run_compatibility_tests.sh

# Or run directly
python tests/compatibility_matrix/run_matrix_tests.py
```

This will:
- Build Docker images for each Python/PySpark combination
- Run critical tests in isolated containers
- Generate `COMPATIBILITY_REPORT.md` with results

### Test a Single Combination

```bash
# Test Python 3.10 + PySpark 3.3.4
./tests/compatibility_matrix/test_single_combination.sh 3.10 3.3.4
```

See [tests/compatibility_matrix/README.md](https://github.com/eddiethedean/mock-spark/blob/main/tests/compatibility_matrix/README.md) for more details.

---

## License

MIT License - see [LICENSE](https://github.com/eddiethedean/mock-spark/blob/main/LICENSE) file for details.

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
