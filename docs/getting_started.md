# Getting Started with Mock Spark

## Installation

Install Mock Spark using pip:

```bash
pip install mock-spark
```

For development with testing tools:

```bash
pip install mock-spark[dev]
```

## Quick Start

### Basic Example

```python
from mock_spark import MockSparkSession, F

# Create session
spark = MockSparkSession("MyApp")

# Create DataFrame
data = [
    {"id": 1, "name": "Alice", "age": 25},
    {"id": 2, "name": "Bob", "age": 30},
]
df = spark.createDataFrame(data)

# Operations work just like PySpark
result = df.filter(F.col("age") > 25).select("name")
print(result.collect())  # [Row(name='Bob')]
```

### Drop-in PySpark Replacement

Mock Spark is designed to be a drop-in replacement for PySpark:

```python
# Before (PySpark)
from pyspark.sql import SparkSession

# After (Mock Spark)
from mock_spark import MockSparkSession as SparkSession
```

That's it! Your existing PySpark code works unchanged.

## Core Features

### DataFrame Operations

```python
from mock_spark import MockSparkSession, F

spark = MockSparkSession("Example")
data = [
    {"name": "Alice", "dept": "Engineering", "salary": 80000},
    {"name": "Bob", "dept": "Sales", "salary": 75000},
    {"name": "Charlie", "dept": "Engineering", "salary": 90000},
]
df = spark.createDataFrame(data)

# Filter
high_earners = df.filter(F.col("salary") > 75000)

# Select
names = df.select("name", "dept")

# Aggregations
dept_avg = df.groupBy("dept").avg("salary")
```

### Window Functions

```python
from mock_spark.window import MockWindow as Window

# Ranking within departments
window_spec = Window.partitionBy("dept").orderBy(F.desc("salary"))
ranked = df.select(
    "name",
    "dept",
    "salary",
    F.row_number().over(window_spec).alias("rank")
)
```

### SQL Queries

```python
# Create temporary view
df.createOrReplaceTempView("employees")

# Run SQL queries
result = spark.sql("SELECT name, salary FROM employees WHERE salary > 80000")
result.show()
```

## Testing with Mock Spark

### Unit Test Example

```python
import pytest
from mock_spark import MockSparkSession, F

def test_data_transformation():
    """Test DataFrame transformation logic."""
    spark = MockSparkSession("TestApp")
    
    # Test data
    data = [{"value": 10}, {"value": 20}, {"value": 30}]
    df = spark.createDataFrame(data)
    
    # Apply transformation
    result = df.filter(F.col("value") > 15)
    
    # Assertions
    assert result.count() == 2
    rows = result.collect()
    assert rows[0]["value"] == 20
    assert rows[1]["value"] == 30

def test_aggregation():
    """Test aggregation logic."""
    spark = MockSparkSession("TestApp")
    
    data = [
        {"category": "A", "value": 100},
        {"category": "A", "value": 200},
        {"category": "B", "value": 150},
    ]
    df = spark.createDataFrame(data)
    
    # Group and aggregate
    result = df.groupBy("category").sum("value")
    
    # Verify results
    assert result.count() == 2
```

## Lazy Evaluation

Mock Spark mirrors PySpark's lazy evaluation model:

```python
# Transformations are queued (not executed)
filtered = df.filter(F.col("age") > 25)
selected = filtered.select("name")
# No execution yet!

# Actions trigger execution
rows = selected.collect()  # ← Executes now
count = selected.count()   # ← Executes now
```

Control evaluation mode:

```python
# Lazy (default, recommended)
spark = MockSparkSession("App", enable_lazy_evaluation=True)

# Eager (for legacy tests)
spark = MockSparkSession("App", enable_lazy_evaluation=False)
```

## Performance

Mock Spark provides significant speed improvements:

| Operation | PySpark | Mock Spark | Speedup |
|-----------|---------|------------|---------|
| Session Creation | 30-45s | 0.1s | 300x |
| Simple Query | 2-5s | 0.01s | 200x |
| Full Test Suite | 5-10min | 30-60s | 10x |

## Next Steps

- **[API Reference](api_reference.md)** - Complete API documentation
- **[SQL Operations](sql_operations_guide.md)** - SQL query examples
- **[Testing Utilities](testing_utilities_guide.md)** - Test helpers and fixtures
- **[Examples](../examples/)** - More code examples

## Getting Help

- **GitHub**: [github.com/eddiethedean/mock-spark](https://github.com/eddiethedean/mock-spark)
- **Issues**: [github.com/eddiethedean/mock-spark/issues](https://github.com/eddiethedean/mock-spark/issues)
- **Documentation**: [docs/](.)
