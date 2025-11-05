# Threading Support

Mock-Spark's DuckDB backend includes thread-safe schema creation to support parallel execution contexts such as `ThreadPoolExecutor` and `pytest-xdist`.

## DuckDB Connection Isolation Model

DuckDB connections are **thread-local**, meaning each thread gets its own connection from the SQLAlchemy connection pool. This has important implications:

1. **Schema Visibility**: Schemas created in one thread's connection are **not visible** to other threads (especially for in-memory databases)
2. **Connection Pooling**: SQLAlchemy manages a connection pool, but each thread receives its own connection instance
3. **In-Memory Databases**: For in-memory DuckDB databases, each thread has a completely isolated database instance

## Thread-Safe Schema Creation

Mock-Spark automatically handles thread-local schema creation:

```python
from mock_spark import MockSparkSession
from concurrent.futures import ThreadPoolExecutor

spark = MockSparkSession("MyApp")

def create_table_in_thread(schema_name, table_name):
    """Create a table in a worker thread."""
    # Schema is automatically created in this thread's connection
    df = spark.createDataFrame([{"id": 1, "name": "Alice"}])
    df.write.saveAsTable(f"{schema_name}.{table_name}")

# Use ThreadPoolExecutor
with ThreadPoolExecutor(max_workers=4) as executor:
    futures = [
        executor.submit(create_table_in_thread, f"schema_{i}", f"table_{i}")
        for i in range(10)
    ]
    for future in futures:
        future.result()  # All operations succeed
```

## How It Works

Mock-Spark implements thread-safe schema creation through:

1. **Thread-Local Caching**: Each thread maintains a cache of schemas it has created
2. **Automatic Schema Creation**: When a schema is needed, it's automatically created in the current thread's connection
3. **Retry Logic**: Schema creation includes retry logic (3 attempts) with exponential backoff to handle race conditions
4. **Connection Verification**: After creation, the schema is verified to exist in the thread's connection

## Best Practices

### Using with ThreadPoolExecutor

When using `ThreadPoolExecutor` or similar parallel execution frameworks:

```python
from concurrent.futures import ThreadPoolExecutor

def process_data(schema_name, data):
    """Process data in a worker thread."""
    spark = MockSparkSession("MyApp")
    df = spark.createDataFrame(data)
    # Schema is automatically created in this thread
    df.write.saveAsTable(f"{schema_name}.results")

with ThreadPoolExecutor(max_workers=8) as executor:
    futures = [
        executor.submit(process_data, f"schema_{i}", data_list[i])
        for i in range(10)
    ]
    for future in futures:
        future.result()
```

### Using with pytest-xdist

When running tests with `pytest-xdist`:

```bash
# Run tests in parallel with 8 workers
pytest -n 8 tests/

# Mock-Spark automatically handles schema creation in each process/thread
```

No special configuration is needed - Mock-Spark handles thread-local schema creation automatically.

## Limitations

### In-Memory Databases

For in-memory DuckDB databases, each thread has a completely isolated database. This means:

- **Data Isolation**: Data created in one thread is not visible to other threads
- **Schema Isolation**: Schemas must be created in each thread that needs them
- **Use Case**: This is typically fine for testing where each test gets its own isolated environment

### Persistent Databases

For persistent databases (using `db_path` parameter), schemas are shared across threads within the same process, but:

- **Schema Creation**: Still needs to be done per-thread to ensure visibility
- **Data Sharing**: Data is shared across threads (within the same process)
- **Process Isolation**: Different processes (e.g., pytest-xdist workers) have separate database files

## Error Handling

If schema creation fails, Mock-Spark raises clear error messages:

```python
try:
    storage.create_schema("my_schema")
except RuntimeError as e:
    # Error message indicates threading issue
    print(f"Schema creation failed: {e}")
```

Error messages include:
- Schema name that failed to create
- Indication that it may be a threading issue
- Suggestion to check DuckDB connection

## Performance Considerations

Thread-local schema tracking adds minimal overhead:

- **Fast Path**: Thread-local cache provides O(1) lookup for existing schemas
- **Retry Logic**: Only activates when schema doesn't exist (rare case)
- **Overhead**: < 5% overhead for schema operations

## Example: Parallel Pipeline Execution

```python
from mock_spark import MockSparkSession
from concurrent.futures import ThreadPoolExecutor

def run_pipeline_step(step_id, input_data):
    """Run a pipeline step in a worker thread."""
    spark = MockSparkSession("Pipeline")
    
    # Create input DataFrame
    df = spark.createDataFrame(input_data)
    
    # Process data
    result = df.select("id", "value").filter("value > 10")
    
    # Save to table (schema automatically created in this thread)
    result.write.saveAsTable(f"step_{step_id}.results")
    
    return result.count()

# Run multiple pipeline steps in parallel
with ThreadPoolExecutor(max_workers=4) as executor:
    futures = [
        executor.submit(run_pipeline_step, i, data[i])
        for i in range(10)
    ]
    results = [future.result() for future in futures]
```

## Troubleshooting

### Schema Not Visible in Thread

If you encounter issues where a schema created in one thread isn't visible in another:

1. **Check Thread Isolation**: Ensure you're not expecting cross-thread schema visibility for in-memory databases
2. **Verify Schema Creation**: Use `storage.schema_exists()` to check if schema exists in current thread
3. **Check Error Messages**: Look for threading-related error messages

### Segmentation Faults

If you encounter segmentation faults when running tests in parallel:

1. **Update Mock-Spark**: Ensure you're using a version with thread-safe schema creation (2.16.1+)
2. **Check Test Isolation**: Ensure tests properly clean up between runs
3. **Run with Fewer Workers**: Try `pytest -n 2` instead of `-n 8` to isolate the issue

## See Also

- [Configuration Guide](./configuration.md) - Configuration options for Mock-Spark
- [Pytest Integration Guide](./pytest_integration.md) - Using Mock-Spark with pytest
- [Memory Management Guide](./memory_management.md) - Managing memory in parallel contexts

