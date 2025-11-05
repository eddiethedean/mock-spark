# Configuration

Mock-Spark configuration is managed via MockSparkSession constructor options and the session builder.

## Basic Configuration

```python
from mock_spark import MockSparkSession

spark = MockSparkSession(
    validation_mode="relaxed",           # strict | relaxed | minimal
    enable_type_coercion=True,
)
```

Key settings:
- validation_mode: controls strictness of schema/data checks
- enable_type_coercion: attempts to coerce types during DataFrame creation

## Backend Configuration (v3.0.0+)

### Default Backend (Polars)

```python
# Polars is the default backend in v3.0.0+
spark = MockSparkSession("MyApp")
```

### Explicit Backend Selection

```python
# Use Polars explicitly
spark = MockSparkSession.builder \
    .config("spark.mock.backend", "polars") \
    .getOrCreate()

# Use DuckDB backend (legacy, requires duckdb package)
spark = MockSparkSession.builder \
    .config("spark.mock.backend", "duckdb") \
    .config("spark.mock.backend.maxMemory", "4GB") \
    .config("spark.mock.backend.allowDiskSpillover", True) \
    .getOrCreate()

# Use memory backend
spark = MockSparkSession.builder \
    .config("spark.mock.backend", "memory") \
    .getOrCreate()
```

### Backend-Specific Options

**Polars Backend (default):**
- No configuration needed - Polars handles memory and performance automatically
- Thread-safe by design
- Uses Parquet files for persistence

**DuckDB Backend (legacy):**
- `spark.mock.backend.maxMemory`: Maximum memory (e.g., "1GB", "4GB")
- `spark.mock.backend.allowDiskSpillover`: Allow disk spillover when memory is full

**Note**: `maxMemory` and `allowDiskSpillover` options are ignored for Polars backend.
