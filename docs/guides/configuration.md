# Configuration

Sparkless configuration is managed via SparkSession constructor options and the session builder.

## Basic Configuration

```python
from sparkless.sql import SparkSession

spark = SparkSession(
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
spark = SparkSession("MyApp")
```

### Explicit Backend Selection

```python
# Use Polars explicitly
spark = SparkSession.builder \
    .config("spark.sparkless.backend", "polars") \
    .getOrCreate()

# Use DuckDB backend (legacy, requires duckdb package)
spark = SparkSession.builder \
    .config("spark.sparkless.backend", "duckdb") \
    .config("spark.sparkless.backend.maxMemory", "4GB") \
    .config("spark.sparkless.backend.allowDiskSpillover", True) \
    .getOrCreate()

# Use memory backend
spark = SparkSession.builder \
    .config("spark.sparkless.backend", "memory") \
    .getOrCreate()
```

### Backend-Specific Options

**Polars Backend (default):**
- No configuration needed - Polars handles memory and performance automatically
- Thread-safe by design
- Uses Parquet files for persistence

**DuckDB Backend (legacy):**
- `spark.sparkless.backend.maxMemory`: Maximum memory (e.g., "1GB", "4GB")
- `spark.sparkless.backend.allowDiskSpillover`: Allow disk spillover when memory is full

**Note**: `maxMemory` and `allowDiskSpillover` options are ignored for Polars backend.
