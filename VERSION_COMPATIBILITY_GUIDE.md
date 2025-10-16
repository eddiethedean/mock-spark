# PySpark Version Compatibility Guide

Mock-spark supports version-specific API compatibility to match different PySpark versions. This allows you to test your code against specific PySpark versions and ensure you're not using features that don't exist in your target environment.

## Quick Start

### Method 1: Environment Variable (Recommended)

Set the environment variable before running your code:

```bash
export MOCK_SPARK_PYSPARK_VERSION=3.0
python my_script.py
```

Or inline:

```bash
MOCK_SPARK_PYSPARK_VERSION=3.1 python my_script.py
```

### Method 2: Installation with Environment Variable

Install with a specific version mode:

```bash
MOCK_SPARK_INSTALL_VERSION=3.0 pip install .
```

This creates a `.pyspark_3_0_compat` marker file in the package directory that persists across runs.

### Method 3: Programmatic (In Code)

Set the version before importing functions:

```python
from mock_spark._version_compat import set_pyspark_version

# Must be called BEFORE importing from mock_spark.functions
set_pyspark_version('3.0')

# Now imports will only expose PySpark 3.0 APIs
from mock_spark import MockSparkSession, F

# This will work (3.0 function)
df.select(F.log10(F.col("value")))

# This will raise AttributeError (3.1+ function)
df.select(F.timestamp_seconds(F.col("epoch")))
# AttributeError: module 'pyspark.sql.functions' has no attribute 'timestamp_seconds' (PySpark 3.0.3 compatibility mode)
```

## Supported Versions

| Version | Install Command | Environment Variable |
|---------|----------------|---------------------|
| 3.0.3 | `MOCK_SPARK_INSTALL_VERSION=3.0 pip install .` | `MOCK_SPARK_PYSPARK_VERSION=3.0` |
| 3.1.3 | `MOCK_SPARK_INSTALL_VERSION=3.1 pip install .` | `MOCK_SPARK_PYSPARK_VERSION=3.1` |
| 3.2.4 | `MOCK_SPARK_INSTALL_VERSION=3.2 pip install .` | `MOCK_SPARK_PYSPARK_VERSION=3.2` |
| 3.3.4 | `MOCK_SPARK_INSTALL_VERSION=3.3 pip install .` | `MOCK_SPARK_PYSPARK_VERSION=3.3` |
| 3.4.3 | `MOCK_SPARK_INSTALL_VERSION=3.4 pip install .` | `MOCK_SPARK_PYSPARK_VERSION=3.4` |
| 3.5.2 | `MOCK_SPARK_INSTALL_VERSION=3.5 pip install .` | `MOCK_SPARK_PYSPARK_VERSION=3.5` |
| All (default) | `pip install .` | `unset MOCK_SPARK_PYSPARK_VERSION` |

## How It Works

### API Matrix

Mock-spark includes a comprehensive API matrix (`pyspark_api_matrix.json`) that maps every function and DataFrame method to the PySpark versions where they're available.

When you set a version:
1. The `_version_compat.py` module loads the API matrix
2. Function/method access is intercepted via `__getattr__`
3. If a function isn't available in the target version, an `AttributeError` is raised

### Example: Checking Availability

```python
from mock_spark._version_compat import is_available, get_pyspark_version

# Check if function is available
print(is_available('timestamp_seconds', 'function'))  # False in 3.0, True in 3.1+
print(is_available('make_date', 'function'))  # False in 3.0-3.2, True in 3.3+

# Get current version
print(get_pyspark_version())  # Returns '3.0.3', '3.1.3', etc., or None
```

## Version-Specific Features

### PySpark 3.0.3 Functions

All core PySpark 3.0 functions including:
- Math: `log10`, `log2`, `log1p`, `expm1`, `atan2`
- Hash: `md5`, `sha1`, `sha2`, `crc32`
- Arrays: `array`, `array_repeat`, `sort_array`, `array_distinct`, etc.
- Many more...

### PySpark 3.1.3 Additions

New in 3.1:
- `timestamp_seconds()` - Convert epoch seconds to timestamp
- `raise_error()` - Raise exception with message
- `map_filter()`, `map_zip_with()` - Higher-order map functions
- DataFrame methods: `inputFiles()`, `sameSemantics()`, `semanticHash()`

### PySpark 3.3.4 Additions

New in 3.3:
- `make_date()` - Create date from year/month/day
- Boolean aggregates: `bool_and()`, `bool_or()`, `every()`, `some()`

### PySpark 3.5.2 Additions

New in 3.5:
- `overlay()` - String overlay function
- Advanced aggregates: `max_by()`, `min_by()`, `count_if()`, `any_value()`
- `version()` - Get Spark version

See `PYSPARK_FUNCTION_MATRIX.md` for complete matrix of function availability.

## Testing Different Versions

### In Test Suites

```python
import pytest
from mock_spark._version_compat import set_pyspark_version

@pytest.fixture
def spark_30():
    """Mock Spark session in 3.0 compatibility mode."""
    set_pyspark_version('3.0')
    from mock_spark import MockSparkSession
    return MockSparkSession("test_30")

@pytest.fixture
def spark_31():
    """Mock Spark session in 3.1 compatibility mode."""
    set_pyspark_version('3.1')
    from mock_spark import MockSparkSession
    return MockSparkSession("test_31")

def test_version_specific_feature(spark_31):
    """Test PySpark 3.1 feature."""
    df = spark_31.createDataFrame([{"epoch": 1609459200}])
    # This works in 3.1 mode
    result = df.select(F.timestamp_seconds(F.col("epoch")))
```

### CI/CD Integration

```yaml
# .github/workflows/test.yml
jobs:
  test-pyspark-30:
    name: Test PySpark 3.0 compatibility
    runs-on: ubuntu-latest
    env:
      MOCK_SPARK_PYSPARK_VERSION: "3.0"
    steps:
      - uses: actions/checkout@v3
      - run: pip install -e .
      - run: pytest tests/
  
  test-pyspark-31:
    name: Test PySpark 3.1 compatibility
    runs-on: ubuntu-latest
    env:
      MOCK_SPARK_PYSPARK_VERSION: "3.1"
    steps:
      - uses: actions/checkout@v3
      - run: pip install -e .
      - run: pytest tests/
```

## Clearing Version Settings

### Remove Marker File

```bash
rm mock_spark/.pyspark_*_compat
```

### Unset Environment Variable

```bash
unset MOCK_SPARK_PYSPARK_VERSION
```

### Reset in Code

```python
from mock_spark._version_compat import set_pyspark_version

# Reset to all features
set_pyspark_version(None)
```

## Troubleshooting

### "AttributeError: module 'pyspark.sql.functions' has no attribute 'X'"

This means you're trying to use a function that doesn't exist in your target PySpark version.

**Solution:** Either:
1. Update your code to use functions available in your target version
2. Upgrade your target version
3. Disable version gating: `set_pyspark_version(None)`

### Version not detecting during install

If the automatic detection doesn't work:

```bash
# Explicitly set version via environment variable
MOCK_SPARK_INSTALL_VERSION=3.0 pip install .

# Or use runtime environment variable instead
export MOCK_SPARK_PYSPARK_VERSION=3.0
```

### Checking current version

```python
from mock_spark._version_compat import get_pyspark_version

version = get_pyspark_version()
if version:
    print(f"Running in PySpark {version} compatibility mode")
else:
    print("Running with all features available")
```

## Best Practices

1. **Use environment variables for flexibility** - Easy to change without reinstalling
2. **Document your target version** - Add to README or requirements
3. **Test against actual PySpark** - Mock-spark helps catch API issues early
4. **Be aware of approximations** - Some functions (sha1, crc32) use approximations

## Implementation Details

The version compatibility system uses:
- `mock_spark/pyspark_api_matrix.json` - Function/method availability data
- `mock_spark/_version_compat.py` - Version management logic
- `mock_spark/functions/__init__.py` - `__getattr__` hook for function gating
- `mock_spark/dataframe/dataframe.py` - `__getattribute__` hook for method gating
- `setup.py` - Custom install commands for marker file creation

See `PYSPARK_FUNCTION_MATRIX.md` for the complete availability matrix.

