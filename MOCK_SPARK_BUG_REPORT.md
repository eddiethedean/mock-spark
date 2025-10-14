# Mock-Spark Bug Report: DDL Schema String Support

## Executive Summary

Mock-spark accepts DDL schema strings (e.g., `"id long, name string"`) in `createDataFrame()`, but the `columns` property fails when the schema is stored as a string instead of a `MockStructType` object. This breaks compatibility with PySpark's behavior and prevents libraries like polyspark from working correctly with mock-spark.

## Bug Details

### Version
- **Package**: mock-spark
- **Version**: 2.1.1
- **Python**: 3.8.18

### Location
- **File**: `mock_spark/dataframe/dataframe.py`
- **Line**: ~280 (in `columns` property)

### Current Implementation

```python
@property
def columns(self) -> List[str]:
    """Get column names."""
    return [field.name for field in self.schema.fields]
```

### The Problem

When `createDataFrame()` is called with a DDL schema string:
```python
df = spark.createDataFrame(data, schema='id long, name string')
```

The DataFrame's `schema` attribute is stored as a **string** (`"id long, name string"`), not a `MockStructType` object. When the `columns` property tries to access `self.schema.fields`, it fails because strings don't have a `fields` attribute.

### Affected Operations

1. **`df.columns`** - Raises `AttributeError: 'str' object has no attribute 'fields'`
2. **`df.select()`** - Uses `columns` internally, fails with same error
3. **Any operation that accesses `df.columns`**

### Working Operations

1. **`df.count()`** - Works correctly
2. **`df.schema`** - Returns the string schema
3. **`df.toPandas()`** - Works (if pandas is installed)

## Root Cause Analysis

### In `_real_createDataFrame` Method

The method signature shows it accepts:
```python
schema: Optional[Union[MockStructType, List[str]]] = None
```

**Issue**: DDL schema strings are not in the type hint, but they are accepted and stored directly without conversion to `MockStructType`.

When a string is passed:
```python
df = MockDataFrame(data, schema, self.storage)  # Line 85
```

The `schema` parameter is passed directly to `MockDataFrame.__init__()`, which stores it as-is without parsing.

### Expected Behavior

PySpark's `createDataFrame()` accepts DDL schema strings and converts them internally to `StructType` objects. Mock-spark should do the same.

## Test Cases

### Test 1: DDL Schema String (Current Behavior)
```python
from mock_spark import MockSparkSession

spark = MockSparkSession()
data = [{'id': 1, 'name': 'test'}]
df = spark.createDataFrame(data, schema='id long, name string')

print(df.schema)  # ✓ Works: "id long, name string"
print(df.count())  # ✓ Works: 1
print(df.columns)  # ✗ Fails: AttributeError: 'str' object has no attribute 'fields'
```

### Test 2: StructType Schema (Working)
```python
from mock_spark import StructType, StructField, StringType, LongType

schema = StructType([
    StructField('id', LongType()),
    StructField('name', StringType())
])

df = spark.createDataFrame(data, schema=schema)

print(df.schema)  # ✓ Works: MockStructType object
print(df.count())  # ✓ Works: 1
print(df.columns)  # ✓ Works: ['id', 'name']
```

## Proposed Fix

### Option 1: Parse DDL Strings to StructType (Recommended)

Add a DDL parser to convert schema strings to `MockStructType` objects in `_real_createDataFrame`:

```python
def _real_createDataFrame(
    self,
    data: Union[List[Dict[str, Any]], List[Any]],
    schema: Optional[Union[MockStructType, List[str], str]] = None,  # Add str to type hint
) -> "MockDataFrame":
    # ... existing code ...
    
    # NEW: Handle DDL schema strings
    if isinstance(schema, str):
        schema = self._parse_ddl_schema(schema)
    
    # ... rest of method ...

def _parse_ddl_schema(self, ddl_string: str) -> MockStructType:
    """Parse DDL schema string into MockStructType.
    
    Example: "id long, name string" -> MockStructType with 2 fields
    """
    # Parse the DDL string and create MockStructField objects
    # This is similar to PySpark's StructType.fromDDL()
    pass
```

### Option 2: Make `columns` Property Defensive

Update the `columns` property to handle both string and StructType schemas:

```python
@property
def columns(self) -> List[str]:
    """Get column names."""
    if isinstance(self.schema, str):
        # Parse DDL string to extract column names
        # "id long, name string" -> ['id', 'name']
        return [field.split(':')[0].strip() for field in self.schema.split(',')]
    return [field.name for field in self.schema.fields]
```

**Pros**: Quick fix, minimal changes
**Cons**: Doesn't fully parse types, just extracts names

### Option 3: Store Parsed Schema

Parse DDL strings at DataFrame creation time and store the parsed `MockStructType`:

```python
# In MockDataFrame.__init__()
if isinstance(schema, str):
    self._schema_string = schema  # Keep original for display
    self._schema = self._parse_ddl_schema(schema)  # Parse to StructType
else:
    self._schema = schema
```

## Impact Assessment

### Libraries Affected

1. **polyspark** - Uses DDL schema strings when PySpark is not available
2. **Any library** that generates DDL schemas for testing with mock-spark

### Use Cases Blocked

1. Testing PySpark code with mock-spark using DDL schema strings
2. Schema inference libraries that generate DDL strings
3. Cross-compatibility between PySpark and mock-spark

## Recommendation

**Implement Option 1** (Parse DDL Strings to StructType) because:

1. ✅ **Full compatibility** with PySpark's behavior
2. ✅ **Type information preserved** - enables proper type checking
3. ✅ **Future-proof** - supports all PySpark DDL features
4. ✅ **Clean architecture** - schema is always a `MockStructType`

### Implementation Priority

**HIGH** - This is a critical bug that breaks basic DataFrame operations when using DDL schema strings, which is a common pattern in PySpark.

## Additional Context

### PySpark's Behavior

PySpark's `StructType.fromDDL()` method parses DDL strings:
```python
from pyspark.sql.types import StructType

schema = StructType.fromDDL("id long, name string")
print(schema)  # StructType([StructField('id', LongType(), True), StructField('name', StringType(), True)])
```

Mock-spark should provide similar functionality to maintain API compatibility.

### DDL Schema Format

PySpark DDL format examples:
- Simple: `"id long, name string"`
- With nullability: `"id long, name string, age int"`
- Nested: `"id long, address struct<street:string,city:string>"`
- Arrays: `"tags array<string>"`
- Maps: `"metadata map<string,string>"`

## References

- Mock-spark GitHub: https://github.com/mock-spark/mock-spark
- PySpark StructType.fromDDL(): https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.types.StructType.fromDDL.html
- Polyspark DDL Implementation: `/Users/odosmatthews/Documents/polyspark/polyspark/schema.py`

## Test Script

See attached test script that reproduces the bug:

```python
# test_mock_spark_bug.py
from mock_spark import MockSparkSession

spark = MockSparkSession()
data = [{'id': 1, 'name': 'test'}]

# This works
df = spark.createDataFrame(data, schema='id long, name string')
print(f"✓ DataFrame created")
print(f"✓ Count: {df.count()}")
print(f"✓ Schema: {df.schema}")

# This fails
try:
    columns = df.columns
    print(f"✓ Columns: {columns}")
except AttributeError as e:
    print(f"✗ Error: {e}")
```

## Contact

For questions about this bug report, contact the mock-spark maintainers or open an issue on the mock-spark GitHub repository.

---

**Report Generated**: 2025-01-27
**Reporter**: Polyspark Development Team
**Severity**: HIGH
**Priority**: P1 (Blocks core functionality)

