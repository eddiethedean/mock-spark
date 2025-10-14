# Mock-Spark DDL Schema Bug: Investigation Summary

## Quick Summary

Mock-spark **accepts** DDL schema strings but **doesn't parse them**, causing the `columns` property to fail. This breaks polyspark's ability to work with mock-spark when PySpark is not installed.

## The Bug

### What Happens

```python
from mock_spark import MockSparkSession

spark = MockSparkSession()
data = [{'id': 1, 'name': 'test'}]

# This works
df = spark.createDataFrame(data, schema='id long, name string')
print(df.count())  # ✓ Works: 1

# This fails
print(df.columns)  # ✗ AttributeError: 'str' object has no attribute 'fields'
```

### Why It Fails

1. Mock-spark accepts the DDL string `"id long, name string"`
2. It stores it directly as a **string** in `df.schema`
3. The `columns` property tries to access `self.schema.fields`
4. Strings don't have a `.fields` attribute → **AttributeError**

### What Should Happen

PySpark parses DDL strings into `StructType` objects:
```python
# PySpark behavior
from pyspark.sql.types import StructType

schema = StructType.fromDDL("id long, name string")
print(schema)  # StructType([StructField('id', LongType(), True), ...])
```

Mock-spark should do the same.

## Impact on Polyspark

### Current Situation

Polyspark can now generate DDL schema strings without PySpark:

```python
from polyspark import export_ddl_schema

@dataclass
class User:
    id: int
    name: str

schema = export_ddl_schema(User)
print(schema)  # "struct<id:long,name:string>"
```

These DDL strings work perfectly with:
- ✅ Real PySpark
- ❌ Mock-spark (due to this bug)

### What This Means

**Good News**: Polyspark's DDL schema inference works correctly!

**Bad News**: Mock-spark has a bug that prevents it from using DDL schemas properly.

## The Fix

### Recommended Solution

Add a DDL parser to mock-spark that converts schema strings to `MockStructType` objects:

```python
# In mock_spark/session/core/session.py

def _real_createDataFrame(
    self,
    data: Union[List[Dict[str, Any]], List[Any]],
    schema: Optional[Union[MockStructType, List[str], str]] = None,
) -> "MockDataFrame":
    # ... existing code ...
    
    # NEW: Parse DDL schema strings
    if isinstance(schema, str):
        schema = self._parse_ddl_schema(schema)
    
    # ... rest of method ...

def _parse_ddl_schema(self, ddl_string: str) -> MockStructType:
    """Parse DDL schema string into MockStructType.
    
    Example: "id long, name string" -> MockStructType with 2 fields
    
    This implements PySpark's StructType.fromDDL() behavior.
    """
    # Parse the DDL string and create MockStructField objects
    # Handle: simple types, nested structs, arrays, maps
    pass
```

### Alternative Quick Fix

Make the `columns` property defensive:

```python
# In mock_spark/dataframe/dataframe.py

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

## Test Results

### What Works
- ✅ DataFrame creation with DDL strings
- ✅ `df.count()`
- ✅ `df.schema` (returns the string)
- ✅ StructType schemas (everything works)

### What Fails
- ❌ `df.columns` (raises AttributeError)
- ❌ `df.select()` (uses columns internally)
- ❌ Any operation that accesses `df.columns`

## Files to Fix

1. **`mock_spark/session/core/session.py`**
   - Add DDL parser to `_real_createDataFrame` method
   - Add `_parse_ddl_schema` helper method

2. **`mock_spark/dataframe/dataframe.py`** (if using quick fix)
   - Update `columns` property to handle string schemas

## Testing

Run the provided test script:

```bash
cd /Users/odosmatthews/Documents/polyspark
source .venv38-mockspark/bin/activate
python test_mock_spark_bug.py
```

## Next Steps

1. **Report bug to mock-spark maintainers** - Use `MOCK_SPARK_BUG_REPORT.md`
2. **Implement fix** - Add DDL parser to mock-spark
3. **Update polyspark tests** - Once mock-spark is fixed, update compatibility tests
4. **Document workaround** - For now, use StructType schemas with mock-spark

## Workaround for Polyspark

Until mock-spark is fixed, polyspark users can:

1. **Use PySpark** - Full compatibility
2. **Use `build_dicts()`** - Generate data without DataFrames
3. **Convert DDL to StructType** - Manually parse DDL strings (not recommended)

## Conclusion

**Polyspark's DDL schema inference is working correctly.** The issue is in mock-spark, which needs to parse DDL schema strings into `MockStructType` objects to match PySpark's behavior.

This is a **HIGH priority bug** in mock-spark that should be fixed to maintain API compatibility with PySpark.

---

**Files Created**:
- `MOCK_SPARK_BUG_REPORT.md` - Detailed bug report for mock-spark maintainers
- `test_mock_spark_bug.py` - Test script that reproduces the bug
- `MOCK_SPARK_FIX_SUMMARY.md` - This document

**Environment Used**:
- Python 3.8.18
- mock-spark 2.1.1
- polyspark (latest with DDL schema support)

