# PySpark 3.0.3 Missing Features Implementation Plan

## Overview

This plan covers implementing all PySpark 3.0.3 functions that are currently missing from mock-spark. Based on the PYSPARK_FUNCTION_MATRIX.md analysis, we have **13 functions** to implement (excluding type aliases, UDF-related, and deprecated functions).

## Excluded Functions

The following functions are **intentionally excluded**:

### UDF-Related (Requires JVM Integration)
- `udf`, `pandas_udf`, `UserDefinedFunction` - UDF support requires Py4J/JVM integration
- Type aliases: `Column`, `DataFrame`, `DataType`, `SparkContext`, `StringType`, `PandasUDFType`, `PythonEvalType`

### Deprecated Functions (Modern Alternatives Exist)
- `approxCountDistinct` → use `approx_count_distinct` (✅ already implemented)
- `sumDistinct` → use `sum(distinct(...))` 
- `toDegrees` → use `degrees` (✅ already implemented)
- `toRadians` → use `radians` (✅ already implemented)
- `bitwiseNOT` → use `bitwise_not` (✅ already implemented)
- `shiftLeft`, `shiftRight`, `shiftRightUnsigned` → camelCase deprecated (lowercase versions exist in later PySpark)

### Python 2 Legacy / Metadata
- `basestring`, `since`, `ignore_unicode_prefix`, `to_str` - Python 2 legacy or metadata decorators

### Complex/Low Priority
- `window` - Streaming windowing function, complex implementation, defer for now

## Implementation Phases

### Phase 1: Hash & Cryptographic Functions (5 functions) - HIGH PRIORITY
**Effort:** 4-6 hours | **Priority:** HIGH | **Complexity:** LOW

Hash functions are commonly used for data deduplication, partitioning, and checksums.

| Function | Description | DuckDB Equivalent | Test Strategy |
|----------|-------------|-------------------|---------------|
| `md5(col)` | MD5 hash of string | `MD5(col)` | Hash known strings, verify hex output |
| `sha1(col)` | SHA-1 hash | `SHA1(col)` or `SHA256` | Hash known strings |
| `sha2(col, bits)` | SHA-2 family (224/256/384/512) | `SHA256(col)`, `SHA512(col)` | Test all bit lengths |
| `crc32(col)` | CRC32 checksum | Custom implementation | Test with known inputs |
| `xxhash64(col, [seed])` | xxHash 64-bit | Custom implementation or UDF | Test deterministic hashing |

**Implementation Notes:**
- DuckDB has native `MD5()` and `SHA256()` functions
- `crc32` may need Python fallback with `binascii.crc32`
- `xxhash64` may need Python `xxhash` library
- All return hex string (lowercase for md5/sha, integer for crc32/xxhash64)

**Research Links:**
- https://spark.apache.org/docs/3.0.3/api/python/reference/api/pyspark.sql.functions.md5.html
- https://spark.apache.org/docs/3.0.3/api/python/reference/api/pyspark.sql.functions.sha1.html
- https://spark.apache.org/docs/3.0.3/api/python/reference/api/pyspark.sql.functions.sha2.html

### Phase 2: Math Functions (4 functions) - HIGH PRIORITY
**Effort:** 2-3 hours | **Priority:** HIGH | **Complexity:** LOW

Logarithmic functions for data transformations.

| Function | Description | DuckDB Equivalent | Test Strategy |
|----------|-------------|-------------------|---------------|
| `log10(col)` | Base-10 logarithm | `LOG10(col)` | Test with powers of 10 |
| `log2(col)` | Base-2 logarithm | `LOG2(col)` | Test with powers of 2 |
| `log1p(col)` | ln(1 + x) for small x | `LN(1 + col)` | Test precision near 0 |
| `expm1(col)` | exp(x) - 1 for small x | `EXP(col) - 1` | Test precision near 0 |

**Implementation Notes:**
- All functions have direct DuckDB equivalents
- `log1p` and `expm1` handle numerical stability for small values
- These are commonly used in machine learning transformations

**Research Links:**
- https://spark.apache.org/docs/3.0.3/api/python/reference/api/pyspark.sql.functions.log10.html
- https://spark.apache.org/docs/3.0.3/api/python/reference/api/pyspark.sql.functions.expm1.html
- https://spark.apache.org/docs/3.0.3/api/python/reference/api/pyspark.sql.functions.log1p.html

### Phase 3: Array Functions (3 functions) - HIGH PRIORITY
**Effort:** 4-5 hours | **Priority:** HIGH | **Complexity:** MEDIUM

Array manipulation functions.

| Function | Description | DuckDB Equivalent | Test Strategy |
|----------|-------------|-------------------|---------------|
| `array(*cols)` | Create array from columns | `[col1, col2, ...]` | Test with multiple columns |
| `array_repeat(col, count)` | Repeat value N times | `ARRAY_REPEAT(col, count)` or custom | Test with various counts |
| `sort_array(col, [asc])` | Sort array elements | `ARRAY_SORT(col)` or `LIST_SORT(col)` | Test asc/desc sorting |

**Implementation Notes:**
- `array()` creates array from multiple columns (different from `array_contains`)
- DuckDB has `LIST_REPEAT` for array_repeat
- DuckDB has `LIST_SORT` for sort_array (with optional reverse parameter)

**Research Links:**
- https://spark.apache.org/docs/3.0.3/api/python/reference/api/pyspark.sql.functions.array.html
- https://spark.apache.org/docs/3.0.3/api/python/reference/api/pyspark.sql.functions.sort_array.html

### Phase 4: Trigonometric Functions (1 function) - MEDIUM PRIORITY
**Effort:** 1 hour | **Priority:** MEDIUM | **Complexity:** LOW

Two-argument arctangent function.

| Function | Description | DuckDB Equivalent | Test Strategy |
|----------|-------------|-------------------|---------------|
| `atan2(y, x)` | 2-argument arctangent | `ATAN2(y, x)` | Test all 4 quadrants, special cases |

**Implementation Notes:**
- `atan2` has direct DuckDB support: `ATAN2(y, x)`
- Returns angle in radians from -π to π
- Handles signs of both arguments to determine quadrant
- Special cases: atan2(0, 0), atan2(±∞, x), etc.

**Research Links:**
- https://spark.apache.org/docs/3.0.3/api/python/reference/api/pyspark.sql.functions.atan2.html

## Test-Driven Development Approach

### Test Structure

For each phase:
1. **Unit Tests** (`tests/unit/test_pyspark_30_phase{N}.py`):
   - Test basic functionality with mock data
   - Test edge cases (nulls, negatives, empty arrays)
   - Test type inference

2. **Compatibility Tests** (`tests/compatibility/test_pyspark_30_phase{N}_compat.py`):
   - Run same operations on mock-spark and real PySpark
   - Assert identical results (accounting for floating-point precision)
   - Use `@pytest.mark.skipif` for version checks

### Example Test Template

```python
# tests/compatibility/test_hash_functions_compat.py
import pytest
from pyspark.sql import SparkSession
import pyspark.sql.functions as F_real

from mock_spark import MockSparkSession
import mock_spark.functions as F_mock

@pytest.fixture
def real_spark():
    return SparkSession.builder.master("local[1]").getOrCreate()

@pytest.fixture
def mock_spark():
    return MockSparkSession("test")

class TestHashFunctionsCompat:
    def test_md5_compat(self, real_spark, mock_spark):
        data = [{"text": "hello"}, {"text": "world"}]
        
        real_df = real_spark.createDataFrame(data)
        real_result = real_df.select(F_real.md5(F_real.col("text"))).collect()
        
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.select(F_mock.md5(F_mock.col("text"))).collect()
        
        assert [r[0] for r in real_result] == [r[0] for r in mock_result]
```

## Implementation Order

**Recommended sequence based on impact and dependencies:**

1. **Phase 2: Math Functions** (easiest, high impact) - 4 functions
2. **Phase 1: Hash Functions** (high impact, commonly used) - 5 functions
3. **Phase 3: Array Functions** (high impact) - 3 functions
4. **Phase 4: Trigonometric Functions** (simple, one function) - 1 function

**Total: 13 functions**

## Estimated Timeline

- **Phase 2 (Math):** Half day
- **Phase 1 (Hash):** 1 day
- **Phase 3 (Array):** 1 day
- **Phase 4 (Trig):** 2 hours
- **Testing & Documentation:** 1 day

**Total:** ~3-4 days for complete implementation with tests

## Success Criteria

- ✅ All 13 functions implemented and exported
- ✅ Unit tests passing (100% coverage for new functions)
- ✅ Compatibility tests passing (verified against PySpark 3.0.3)
- ✅ PYSPARK_FUNCTION_MATRIX.md updated (non-deprecated 3.0.3 functions marked ✅)
- ✅ Deprecated functions marked with ⚠️ in matrix
- ✅ Type hints added for all new functions
- ✅ Documentation strings following PySpark conventions
- ✅ No ruff or mypy errors

## Deferred/Out of Scope

- **UDF Functions** (`udf`, `pandas_udf`) - Require JVM/Py4J integration
- **Deprecated Functions** - Marked with ⚠️ in matrix, not implementing in mock-spark
- **Streaming Window Function** - Complex implementation, out of scope for testing library
- **DataFrame Methods** - Will be covered in separate plan
- **Python 2 Legacy** - Skip `basestring`, `ignore_unicode_prefix`, `since`

## Next Steps

1. Review and approve this plan
2. Create feature branch: `feature/pyspark-30-missing-functions`
3. Implement Phase 2 (Math) first as proof of concept
4. Continue with remaining phases
5. Update PYSPARK_FUNCTION_MATRIX.md
6. Merge to main when all tests pass

