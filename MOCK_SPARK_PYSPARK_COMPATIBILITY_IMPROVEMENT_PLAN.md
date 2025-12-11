# Mock-Spark PySpark Compatibility Improvement Plan

## Overview

This document outlines required improvements for mock-spark to achieve full PySpark compatibility and prevent false positives in tests. These improvements are critical to ensure that tests passing with mock-spark will also pass with real PySpark in production environments.

**Priority**: CRITICAL  
**Impact**: Prevents 223 false positive test results (12.6% of test suite)  
**Target Version**: Next major release (3.11.0+)  
**Status**: Planning Phase

---

## Problem Statement

Mock-spark currently exhibits lenient behavior in several critical areas where PySpark is strict, causing **223 tests (12.6% of the test suite)** to pass with mock-spark but fail with PySpark. This creates false confidence in test results and can lead to production failures.

### Key Statistics

- **Total False Positives**: 223 tests
- **SparkContext/JVM Errors**: 63 tests (28.3%)
- **Fixture/Setup Errors**: 25 tests (11.2%)
- **Column Reference Errors**: 32 tests (14.3%)
- **Function API Differences**: 8 tests (3.6%)
- **Validation Test Issues**: 95 tests (42.6%)

---

## Issue 1: SparkContext Dependency for Function Calls

### Current Behavior

**Mock-spark:**
```python
# This succeeds with mock-spark (incorrectly)
from mock_spark import functions as F

# No SparkSession needed
col_expr = F.col("id")  # Works without SparkContext
```

**PySpark:**
```python
# This fails with PySpark (correctly)
from pyspark.sql import functions as F

# Requires active SparkContext
col_expr = F.col("id")  # Error: AttributeError: 'NoneType' object has no attribute '_jvm'
```

### Root Cause

Mock-spark's function implementations don't require an active SparkContext because they don't use a JVM. PySpark's functions require access to the JVM through SparkContext to create column expressions.

### Impact

- 63+ tests pass with mock-spark but fail with PySpark
- Developers may write code that works in tests but fails in production
- False confidence in validation and rule creation code

### Required Changes

#### 1. Add SparkContext Validation

**Location**: `mock_spark/functions/functions.py` (or equivalent)

**Current Implementation** (pseudo-code):
```python
def col(col_name: str):
    """Create a column reference."""
    return Column(col_name)  # No validation
```

**Required Implementation**:
```python
def col(col_name: str):
    """
    Create a column reference.
    
    Raises:
        RuntimeError: If no active SparkSession/SparkContext is available
    """
    # Check if we're in a context where SparkSession should be available
    if not _has_active_session():
        raise RuntimeError(
            f"Cannot create column expression '{col_name}': "
            "No active SparkSession found. "
            "Column expressions require an active SparkSession, similar to PySpark. "
            "Create a SparkSession first: spark = SparkSession('app_name')"
        )
    return Column(col_name)

def _has_active_session() -> bool:
    """
    Check if there's an active SparkSession.
    
    Mock-spark should track active sessions similar to PySpark's SparkContext._active_spark_context.
    """
    # Implementation: Track active sessions in a thread-local or global registry
    # Return True if there's at least one active session
    pass
```

#### 2. Implement Session Tracking

**Location**: `mock_spark/session/core/session.py`

**Required Implementation**:
```python
class SparkSession:
    _active_sessions = []  # Thread-safe list of active sessions
    
    def __init__(self, app_name: str):
        # ... existing initialization ...
        SparkSession._active_sessions.append(self)
    
    def stop(self):
        # ... existing cleanup ...
        if self in SparkSession._active_sessions:
            SparkSession._active_sessions.remove(self)
    
    @classmethod
    def get_active_session(cls):
        """Get the most recently created active session."""
        if cls._active_sessions:
            return cls._active_sessions[-1]
        return None
```

#### 3. Update All Function Implementations

Apply the same validation to all functions that create column expressions:
- `F.col()`
- `F.lit()`
- `F.expr()`
- `F.when()`
- All aggregation functions
- All window functions

### Test Cases

```python
def test_col_requires_active_session():
    """Test that col() raises error without active session."""
    from mock_spark import functions as F
    
    # No active session
    with pytest.raises(RuntimeError, match="No active SparkSession found"):
        F.col("id")

def test_col_works_with_active_session():
    """Test that col() works with active session."""
    from mock_spark import SparkSession, functions as F
    
    spark = SparkSession("test")
    try:
        col_expr = F.col("id")
        assert col_expr is not None
    finally:
        spark.stop()

def test_col_fails_after_session_stopped():
    """Test that col() fails after session is stopped."""
    from mock_spark import SparkSession, functions as F
    
    spark = SparkSession("test")
    spark.stop()
    
    with pytest.raises(RuntimeError, match="No active SparkSession found"):
        F.col("id")
```

### Migration Path

1. **Phase 1**: Add deprecation warning (non-breaking)
   - Warning: "Creating column expressions without active SparkSession is deprecated. Create a SparkSession first."
   - Still allow the operation but warn users

2. **Phase 2**: Change to error (breaking change)
   - Raise `RuntimeError` matching PySpark's behavior pattern
   - Update all internal mock-spark tests to create sessions

3. **Phase 3**: Update documentation
   - Document that functions require active SparkSession
   - Provide migration examples

---

## Issue 2: Column Availability Timing

### Current Behavior

**Mock-spark:**
```python
# This succeeds with mock-spark (incorrectly)
df = df.withColumn("new_col", F.col("old_col") + 1)
# Column immediately available
df.select("new_col")  # Works
validation_rules = {"new_col": [F.col("new_col").isNotNull()]}  # Works
```

**PySpark:**
```python
# This may fail with PySpark (correctly)
df = df.withColumn("new_col", F.col("old_col") + 1)
# Column not immediately available until DataFrame is materialized
df.select("new_col")  # May work (lazy evaluation)
validation_rules = {"new_col": [F.col("new_col").isNotNull()]}  # May fail
```

### Root Cause

Mock-spark's lazy evaluation and schema tracking is more permissive. It allows immediate access to columns created in transforms, while PySpark requires DataFrame materialization in some contexts.

### Impact

- 32+ tests pass with mock-spark but fail with PySpark
- Pipeline code that relies on immediate column access may work in tests but fail in production
- Validation rules referencing transform-created columns may fail

### Required Changes

#### 1. Implement Stricter Column Availability

**Location**: `mock_spark/dataframe/dataframe.py`

**Current Implementation** (pseudo-code):
```python
def withColumn(self, col_name: str, col_expr):
    # Add column to schema immediately
    self._schema.fields.append(StructField(col_name, ...))
    return self  # Column immediately available
```

**Required Implementation**:
```python
def withColumn(self, col_name: str, col_expr):
    """
    Add or replace a column.
    
    Note: Column is added to the logical plan but may not be immediately
    available for validation until DataFrame is materialized.
    """
    # Track column in logical plan (not immediately in schema)
    self._logical_plan.add_column(col_name, col_expr)
    
    # Column is available for selection but validation should check materialization
    return self

def _get_available_columns(self) -> list[str]:
    """
    Get columns that are actually available (materialized).
    
    For validation purposes, only return columns that have been materialized.
    """
    # Return columns from materialized data, not just logical plan
    if self._materialized:
        return list(self._data[0].keys() if self._data else [])
    return [f.name for f in self.schema.fields if f.name in self._materialized_columns]
```

#### 2. Update Validation to Check Materialization

**Location**: `mock_spark/dataframe/validation/column_validator.py`

**Required Implementation**:
```python
def validate_column_exists(df, column_name: str):
    """
    Validate that a column exists and is available.
    
    Raises:
        ColumnNotFoundException: If column doesn't exist or isn't materialized
    """
    available_columns = df._get_available_columns()
    
    if column_name not in available_columns:
        # Check if it's in logical plan but not materialized
        if column_name in df._logical_plan.columns:
            raise ColumnNotFoundException(
                f"Column '{column_name}' exists in logical plan but is not yet materialized. "
                "Materialize the DataFrame first using .cache(), .collect(), or by executing an action."
            )
        else:
            raise ColumnNotFoundException(
                f"Column '{column_name}' not found. Available columns: {available_columns}"
            )
```

### Test Cases

```python
def test_column_not_available_until_materialized():
    """Test that transform-created columns require materialization."""
    spark = SparkSession("test")
    df = spark.createDataFrame([{"id": 1}], ["id"])
    
    # Create new column
    df = df.withColumn("new_col", F.col("id") + 1)
    
    # Should fail - column not materialized
    with pytest.raises(ColumnNotFoundException, match="not yet materialized"):
        df.select("new_col").show()  # Or validation that references new_col
    
    # Materialize first
    df = df.cache()  # or .collect() or other action
    
    # Now should work
    result = df.select("new_col")
    assert result is not None
```

### Migration Path

1. **Phase 1**: Add warning when accessing non-materialized columns
   - Warning: "Column 'X' may not be available until DataFrame is materialized"
   - Still allow the operation but warn users

2. **Phase 2**: Change to error (breaking change)
   - Raise `ColumnNotFoundException` for non-materialized columns
   - Update all internal mock-spark tests

3. **Phase 3**: Update documentation
   - Document materialization requirements
   - Provide examples of correct patterns

---

## Issue 3: Type System Leniency

### Current Behavior

**Mock-spark:**
```python
# This succeeds with mock-spark (incorrectly)
df = df.withColumn("date_parsed", F.to_timestamp(F.col("date_str"), "yyyy-MM-dd"))
# Type conversion is lenient
df.select("date_parsed")  # Works, may infer wrong type
```

**PySpark:**
```python
# This may fail with PySpark (correctly)
df = df.withColumn("date_parsed", F.to_timestamp(F.col("date_str"), "yyyy-MM-dd"))
# Type conversion is strict
# Error: invalid series dtype: expected `String`, got `datetime[μs]`
```

### Root Cause

Mock-spark's type system is more lenient with type conversions and schema inference. It allows type mismatches that PySpark would reject.

### Impact

- Several tests pass with mock-spark but fail with PySpark
- Type-related errors are hidden during testing
- Production code may fail with type errors

### Required Changes

#### 1. Implement Stricter Type Checking

**Location**: `mock_spark/dataframe/casting/type_converter.py`

**Required Implementation**:
```python
def to_timestamp(col, format: str = None):
    """
    Convert to timestamp with strict type checking.
    
    Raises:
        TypeError: If input column type is incompatible
    """
    input_type = col.dataType
    
    # PySpark requires string input for to_timestamp
    if not isinstance(input_type, StringType):
        raise TypeError(
            f"to_timestamp() requires StringType input, got {input_type}. "
            f"Cast the column to string first: F.col('{col.name}').cast('string')"
        )
    
    # Perform conversion
    return TimestampColumn(col, format)
```

#### 2. Add Type Validation in Operations

**Location**: `mock_spark/dataframe/operations/`

**Required Implementation**:
- Validate input types match expected types
- Raise `TypeError` for incompatible types
- Match PySpark's type error messages

### Test Cases

```python
def test_to_timestamp_requires_string():
    """Test that to_timestamp requires string input."""
    spark = SparkSession("test")
    df = spark.createDataFrame([{"date": datetime.now()}], ["date"])
    
    with pytest.raises(TypeError, match="requires StringType input"):
        df.withColumn("parsed", F.to_timestamp(F.col("date")))
```

### Migration Path

1. **Phase 1**: Add type validation with warnings
   - Warning: "Type mismatch may cause errors in PySpark"
   - Still allow the operation but warn users

2. **Phase 2**: Change to error (breaking change)
   - Raise `TypeError` for type mismatches
   - Update all internal mock-spark tests

---

## Issue 4: Function API Differences

### Current Behavior

**Mock-spark:**
```python
# This may work differently in mock-spark
F.current_date()  # May be a method or function
df.current_date()  # May work as DataFrame method
```

**PySpark:**
```python
# This is the correct pattern
F.current_date()  # Function call, returns Column
# df.current_date() does NOT exist
```

### Root Cause

Some function implementations in mock-spark have different APIs or allow different calling patterns than PySpark.

### Impact

- 8+ tests pass with mock-spark but fail with PySpark
- Function call patterns that work in tests fail in production

### Required Changes

#### 1. Audit All Function APIs

**Location**: `mock_spark/functions/`

**Required Actions**:
- Compare all function signatures with PySpark
- Ensure exact API compatibility
- Remove any DataFrame method aliases that don't exist in PySpark
- Fix function signatures to match PySpark exactly

#### 2. Function Signature Examples

**Functions to Audit**:
- `F.current_date()` - Should be function, not DataFrame method
- `F.current_timestamp()` - Should be function, not DataFrame method
- `F.datediff()` - Check parameter order and types
- All aggregation functions - Check signatures
- All window functions - Check signatures

### Test Cases

```python
def test_current_date_is_function_not_method():
    """Test that current_date is a function, not DataFrame method."""
    spark = SparkSession("test")
    df = spark.createDataFrame([{"id": 1}], ["id"])
    
    # Should work as function
    result = df.withColumn("today", F.current_date())
    assert result is not None
    
    # Should NOT work as DataFrame method
    with pytest.raises(AttributeError):
        df.current_date()
```

---

## Issue 5: Fixture/Setup Compatibility

### Current Behavior

**Mock-spark:**
```python
# This works with mock-spark
config = WriterConfig(...)  # No SparkContext needed
writer = LogWriter(spark=None, config=config)  # Works
```

**PySpark:**
```python
# This fails with PySpark
config = WriterConfig(...)  # Requires SparkContext
# Error: TypeError: __init__() missing 1 required positional argument: 'sparkContext'
```

### Root Cause

Mock-spark objects don't require SparkContext initialization, while some PySpark objects do.

### Impact

- 25+ tests fail during setup with PySpark
- Test fixtures that work with mock-spark don't work with PySpark

### Required Changes

#### 1. Add SparkContext Requirements

**Location**: `mock_spark/` (various modules)

**Required Implementation**:
- Identify objects that should require SparkContext
- Add validation to ensure SparkContext is available when needed
- Match PySpark's initialization requirements

#### 2. Update Object Initialization

**Example**:
```python
class WriterConfig:
    def __init__(self, sparkContext=None, ...):
        """
        Initialize WriterConfig.
        
        Args:
            sparkContext: Required for PySpark compatibility
        """
        if sparkContext is None:
            # Check if we should require it for compatibility
            if _should_require_spark_context():
                raise TypeError(
                    "__init__() missing 1 required positional argument: 'sparkContext'. "
                    "Provide SparkContext for PySpark compatibility."
                )
        # ... rest of initialization
```

---

## Implementation Priority

### Phase 1: Critical (High Priority)

1. **SparkContext Dependency** (Issue 1)
   - Prevents 63+ false positives
   - Most critical for test reliability
   - **Estimated Effort**: Medium
   - **Breaking Change**: Yes

2. **Column Availability Timing** (Issue 2)
   - Prevents 32+ false positives
   - Important for pipeline reliability
   - **Estimated Effort**: High
   - **Breaking Change**: Yes

### Phase 2: Important (Medium Priority)

3. **Type System Leniency** (Issue 3)
   - Prevents type-related false positives
   - Important for data quality
   - **Estimated Effort**: Medium
   - **Breaking Change**: Yes

4. **Function API Differences** (Issue 4)
   - Prevents 8+ false positives
   - Important for API compatibility
   - **Estimated Effort**: Low-Medium
   - **Breaking Change**: Possibly

### Phase 3: Nice to Have (Lower Priority)

5. **Fixture/Setup Compatibility** (Issue 5)
   - Prevents 25+ setup failures
   - Important for test infrastructure
   - **Estimated Effort**: Low
   - **Breaking Change**: Possibly

---

## Testing Strategy

### Unit Tests

Create comprehensive unit tests for each improvement:
- Test that errors are raised when appropriate
- Test that operations work when conditions are met
- Test error messages match PySpark patterns

### Integration Tests

Test with real-world scenarios:
- Run PySpark test suite against mock-spark
- Verify that tests that fail with PySpark also fail with mock-spark
- Verify that tests that pass with PySpark also pass with mock-spark

### Compatibility Tests

Create a compatibility test suite:
- Test all function signatures match PySpark
- Test all error messages match PySpark
- Test all behavior matches PySpark

---

## Migration Guide

### For Mock-Spark Users

1. **Update Function Calls**
   ```python
   # Before (works but incorrect)
   from mock_spark import functions as F
   col_expr = F.col("id")  # No session needed
   
   # After (correct)
   from mock_spark import SparkSession, functions as F
   spark = SparkSession("app")
   col_expr = F.col("id")  # Session required
   ```

2. **Materialize DataFrames**
   ```python
   # Before (works but incorrect)
   df = df.withColumn("new", F.col("old") + 1)
   df.select("new")  # May fail
   
   # After (correct)
   df = df.withColumn("new", F.col("old") + 1)
   df = df.cache()  # Materialize
   df.select("new")  # Works
   ```

3. **Fix Type Conversions**
   ```python
   # Before (works but incorrect)
   df.withColumn("date", F.to_timestamp(F.col("datetime_col")))
   
   # After (correct)
   df.withColumn("date", F.to_timestamp(F.col("datetime_col").cast("string")))
   ```

---

## Expected Outcomes

After implementing these improvements:

1. ✅ Tests that pass with mock-spark will pass with PySpark
2. ✅ Tests that fail with mock-spark will fail with PySpark (same errors)
3. ✅ Error messages match PySpark exactly
4. ✅ Function APIs match PySpark exactly
5. ✅ Developers catch issues during testing, not in production
6. ✅ Mock-spark becomes a reliable proxy for PySpark behavior

---

## Success Metrics

- **False Positive Rate**: Reduce from 12.6% to <1%
- **Test Compatibility**: 99%+ of tests pass with both mock-spark and PySpark
- **Error Message Match**: 100% of error messages match PySpark patterns
- **API Compatibility**: 100% of function signatures match PySpark

---

## Related Issues

- Schema compatibility improvements (already implemented in 3.10.4)
- Union operation validation (already implemented in 3.10.4)
- Empty DataFrame schema requirements (already implemented in 3.10.4)

---

## References

- PySpark Documentation: [SparkSession](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.html)
- PySpark Documentation: [Functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)
- PySpark Source: `pyspark/sql/functions.py`
- PySpark Source: `pyspark/sql/session.py`
- Analysis Report: `docs/PYSPARK_TEST_FAILURE_ANALYSIS_REPORT.md`

---

## Contact

For questions or clarifications about this improvement plan, please contact the mock-spark development team or open an issue in the mock-spark repository.

**Plan Version**: 1.0  
**Last Updated**: December 10, 2025  
**Status**: Ready for Implementation

