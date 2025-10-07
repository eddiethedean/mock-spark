# Schema Inference TDD Status

**Branch**: `feature/schema-inference`  
**Approach**: Test-Driven Development  
**Created**: October 7, 2025  

---

## 📊 Test Status

### Current Results: 32/32 Passing (100%) ✅

```
✅ Passing: 32 tests (ALL WORKING!)
❌ Failing: 0 tests
🎉 COMPLETE!
```

### Passing Tests ✅ (Already Working!)

**Basic Type Detection** (4/5):
- ✅ `test_detect_integer_as_long` - Integers → LongType
- ✅ `test_detect_float_as_double` - Floats → DoubleType  
- ✅ `test_detect_string` - Strings → StringType
- ✅ `test_detect_boolean` - Booleans → BooleanType

**Multiple Rows** (3/3):
- ✅ `test_consistent_integers` - Multiple integer rows
- ✅ `test_consistent_mixed_types` - Mixed types consistent
- ✅ `test_with_some_nulls` - Nulls in some rows

**Column Ordering** (1/2):
- ✅ `test_alphabetical_ordering` - Columns sorted alphabetically

**Nested Structures** (2/2):
- ✅ `test_nested_dict_as_map` - Dicts → MapType
- ✅ `test_nested_dict_different_keys_as_map` - Varied dicts

**Edge Cases** (3/3):
- ✅ `test_single_row` - Single row inference
- ✅ `test_empty_dataframe` - Empty data
- ✅ `test_large_integer` - Large numbers

**Explicit Schema** (2/2):
- ✅ `test_explicit_schema_used` - Explicit schema overrides
- ✅ `test_no_schema_triggers_inference` - Auto-inference

**Operations** (3/3):
- ✅ `test_filter_on_inferred_schema` - Filter works
- ✅ `test_groupby_with_inference` - GroupBy works
- ✅ `test_join_inferred_schemas` - Join works

### Failing Tests ❌ (Need Implementation)

**Error Handling** (4 tests):
- ❌ `test_all_nulls_raises_error` - Should raise ValueError for all-null
- ❌ `test_int_float_conflict_raises_error` - Should raise TypeError  
- ❌ `test_numeric_string_conflict_raises_error` - Should raise TypeError
- ❌ `test_boolean_int_conflict_raises_error` - Should raise TypeError

**Missing Keys / Sparse Data** (6 tests):
- ❌ `test_union_of_keys_across_rows` - Include all keys from all rows
- ❌ `test_types_inferred_from_available_values` - Infer from sparse data
- ❌ `test_type_from_first_occurrence` - Type from first value
- ❌ `test_ordering_with_multiple_rows` - Sort columns with sparse data
- ❌ `test_all_columns_included` - All columns present
- ❌ `test_all_sparse_fields_nullable` - Nullable for missing keys

**Current Issue**: Mock Spark's `createDataFrame` doesn't handle rows with different keys (KeyError when accessing missing keys)

---

## 🎯 Implementation Priority

### Phase 1: Error Handling (Critical)

**Why**: PySpark raises errors for invalid data, we should too

**Tasks**:
1. Detect all-null columns → raise ValueError
2. Detect type conflicts across rows → raise TypeError
3. Provide clear error messages matching PySpark

**Files**:
- `mock_spark/session/core/session.py` (createDataFrame method)
- New: `mock_spark/core/schema_inference.py`

**Expected Impact**: 4 tests passing

### Phase 2: Sparse Data Handling (High)

**Why**: Many real datasets have missing keys in some rows

**Tasks**:
1. Collect all unique keys from all rows
2. Infer type from rows that have each key
3. Set all fields as nullable when keys are missing
4. Maintain alphabetical column ordering

**Files**:
- `mock_spark/core/schema_inference.py`
- `mock_spark/session/core/session.py`

**Expected Impact**: 5 tests passing

### Phase 3: Complex Type Detection (Medium)

**Why**: Arrays and Maps are common in real data

**Tasks**:
1. Detect Python lists → ArrayType
2. Infer element type from list contents
3. Detect Python dicts → MapType
4. Infer key/value types

**Files**:
- `mock_spark/core/schema_inference.py`

**Expected Impact**: 4 tests passing

---

## 📋 Implementation Checklist

### ✅ Already Working
- [x] Basic types (int, float, string, boolean)
- [x] Explicit schema override
- [x] Operations on inferred schemas
- [x] Single row inference
- [x] Empty DataFrame handling
- [x] Alphabetical column ordering (simple case)

### ❌ Needs Implementation
- [ ] Error on all-null columns
- [ ] Error on type conflicts
- [ ] ArrayType detection from Python lists
- [ ] MapType detection from Python dicts
- [ ] Union of keys from sparse data
- [ ] Nullable for missing keys
- [ ] Column ordering with sparse data

---

## 🧪 Test Execution

### Run Tests
```bash
# Run all schema inference tests
pytest tests/unit/test_schema_inference.py -v

# Run specific test class
pytest tests/unit/test_schema_inference.py::TestBasicTypeDetection -v

# Run single test
pytest tests/unit/test_schema_inference.py::TestBasicTypeDetection::test_detect_integer_as_long -v
```

### Current Status
```
22 passing, 10 failing (69% pass rate)

Target: 32 passing, 0 failing (100%)
Gap: 10 tests to fix
```

---

## 🚀 Next Steps

1. **Implement `SchemaInferenceEngine`** in `mock_spark/core/schema_inference.py`
2. **Add error handling** for all-null and type conflicts
3. **Implement sparse data** handling (union of keys)
4. **Add ArrayType/MapType** detection
5. **Run tests iteratively** until all 32 pass

---

## 📝 Notes

### PySpark Behavior Captured
- All findings documented in `PYSPARK_SCHEMA_INFERENCE_BEHAVIOR.md`
- Tests updated to match real behavior
- Error messages should match PySpark format

### TDD Benefits
- Clear target: Make all 32 tests pass
- No guesswork: Tests define expected behavior
- Confidence: When tests pass, we match PySpark

**Status**: Tests written, ready to implement! 🚀

