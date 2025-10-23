# Fix 10 Failed Compatibility Tests - Comprehensive Plan

## Current Status

- **Compatibility Tests:** 10 failed, 358 passed, 74 skipped
- **Unit Tests:** 5 failed, 717 passed, 31 skipped
- **Total Test Suite:** 67 failed, 1149 passed, 110 skipped

## Failed Compatibility Tests Analysis

### 1. **DateTime Functions (2 tests)**
- `test_current_timestamp_function` - CTE optimization failure, column "current_ts" not found
- `test_current_date_function` - Similar CTE optimization failure

**Root Cause:** Current date/timestamp functions are not properly implemented in the SQL generation layer. The CTE optimization fails because these functions generate SQL expressions that aren't properly handled in the column resolution.

### 2. **Error Handling (2 tests)**
- `test_column_not_found_error` - MockSparkColumnNotFoundError raised instead of expected exception
- `test_null_handling_in_operations` - Null handling issues in operations

**Root Cause:** Error handling validation is not properly implemented. The compatibility tests expect specific PySpark error types and messages, but Mock-Spark is raising different exceptions.

### 3. **Lazy Evaluation (1 test)**
- `test_error_deferred_to_action_time` - Error not properly deferred to action time

**Root Cause:** Lazy evaluation error handling is not properly implemented. Errors should be deferred until materialization time.

### 4. **Column Functions (1 test)**
- `test_operation_chaining` - Operation chaining validation issues

**Root Cause:** Complex operation chaining validation is not implemented.

### 5. **New Features (1 test)**
- `test_selectExpr_and_expr` - selectExpr and expr functions not implemented

**Root Cause:** These PySpark functions are not implemented in Mock-Spark.

### 6. **Watermark Features (2 tests)**
- `test_with_watermark_basic` - Watermark functionality not implemented
- `test_with_watermark_chainable` - Chainable watermark functionality not implemented

**Root Cause:** Watermark functionality is not implemented in Mock-Spark.

### 7. **Storage Integration (1 test)**
- `test_table_operations` - Table operations not properly implemented

**Root Cause:** Table operations and storage integration are not fully implemented.

## Implementation Plan

### Phase 1: Fix DateTime Functions (2 tests)

**Files to modify:**
- `mock_spark/backend/duckdb/query_executor.py`
- `mock_spark/functions/datetime.py`

**Actions:**
1. Fix current_date() and current_timestamp() SQL generation
2. Ensure proper column resolution in CTE optimization
3. Add proper handling for these functions in the SQL generation layer

**Expected outcome:** 2 tests fixed

### Phase 2: Fix Error Handling (2 tests)

**Files to modify:**
- `mock_spark/core/exceptions/operation.py`
- `mock_spark/dataframe/dataframe.py`

**Actions:**
1. Implement proper error validation and exception raising
2. Ensure error messages match PySpark format
3. Fix null handling in operations

**Expected outcome:** 2 tests fixed

### Phase 3: Fix Lazy Evaluation (1 test)

**Files to modify:**
- `mock_spark/dataframe/lazy.py`
- `mock_spark/backend/duckdb/query_executor.py`

**Actions:**
1. Implement proper error deferral in lazy evaluation
2. Ensure errors are only raised at materialization time

**Expected outcome:** 1 test fixed

### Phase 4: Fix Column Functions (1 test)

**Files to modify:**
- `mock_spark/dataframe/dataframe.py`
- `mock_spark/functions/core/column.py`

**Actions:**
1. Implement operation chaining validation
2. Add proper validation for complex column operations

**Expected outcome:** 1 test fixed

### Phase 5: Implement New Features (1 test)

**Files to modify:**
- `mock_spark/dataframe/dataframe.py`
- `mock_spark/functions/functions.py`

**Actions:**
1. Implement selectExpr() function
2. Implement expr() function

**Expected outcome:** 1 test fixed

### Phase 6: Implement Watermark Features (2 tests)

**Files to modify:**
- `mock_spark/dataframe/dataframe.py`
- `mock_spark/session/core/session.py`

**Actions:**
1. Implement withWatermark() functionality
2. Add watermark support to DataFrame operations
3. Implement chainable watermark operations

**Expected outcome:** 2 tests fixed

### Phase 7: Fix Storage Integration (1 test)

**Files to modify:**
- `mock_spark/storage/manager.py`
- `mock_spark/session/catalog.py`

**Actions:**
1. Implement proper table operations
2. Fix storage integration issues
3. Ensure proper table management

**Expected outcome:** 1 test fixed

## Priority Order

1. **High Priority:** DateTime Functions (Phase 1) - These are core functionality
2. **High Priority:** Error Handling (Phase 2) - These affect user experience
3. **Medium Priority:** Lazy Evaluation (Phase 3) - Important for performance
4. **Medium Priority:** Column Functions (Phase 4) - Core functionality
5. **Low Priority:** New Features (Phase 5) - Nice to have
6. **Low Priority:** Watermark Features (Phase 6) - Advanced features
7. **Low Priority:** Storage Integration (Phase 7) - Infrastructure

## Success Metrics

**Target:** Reduce compatibility test failures from 10 to 0

**Phase 1-2:** 4 tests fixed (High priority)
**Phase 3-4:** 2 tests fixed (Medium priority)
**Phase 5-7:** 4 tests fixed (Low priority)

## Implementation Notes

1. **DateTime Functions:** Focus on proper SQL generation and column resolution
2. **Error Handling:** Ensure compatibility with PySpark error messages and types
3. **Lazy Evaluation:** Implement proper error deferral mechanism
4. **Column Functions:** Add validation for complex operations
5. **New Features:** Implement missing PySpark functions
6. **Watermark Features:** Add watermark support to DataFrame operations
7. **Storage Integration:** Fix table operations and storage management

## Risk Assessment

**Low Risk:** Phases 1-2 (DateTime and Error Handling) - Well-defined issues
**Medium Risk:** Phases 3-4 (Lazy Evaluation and Column Functions) - Some complexity
**High Risk:** Phases 5-7 (New Features, Watermark, Storage) - Significant implementation work

## Timeline

- **Phase 1-2:** 2-3 hours (High priority)
- **Phase 3-4:** 2-3 hours (Medium priority)
- **Phase 5-7:** 4-6 hours (Low priority)

**Total Estimated Time:** 8-12 hours

## Next Steps

1. Start with Phase 1 (DateTime Functions) - highest impact
2. Move to Phase 2 (Error Handling) - user experience
3. Continue with remaining phases based on priority
4. Test each phase before moving to the next
5. Verify compatibility test results after each phase
