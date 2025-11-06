# Plan to Fix Remaining 4 Test Failures

## Current Status
- **Total failures:** 4 (down from 26)
- **Passing:** 197 tests
- **Skipped:** 132 tests

## Summary
We've made excellent progress, fixing 22 out of 26 test failures. The remaining 4 failures require focused debugging and targeted fixes.

## Failure Analysis

### 1. `test_map_concat` - Map Concatenation
**Error:** `Map key mismatch: missing keys ['c', 'd'] extra keys ['p', 'q', 'x', 'y']` (row 0)

**Issue:** Map2 values are not being included in the merged struct when using `map_concat(map1, map2)`.

**Root Cause:**
- Polars creates union structs: map1 has `['a', 'b', 'x', 'y', 'p', 'q']`, map2 has `['c', 'd', 'z', 'r']`
- When building merged struct, we correctly get all fields: `['a', 'b', 'x', 'y', 'p', 'q', 'c', 'd', 'z', 'r']`
- However, map2 values are not being included in the result
- The coalesce logic may not be working correctly - when checking if a field exists in a struct dtype, we may not find it
- OR the filtering step is removing map2 values incorrectly

**Expected Behavior:**
- Row 0: `{'a': 1, 'b': 2, 'c': 3, 'd': 4}` (map1 + map2)
- Row 1: `{'x': 10, 'y': 20, 'z': 30}` (map1 + map2)
- Row 2: `{'p': 100, 'q': 200, 'r': 300}` (map1 + map2)

**Current Output:**
- Row 0: `{'a': 1, 'b': 2}` (only map1, missing map2)
- Row 1: `{'x': 10, 'y': 20}` (only map1, missing map2)
- Row 2: `{'p': 100, 'q': 200}` (only map1, missing map2)

**Fix Strategy:**
1. **Debug map_cols extraction:** Verify `map_cols = ['map1', 'map2']` correctly
2. **Debug field detection:** For each field in `all_field_names`, check if it exists in each map's struct dtype
3. **Fix coalesce logic:** For field 'c' (only in map2), ensure we add `pl.col('map2').struct.field('c')` to value_exprs
4. **Verify struct building:** Ensure all fields from both maps are included in `struct_field_exprs`
5. **Fix filtering:** The `map_elements` filter should preserve all non-null values, not just map1 values

**Implementation Steps:**
- Add temporary debug prints to see what `map_cols`, `all_field_names`, and `struct_field_exprs` contain
- Verify that for field 'c', we check `any(f.name == 'c' for f in df['map2'].dtype.fields)` and it returns True
- Ensure `value_exprs.append(pl.col('map2').struct.field('c'))` is called for field 'c'
- Check that `struct_field_exprs` includes expressions for both map1 and map2 fields
- Test the filtering lambda - it should convert struct to dict and filter nulls, preserving all non-null keys

### 2. `test_reverse_array` - Array Reverse
**Error:** `Row count mismatch: mock=0, expected=3` (returns empty DataFrame)

**Issue:** Array reverse function returns empty DataFrame instead of reversed arrays.

**Root Cause:**
- `F.reverse()` defaults to `StringFunctions.reverse()` which does string reverse
- For arrays, we need to use `ArrayFunctions.reverse()` OR detect array type in `_reverse_expr`
- The `_reverse_expr` function checks `op.column.column_type` but may not correctly detect array types
- OR the expression translation is failing and returning empty result

**Expected Behavior:**
- Row 0: `reverse(arr1)` = `[3, 2, 1]` (reversed `[1, 2, 3]`)
- Row 1: `reverse(arr1)` = `[20, 10]` (reversed `[10, 20]`)
- Row 2: `reverse(arr1)` = `[15, 10, 5]` (reversed `[5, 10, 15]`)

**Fix Strategy:**
1. **Check function routing:** `F.reverse()` calls `StringFunctions.reverse()` - need to detect array type
2. **Fix `_reverse_expr`:** Improve array type detection by checking schema or column type
3. **Verify Polars expression:** Ensure `expr.list.reverse()` is used correctly for arrays
4. **Check schema inference:** Ensure array columns are correctly identified in schema

**Implementation Steps:**
- Review `_reverse_expr` in `expression_translator.py` (line ~906)
- Check if `op.column.column_type` is `ArrayType` - may need to check schema instead
- For array types, use `expr.list.reverse()` instead of `expr.str.reverse()`
- Test that `expr.list.reverse()` works with Polars list columns
- Verify the expression is being translated correctly and not raising errors

### 3. `test_column_available_after_select` - Column Lifecycle
**Error:** `TypeError: unhashable type: 'Expr'` when casting `F.col("age").cast("string")`

**Issue:** The `cast` operation is receiving a Polars `Expr` object instead of a string type name.

**Root Cause:**
- In `test_column_available_after_select`, the test does: `F.col("age").cast("string")`
- The `cast` operation expects a type string like `"string"`, but it's receiving a Polars expression
- This happens when `F.col("age")` is translated to a Polars expression, then `.cast("string")` is called
- The `cast` translation may be trying to use the Polars expression as a dictionary key or in a hashable context

**Expected Behavior:**
- `df.withColumn("full_info", F.concat_ws(" - ", F.col("name"), F.col("age").cast("string")))` should work
- `df.select("id", "full_info")` should return the selected columns

**Fix Strategy:**
1. **Check cast translation:** In `_translate_operation`, when operation is "cast", verify `op.value` is a string
2. **Fix type handling:** Ensure `cast("string")` is translated to `expr.cast(pl.Utf8)` correctly
3. **Verify expression chaining:** Ensure `F.col("age").cast("string")` creates correct `MockColumnOperation`
4. **Check schema projection:** After `withColumn`, the new column should be in schema; after `select`, only selected columns should be available

**Implementation Steps:**
- Review `cast` operation handling in `expression_translator.py`
- Check if `op.value` for cast is correctly extracted as a string
- Verify `expr.cast(pl.Utf8)` works correctly for string casting
- Test the expression chain: `F.col("age").cast("string")` should translate correctly
- Ensure schema projection includes new columns from `withColumn`

### 4. `test_select_expr_groupby_agg_orderby_chain` - Complex Scenario
**Error:** `ValueError: F.expr() SQL expressions should be handled by SQL executor, not Polars backend`

**Issue:** `selectExpr()` uses `F.expr()` to parse SQL expressions, which raises ValueError in Polars backend.

**Root Cause:**
- The test uses `df.selectExpr("CASE WHEN age > 30 THEN 'Senior' ELSE 'Junior' END as level")`
- `selectExpr()` internally uses `F.expr()` to parse SQL strings
- `F.expr()` currently raises `ValueError` in `PolarsExpressionTranslator` (line ~734)
- The Polars backend cannot handle SQL expressions directly - they need SQL parsing/translation

**Expected Behavior:**
- `selectExpr("CASE WHEN age > 30 THEN 'Senior' ELSE 'Junior' END as level")` should:
  - Parse the SQL expression
  - Create a CASE WHEN expression
  - Execute it and return results

**Fix Strategy:**
1. **Option A: Implement basic SQL parsing in Polars backend**
   - Parse simple SQL expressions like `CASE WHEN ... THEN ... ELSE ... END`
   - Translate to Polars expressions (`pl.when().then().otherwise()`)
   - Handle column references and literals

2. **Option B: Route to SQL executor for F.expr()**
   - When `F.expr()` is encountered, fall back to SQL-based materialization
   - Use DuckDB backend for SQL expressions
   - This requires detecting `F.expr()` and switching backends

3. **Option C: Skip/handle gracefully**
   - Mark test as skipped if SQL parsing is too complex
   - OR implement minimal SQL parsing for common cases (CASE WHEN, basic functions)

**Implementation Steps:**
- Review `F.expr()` implementation and how `selectExpr()` uses it
- Check if we can parse simple CASE WHEN expressions
- If parsing is needed, implement minimal SQL parser for:
  - CASE WHEN ... THEN ... ELSE ... END
  - Column references
  - Literals
  - Basic operators
- Translate parsed SQL to Polars expressions
- Test with the specific expression from the test

## Implementation Priority

1. **High Priority (Easiest to Fix):**
   - `test_reverse_array` - Simple fix: improve array type detection in `_reverse_expr`
   - `test_column_available_after_select` - Fix cast operation to handle string type correctly

2. **Medium Priority:**
   - `test_map_concat` - Requires debugging coalesce logic and struct field merging

3. **Lower Priority (Most Complex):**
   - `test_select_expr_groupby_agg_orderby_chain` - Requires SQL expression parsing or backend routing

## Detailed Fix Plans

### Fix 1: `test_reverse_array` (Priority: High, Complexity: Low)
**Problem:** `F.reverse()` on array column returns empty DataFrame (0 rows)

**Solution:**
1. Check `_reverse_expr` function - it checks `op.column.column_type` for `ArrayType`
2. If `column_type` is not available, check schema to determine if column is array type
3. For arrays, use `expr.list.reverse()`; for strings, use `expr.str.reverse()`
4. Ensure the expression is correctly translated and not failing silently

**Files to Modify:**
- `mock_spark/backend/polars/expression_translator.py` - `_reverse_expr` method

**Testing:**
- Run `test_reverse_array` - should return 3 rows with reversed arrays

### Fix 2: `test_column_available_after_select` (Priority: High, Complexity: Medium)
**Problem:** `TypeError: unhashable type: 'Expr'` when casting `F.col("age").cast("string")`

**Solution:**
1. In `_translate_operation`, when operation is "cast", ensure `op.value` is a string type name
2. Check if `op.value` is a Polars expression (shouldn't be) - if so, extract string value
3. Map type names: `"string"` -> `pl.Utf8`, `"int"` -> `pl.Int64`, etc.
4. Use `expr.cast(polars_dtype)` correctly

**Files to Modify:**
- `mock_spark/backend/polars/expression_translator.py` - cast operation handling

**Testing:**
- Run `test_column_available_after_select` - should work without TypeError

### Fix 3: `test_map_concat` (Priority: Medium, Complexity: Medium)
**Problem:** Map2 values not included in merged struct (missing keys ['c', 'd', 'z', 'r'])

**Solution:**
1. Add debug logging to verify `map_cols = ['map1', 'map2']`
2. Verify `all_field_names` includes both map1 and map2 fields
3. For each field, check if it exists in each map's struct dtype correctly
4. Ensure `value_exprs` includes values from both maps
5. Fix coalesce logic: for fields only in map2, use map2 value; for fields in both, map2 overrides map1
6. Verify filtering preserves all non-null values from both maps

**Files to Modify:**
- `mock_spark/backend/polars/operation_executor.py` - `apply_select` method, map_concat handling

**Testing:**
- Run `test_map_concat` - should include both map1 and map2 values per row

### Fix 4: `test_select_expr_groupby_agg_orderby_chain` (Priority: Lower, Complexity: High)
**Problem:** `ValueError: F.expr() SQL expressions should be handled by SQL executor`

**Solution Options:**
- **Option A (Recommended):** Implement minimal SQL parsing for CASE WHEN expressions
  - Parse `CASE WHEN condition THEN value1 ELSE value2 END`
  - Translate to `pl.when(condition).then(value1).otherwise(value2)`
  - Handle column references and literals
- **Option B:** Route F.expr() to SQL executor (DuckDB backend)
  - Detect F.expr() in expression translation
  - Fall back to SQL-based materialization for that expression
- **Option C:** Skip test if SQL parsing is too complex

**Files to Modify:**
- `mock_spark/backend/polars/expression_translator.py` - `F.expr()` handling
- OR `mock_spark/dataframe/lazy.py` - Backend routing for SQL expressions

**Testing:**
- Run `test_select_expr_groupby_agg_orderby_chain` - should handle SQL expressions

## Testing Strategy

1. Fix one test at a time, starting with easiest (reverse_array)
2. Run full compatibility suite after each fix: `pytest tests/compatibility/ -n 8 --tb=no -q`
3. Verify no regressions - test count should increase, failures should decrease
4. Commit after each successful fix with clear commit message

## Estimated Complexity & Time

- `test_reverse_array`: **Low** (30-45 min) - Array type detection fix
- `test_column_available_after_select`: **Low-Medium** (45-90 min) - Cast operation fix
- `test_map_concat`: **Medium** (1-2 hours) - Debugging and coalesce logic
- `test_select_expr_groupby_agg_orderby_chain`: **High** (2-4 hours) - SQL parsing or backend routing

**Total Estimated Time:** 4.5-8 hours

## Success Criteria

- All 4 tests pass
- Total failures: 0 (down from 4)
- Total passing: 201 (up from 197)
- No regressions in existing passing tests

