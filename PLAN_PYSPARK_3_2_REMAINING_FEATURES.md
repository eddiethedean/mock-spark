# PySpark 3.2 Remaining Features - Implementation Plan

**Version:** 2.6.0 - 3.0.0  
**Status:** Planning  
**Date:** October 16, 2025  
**Current Version:** 2.5.0 (Published to PyPI)

---

## Executive Summary

This document outlines the implementation plan for the **remaining PySpark 3.2 features** not included in version 2.5.0. While v2.5.0 successfully implemented the most commonly used PySpark 3.2 features, there are approximately **46 additional functions** from the PySpark 3.2 specification that could be added for complete API coverage.

### What's Already in v2.5.0 ✅

- ✅ Core array functions (6): `array_distinct`, `array_intersect`, `array_union`, `array_except`, `array_position`, `array_remove`
- ✅ Core map functions (5): `map_keys`, `map_values`, `map_entries`, `map_concat`, `map_from_arrays`
- ✅ Timestamp functions (2): `timestampadd`, `timestampdiff`
- ✅ String functions (5): `initcap`, `soundex`, `repeat`, `array_join`, `regexp_extract_all`
- ✅ Pandas API (3): `mapInPandas`, `applyInPandas`, `GroupedData.transform`
- ✅ DataFrame methods (3): `transform`, `unpivot`, `mapPartitions`
- ✅ SQL features: Parameterized queries, ORDER BY ALL, GROUP BY ALL
- ✅ Enhanced error messages with suggestions

**Total v2.5.0: ~25 features implemented, 569 tests passing**

---

## Feature Gap Overview

### Missing Features by Category

| Category | Missing Count | Priority | Estimated Effort |
|----------|--------------|----------|------------------|
| Higher-Order Array Functions | 6 | 🔴 High | 2-3 weeks |
| Basic Array Functions | 8 | 🟡 Medium | 1-2 weeks |
| Advanced Map Functions | 6 | 🟡 Medium | 1 week |
| Bitwise Functions | 3 | 🟢 Low | 3-5 days |
| Timezone Functions | 4 | 🟡 Medium | 1 week |
| URL Functions | 3 | 🟢 Low | 3-5 days |
| Struct Functions | 2 | 🟡 Medium | 3-5 days |
| Miscellaneous | 5 | 🟢 Low | 1 week |
| XML Functions | 11 | 🟢 Low | 2 weeks |
| **TOTAL** | **48** | - | **8-12 weeks** |

---

## Phased Implementation Plan

### Phase 1: Higher-Order Array Functions (v2.6.0)
**Priority:** 🔴 High  
**Effort:** 2-3 weeks  
**Impact:** High - These are commonly used in production PySpark code

#### Features

1. **`transform(col, function)`**
   - Apply lambda function to each array element
   - Example: `F.transform(F.col("numbers"), lambda x: x * 2)`
   - **Complexity:** High (requires lambda/UDF support)
   - **DuckDB:** Use `LIST_TRANSFORM(array, x -> expression)`
   - **Estimated Time:** 3-4 days

2. **`filter(col, function)`**
   - Filter array elements with predicate
   - Example: `F.filter(F.col("numbers"), lambda x: x > 10)`
   - **Complexity:** High (requires lambda support)
   - **DuckDB:** Use `LIST_FILTER(array, x -> condition)`
   - **Estimated Time:** 3-4 days

3. **`exists(col, function)`**
   - Check if any element satisfies predicate
   - Example: `F.exists(F.col("numbers"), lambda x: x > 100)`
   - **Complexity:** Medium
   - **DuckDB:** Use `LIST_ANY(array, x -> condition)` or custom SQL
   - **Estimated Time:** 2-3 days

4. **`forall(col, function)`**
   - Check if all elements satisfy predicate
   - Example: `F.forall(F.col("numbers"), lambda x: x > 0)`
   - **Complexity:** Medium
   - **DuckDB:** Use `LIST_ALL(array, x -> condition)` or custom SQL
   - **Estimated Time:** 2-3 days

5. **`aggregate(col, initialValue, merge, finish=None)`**
   - Reduce array to single value
   - Example: `F.aggregate(F.col("numbers"), lit(0), lambda acc, x: acc + x)`
   - **Complexity:** High (complex lambda with accumulator)
   - **DuckDB:** Use `LIST_REDUCE` or custom implementation
   - **Estimated Time:** 4-5 days

6. **`zip_with(left, right, function)`**
   - Merge two arrays element-wise
   - Example: `F.zip_with(F.col("arr1"), F.col("arr2"), lambda x, y: x + y)`
   - **Complexity:** High (two-argument lambda)
   - **DuckDB:** Custom SQL with array indexing
   - **Estimated Time:** 4-5 days

#### Implementation Strategy

**Lambda Function Support:**
- Create `MockLambdaExpression` class to represent lambda functions
- Parse lambda expressions into SQL-compatible format
- Support for 1-arg (transform, filter, exists, forall) and 2-arg (zip_with) lambdas
- Handle variable names (x, y, acc, etc.) in lambda bodies

**DuckDB Integration:**
- Leverage DuckDB's lambda syntax: `x -> expression`
- Map Python lambda to DuckDB lambda format
- Handle nested lambdas and complex expressions

**Testing:**
- 12 new tests (2 per function)
- Compare against real PySpark behavior
- Edge cases: empty arrays, null elements, nested arrays

**Files to Modify:**
- `mock_spark/functions/array.py` - Add 6 new functions
- `mock_spark/functions/base.py` - Add `MockLambdaExpression` class
- `mock_spark/backend/duckdb/query_executor.py` - Lambda translation logic
- `mock_spark/functions/functions.py` - Export new functions
- `tests/compatibility/test_pyspark_3_2_higher_order.py` - New test file

---

### Phase 2: Remaining Array Functions (v2.6.0 or v2.7.0)
**Priority:** 🟡 Medium  
**Effort:** 1-2 weeks  
**Impact:** Medium - Useful but less critical

#### Features

7. **`array_compact(col)`**
   - Remove null values from array
   - **Complexity:** Low
   - **DuckDB:** `LIST_FILTER(array, x -> x IS NOT NULL)`
   - **Estimated Time:** 1 day

8. **`slice(col, start, length)`**
   - Extract array slice
   - **Complexity:** Low
   - **DuckDB:** `LIST_SLICE(array, start, start+length)`
   - **Estimated Time:** 1 day

9. **`element_at(col, index)`**
   - Get element at index (1-based in Spark, negative for reverse)
   - **Complexity:** Low
   - **DuckDB:** `LIST_EXTRACT(array, index)` with index adjustment
   - **Estimated Time:** 1-2 days

10. **`array_append(col, element)`**
    - Append element to end of array
    - **Complexity:** Low
    - **DuckDB:** `LIST_APPEND(array, element)` or `array || [element]`
    - **Estimated Time:** 1 day

11. **`array_prepend(col, element)`**
    - Prepend element to start of array
    - **Complexity:** Low
    - **DuckDB:** `LIST_PREPEND(array, element)` or `[element] || array`
    - **Estimated Time:** 1 day

12. **`array_insert(col, pos, val)`**
    - Insert element at position
    - **Complexity:** Medium
    - **DuckDB:** Custom SQL with slice and concatenation
    - **Estimated Time:** 2 days

13. **`array_size(col)`**
    - Get array length (alias for `size()`)
    - **Complexity:** Low
    - **DuckDB:** `LIST_LENGTH(array)` or `ARRAY_LENGTH(array)`
    - **Estimated Time:** 1 day

14. **`array_sort(col, asc=True)`**
    - Sort array elements
    - **Complexity:** Low
    - **DuckDB:** `LIST_SORT(array)` or `LIST_REVERSE_SORT(array)`
    - **Estimated Time:** 1 day

15. **`arrays_overlap(col1, col2)`**
    - Check if arrays have any common elements
    - **Complexity:** Low
    - **DuckDB:** `LIST_HAS_ANY(col1, col2)` or custom check
    - **Estimated Time:** 1 day

#### Implementation Strategy

**Simple Approach:**
- Most functions map directly to DuckDB list functions
- Add to `ArrayFunctions` class in `mock_spark/functions/array.py`
- Custom SQL generation in `query_executor.py`

**Testing:**
- 9-12 new tests
- Edge cases: empty arrays, null handling, boundary indices

**Files to Modify:**
- `mock_spark/functions/array.py` - Add 9 new functions
- `mock_spark/backend/duckdb/query_executor.py` - Add special SQL handlers
- `mock_spark/storage/spark_function_mapper.py` - Add function mappings
- `tests/compatibility/test_pyspark_3_2_arrays_extended.py` - New test file

---

### Phase 3: Advanced Map Functions (v2.7.0)
**Priority:** 🟡 Medium  
**Effort:** 1 week  
**Impact:** Medium - Completes map API

#### Features

16. **`create_map(*cols)`**
    - Create map from alternating key-value columns
    - **Complexity:** Medium
    - **DuckDB:** `MAP([keys], [values])` with column collection
    - **Estimated Time:** 2 days

17. **`map_contains_key(col, key)`**
    - Check if map has specific key
    - **Complexity:** Low
    - **DuckDB:** `MAP_EXTRACT(map, key) IS NOT NULL`
    - **Estimated Time:** 1 day

18. **`map_from_entries(col)`**
    - Convert array of structs to map
    - **Complexity:** Medium (requires struct handling)
    - **DuckDB:** Custom SQL or `MAP_FROM_ENTRIES`
    - **Estimated Time:** 2-3 days

19. **`map_filter(col, function)`**
    - Filter map entries with lambda
    - **Complexity:** High (requires lambda + struct)
    - **DuckDB:** Custom SQL with MAP iteration
    - **Estimated Time:** 3-4 days

20. **`transform_keys(col, function)`**
    - Transform all map keys with lambda
    - **Complexity:** High (requires lambda)
    - **DuckDB:** Custom SQL with MAP manipulation
    - **Estimated Time:** 3-4 days

21. **`transform_values(col, function)`**
    - Transform all map values with lambda
    - **Complexity:** High (requires lambda)
    - **DuckDB:** Custom SQL with MAP manipulation
    - **Estimated Time:** 3-4 days

#### Implementation Strategy

**Dependencies:**
- Requires lambda support from Phase 1
- Requires struct type handling
- MAP type system already in place from v2.5.0

**Testing:**
- 6-8 new tests
- Integration with existing MAP infrastructure

**Files to Modify:**
- `mock_spark/functions/map.py` - Add 6 new functions
- `mock_spark/backend/duckdb/query_executor.py` - MAP lambda handlers
- `tests/compatibility/test_pyspark_3_2_maps_extended.py` - New test file

---

### Phase 4: Struct Functions (v2.7.0)
**Priority:** 🟡 Medium  
**Effort:** 3-5 days  
**Impact:** Medium - Needed for complex data types

#### Features

22. **`struct(*cols)`**
    - Create struct column from multiple columns
    - **Complexity:** Medium
    - **DuckDB:** `STRUCT_PACK(col1, col2, ...)`
    - **Estimated Time:** 2 days

23. **`named_struct(*cols)`**
    - Create struct with explicit field names
    - **Complexity:** Medium
    - **DuckDB:** `{'name1': col1, 'name2': col2}` syntax
    - **Estimated Time:** 2 days

#### Implementation Strategy

**Approach:**
- Leverage existing `MockStructType` infrastructure
- Map to DuckDB STRUCT operations
- Handle field naming and type inference

**Testing:**
- 4 new tests
- Nested struct scenarios

**Files to Modify:**
- `mock_spark/functions/functions.py` - Add struct functions
- `mock_spark/backend/duckdb/query_executor.py` - STRUCT handlers
- `tests/compatibility/test_struct_functions.py` - New test file

---

### Phase 5: Bitwise Functions (v2.7.0 or v2.8.0)
**Priority:** 🟢 Low  
**Effort:** 3-5 days  
**Impact:** Low - Niche use cases

#### Features

24. **`bit_count(col)`**
    - Count number of set bits (1s)
    - **Complexity:** Low
    - **DuckDB:** `BIT_COUNT(col)`
    - **Estimated Time:** 1 day

25. **`bit_get(col, pos)`**
    - Get bit value at position
    - **Complexity:** Low
    - **DuckDB:** `BIT_GET(col, pos)`
    - **Estimated Time:** 1 day

26. **`bitwise_not(col)`**
    - Bitwise NOT operation
    - **Complexity:** Low
    - **DuckDB:** `~col` or `BITWISE_NOT(col)`
    - **Estimated Time:** 1 day

#### Implementation Strategy

**Simple Approach:**
- Create `BitwiseFunctions` class
- Direct mapping to DuckDB bitwise operators
- Minimal custom logic needed

**Testing:**
- 3-4 new tests
- Integer type validation

**Files to Modify:**
- `mock_spark/functions/bitwise.py` - New module
- `mock_spark/functions/functions.py` - Export functions
- `tests/unit/test_bitwise_functions.py` - New test file

---

### Phase 6: Timezone Functions (v2.8.0)
**Priority:** 🟡 Medium  
**Effort:** 1 week  
**Impact:** Medium - Important for global applications

#### Features

27. **`convert_timezone(sourceTz, targetTz, sourceTs)`**
    - Convert timestamp between timezones
    - **Complexity:** Medium
    - **DuckDB:** `timezone(targetTz, timezone(sourceTz, sourceTs))`
    - **Estimated Time:** 2 days

28. **`current_timezone()`**
    - Get current session timezone
    - **Complexity:** Low
    - **DuckDB:** `current_setting('TimeZone')`
    - **Estimated Time:** 1 day

29. **`from_utc_timestamp(ts, tz)`**
    - Convert from UTC to timezone
    - **Complexity:** Low
    - **DuckDB:** `timezone(tz, ts)`
    - **Estimated Time:** 1 day

30. **`to_utc_timestamp(ts, tz)`**
    - Convert to UTC from timezone
    - **Complexity:** Low
    - **DuckDB:** Reverse of `from_utc_timestamp`
    - **Estimated Time:** 1 day

#### Implementation Strategy

**Approach:**
- Add to `DateTimeFunctions` class
- Leverage DuckDB timezone support
- Handle timezone string validation

**Testing:**
- 4-6 new tests
- Multiple timezone scenarios

**Files to Modify:**
- `mock_spark/functions/datetime.py` - Add 4 functions
- `mock_spark/backend/duckdb/query_executor.py` - Timezone handlers
- `tests/compatibility/test_timezone_functions.py` - New test file

---

### Phase 7: URL Functions (v2.8.0)
**Priority:** 🟢 Low  
**Effort:** 3-5 days  
**Impact:** Low - Specialized use case

#### Features

31. **`parse_url(url, partToExtract, key=None)`**
    - Extract URL component (protocol, host, path, query, etc.)
    - **Complexity:** Medium
    - **DuckDB:** Custom parsing or Python urllib
    - **Estimated Time:** 2 days

32. **`url_encode(str)`**
    - URL encode string
    - **Complexity:** Low
    - **DuckDB:** Custom UDF using Python urllib.parse.quote
    - **Estimated Time:** 1 day

33. **`url_decode(str)`**
    - URL decode string
    - **Complexity:** Low
    - **DuckDB:** Custom UDF using Python urllib.parse.unquote
    - **Estimated Time:** 1 day

#### Implementation Strategy

**Approach:**
- Create `URLFunctions` class
- Use Python's urllib.parse for implementation
- May need Python UDF support in DuckDB backend

**Testing:**
- 3-4 new tests
- Various URL formats and edge cases

**Files to Modify:**
- `mock_spark/functions/url.py` - New module
- `mock_spark/functions/functions.py` - Export functions
- `mock_spark/backend/duckdb/query_executor.py` - URL handlers
- `tests/unit/test_url_functions.py` - New test file

---

### Phase 8: Miscellaneous Functions (v2.8.0)
**Priority:** 🟡 Medium  
**Effort:** 1 week  
**Impact:** Low-Medium

#### Features

34. **`date_part(field, source)`** / **`datepart(field, source)`**
    - Extract date part (year, month, day, hour, etc.)
    - **Complexity:** Low
    - **DuckDB:** `DATE_PART(field, source)`
    - **Estimated Time:** 1 day

35. **`dayname(col)`**
    - Get day name abbreviation (Mon, Tue, etc.)
    - **Complexity:** Low
    - **DuckDB:** `DAYNAME(col)` or `strftime(col, '%a')`
    - **Estimated Time:** 1 day

36. **`assert_true(col, errMsg=None)`**
    - Assert condition is true, raise error if false
    - **Complexity:** Medium (requires error handling)
    - **DuckDB:** CASE WHEN col THEN NULL ELSE ERROR(errMsg)
    - **Estimated Time:** 2 days

37. **`arrays_overlap(col1, col2)`** (note: already listed in Phase 2, removing duplicate)
    - Check if two arrays have common elements
    - **Complexity:** Low
    - **DuckDB:** `LIST_HAS_ANY(col1, col2)` or intersection check
    - **Estimated Time:** 1 day

38. **`concat_ws(sep, *cols)`** (may already be implemented - verify)
    - Concatenate strings with separator
    - **Complexity:** Low
    - **DuckDB:** `CONCAT_WS(sep, col1, col2, ...)`
    - **Estimated Time:** 1 day (if not already done)

#### Implementation Strategy

**Approach:**
- Add to appropriate function classes
- Most are straightforward DuckDB mappings
- `assert_true` requires special error handling

**Testing:**
- 4-5 new tests

**Files to Modify:**
- `mock_spark/functions/datetime.py` - `date_part`, `dayname`
- `mock_spark/functions/array.py` - `arrays_overlap`
- `mock_spark/functions/conditional.py` - `assert_true`
- `mock_spark/functions/string.py` - `concat_ws` (if missing)

---

### Phase 9: XML Functions (v3.0.0)
**Priority:** 🟢 Low  
**Effort:** 2 weeks  
**Impact:** Low - Very specialized

#### Features (11 total)

39. **`from_xml(col, schema, options=None)`**
40. **`to_xml(col, options=None)`**
41. **`schema_of_xml(xml, options=None)`**
42. **`xpath(xml, path)`**
43. **`xpath_boolean(xml, path)`**
44. **`xpath_double(xml, path)`**
45. **`xpath_float(xml, path)`**
46. **`xpath_int(xml, path)`**
47. **`xpath_long(xml, path)`**
48. **`xpath_short(xml, path)`**
49. **`xpath_string(xml, path)`**

#### Implementation Notes

**Complexity:** High overall  
**Dependencies:** Requires XML parsing library (lxml or xml.etree)  
**DuckDB Support:** Limited - may need Python UDF or custom implementation  
**Recommendation:** Skip for now unless there's specific user demand

---

## Implementation Priorities

### Recommended Roadmap

**Version 2.6.0 (Next Release)**
- Phase 1: Higher-Order Array Functions (6 functions)
- Phase 2: Basic Array Functions (8 functions)
- **Total:** 14 functions, ~3-4 weeks effort
- **Test Count:** +20-25 tests

**Version 2.7.0**
- Phase 3: Advanced Map Functions (6 functions)
- Phase 4: Struct Functions (2 functions)
- Phase 5: Bitwise Functions (3 functions)
- **Total:** 11 functions, ~2-3 weeks effort
- **Test Count:** +12-15 tests

**Version 2.8.0**
- Phase 6: Timezone Functions (4 functions)
- Phase 7: URL Functions (3 functions)
- Phase 8: Miscellaneous Functions (5 functions)
- **Total:** 12 functions, ~2-3 weeks effort
- **Test Count:** +12-15 tests

**Version 3.0.0 (Future)**
- Phase 9: XML Functions (11 functions) - If there's demand
- Other advanced features (Streaming, UDTFs, etc.)

---

## Technical Challenges

### Challenge 1: Lambda Function Support

**Problem:**  
Higher-order functions like `transform`, `filter`, `exists` require lambda expressions:
```python
F.transform(F.col("numbers"), lambda x: x * 2)
```

**Solution Options:**

1. **String-Based Lambdas (Simple)**
   - Accept lambda as string: `"x -> x * 2"`
   - Parse and translate to DuckDB lambda syntax
   - **Pros:** Easy to implement, DuckDB native
   - **Cons:** Not true Python lambda, different API

2. **Python Lambda Translation (Complex)**
   - Accept Python lambda: `lambda x: x * 2`
   - Parse AST and translate to SQL
   - **Pros:** True PySpark API compatibility
   - **Cons:** Complex, requires AST parsing

3. **Hybrid Approach (Recommended)**
   - Accept both lambda and expression
   - For common cases, use predefined operations
   - For complex cases, require string format
   - **Pros:** Flexible, covers most use cases
   - **Cons:** Not 100% API compatible

**Recommendation:** Start with hybrid approach, add full lambda support if needed.

### Challenge 2: DuckDB Function Coverage

Some functions may not have direct DuckDB equivalents:

| Function | DuckDB Support | Workaround |
|----------|----------------|------------|
| `transform` | ✅ `LIST_TRANSFORM` | Native |
| `filter` | ✅ `LIST_FILTER` | Native |
| `exists` | ⚠️ No `LIST_ANY` | Use `LIST_FILTER` + `LEN() > 0` |
| `forall` | ⚠️ No `LIST_ALL` | Use `LIST_FILTER` + `LEN() = original` |
| `aggregate` | ⚠️ Limited | Custom implementation |
| `zip_with` | ❌ Not available | Custom SQL with indexing |
| `map_filter` | ❌ Not available | Custom implementation |
| `transform_keys` | ❌ Not available | Custom implementation |
| `transform_values` | ❌ Not available | Custom implementation |

**Strategy:** Implement workarounds using combinations of existing DuckDB functions.

### Challenge 3: Performance vs Compatibility

**Trade-off:**
- Full lambda support = Complex implementation, potential performance impact
- Limited lambda support = Simpler implementation, faster execution

**Recommendation:** 
- Prioritize common use cases (transform, filter)
- Optimize DuckDB SQL generation
- Accept performance trade-offs for complex lambdas

---

## Testing Strategy

### Compatibility Testing

For each new function:
1. **Unit Tests** - Test basic functionality
2. **Compatibility Tests** - Compare against real PySpark
3. **Edge Case Tests** - Nulls, empty arrays, boundary conditions
4. **Integration Tests** - Combine with other operations

### Test Coverage Goals

- **Phase 1:** +20-25 tests (higher-order functions are complex)
- **Phase 2:** +12-15 tests (basic array functions)
- **Phase 3:** +12-15 tests (advanced map functions)
- **Phases 4-8:** +15-20 tests (misc functions)

**Total New Tests:** ~60-75 tests  
**Target Total:** 630-645 tests

### Quality Gates

Each phase must pass:
- ✅ All new tests passing
- ✅ All existing tests still passing (no regressions)
- ✅ `ruff` linting (0 errors)
- ✅ `mypy` type checking (0 errors)
- ✅ Code coverage maintained or improved

---

## Documentation Updates

For each phase:
1. Update README.md with new functions
2. Add docstrings with examples
3. Update function count in badges
4. Create migration guide for new features
5. Add example notebooks

---

## Risk Assessment

### High Risk Items

1. **Lambda Function Implementation**
   - **Risk:** Complex AST parsing, may have bugs
   - **Mitigation:** Start simple, add complexity iteratively
   - **Fallback:** Document limitations, accept string-based lambdas

2. **DuckDB Function Gaps**
   - **Risk:** Some functions don't exist in DuckDB
   - **Mitigation:** Implement workarounds, may have performance impact
   - **Fallback:** Document as "limited support"

3. **Breaking Changes**
   - **Risk:** Lambda API changes might affect existing code
   - **Mitigation:** Maintain backward compatibility
   - **Fallback:** Version bump to 3.0.0 if needed

### Medium Risk Items

4. **Performance Degradation**
   - **Risk:** Complex SQL for lambdas may be slow
   - **Mitigation:** Optimize SQL generation, benchmark
   - **Fallback:** Document performance characteristics

5. **Type Safety**
   - **Risk:** Lambda expressions harder to type-check
   - **Mitigation:** Comprehensive mypy annotations
   - **Fallback:** Use `Any` type where necessary

### Low Risk Items

6. **Array/Map/Struct Functions**
   - **Risk:** Minimal - straightforward implementations
   - **Mitigation:** Follow existing patterns

---

## Success Criteria

### Version 2.6.0
- [ ] 14 new functions implemented (higher-order + basic arrays)
- [ ] 20-25 new tests passing
- [ ] Lambda expression support (at minimum, string-based)
- [ ] All existing tests still passing
- [ ] Documentation updated
- [ ] Published to PyPI

### Version 2.7.0
- [ ] 11 new functions (map + struct + bitwise)
- [ ] 12-15 new tests passing
- [ ] Complete map API coverage
- [ ] Struct type operations working
- [ ] Published to PyPI

### Version 2.8.0
- [ ] 12 new functions (timezone + URL + misc)
- [ ] 12-15 new tests passing
- [ ] Timezone operations fully functional
- [ ] Published to PyPI

### Version 3.0.0 (Optional)
- [ ] XML functions if there's demand
- [ ] 100% PySpark 3.2 function coverage
- [ ] Comprehensive documentation

---

## Decision Points

### Should We Implement All 48 Functions?

**Arguments FOR:**
- ✅ Complete PySpark 3.2 API coverage
- ✅ Better compatibility for edge cases
- ✅ Professional, enterprise-ready library

**Arguments AGAINST:**
- ❌ Some functions rarely used (XML, bitwise)
- ❌ Significant development time (8-12 weeks)
- ❌ Maintenance burden increases
- ❌ May not provide value to most users

### Recommendation

**Phased Approach:**
1. **Implement Phase 1-2 (v2.6.0)** - High value, commonly used
2. **Gather User Feedback** - See what features are actually needed
3. **Prioritize Based on Demand** - Only implement what users request
4. **Document Limitations** - Clear about what's not implemented

### Alternative: Feature Flags

Consider implementing with feature flags:
```python
spark = MockSparkSession("app", enable_experimental_features=True)
```

This allows:
- Beta testing of new features
- Gradual rollout
- Easy rollback if issues arise

---

## Next Steps

### Immediate (Post v2.5.0 Release)

1. **Gather Feedback**
   - Monitor PyPI downloads and usage
   - Check GitHub issues for feature requests
   - Survey users about needed functions

2. **Prioritize Based on Demand**
   - Higher-order array functions likely most requested
   - Timezone functions for global apps
   - Skip XML/URL if no demand

3. **Create Detailed Design Doc**
   - Lambda expression system design
   - AST parsing approach
   - DuckDB integration strategy

### For v2.6.0 Development

1. **Design Lambda System**
   - Decide on approach (string-based vs AST parsing)
   - Create proof of concept
   - Get community feedback

2. **Implement Phase 1**
   - Start with `transform` and `filter` (most common)
   - Add tests as you go
   - Validate against real PySpark

3. **Iterate Based on Feedback**
   - Adjust priorities
   - Add requested features
   - Skip low-value features

---

## Conclusion

**Version 2.5.0 Status:**
- ✅ **Successfully published to PyPI**
- ✅ **Core PySpark 3.2 features implemented** (25 features)
- ✅ **Production-ready quality** (569 tests, 100% typed)
- ✅ **Most commonly used features covered**

**Future Development:**
- 🎯 **48 additional functions** could be added for 100% coverage
- 📊 **Recommended approach:** Phased implementation based on user demand
- ⏰ **Estimated total effort:** 8-12 weeks for complete coverage
- 💡 **Priority:** Focus on higher-order array functions first (v2.6.0)

**Key Decision:** Should we pursue 100% PySpark 3.2 coverage, or focus on the most valuable features based on user feedback?

---

## Appendix A: Complete Feature List

### Already Implemented in v2.5.0 ✅

**Array Functions (6):**
- array_distinct, array_intersect, array_union, array_except, array_position, array_remove

**Map Functions (5):**
- map_keys, map_values, map_entries, map_concat, map_from_arrays

**String Functions (5):**
- initcap, soundex, repeat, array_join, regexp_extract_all

**Timestamp Functions (2):**
- timestampadd, timestampdiff

**DataFrame Methods (6):**
- transform, unpivot, mapPartitions, mapInPandas, GroupedData.applyInPandas, GroupedData.transform

**SQL Features (4):**
- Parameterized queries, ORDER BY ALL, GROUP BY ALL, DEFAULT columns

**Error Handling (1):**
- Enhanced error messages

**TOTAL: ~29 features**

### Missing from PySpark 3.2

**Higher-Order Array Functions (6):**
- transform (lambda), filter (lambda), exists, forall, aggregate, zip_with

**Basic Array Functions (8):**
- array_compact, slice, element_at, array_append, array_prepend, array_insert, array_size, array_sort

**Advanced Map Functions (6):**
- create_map, map_contains_key, map_from_entries, map_filter, transform_keys, transform_values

**Bitwise Functions (3):**
- bit_count, bit_get, bitwise_not

**Timezone Functions (4):**
- convert_timezone, current_timezone, from_utc_timestamp, to_utc_timestamp

**URL Functions (3):**
- parse_url, url_encode, url_decode

**Struct Functions (2):**
- struct, named_struct

**Miscellaneous (5):**
- date_part, datepart, dayname, assert_true, arrays_overlap

**XML Functions (11):**
- from_xml, to_xml, schema_of_xml, xpath*, etc.

**TOTAL: ~48 functions**

---

## Appendix B: Effort Estimates

| Phase | Functions | Complexity | Days | Weeks |
|-------|-----------|------------|------|-------|
| 1: Higher-Order Arrays | 6 | High | 15-20 | 2-3 |
| 2: Basic Arrays | 8 | Low-Medium | 8-10 | 1-2 |
| 3: Advanced Maps | 6 | Medium-High | 10-12 | 1.5-2 |
| 4: Struct Functions | 2 | Medium | 3-4 | 0.5-1 |
| 5: Bitwise Functions | 3 | Low | 3-4 | 0.5-1 |
| 6: Timezone Functions | 4 | Medium | 5-7 | 1 |
| 7: URL Functions | 3 | Low-Medium | 3-5 | 0.5-1 |
| 8: Miscellaneous | 5 | Low-Medium | 5-7 | 1 |
| 9: XML Functions | 11 | High | 10-14 | 2-3 |
| **TOTAL** | **48** | **Mixed** | **62-83** | **10-15** |

**Note:** These are conservative estimates assuming one developer working full-time.

---

## Questions for Stakeholders

1. **Do we need 100% PySpark 3.2 coverage?**
   - Or focus on 80/20 rule (20% of functions used 80% of the time)?

2. **What's the priority for lambda support?**
   - Full Python lambda support vs string-based lambdas?

3. **Which categories are most important?**
   - Higher-order arrays? Advanced maps? Timezone? Bitwise?

4. **Should we wait for user feedback before implementing?**
   - Let users request features vs proactive implementation?

5. **What's the target timeline?**
   - Aggressive (2-3 months) vs conservative (6-12 months)?

---

**Next Action:** Await decision on whether to proceed with Phase 1 implementation for v2.6.0 or gather user feedback first.

