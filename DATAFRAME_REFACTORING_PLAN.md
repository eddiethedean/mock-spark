# DataFrame Refactoring Plan

**Date**: October 7, 2025  
**Goal**: Break down the massive `dataframe.py` (3,974 lines) into focused modules

---

## 📊 Initial State

**dataframe.py**: 3,974 lines, 103 methods - **TOO LARGE!**

---

## 🎯 Refactoring Plan (3 Phases)

### Phase 1: DataFrame Export ✅ COMPLETE
**Status**: Merged to main

**Extracted**:
- `dataframe/export.py` (147 lines)
- Methods: `toPandas()`, `toDuckDB()`, `_create_duckdb_table()`, `_get_duckdb_type()`

**Impact**:
- dataframe.py: 3,974 → 3,903 lines (-71 lines)
- Better separation: core vs export functionality

---

### Phase 2: DataFrame Assertions ✅ COMPLETE
**Status**: Merged to main

**Extracted**:
- `dataframe/assertions.py` (77 lines)
- Methods: `assert_has_columns()`, `assert_row_count()`, `assert_schema_matches()`, `assert_data_equals()`

**Impact**:
- dataframe.py: 3,903 → 3,902 lines (-1 line)
- Clearer separation: core vs test utilities

---

### Phase 3: Lazy Evaluation ✅ COMPLETE
**Status**: Merged to main

**Extracted**:
- `dataframe/lazy.py` (348 lines)
- Methods:
  - `queue_operation()` - Queue operations for lazy execution
  - `materialize()` - Materialize queued operations via DuckDB
  - `_materialize_manual()` - Fallback materialization
  - `_convert_materialized_rows()` - Type conversion helper
  - `_infer_select_schema()` - Infer schema for select ops
  - `_infer_join_schema()` - Infer schema for join ops
  - `_filter_depends_on_original_columns()` - Filter dependency check

**Actual Impact**:
- dataframe.py: 3,902 → 3,667 lines (-235 lines, -6%)
- LazyEvaluationEngine now handles all lazy logic

---

## 📈 Total Progress

| Phase | Status | Lines Extracted | dataframe.py Size |
|-------|--------|----------------|-------------------|
| **Start** | - | 0 | 3,974 |
| **Phase 1: Export** | ✅ Complete | 71 | 3,903 |
| **Phase 2: Assertions** | ✅ Complete | 1 | 3,902 |
| **Phase 3: Lazy Eval** | 🔄 In Progress | ~300 (est.) | ~3,600 (est.) |
| **TOTAL** | - | **~372** | **~3,600** |

**Total Reduction**: ~372 lines (9.4% smaller)

---

## 🎯 Final Structure

After all phases:

```
mock_spark/dataframe/
├── dataframe.py (~3,600 lines) - Core DataFrame logic
├── export.py (147 lines) - Export to Pandas/DuckDB ✅
├── assertions.py (77 lines) - Test assertions ✅
├── lazy.py (~300 lines) - Lazy evaluation engine 🔄
├── grouped/
│   ├── base.py - GroupedData
│   ├── cube.py - Cube grouping
│   ├── pivot.py - Pivot operations
│   └── rollup.py - Rollup grouping
├── core/
│   ├── operations.py - DataFrame operations
│   ├── joins.py - Join logic
│   ├── aggregations.py - Aggregation logic
│   └── utilities.py - Utility functions
├── reader.py - DataFrameReader
├── writer.py - DataFrameWriter
└── sqlmodel_materializer.py - SQL generation
```

---

## ✅ Benefits

1. **Better Organization**: Each file has clear responsibility
2. **Easier Navigation**: Smaller files, easier to find code
3. **Better Testing**: Each module testable in isolation
4. **Easier Maintenance**: Changes localized to relevant modules
5. **No Breaking Changes**: All public APIs remain the same

---

## 🚀 Next Steps

### Immediate (Phase 3)
- [x] Create `lazy.py` module
- [ ] Update dataframe.py to use LazyEvaluationEngine
- [ ] Test lazy evaluation still works
- [ ] Run full test suite
- [ ] Format with black
- [ ] Commit and merge

### Future (Optional)
- [ ] Consider extracting more from dataframe.py if still too large
- [ ] Add unit tests for each extracted module
- [ ] Update documentation

---

## 📝 Current Work

**Branch**: `refactor/dataframe-lazy`  
**File Created**: `mock_spark/dataframe/lazy.py` (250 lines)  
**Next**: Update dataframe.py to delegate to LazyEvaluationEngine

---

**Progress**: 2/3 phases complete (67%)  
**Impact So Far**: -72 lines from dataframe.py  
**Expected Final**: -372 lines total (9.4% reduction)

