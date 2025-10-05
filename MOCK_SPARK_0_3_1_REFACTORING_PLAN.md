# Mock Spark 0.3.1 - Package Structure Refactoring Plan

## 🎯 **Release Goal**
Improve package structure by breaking down large monolithic files into smaller, focused modules for better maintainability, testability, and code organization.

## 📊 **Current State Analysis**

### **Largest Files Requiring Refactoring:**
1. **`mock_spark/dataframe/dataframe.py`** - 3,410 lines, 84 methods
2. **`mock_spark/dataframe/grouped_data.py`** - 950 lines, 22 methods  
3. **`mock_spark/functions/base.py`** - 650 lines, 79 methods
4. **`mock_spark/functions/core.py`** - 585 lines, 84 methods
5. **`mock_spark/session/session.py`** - 531 lines, 33 methods

### **Other Large Files:**
- `mock_spark/data_generation.py` - 474 lines
- `mock_spark/session/sql/parser.py` - 473 lines
- `mock_spark/testing/factories.py` - 455 lines
- `mock_spark/storage/backends/sqlite.py` - 454 lines
- `mock_spark/spark_types.py` - 443 lines

## 🏗️ **Refactoring Strategy**

### **Phase 1: DataFrame Module Restructuring**

#### **Current Structure:**
```
mock_spark/dataframe/
├── dataframe.py (3,410 lines) - TOO LARGE
├── grouped_data.py (950 lines) - TOO LARGE
├── rdd.py (70 lines) - OK
├── reader.py (58 lines) - OK
└── writer.py (76 lines) - OK
```

#### **Proposed Structure:**
```
mock_spark/dataframe/
├── __init__.py
├── core/
│   ├── __init__.py
│   ├── dataframe.py (core DataFrame class, ~800 lines)
│   ├── operations.py (select, filter, withColumn, ~600 lines)
│   ├── joins.py (join, union, intersect, crossJoin, ~400 lines)
│   ├── aggregations.py (groupBy, agg, rollup, cube, ~500 lines)
│   └── utilities.py (show, collect, toPandas, ~300 lines)
├── grouped/
│   ├── __init__.py
│   ├── base.py (MockGroupedData base class, ~200 lines)
│   ├── standard.py (standard groupBy operations, ~300 lines)
│   ├── rollup.py (rollup operations, ~200 lines)
│   ├── cube.py (cube operations, ~200 lines)
│   └── pivot.py (pivot operations, ~200 lines)
├── rdd.py (keep as is)
├── reader.py (keep as is)
└── writer.py (keep as is)
```

### **Phase 2: Functions Module Restructuring**

#### **Current Structure:**
```
mock_spark/functions/
├── base.py (650 lines) - TOO LARGE
├── core.py (585 lines) - TOO LARGE
├── string.py (323 lines) - OK
├── math.py (92 lines) - OK
├── aggregate.py (55 lines) - OK
├── conditional.py (336 lines) - OK
├── datetime.py (370 lines) - OK
└── window_execution.py (98 lines) - OK
```

#### **Proposed Structure:**
```
mock_spark/functions/
├── __init__.py
├── core/
│   ├── __init__.py
│   ├── column.py (MockColumn class, ~300 lines)
│   ├── operations.py (arithmetic, comparison, logical, ~400 lines)
│   ├── expressions.py (F namespace, expr functions, ~300 lines)
│   └── literals.py (MockLiteral, lit function, ~100 lines)
├── string.py (keep as is)
├── math.py (keep as is)
├── aggregate.py (keep as is)
├── conditional.py (keep as is)
├── datetime.py (keep as is)
└── window_execution.py (keep as is)
```

### **Phase 3: Session Module Restructuring**

#### **Current Structure:**
```
mock_spark/session/
├── session.py (531 lines) - TOO LARGE
├── catalog.py (84 lines) - OK
├── context.py (34 lines) - OK
├── config/ (OK)
└── sql/ (OK)
```

#### **Proposed Structure:**
```
mock_spark/session/
├── __init__.py
├── core/
│   ├── __init__.py
│   ├── session.py (core session class, ~200 lines)
│   ├── builder.py (session builder pattern, ~150 lines)
│   └── context.py (spark context, ~100 lines)
├── catalog.py (keep as is)
├── config/ (keep as is)
└── sql/ (keep as is)
```

### **Phase 4: Supporting Module Improvements**

#### **Data Generation Refactoring:**
```
mock_spark/data_generation.py (474 lines) →
mock_spark/data_generation/
├── __init__.py
├── core.py (MockDataGenerator base class, ~150 lines)
├── builders.py (MockDataGeneratorBuilder, ~150 lines)
├── generators.py (specific generators, ~200 lines)
└── utilities.py (helper functions, ~100 lines)
```

#### **Testing Module Improvements:**
```
mock_spark/testing/factories.py (455 lines) →
mock_spark/testing/
├── factories/
│   ├── __init__.py
│   ├── dataframe_factory.py (~150 lines)
│   ├── session_factory.py (~150 lines)
│   └── data_factory.py (~150 lines)
├── generators.py (keep as is)
├── mocks.py (keep as is)
├── fixtures.py (keep as is)
└── simulators.py (keep as is)
```

## 📋 **Implementation Plan**

### **Step 1: Create New Directory Structure**
- [x] Create new subdirectories for organized modules
- [x] Set up `__init__.py` files with proper imports
- [ ] Ensure backward compatibility in imports

### **Step 2: Refactor DataFrame Module**
- [x] Extract core DataFrame class to `dataframe/core/dataframe.py`
- [x] Move operations to `dataframe/core/operations.py`
- [ ] Move joins to `dataframe/core/joins.py`
- [ ] Move aggregations to `dataframe/core/aggregations.py`
- [ ] Move utilities to `dataframe/core/utilities.py`
- [ ] Split grouped_data.py into focused modules
- [ ] Update imports in `dataframe/__init__.py`

## 🚀 **Current Progress**

### **✅ Completed (Step 1 & 2 - Partial)**
1. **Directory Structure Created**
   - Created `mock_spark/dataframe/core/` directory
   - Created `mock_spark/dataframe/grouped/` directory
   - Set up `__init__.py` files with proper imports

2. **Core DataFrame Extracted**
   - Created `mock_spark/dataframe/core/dataframe.py` with core DataFrame class
   - Extracted essential methods: `__init__`, `__repr__`, `show`, `to_markdown`, `collect`, `toPandas`, `count`, `columns`, `printSchema`, `dtypes`, `rdd`, `explain`, `isStreaming`, `write`
   - Maintained all core functionality and type safety

3. **Operations Module Created**
   - Created `mock_spark/dataframe/core/operations.py` with DataFrameOperations mixin
   - Extracted key operations: `select`, `filter`, `withColumn`, `drop`, `withColumnRenamed`, `dropna`, `fillna`, `distinct`, `dropDuplicates`, `selectExpr`
   - Included comprehensive helper methods for expression evaluation
   - Maintained full PySpark API compatibility

### **🔄 In Progress**
- Continuing with DataFrame module refactoring
- Next: Create joins, aggregations, and utilities modules

### **📊 Progress Metrics**
- **Files Created**: 3 new modules
- **Lines Refactored**: ~1,400 lines extracted from original dataframe.py
- **Methods Extracted**: ~25 core methods moved to focused modules
- **Structure Improvement**: DataFrame module now has clear separation of concerns

### **Step 3: Refactor Functions Module**
- [ ] Extract MockColumn to `functions/core/column.py`
- [ ] Move operations to `functions/core/operations.py`
- [ ] Move expressions to `functions/core/expressions.py`
- [ ] Move literals to `functions/core/literals.py`
- [ ] Update imports in `functions/__init__.py`

### **Step 4: Refactor Session Module**
- [ ] Extract core session to `session/core/session.py`
- [ ] Move builder pattern to `session/core/builder.py`
- [ ] Move context to `session/core/context.py`
- [ ] Update imports in `session/__init__.py`

### **Step 5: Refactor Supporting Modules**
- [ ] Split data_generation.py into focused modules
- [ ] Split testing/factories.py into focused modules
- [ ] Update all imports throughout the package

### **Step 6: Update Documentation**
- [ ] Update API reference documentation
- [ ] Update examples to use new import paths
- [ ] Update README with new structure
- [ ] Create migration guide for users

### **Step 7: Testing & Validation**
- [ ] Run all existing tests to ensure no regressions
- [ ] Add new tests for refactored modules
- [ ] Update import tests
- [ ] Validate backward compatibility

## 🎯 **Success Metrics**

### **File Size Targets:**
- No single file > 500 lines
- Average file size < 300 lines
- Maximum methods per class < 20

### **Structure Improvements:**
- Clear separation of concerns
- Logical grouping of related functionality
- Easier navigation and maintenance
- Better testability

### **Backward Compatibility:**
- All existing imports continue to work
- No breaking changes for users
- Seamless migration path

## 🚀 **Benefits**

### **For Developers:**
- **Easier Navigation**: Smaller, focused files
- **Better Maintainability**: Clear separation of concerns
- **Improved Testability**: Isolated functionality
- **Reduced Complexity**: Easier to understand and modify

### **For Users:**
- **No Breaking Changes**: All existing code continues to work
- **Better Documentation**: Clearer module organization
- **Improved Performance**: Faster imports and loading
- **Future-Proof**: Easier to extend and enhance

## 📝 **Migration Strategy**

### **Gradual Migration:**
1. **Phase 1**: Create new structure alongside existing
2. **Phase 2**: Update internal imports to use new structure
3. **Phase 3**: Update public API imports (backward compatible)
4. **Phase 4**: Deprecate old import paths (with warnings)
5. **Phase 5**: Remove old import paths (future release)

### **Backward Compatibility:**
- All existing imports will continue to work
- New imports will be available for better organization
- Deprecation warnings will guide users to new structure
- Migration guide will be provided

## 🔧 **Technical Considerations**

### **Import Management:**
- Use `__init__.py` files to maintain backward compatibility
- Implement lazy imports where appropriate
- Ensure circular import prevention

### **Testing Strategy:**
- Maintain all existing tests
- Add tests for new module boundaries
- Ensure no performance regressions
- Validate import performance

### **Documentation Updates:**
- Update API reference for new structure
- Create migration guide
- Update examples and tutorials
- Maintain comprehensive documentation

## 📅 **Timeline**

- **Week 1**: Create new directory structure and core modules
- **Week 2**: Refactor DataFrame module
- **Week 3**: Refactor Functions module  
- **Week 4**: Refactor Session module
- **Week 5**: Refactor supporting modules
- **Week 6**: Update documentation and testing
- **Week 7**: Final testing and release preparation

## 🎉 **Expected Outcomes**

- **Improved Maintainability**: Easier to understand and modify code
- **Better Organization**: Logical grouping of related functionality
- **Enhanced Testability**: Isolated modules easier to test
- **Future-Proof**: Easier to extend and enhance
- **No Breaking Changes**: Seamless upgrade for users
- **Better Performance**: Faster imports and loading

This refactoring will significantly improve the package structure while maintaining full backward compatibility and enhancing the developer experience.
