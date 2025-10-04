# PySpark 3.2 API Completion Plan

## Overview
This document outlines a comprehensive plan to implement the missing PySpark 3.2 API functionality in mock-spark, bringing our coverage from ~66% to ~95% of the official PySpark 3.2 API.

## Current Status
- **DataFrame Methods**: ~35/65 implemented (~54% coverage)
- **Functions**: ~85/120 implemented (~71% coverage)  
- **Session Features**: ~18/25 implemented (~72% coverage)
- **Overall API Coverage**: ~138/210 components (~66% coverage)

## Progress Update (Release 0.3.0)
**Date**: Current session
**Phase 1.1 Advanced Aggregations**: ✅ **COMPLETED**
- ✅ `rollup()` method - Creates hierarchical groupings with null values for higher-level summaries
- ✅ `cube()` method - Creates multi-dimensional groupings with all possible combinations (2^n)
- ✅ `pivot()` method - Transforms pivot columns into separate columns with aggregation
- ✅ All methods tested and working correctly
- ✅ Compatible with PySpark 3.2 behavior

**Phase 1.2 Set Operations**: ✅ **COMPLETED**
- ✅ `intersect()` method - Finds common rows between DataFrames
- ✅ `exceptAll()` method - Set difference with duplicates preserved
- ✅ `crossJoin()` method - Cartesian product with column name conflict resolution
- ✅ `unionByName()` method - Union by column names with missing column handling
- ✅ All methods tested and working correctly
- ✅ Compatible with PySpark 3.2 behavior

**Phase 1.3 Sampling & Statistics**: ✅ **COMPLETED**
- ✅ `sample()` method - Random sampling with/without replacement and seed support
- ✅ `randomSplit()` method - Random splitting with weighted distribution
- ✅ `describe()` method - Basic statistics (count, mean, stddev, min, max) for numeric columns
- ✅ `summary()` method - Extended statistics including percentiles for numeric columns
- ✅ All methods tested and working correctly
- ✅ Compatible with PySpark 3.2 behavior

## Target Goals
- **DataFrame Methods**: 60/65 implemented (~92% coverage)
- **Functions**: 110/120 implemented (~92% coverage)
- **Session Features**: 23/25 implemented (~92% coverage)
- **Overall API Coverage**: ~193/210 components (~92% coverage)

---

## Phase 1: High-Priority DataFrame Methods (Week 1-2)

### 1.1 Advanced Aggregations
**Priority**: HIGH | **Complexity**: MEDIUM | **Effort**: 3-4 days

#### Tasks:
- [x] Implement `rollup()` method
  - Create `MockRollupGroupedData` class
  - Support hierarchical grouping with null values
  - Add unit tests for rollup operations
  - Add compatibility tests against PySpark 3.2

- [x] Implement `cube()` method  
  - Create `MockCubeGroupedData` class
  - Support multi-dimensional grouping
  - Add unit tests for cube operations
  - Add compatibility tests against PySpark 3.2

- [x] Implement `pivot()` method
  - Add pivot functionality to `MockGroupedData`
  - Support value aggregation and column pivoting
  - Add unit tests for pivot operations
  - Add compatibility tests against PySpark 3.2

#### Files to Modify:
- `mock_spark/dataframe/grouped_data.py`
- `mock_spark/dataframe/dataframe.py`
- `tests/unit/test_dataframe_advanced.py`
- `tests/compatibility/test_dataframe_operations.py`

#### Testing Strategy:
- **Unit Tests**: Test each aggregation type with various data scenarios
- **Compatibility Tests**: Verify behavior matches PySpark 3.2 exactly
- **Edge Cases**: Empty DataFrames, null values, single columns

### 1.2 Set Operations
**Priority**: HIGH | **Complexity**: MEDIUM | **Effort**: 2-3 days

#### Tasks:
- [x] Implement `intersect()` method
  - Add set intersection logic
  - Handle schema compatibility
  - Add unit tests for intersection operations
  - Add compatibility tests against PySpark 3.2

- [x] Implement `exceptAll()` method
  - Add set difference logic with duplicates
  - Handle schema compatibility  
  - Add unit tests for exceptAll operations
  - Add compatibility tests against PySpark 3.2

- [x] Implement `crossJoin()` method
  - Add Cartesian product logic
  - Handle large result sets efficiently
  - Add unit tests for cross join operations
  - Add compatibility tests against PySpark 3.2

- [x] Implement `unionByName()` method
  - Add union by column name matching
  - Handle different column orders
  - Add unit tests for unionByName operations
  - Add compatibility tests against PySpark 3.2

#### Files to Modify:
- `mock_spark/dataframe/dataframe.py`
- `tests/unit/test_dataframe_advanced.py`
- `tests/compatibility/test_dataframe_operations.py`

### 1.3 Sampling & Statistics
**Priority**: MEDIUM | **Complexity**: LOW | **Effort**: 2-3 days

#### Tasks:
- [x] Implement `sample()` method
  - Add random sampling with seed support
  - Support withReplacement and fraction parameters
  - Add unit tests for sampling operations
  - Add compatibility tests against PySpark 3.2

- [x] Implement `randomSplit()` method
  - Add random splitting with weights
  - Support multiple split ratios
  - Add unit tests for randomSplit operations
  - Add compatibility tests against PySpark 3.2

- [x] Implement `describe()` method
  - Add statistical summary functionality
  - Support column-specific statistics
  - Add unit tests for describe operations
  - Add compatibility tests against PySpark 3.2

- [x] Implement `summary()` method
  - Add extended statistical summary
  - Support custom statistics
  - Add unit tests for summary operations
  - Add compatibility tests against PySpark 3.2

#### Files to Modify:
- `mock_spark/dataframe/dataframe.py`
- `tests/unit/test_dataframe_advanced.py`
- `tests/compatibility/test_dataframe_operations.py`

---

## Phase 2: Missing Functions (Week 3-4)

### 2.1 Advanced String Functions
**Priority**: MEDIUM | **Complexity**: LOW | **Effort**: 2-3 days

#### Tasks:
- [ ] Implement `format_string()` function
  - Add string formatting with placeholders
  - Support multiple format specifiers
  - Add unit tests for format_string
  - Add compatibility tests against PySpark 3.2

- [ ] Implement `translate()` function
  - Add character translation functionality
  - Support character mapping
  - Add unit tests for translate
  - Add compatibility tests against PySpark 3.2

- [ ] Implement `ascii()`, `base64()` functions
  - Add encoding/decoding functions
  - Support various encodings
  - Add unit tests for encoding functions
  - Add compatibility tests against PySpark 3.2

#### Files to Modify:
- `mock_spark/functions/string.py`
- `mock_spark/functions/__init__.py`
- `tests/unit/test_functions_comprehensive.py`
- `tests/compatibility/test_column_functions.py`

### 2.2 Advanced Math Functions
**Priority**: MEDIUM | **Complexity**: LOW | **Effort**: 1-2 days

#### Tasks:
- [ ] Implement `sign()`, `greatest()`, `least()` functions
  - Add comparison math functions
  - Support multiple argument handling
  - Add unit tests for comparison functions
  - Add compatibility tests against PySpark 3.2

#### Files to Modify:
- `mock_spark/functions/math.py`
- `mock_spark/functions/__init__.py`
- `tests/unit/test_functions_comprehensive.py`
- `tests/compatibility/test_column_functions.py`

### 2.3 Advanced Aggregate Functions
**Priority**: MEDIUM | **Complexity**: MEDIUM | **Effort**: 2-3 days

#### Tasks:
- [ ] Implement `percentile_approx()` function
  - Add approximate percentile calculation
  - Support multiple percentiles
  - Add unit tests for percentile functions
  - Add compatibility tests against PySpark 3.2

- [ ] Implement `corr()`, `covar_samp()` functions
  - Add correlation and covariance functions
  - Support pairwise calculations
  - Add unit tests for correlation functions
  - Add compatibility tests against PySpark 3.2

#### Files to Modify:
- `mock_spark/functions/aggregate.py`
- `mock_spark/functions/__init__.py`
- `tests/unit/test_functions_comprehensive.py`
- `tests/compatibility/test_column_functions.py`

### 2.4 Advanced DateTime Functions
**Priority**: MEDIUM | **Complexity**: MEDIUM | **Effort**: 2-3 days

#### Tasks:
- [ ] Implement `hour()`, `minute()`, `second()` functions
  - Add time part extraction
  - Support timestamp and time types
  - Add unit tests for time functions
  - Add compatibility tests against PySpark 3.2

- [ ] Implement `add_months()`, `months_between()` functions
  - Add month arithmetic functions
  - Handle month boundaries correctly
  - Add unit tests for month functions
  - Add compatibility tests against PySpark 3.2

- [ ] Implement `date_add()`, `date_sub()` functions
  - Add date arithmetic functions
  - Support day-level operations
  - Add unit tests for date functions
  - Add compatibility tests against PySpark 3.2

#### Files to Modify:
- `mock_spark/functions/datetime.py`
- `mock_spark/functions/__init__.py`
- `tests/unit/test_functions_comprehensive.py`
- `tests/compatibility/test_column_functions.py`

### 2.5 Advanced Window Functions
**Priority**: MEDIUM | **Complexity**: MEDIUM | **Effort**: 2-3 days

#### Tasks:
- [ ] Implement `nth_value()` function
  - Add nth value in window functionality
  - Support null handling
  - Add unit tests for nth_value
  - Add compatibility tests against PySpark 3.2

- [ ] Implement `ntile()` function
  - Add NTILE window function
  - Support bucket distribution
  - Add unit tests for ntile
  - Add compatibility tests against PySpark 3.2

- [ ] Implement `cume_dist()`, `percent_rank()` functions
  - Add distribution functions
  - Support cumulative distribution
  - Add unit tests for distribution functions
  - Add compatibility tests against PySpark 3.2

#### Files to Modify:
- `mock_spark/functions/window_execution.py`
- `mock_spark/functions/__init__.py`
- `tests/unit/test_window_functions.py`
- `tests/compatibility/test_advanced_window_functions.py`

---

## Phase 3: Session & Advanced Features (Week 5-6)

### 3.1 Session Enhancements
**Priority**: MEDIUM | **Complexity**: LOW | **Effort**: 1-2 days

#### Tasks:
- [ ] Implement `getOrCreate()` method
  - Add singleton session pattern
  - Support session reuse
  - Add unit tests for getOrCreate
  - Add compatibility tests against PySpark 3.2

- [ ] Implement `createOrReplaceTempView()` method
  - Add temp view replacement
  - Handle view conflicts
  - Add unit tests for temp views
  - Add compatibility tests against PySpark 3.2

- [ ] Implement `createGlobalTempView()` method
  - Add global temp views
  - Support cross-session access
  - Add unit tests for global views
  - Add compatibility tests against PySpark 3.2

#### Files to Modify:
- `mock_spark/session/session.py`
- `mock_spark/dataframe/dataframe.py`
- `tests/unit/test_session_management.py`
- `tests/compatibility/test_session_management.py`

### 3.2 DataFrame Enhancements
**Priority**: MEDIUM | **Complexity**: LOW | **Effort**: 2-3 days

#### Tasks:
- [ ] Implement `selectExpr()` method
  - Add SQL-like expression selection
  - Support complex expressions
  - Add unit tests for selectExpr
  - Add compatibility tests against PySpark 3.2

- [ ] Implement `expr()` function
  - Add SQL expression function
  - Support dynamic expressions
  - Add unit tests for expr
  - Add compatibility tests against PySpark 3.2

- [ ] Implement `head()`, `tail()` methods
  - Add first/last N rows functionality
  - Support efficient row access
  - Add unit tests for head/tail
  - Add compatibility tests against PySpark 3.2

- [ ] Implement `schema` property
  - Add schema access property
  - Support schema inspection
  - Add unit tests for schema property
  - Add compatibility tests against PySpark 3.2

- [ ] Implement `toJSON()` method
  - Add JSON export functionality
  - Support various JSON formats
  - Add unit tests for toJSON
  - Add compatibility tests against PySpark 3.2

#### Files to Modify:
- `mock_spark/dataframe/dataframe.py`
- `mock_spark/functions/core.py`
- `tests/unit/test_dataframe_advanced.py`
- `tests/compatibility/test_dataframe_operations.py`

### 3.3 Advanced DataFrame Operations
**Priority**: LOW | **Complexity**: MEDIUM | **Effort**: 2-3 days

#### Tasks:
- [ ] Implement `repartition()` method
  - Add repartitioning functionality
  - Support column-based repartitioning
  - Add unit tests for repartition
  - Add compatibility tests against PySpark 3.2

- [ ] Implement `coalesce()` method
  - Add coalescing functionality
  - Support partition reduction
  - Add unit tests for coalesce
  - Add compatibility tests against PySpark 3.2

- [ ] Implement `checkpoint()` method
  - Add checkpointing functionality
  - Support lineage truncation
  - Add unit tests for checkpoint
  - Add compatibility tests against PySpark 3.2

- [ ] Implement `isStreaming` property
  - Add streaming detection
  - Support streaming DataFrame identification
  - Add unit tests for streaming
  - Add compatibility tests against PySpark 3.2

#### Files to Modify:
- `mock_spark/dataframe/dataframe.py`
- `tests/unit/test_dataframe_advanced.py`
- `tests/compatibility/test_dataframe_operations.py`

---

## Phase 4: Testing & Quality Assurance (Week 7-8)

### 4.1 Comprehensive Test Suite Updates
**Priority**: HIGH | **Complexity**: MEDIUM | **Effort**: 3-4 days

#### Tasks:
- [ ] Update unit test coverage
  - Add tests for all new functionality
  - Achieve 95%+ code coverage
  - Add edge case testing
  - Add performance testing

- [ ] Update compatibility test suite
  - Add PySpark 3.2 compatibility tests
  - Verify behavior matches exactly
  - Add regression testing
  - Add integration testing

- [ ] Add stress testing
  - Test with large datasets
  - Test with complex operations
  - Test memory usage
  - Test performance characteristics

#### Files to Modify:
- `tests/unit/` (all test files)
- `tests/compatibility/` (all test files)
- `tests/unit/test_performance_comprehensive.py`

### 4.2 Documentation Updates
**Priority**: MEDIUM | **Complexity**: LOW | **Effort**: 1-2 days

#### Tasks:
- [ ] Update API documentation
  - Document all new methods
  - Add usage examples
  - Update function signatures
  - Add migration guides

- [ ] Update README
  - Update feature list
  - Add new examples
  - Update compatibility matrix
  - Add performance benchmarks

- [ ] Update docstrings
  - Add comprehensive docstrings
  - Include examples and outputs
  - Add parameter descriptions
  - Add return type documentation

#### Files to Modify:
- `README.md`
- `docs/api_reference.md`
- All Python files with new functionality

---

## Phase 5: Performance & Optimization (Week 9)

### 5.1 Performance Optimization
**Priority**: MEDIUM | **Complexity**: HIGH | **Effort**: 2-3 days

#### Tasks:
- [ ] Optimize new implementations
  - Profile performance bottlenecks
  - Optimize critical paths
  - Add caching where appropriate
  - Improve memory usage

- [ ] Add performance benchmarks
  - Create benchmark suite
  - Compare with PySpark 3.2
  - Add performance regression tests
  - Document performance characteristics

#### Files to Modify:
- All new implementation files
- `tests/unit/test_performance_comprehensive.py`

### 5.2 Memory Management
**Priority**: MEDIUM | **Complexity**: MEDIUM | **Effort**: 1-2 days

#### Tasks:
- [ ] Improve memory efficiency
  - Optimize data structures
  - Add memory monitoring
  - Implement garbage collection
  - Add memory leak detection

#### Files to Modify:
- `mock_spark/dataframe/dataframe.py`
- `mock_spark/session/session.py`

---

## Testing Strategy

### Unit Testing
- **Coverage Target**: 95%+ code coverage
- **Test Types**: Functionality, edge cases, error handling
- **Framework**: pytest with comprehensive fixtures
- **Performance**: All tests must complete in <5 seconds

### Compatibility Testing
- **Target**: 100% behavioral compatibility with PySpark 3.2
- **Test Types**: Cross-validation, regression, integration
- **Framework**: pytest with PySpark 3.2 comparison
- **Coverage**: All new functionality must have compatibility tests

### Integration Testing
- **Scope**: End-to-end workflows
- **Test Types**: Complex scenarios, real-world usage
- **Framework**: pytest with comprehensive test data
- **Performance**: Integration tests must complete in <30 seconds

---

## Success Criteria

### Functional Requirements
- [ ] All new methods behave identically to PySpark 3.2
- [ ] 95%+ API coverage achieved
- [ ] All tests pass (unit + compatibility)
- [ ] No performance regressions

### Quality Requirements
- [ ] 95%+ code coverage maintained
- [ ] All new code properly documented
- [ ] Type hints added for all new functionality
- [ ] MyPy compliance maintained

### Performance Requirements
- [ ] No significant performance degradation
- [ ] Memory usage remains reasonable
- [ ] All tests complete within time limits
- [ ] Benchmarks show competitive performance

---

## Risk Assessment & Mitigation

### High Risk Items
1. **Complex Aggregations (rollup, cube, pivot)**
   - **Risk**: Complex logic, potential for bugs
   - **Mitigation**: Extensive testing, incremental implementation

2. **Performance Impact**
   - **Risk**: New features may slow down existing functionality
   - **Mitigation**: Performance testing, optimization, benchmarking

3. **Compatibility Issues**
   - **Risk**: Behavioral differences from PySpark 3.2
   - **Mitigation**: Comprehensive compatibility testing, iterative validation

### Medium Risk Items
1. **Memory Usage**
   - **Risk**: New features may increase memory consumption
   - **Mitigation**: Memory profiling, optimization, monitoring

2. **Test Maintenance**
   - **Risk**: Large test suite becomes difficult to maintain
   - **Mitigation**: Modular test design, good test organization

---

## Timeline Summary

| **Phase** | **Duration** | **Key Deliverables** | **Success Criteria** |
|-----------|--------------|---------------------|---------------------|
| **Phase 1** | Week 1-2 | Advanced aggregations, set operations | 75% API coverage |
| **Phase 2** | Week 3-4 | Missing functions | 85% API coverage |
| **Phase 3** | Week 5-6 | Session enhancements, DataFrame features | 90% API coverage |
| **Phase 4** | Week 7-8 | Comprehensive testing | 95% API coverage + tests |
| **Phase 5** | Week 9 | Performance optimization | Final optimization |

**Total Duration**: 9 weeks
**Target API Coverage**: 95%+ of PySpark 3.2 API
**Quality Target**: 95%+ test coverage, 100% compatibility

---

## Post-Implementation

### Release Strategy
- [ ] Version bump to 0.3.0 (major feature release)
- [ ] Comprehensive release notes
- [ ] Migration guide for users
- [ ] Performance benchmarks

### Maintenance
- [ ] Regular compatibility testing with PySpark updates
- [ ] Performance monitoring
- [ ] User feedback collection
- [ ] Continuous improvement

---

*This plan will be updated as implementation progresses and new requirements are discovered.*
