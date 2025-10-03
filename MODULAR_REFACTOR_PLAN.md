# 🏗️ Modular Refactor Plan: Mock Spark

## 📋 Overview

This document outlines a comprehensive refactoring plan to transform the Mock Spark codebase into a more modular, testable, and maintainable architecture that adheres to the Single Responsibility Principle (SRP) and follows clean architecture patterns.

## 🎯 Goals

- **Modularity**: Break down monolithic modules into focused, single-purpose components
- **Testability**: Improve test coverage and make testing easier through dependency injection
- **Single Responsibility**: Each class/module should have one clear responsibility
- **Maintainability**: Reduce coupling and improve code organization
- **Extensibility**: Make it easier to add new features without affecting existing code
- **Performance**: Optimize for better performance through better separation of concerns
- **Clean Migration**: Delete old modules after successful refactoring
- **Test Integrity**: Ensure ALL tests pass after each refactoring step

## 📊 Current State Analysis

### Current Issues
1. **Monolithic Files**: Large files with multiple responsibilities (e.g., `dataframe.py` - 3000+ lines)
2. **Tight Coupling**: Classes directly depend on each other without abstractions
3. **Mixed Responsibilities**: Single classes handling multiple concerns
4. **Hard to Test**: Difficult to unit test individual components in isolation
5. **Code Duplication**: Similar logic scattered across different modules
6. **Poor Separation**: Business logic mixed with infrastructure concerns

### Current Architecture
```
mock_spark/
├── __init__.py              # Public API exports
├── session.py               # Session management + SQL + Catalog (500+ lines)
├── dataframe.py             # DataFrame + Writer + GroupedData + RDD (3000+ lines)
├── functions.py             # All functions + operations (1000+ lines)
├── spark_types.py           # Data types + schema (400+ lines)
├── storage.py               # Storage + Tables (400+ lines)
├── window.py                # Window functions (200+ lines)
├── errors.py                # Error definitions (200+ lines)
├── error_simulation.py      # Error simulation (200+ lines)
├── performance_simulation.py # Performance simulation (200+ lines)
└── data_generation.py       # Data generation (400+ lines)
```

## 🏛️ Target Architecture

### New Modular Structure
```
mock_spark/
├── __init__.py                    # Public API exports
├── core/                          # Core abstractions and interfaces
│   ├── __init__.py
│   ├── interfaces/                # Abstract interfaces
│   │   ├── __init__.py
│   │   ├── dataframe.py          # DataFrame interface
│   │   ├── session.py            # Session interface
│   │   ├── storage.py            # Storage interface
│   │   └── functions.py          # Functions interface
│   ├── exceptions/                # Exception hierarchy
│   │   ├── __init__.py
│   │   ├── base.py               # Base exceptions
│   │   ├── analysis.py           # Analysis exceptions
│   │   ├── execution.py          # Execution exceptions
│   │   └── validation.py         # Validation exceptions
│   └── types/                     # Core type definitions
│       ├── __init__.py
│       ├── schema.py             # Schema types
│       ├── data_types.py         # Data type definitions
│       └── metadata.py           # Metadata types
├── session/                       # Session management
│   ├── __init__.py
│   ├── session.py                # MockSparkSession implementation
│   ├── context.py                # SparkContext implementation
│   ├── catalog.py                # Catalog operations
│   ├── sql/                      # SQL processing
│   │   ├── __init__.py
│   │   ├── parser.py             # SQL parsing
│   │   ├── executor.py           # SQL execution
│   │   └── optimizer.py          # Query optimization
│   └── config.py                 # Configuration management
├── dataframe/                     # DataFrame operations
│   ├── __init__.py
│   ├── dataframe.py              # Core DataFrame implementation
│   ├── writer.py                 # DataFrameWriter
│   ├── reader.py                 # DataFrameReader
│   ├── operations/               # DataFrame operations
│   │   ├── __init__.py
│   │   ├── selection.py          # Select operations
│   │   ├── filtering.py          # Filter operations
│   │   ├── grouping.py           # GroupBy operations
│   │   ├── joining.py            # Join operations
│   │   ├── ordering.py           # OrderBy operations
│   │   └── aggregation.py        # Aggregation operations
│   ├── rdd/                      # RDD implementation
│   │   ├── __init__.py
│   │   ├── rdd.py                # RDD core
│   │   ├── transformations.py    # RDD transformations
│   │   └── actions.py            # RDD actions
│   └── evaluation/               # Expression evaluation
│       ├── __init__.py
│       ├── evaluator.py          # Expression evaluator
│       ├── column_evaluator.py   # Column expression evaluator
│       └── function_evaluator.py # Function expression evaluator
├── functions/                     # Function implementations
│   ├── __init__.py
│   ├── base.py                   # Base function classes
│   ├── column_functions.py       # Column functions
│   ├── aggregate_functions.py    # Aggregate functions
│   ├── window_functions.py       # Window functions
│   ├── string_functions.py       # String functions
│   ├── math_functions.py         # Mathematical functions
│   ├── datetime_functions.py     # Date/time functions
│   ├── conditional_functions.py  # Conditional functions
│   └── case_when.py              # CASE WHEN implementation
├── storage/                       # Storage layer
│   ├── __init__.py
│   ├── manager.py                # Storage manager
│   ├── table.py                  # Table implementation
│   ├── schema.py                 # Schema management
│   ├── backends/                 # Storage backends
│   │   ├── __init__.py
│   │   ├── memory.py             # In-memory storage
│   │   ├── sqlite.py             # SQLite backend
│   │   └── file.py               # File-based storage
│   └── serialization/            # Data serialization
│       ├── __init__.py
│       ├── json.py               # JSON serialization
│       ├── parquet.py            # Parquet serialization
│       └── csv.py                # CSV serialization
├── window/                        # Window functions
│   ├── __init__.py
│   ├── window.py                 # Window specification
│   ├── functions.py              # Window function implementations
│   └── partitioning.py           # Partitioning logic
├── testing/                       # Testing utilities
│   ├── __init__.py
│   ├── fixtures.py               # Test fixtures
│   ├── generators.py             # Data generators
│   ├── simulators.py             # Error/performance simulators
│   └── mocks.py                  # Mock utilities
└── utils/                         # Utility functions
    ├── __init__.py
    ├── validation.py             # Input validation
    ├── type_utils.py             # Type utilities
    ├── string_utils.py           # String utilities
    └── math_utils.py             # Mathematical utilities
```

## 📋 Refactoring Phases

### Phase 1: Core Foundation (Week 1-2)
**Goal**: Establish core abstractions and interfaces

#### Tasks:
1. **Pre-Refactoring Validation**
   - [ ] Run ALL tests to establish baseline (compatibility + unit tests)
   - [ ] Document current test results and coverage
   - [ ] Create backup branch for rollback capability

2. **Create Core Interfaces**
   - [ ] Define `IDataFrame` interface
   - [ ] Define `ISession` interface
   - [ ] Define `IStorage` interface
   - [ ] Define `IFunction` interface
   - [ ] Create base exception hierarchy

3. **Extract Core Types**
   - [ ] Move schema types to `core/types/schema.py`
   - [ ] Move data types to `core/types/data_types.py`
   - [ ] Create metadata types in `core/types/metadata.py`

4. **Create Exception Hierarchy**
   - [ ] Refactor `errors.py` into focused exception modules
   - [ ] Create base exception classes
   - [ ] Organize exceptions by category

5. **Setup Directory Structure**
   - [ ] Create new directory structure
   - [ ] Move files to appropriate locations
   - [ ] Update imports throughout codebase

6. **Validation and Cleanup**
   - [ ] Run ALL tests to ensure no regressions
   - [ ] Fix any test failures immediately
   - [ ] Update imports in test files
   - [ ] Verify 100% test pass rate

**Deliverables**:
- Core interfaces defined
- Exception hierarchy established
- Basic directory structure created
- ALL tests passing (192 compatibility + 77 unit tests)
- No regressions introduced

---

### Phase 2: Session Layer Refactoring (Week 2-3)
**Goal**: Modularize session management and SQL processing

#### Tasks:
1. **Extract Session Components**
   - [ ] Move `MockSparkSession` to `session/session.py`
   - [ ] Extract `MockSparkContext` to `session/context.py`
   - [ ] Extract catalog operations to `session/catalog.py`
   - [ ] Create configuration management in `session/config.py`

2. **Create SQL Processing Module**
   - [ ] Extract SQL parsing logic to `session/sql/parser.py`
   - [ ] Create SQL executor in `session/sql/executor.py`
   - [ ] Add query optimizer in `session/sql/optimizer.py`

3. **Implement Dependency Injection**
   - [ ] Create session factory
   - [ ] Implement dependency injection container
   - [ ] Update session initialization

4. **Add Session Tests**
   - [ ] Create unit tests for session components
   - [ ] Add integration tests for SQL processing
   - [ ] Test session lifecycle management

**Deliverables**:
- Session layer fully modularized
- SQL processing separated from session
- Dependency injection implemented
- Comprehensive session tests

---

### Phase 3: DataFrame Layer Refactoring (Week 3-5)
**Goal**: Break down the monolithic DataFrame implementation

#### Tasks:
1. **Extract DataFrame Operations**
   - [ ] Move selection logic to `dataframe/operations/selection.py`
   - [ ] Extract filtering to `dataframe/operations/filtering.py`
   - [ ] Move grouping to `dataframe/operations/grouping.py`
   - [ ] Extract joining to `dataframe/operations/joining.py`
   - [ ] Move ordering to `dataframe/operations/ordering.py`
   - [ ] Extract aggregation to `dataframe/operations/aggregation.py`

2. **Create Expression Evaluation System**
   - [ ] Extract evaluator to `dataframe/evaluation/evaluator.py`
   - [ ] Create column evaluator in `dataframe/evaluation/column_evaluator.py`
   - [ ] Move function evaluator to `dataframe/evaluation/function_evaluator.py`

3. **Refactor RDD Implementation**
   - [ ] Move RDD to `dataframe/rdd/rdd.py`
   - [ ] Extract transformations to `dataframe/rdd/transformations.py`
   - [ ] Move actions to `dataframe/rdd/actions.py`

4. **Separate Reader/Writer**
   - [ ] Extract `DataFrameWriter` to `dataframe/writer.py`
   - [ ] Create `DataFrameReader` in `dataframe/reader.py`
   - [ ] Move storage integration to appropriate modules

5. **Add DataFrame Tests**
   - [ ] Create unit tests for each operation
   - [ ] Add integration tests for complex operations
   - [ ] Test expression evaluation thoroughly

**Deliverables**:
- DataFrame operations fully modularized
- Expression evaluation system created
- RDD implementation separated
- Comprehensive DataFrame tests

---

### Phase 4: Functions Layer Refactoring (Week 5-6)
**Goal**: Organize functions by category and responsibility

#### Tasks:
1. **Categorize Functions**
   - [ ] Move column functions to `functions/column_functions.py`
   - [ ] Extract aggregate functions to `functions/aggregate_functions.py`
   - [ ] Move window functions to `functions/window_functions.py`
   - [ ] Extract string functions to `functions/string_functions.py`
   - [ ] Move math functions to `functions/math_functions.py`
   - [ ] Extract datetime functions to `functions/datetime_functions.py`
   - [ ] Move conditional functions to `functions/conditional_functions.py`

2. **Create Function Base Classes**
   - [ ] Define `IFunction` interface
   - [ ] Create `BaseFunction` abstract class
   - [ ] Implement `ColumnFunction` base class
   - [ ] Create `AggregateFunction` base class

3. **Implement Function Registry**
   - [ ] Create function registry system
   - [ ] Implement function discovery
   - [ ] Add function validation

4. **Refactor CASE WHEN**
   - [ ] Extract CASE WHEN to `functions/case_when.py`
   - [ ] Create proper evaluation logic
   - [ ] Add comprehensive tests

5. **Add Function Tests**
   - [ ] Create unit tests for each function category
   - [ ] Add integration tests for function combinations
   - [ ] Test function registry system

**Deliverables**:
- Functions organized by category
- Function base classes implemented
- Function registry system created
- Comprehensive function tests

---

### Phase 5: Storage Layer Refactoring (Week 6-7)
**Goal**: Create pluggable storage architecture

#### Tasks:
1. **Create Storage Interfaces**
   - [ ] Define `IStorageManager` interface
   - [ ] Create `ITable` interface
   - [ ] Define `ISchema` interface

2. **Implement Storage Backends**
   - [ ] Refactor memory storage to `storage/backends/memory.py`
   - [ ] Create SQLite backend in `storage/backends/sqlite.py`
   - [ ] Implement file-based storage in `storage/backends/file.py`

3. **Create Serialization Layer**
   - [ ] Implement JSON serialization in `storage/serialization/json.py`
   - [ ] Create Parquet serialization in `storage/serialization/parquet.py`
   - [ ] Add CSV serialization in `storage/serialization/csv.py`

4. **Refactor Storage Manager**
   - [ ] Move storage manager to `storage/manager.py`
   - [ ] Extract table implementation to `storage/table.py`
   - [ ] Create schema management in `storage/schema.py`

5. **Add Storage Tests**
   - [ ] Create unit tests for each backend
   - [ ] Add integration tests for storage operations
   - [ ] Test serialization/deserialization

**Deliverables**:
- Pluggable storage architecture
- Multiple storage backends implemented
- Serialization layer created
- Comprehensive storage tests

---

### Phase 6: Testing Infrastructure (Week 7-8)
**Goal**: Create comprehensive testing infrastructure

#### Tasks:
1. **Create Testing Utilities**
   - [ ] Move fixtures to `testing/fixtures.py`
   - [ ] Extract generators to `testing/generators.py`
   - [ ] Move simulators to `testing/simulators.py`
   - [ ] Create mock utilities in `testing/mocks.py`

2. **Implement Test Factories**
   - [ ] Create DataFrame test factory
   - [ ] Implement session test factory
   - [ ] Add function test factory

3. **Create Test Data Builders**
   - [ ] Implement builder pattern for test data
   - [ ] Create realistic data generators
   - [ ] Add edge case data generators

4. **Add Performance Testing**
   - [ ] Create performance test framework
   - [ ] Add memory usage testing
   - [ ] Implement benchmark utilities

5. **Update Test Suite**
   - [ ] Refactor existing tests to use new structure
   - [ ] Add missing unit tests
   - [ ] Improve test coverage

**Deliverables**:
- Comprehensive testing infrastructure
- Test factories and builders
- Performance testing framework
- Updated test suite

---

### Phase 7: Utilities and Cleanup (Week 8-9)
**Goal**: Extract utilities and clean up the codebase

#### Tasks:
1. **Create Utility Modules**
   - [ ] Extract validation to `utils/validation.py`
   - [ ] Move type utilities to `utils/type_utils.py`
   - [ ] Extract string utilities to `utils/string_utils.py`
   - [ ] Create math utilities in `utils/math_utils.py`

2. **Refactor Window Functions**
   - [ ] Move window functions to `window/window.py`
   - [ ] Extract partitioning logic to `window/partitioning.py`
   - [ ] Create window function implementations in `window/functions.py`

3. **Update Public API**
   - [ ] Refactor `__init__.py` to use new structure
   - [ ] Update imports throughout codebase
   - [ ] Ensure backward compatibility

4. **Code Quality Improvements**
   - [ ] Run code quality tools (mypy, black, flake8)
   - [ ] Fix any type errors
   - [ ] Improve documentation
   - [ ] Add docstrings to all public APIs

5. **Final Testing**
   - [ ] Run full test suite
   - [ ] Perform integration testing
   - [ ] Test backward compatibility

**Deliverables**:
- Utility modules extracted
- Window functions refactored
- Public API updated
- Code quality improved
- Full test suite passing

---

## 🧪 Testing Strategy

### Test Categories
1. **Unit Tests**: Test individual components in isolation
2. **Integration Tests**: Test component interactions
3. **Compatibility Tests**: Ensure PySpark compatibility
4. **Performance Tests**: Measure performance characteristics
5. **Regression Tests**: Prevent breaking changes

### Test Organization
```
tests/
├── unit/                          # Unit tests
│   ├── core/                      # Core component tests
│   ├── session/                   # Session layer tests
│   ├── dataframe/                 # DataFrame tests
│   ├── functions/                 # Function tests
│   ├── storage/                   # Storage tests
│   └── utils/                     # Utility tests
├── integration/                   # Integration tests
│   ├── session_dataframe/         # Session-DataFrame integration
│   ├── storage_functions/         # Storage-Functions integration
│   └── end_to_end/                # End-to-end tests
├── compatibility/                 # PySpark compatibility tests
└── performance/                   # Performance tests
```

## 📊 Success Metrics

### Code Quality Metrics
- **Cyclomatic Complexity**: < 10 per function
- **Lines of Code**: < 200 per file
- **Test Coverage**: > 95%
- **Type Coverage**: 100% (mypy compliance)
- **Code Duplication**: < 5%

### Architecture Metrics
- **Coupling**: Low coupling between modules
- **Cohesion**: High cohesion within modules
- **Dependency Depth**: < 3 levels
- **Interface Segregation**: Small, focused interfaces

### Performance Metrics
- **Test Execution Time**: < 2 minutes for full suite
- **Memory Usage**: < 100MB for typical operations
- **Startup Time**: < 1 second for session creation

## 🚀 Implementation Guidelines

### Coding Standards
1. **Single Responsibility**: Each class/module has one clear purpose
2. **Dependency Injection**: Use interfaces and dependency injection
3. **Interface Segregation**: Small, focused interfaces
4. **Open/Closed Principle**: Open for extension, closed for modification
5. **Don't Repeat Yourself**: Eliminate code duplication

### Migration Strategy
1. **Incremental Refactoring**: Refactor one module at a time
2. **Backward Compatibility**: Maintain existing API during transition
3. **Feature Flags**: Use feature flags for new functionality
4. **Gradual Migration**: Migrate tests and consumers gradually
5. **Rollback Plan**: Maintain ability to rollback changes

### **CRITICAL WORKFLOW RULES**
1. **Test-First Approach**: Run ALL tests before starting any refactoring
2. **Incremental Testing**: Run tests after each major change
3. **Module Cleanup**: Delete old modules ONLY after new modules are fully tested
4. **Zero Tolerance**: NO test failures allowed at any point
5. **Rollback Ready**: Maintain ability to revert if tests fail

### Quality Gates
1. **All Tests Pass**: No test failures allowed - ZERO TOLERANCE
2. **Type Safety**: 100% mypy compliance
3. **Code Coverage**: Maintain > 95% coverage
4. **Performance**: No performance regressions
5. **Documentation**: All public APIs documented
6. **Clean Codebase**: Old modules deleted after successful migration

## 📅 Timeline Summary

| Phase | Duration | Focus | Key Deliverables |
|-------|----------|-------|------------------|
| 1 | 2 weeks | Core Foundation | Interfaces, types, exceptions |
| 2 | 1 week | Session Layer | Session, SQL, catalog modularization |
| 3 | 2 weeks | DataFrame Layer | Operations, evaluation, RDD separation |
| 4 | 1 week | Functions Layer | Function categorization, registry |
| 5 | 1 week | Storage Layer | Pluggable storage, serialization |
| 6 | 1 week | Testing Infrastructure | Test utilities, factories, coverage |
| 7 | 1 week | Utilities & Cleanup | Final refactoring, API updates |

**Total Duration**: 9 weeks

## 🎯 Expected Benefits

### For Developers
- **Easier Testing**: Isolated components are easier to test
- **Better Maintainability**: Clear separation of concerns
- **Improved Readability**: Smaller, focused files
- **Enhanced Extensibility**: Easy to add new features

### For Users
- **Better Performance**: Optimized architecture
- **Improved Reliability**: Better tested components
- **Enhanced Features**: Easier to add new functionality
- **Maintained Compatibility**: No breaking changes

### For the Project
- **Reduced Technical Debt**: Clean, well-organized code
- **Improved Code Quality**: Better adherence to SOLID principles
- **Enhanced Testability**: Comprehensive test coverage
- **Future-Proof Architecture**: Easy to extend and modify

## 🔄 Risk Mitigation

### Technical Risks
- **Breaking Changes**: Maintain backward compatibility
- **Performance Regression**: Continuous performance monitoring
- **Test Failures**: Comprehensive test suite before refactoring
- **Integration Issues**: Incremental refactoring approach

### Process Risks
- **Timeline Delays**: Buffer time in each phase
- **Resource Constraints**: Prioritize critical components
- **Knowledge Transfer**: Document all changes thoroughly
- **Rollback Complexity**: Maintain rollback capabilities

## 📚 Documentation Plan

### Technical Documentation
- [ ] Architecture Decision Records (ADRs)
- [ ] API documentation updates
- [ ] Migration guides for each phase
- [ ] Performance benchmarks

### User Documentation
- [ ] Updated README with new structure
- [ ] Migration guide for existing users
- [ ] Examples using new architecture
- [ ] Troubleshooting guide

---

**Ready to revolutionize Mock Spark's architecture! 🚀**

This refactor plan will transform Mock Spark into a highly modular, testable, and maintainable codebase that follows clean architecture principles while maintaining full PySpark compatibility.
