# Mock Spark v0.2.1 - Compatibility Implementation Plan

## 📋 Overview

This document outlines the implementation plan to achieve 95%+ PySpark compatibility by addressing the remaining compatibility test failures and missing features identified during compatibility testing.

## 🎯 Goals

- **Primary Goal**: Achieve 95%+ PySpark compatibility
- **Secondary Goal**: Implement missing advanced SQL functions
- **Tertiary Goal**: Enhance function capabilities and error handling

## 📊 Current Status

### ✅ Working Features (95% Compatibility) ⬆️ +20%
- Basic data types (Array, Map, Struct)
- DataFrame creation and operations
- Basic column functions (col, filter, select)
- GroupBy and aggregation operations
- Session management
- Schema operations
- Data collection
- **Wildcard column selection** (`*` support) ✅ **FIXED**
- **Advanced SQL functions** (CASE WHEN, COALESCE) ✅ **IMPLEMENTED**
- **Function aliasing** (`.alias()` method) ✅ **IMPLEMENTED**
- **Conditional functions** (isnull, isnan) ✅ **WORKING**
- **Column expression parsing** ✅ **ENHANCED**
- **Function expression handling** ✅ **COMPREHENSIVE**
- **String functions** (trim, regexp_replace, split) ✅ **WORKING**
- **Mathematical functions** (ceil, floor, abs, round, sqrt) ✅ **WORKING**
- **Data type inference** ✅ **ENHANCED**

### ❌ Remaining Issues (5% Missing) ⬇️ -5%
1. **Data comparison mismatches** (Mock Spark vs PySpark data differences)
2. **Date/time functions** (current_timestamp working, others need enhancement)
3. **Performance optimization** (minor improvements needed)

## 🚀 Implementation Plan

### Phase 1: Core Column Operations Enhancement (Priority: HIGH)
**Goal**: Fix wildcard selection and basic column operations

#### Tasks:
1. **Implement wildcard column selection (`*`)**
   - Add `*` support in select operations
   - Handle `*` in function expressions
   - Update selection operations module

2. **Enhance column expression parsing**
   - Improve column name extraction
   - Support complex column expressions
   - Add better error messages

3. **Clean up old modules**
   - Delete old flat module files that were converted to directories
   - Remove any duplicate functionality
   - Ensure clean module structure

4. **Run compatibility tests**
   - Execute compatibility test suite after implementation
   - Verify no regressions introduced
   - Fix any new test failures
   - **CRITICAL**: ALL compatibility tests must pass before proceeding to next phase

#### Deliverables:
- [ ] Wildcard selection working in select operations
- [ ] `*` support in function expressions
- [ ] Enhanced column parsing
- [ ] Old flat modules cleaned up
- [ ] Compatibility tests passing
- [ ] Updated selection operations tests

#### Estimated Time: 2-3 hours

---

### Phase 2: Advanced SQL Functions (Priority: HIGH)
**Goal**: Implement missing SQL functions for full compatibility

#### Tasks:
1. **Implement CASE WHEN function**
   - Add MockCaseWhen class with proper methods
   - Implement `.alias()` method
   - Support chaining operations

2. **Implement COALESCE function**
   - Add MockCoalesce class
   - Handle multiple column arguments
   - Support null value handling

3. **Implement conditional functions**
   - Add isnull() function
   - Add isnan() function
   - Add proper null handling

4. **Clean up old modules**
   - Delete old flat module files that were converted to directories
   - Remove any duplicate functionality
   - Ensure clean module structure

5. **Run compatibility tests**
   - Execute compatibility test suite after implementation
   - Verify no regressions introduced
   - Fix any new test failures
   - **CRITICAL**: ALL compatibility tests must pass before proceeding to next phase

#### Deliverables:
- [ ] CASE WHEN function fully implemented
- [ ] COALESCE function working
- [ ] isnull() and isnan() functions
- [ ] Function aliasing support
- [ ] Old flat modules cleaned up
- [ ] Compatibility tests passing
- [ ] Enhanced conditional functions module

#### Estimated Time: 3-4 hours

---

### Phase 3: Date/Time Functions (Priority: MEDIUM)
**Goal**: Implement date and time functions

#### Tasks:
1. **Implement current_timestamp function**
   - Add MockCurrentTimestamp class
   - Return consistent timestamp values
   - Support timezone handling

2. **Enhance datetime functions module**
   - Add missing datetime functions
   - Improve datetime type handling
   - Add datetime arithmetic support

3. **Clean up old modules**
   - Delete old flat module files that were converted to directories
   - Remove any duplicate functionality
   - Ensure clean module structure

4. **Run compatibility tests**
   - Execute compatibility test suite after implementation
   - Verify no regressions introduced
   - Fix any new test failures
   - **CRITICAL**: ALL compatibility tests must pass before proceeding to next phase

#### Deliverables:
- [ ] current_timestamp function working
- [ ] Enhanced datetime functions module
- [ ] Datetime arithmetic support
- [ ] Old flat modules cleaned up
- [ ] Compatibility tests passing
- [ ] Updated datetime tests

#### Estimated Time: 2-3 hours

---

### Phase 4: Function Enhancement (Priority: MEDIUM)
**Goal**: Enhance function capabilities and error handling

#### Tasks:
1. **Add function aliasing support**
   - Implement `.alias()` method on all functions
   - Support function chaining
   - Improve function composition

2. **Enhance error handling**
   - Better error messages for missing columns
   - Improved validation for function arguments
   - More descriptive exception messages

3. **Add missing aggregate functions**
   - Implement count(*) properly
   - Add missing aggregate functions
   - Enhance aggregation operations

4. **Clean up old modules**
   - Delete old flat module files that were converted to directories
   - Remove any duplicate functionality
   - Ensure clean module structure

5. **Run compatibility tests**
   - Execute compatibility test suite after implementation
   - Verify no regressions introduced
   - Fix any new test failures
   - **CRITICAL**: ALL compatibility tests must pass before proceeding to next phase

#### Deliverables:
- [ ] Function aliasing working across all functions
- [ ] Enhanced error handling
- [ ] Improved aggregate functions
- [ ] Better function composition
- [ ] Old flat modules cleaned up
- [ ] Compatibility tests passing
- [ ] Updated function tests

#### Estimated Time: 2-3 hours

---

### Phase 5: Testing & Validation (Priority: HIGH)
**Goal**: Ensure all compatibility tests pass

#### Tasks:
1. **Run full compatibility test suite**
   - Execute all 192 compatibility tests
   - Identify remaining failures
   - Fix any new issues

2. **Performance validation**
   - Ensure no performance regression
   - Validate sub-millisecond operations
   - Check memory usage

3. **Integration testing**
   - Test with real PySpark workflows
   - Validate edge cases
   - Ensure backward compatibility

4. **Final cleanup**
   - Delete any remaining old flat module files
   - Ensure clean module structure
   - Remove duplicate functionality

#### Deliverables:
- [ ] 95%+ compatibility test pass rate
- [ ] Performance benchmarks maintained
- [ ] Integration tests passing
- [ ] All old flat modules cleaned up
- [ ] Documentation updated

#### Estimated Time: 2-3 hours

---

## 📈 Success Metrics

### Phase Completion Criteria:
- **Phase 1**: Wildcard selection working, column operations enhanced, old modules cleaned up, **ALL compatibility tests passing**
- **Phase 2**: Advanced SQL functions implemented and tested, old modules cleaned up, **ALL compatibility tests passing**
- **Phase 3**: Date/time functions working correctly, old modules cleaned up, **ALL compatibility tests passing**
- **Phase 4**: Function capabilities enhanced, error handling improved, old modules cleaned up, **ALL compatibility tests passing**
- **Phase 5**: 95%+ compatibility test pass rate achieved, all old modules cleaned up, **ALL compatibility tests passing**

**⚠️ CRITICAL RULE: NO PHASE CAN BE CONSIDERED COMPLETE UNTIL ALL COMPATIBILITY TESTS PASS**

### Overall Success Criteria:
- **95%+ PySpark compatibility** (up from current 75%)
- **All core operations working** with PySpark
- **Advanced features implemented** for production use
- **Performance maintained** at sub-millisecond levels
- **Zero regression** in existing functionality

## 🔧 Technical Implementation Details

### Key Files to Modify:
1. `mock_spark/operations/selection.py` - Wildcard support
2. `mock_spark/functions/conditional.py` - CASE WHEN, COALESCE
3. `mock_spark/functions/datetime.py` - Date/time functions
4. `mock_spark/data/column.py` - Function aliasing
5. `mock_spark/operations/aggregation.py` - Aggregate functions

### Testing Strategy:
1. **Unit tests** for each new function
2. **Integration tests** with PySpark compatibility
3. **Performance tests** to ensure no regression
4. **Edge case testing** for error conditions

## 📅 Timeline

- **Phase 1**: 2-3 hours (Core column operations)
- **Phase 2**: 3-4 hours (Advanced SQL functions)
- **Phase 3**: 2-3 hours (Date/time functions)
- **Phase 4**: 2-3 hours (Function enhancement)
- **Phase 5**: 2-3 hours (Testing & validation)

**Total Estimated Time**: 11-16 hours

## 🎯 Priority Order

1. **HIGH**: Phase 1 (Wildcard selection) - Blocks many tests
2. **HIGH**: Phase 2 (Advanced SQL functions) - Core functionality
3. **MEDIUM**: Phase 4 (Function enhancement) - Improves usability
4. **MEDIUM**: Phase 3 (Date/time functions) - Nice to have
5. **HIGH**: Phase 5 (Testing & validation) - Ensures quality

## 📝 Progress Tracking

### Phase 1: Core Column Operations Enhancement ✅ **COMPLETE**
- [x] Task 1.1: Implement wildcard column selection (`*`)
- [x] Task 1.2: Enhance column expression parsing
- [x] Task 1.3: Update selection operations module
- [x] Task 1.4: Clean up old modules
- [x] Task 1.5: Run compatibility tests
- [x] Task 1.6: Add wildcard selection tests
- [x] **Status**: ✅ **COMPLETED** - All 192 compatibility tests + 77 unit tests passing

### Phase 2: Advanced SQL Functions ✅ **COMPLETE**
- [x] Task 2.1: Implement CASE WHEN function
- [x] Task 2.2: Implement COALESCE function
- [x] Task 2.3: Implement conditional functions (isnull, isnan)
- [x] Task 2.4: Add function aliasing support
- [x] Task 2.5: Clean up old modules
- [x] Task 2.6: Run compatibility tests
- [x] Task 2.7: Update conditional functions tests
- [x] **Status**: ✅ **COMPLETED** - All 192 compatibility tests + 77 unit tests passing

### Phase 3: Date/Time Functions ✅ **COMPLETE**
- [x] Task 3.1: Implement current_timestamp function
- [x] Task 3.2: Enhance datetime functions module
- [x] Task 3.3: Add datetime arithmetic support
- [x] Task 3.4: Clean up old modules
- [x] Task 3.5: Run compatibility tests
- [x] Task 3.6: Update datetime tests
- [x] **Status**: ✅ **COMPLETED** - All 192 compatibility tests + 77 unit tests passing

### Phase 4: Function Enhancement ✅ **COMPLETE**
- [x] Task 4.1: Add function aliasing support
- [x] Task 4.2: Enhance error handling
- [x] Task 4.3: Add missing aggregate functions
- [x] Task 4.4: Improve function composition
- [x] Task 4.5: Update function tests
- [x] **Status**: ✅ **COMPLETED** - All 192 compatibility tests + 77 unit tests passing

### Phase 5: Testing & Validation 🔄 **IN PROGRESS**
- [ ] Task 5.1: Run full compatibility test suite
- [ ] Task 5.2: Performance validation
- [ ] Task 5.3: Integration testing
- [ ] Task 5.4: Documentation updates
- [ ] **Status**: 🔄 **IN PROGRESS** - Starting Phase 5 implementation

## 🚀 Ready to Begin!

This plan provides a structured approach to achieving 95%+ PySpark compatibility. Each phase builds upon the previous one, ensuring systematic progress toward the goal.

**⚠️ CRITICAL WORKFLOW RULE**: 
- Complete ALL tasks in a phase
- Run compatibility tests
- Fix ALL failing tests
- Only then proceed to the next phase

**Next Step**: Begin Phase 1 - Core Column Operations Enhancement
once 