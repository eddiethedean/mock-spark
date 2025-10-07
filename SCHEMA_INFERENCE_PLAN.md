# Schema Inference Enhancement Plan

**Feature Branch**: `feature/schema-inference`  
**Target Version**: 1.1.0  
**Status**: Planning Phase  
**Created**: October 7, 2025  

---

## 🎯 Executive Summary

Enhance Mock Spark's schema inference capabilities to provide:
- Automatic type detection from Python data structures
- Smart type promotion and widening
- Complex nested type support (arrays, maps, structs)
- CSV/JSON schema inference
- Schema evolution and compatibility checking
- Improved null handling and type coercion

**Goal**: Match or exceed PySpark's schema inference capabilities while maintaining 10x performance advantage.

---

## 📊 Current State Analysis

### What We Have ✅

1. **Basic Schema Inference**
   - `MockDataFrame._infer_select_schema()` - Infers schema for select operations
   - `MockDataFrame._infer_join_schema()` - Infers schema for join operations
   - Type inference during materialization

2. **Type Promotion**
   - Arithmetic operations: `int * float → float`
   - SUM preserves source type: `sum(int) → int`
   - Division always returns float

3. **Type Casting**
   - CAST operations with string type names
   - COALESCE with automatic VARCHAR casting

### What's Missing ❌

1. **Automatic Type Detection**
   - No inference from Python dict/list data
   - Manual schema required for complex types
   - Limited null type handling

2. **Complex Type Support**
   - ArrayType inference incomplete
   - MapType not inferred
   - StructType nesting limited

3. **File Format Inference**
   - No CSV schema detection
   - No JSON schema inference
   - No Parquet metadata reading

4. **Schema Evolution**
   - No schema merging
   - No compatibility checking
   - No type widening rules

5. **Smart Defaults**
   - Integer vs Long ambiguity
   - Float vs Double ambiguity
   - String vs Binary decisions

---

## 🚀 Proposed Enhancements

### Phase 1: Automatic Type Detection (High Priority)

**Goal**: Automatically infer schema from Python data structures

#### 1.1 Basic Type Inference
```python
# Should work without explicit schema
data = [
    {"id": 1, "name": "Alice", "score": 95.5, "active": True},
    {"id": 2, "name": "Bob", "score": 87.0, "active": False},
]
df = spark.createDataFrame(data)  # Auto-infer schema

# Expected schema:
# id: LongType
# name: StringType
# score: DoubleType
# active: BooleanType
```

**Implementation**:
- Add `_infer_schema_from_data()` method in DataFrame
- Analyze first N rows to determine types
- Handle type conflicts across rows
- Default to most general type when conflicts exist

**Files to Modify**:
- `mock_spark/dataframe/dataframe.py`
- `mock_spark/session/core/session.py` (createDataFrame)

#### 1.2 Null Handling
```python
data = [
    {"id": 1, "value": 100},
    {"id": 2, "value": None},  # Null value
    {"id": None, "value": 200},  # Null in different column
]
df = spark.createDataFrame(data)

# Should infer:
# id: LongType (nullable=True)
# value: LongType (nullable=True)
```

**Implementation**:
- Track null occurrences per column
- Set nullable=True if any nulls found
- Don't let nulls determine type (use non-null values)

#### 1.3 Type Conflict Resolution
```python
data = [
    {"value": 100},      # Integer
    {"value": 95.5},     # Float
    {"value": "200"},    # String
]
df = spark.createDataFrame(data)

# Should infer: StringType (most general)
# Or raise informative error with type distribution
```

**Implementation**:
- Type hierarchy: String > Double > Long > Int > Boolean
- Numeric promotion: Int → Long → Double
- Conflicting types → String (with warning)

### Phase 2: Complex Type Inference (Medium Priority)

#### 2.1 ArrayType Detection
```python
data = [
    {"id": 1, "tags": ["python", "spark"]},
    {"id": 2, "tags": ["java", "scala", "spark"]},
]
df = spark.createDataFrame(data)

# Should infer:
# id: LongType
# tags: ArrayType(StringType)
```

**Implementation**:
- Detect Python lists in data
- Infer element type from list contents
- Handle mixed-type arrays
- Support nested arrays

**Files to Modify**:
- `mock_spark/dataframe/dataframe.py`
- `mock_spark/spark_types.py`

#### 2.2 MapType Detection
```python
data = [
    {"id": 1, "metadata": {"country": "US", "dept": "Engineering"}},
    {"id": 2, "metadata": {"country": "UK", "dept": "Sales"}},
]
df = spark.createDataFrame(data)

# Should infer:
# id: LongType
# metadata: MapType(StringType, StringType)
```

**Implementation**:
- Detect Python dicts in data
- Infer key and value types
- Handle heterogeneous maps
- Support nested maps

#### 2.3 StructType Nesting
```python
data = [
    {"id": 1, "address": {"street": "123 Main", "city": "NYC"}},
    {"id": 2, "address": {"street": "456 Oak", "city": "LA"}},
]
df = spark.createDataFrame(data)

# Should infer:
# id: LongType
# address: StructType([
#     StructField("street", StringType),
#     StructField("city", StringType)
# ])
```

**Implementation**:
- Recursively analyze nested dicts
- Build StructType hierarchy
- Handle optional fields across rows

### Phase 3: File Format Inference (Medium Priority)

#### 3.1 CSV Schema Detection
```python
# Automatically infer schema from CSV
df = spark.read.csv("data.csv", inferSchema=True, header=True)

# Features:
# - Detect numeric vs string columns
# - Handle headers
# - Sample N rows for type detection
# - Provide schema override options
```

**Implementation**:
- Sample first 100-1000 rows
- Analyze column value patterns
- Use regex for type detection (integers, floats, dates)
- Allow manual schema override

**Files to Modify**:
- `mock_spark/dataframe/reader.py`

#### 3.2 JSON Schema Inference
```python
df = spark.read.json("data.json", inferSchema=True)

# Features:
# - Nested structure detection
# - Array and map inference
# - Schema from multiple documents
```

**Implementation**:
- Parse JSON structure
- Build schema from nested objects
- Handle arrays and nested types
- Merge schemas from multiple documents

### Phase 4: Schema Evolution (Low Priority)

#### 4.1 Schema Merging
```python
schema1 = StructType([StructField("id", IntegerType()), StructField("name", StringType())])
schema2 = StructType([StructField("id", LongType()), StructField("age", IntegerType())])

merged = merge_schemas(schema1, schema2)
# Result: StructType([
#     StructField("id", LongType),      # Promoted to Long
#     StructField("name", StringType),  # From schema1
#     StructField("age", IntegerType)   # From schema2
# ])
```

**Implementation**:
- Create `SchemaInference` utility class
- Type promotion rules (Int → Long → Double)
- Field merging logic
- Compatibility validation

**New File**:
- `mock_spark/core/schema_inference.py`

#### 4.2 Compatibility Checking
```python
from mock_spark.core.schema_inference import check_schema_compatibility

old_schema = df1.schema
new_schema = df2.schema

compatible, issues = check_schema_compatibility(old_schema, new_schema)
if not compatible:
    print(f"Schema incompatible: {issues}")
```

**Features**:
- Breaking vs non-breaking changes
- Type narrowing detection
- Missing field detection
- Suggestion generation

---

## 🏗️ Implementation Approach

### Architecture

```
mock_spark/core/schema_inference.py (NEW)
├── SchemaInferenceEngine
│   ├── infer_from_data(data: List[Dict]) → MockStructType
│   ├── infer_from_sample(sample: List[Dict], sample_size: int) → MockStructType
│   ├── infer_field_type(values: List[Any]) → MockDataType
│   ├── resolve_type_conflicts(types: List[MockDataType]) → MockDataType
│   └── detect_complex_types(value: Any) → MockDataType
│
├── SchemaEvolution
│   ├── merge_schemas(schema1, schema2) → MockStructType
│   ├── check_compatibility(old, new) → Tuple[bool, List[str]]
│   ├── promote_type(type1, type2) → MockDataType
│   └── suggest_migration(old, new) → List[str]
│
└── TypeInferenceRules
    ├── TYPE_HIERARCHY: Dict[str, int]
    ├── PROMOTION_RULES: Dict[Tuple[type, type], type]
    └── COERCION_RULES: Dict[str, Callable]
```

### Type Detection Logic

```python
def infer_field_type(values: List[Any]) -> MockDataType:
    """Infer type from a list of values."""
    
    # Filter out None values
    non_null_values = [v for v in values if v is not None]
    
    if not non_null_values:
        return NullType()  # All nulls
    
    # Detect Python types
    types_seen = set()
    for value in non_null_values:
        if isinstance(value, bool):
            types_seen.add('boolean')
        elif isinstance(value, int):
            types_seen.add('long')
        elif isinstance(value, float):
            types_seen.add('double')
        elif isinstance(value, str):
            types_seen.add('string')
        elif isinstance(value, list):
            types_seen.add('array')
        elif isinstance(value, dict):
            types_seen.add('map_or_struct')
    
    # Resolve conflicts
    return resolve_type_conflicts(types_seen, non_null_values)
```

### Type Hierarchy

```python
TYPE_HIERARCHY = {
    'null': 0,
    'boolean': 1,
    'byte': 2,
    'short': 3,
    'integer': 4,
    'long': 5,
    'float': 6,
    'double': 7,
    'decimal': 8,
    'string': 9,  # Most general
}

NUMERIC_PROMOTION = {
    ('integer', 'long'): 'long',
    ('integer', 'double'): 'double',
    ('long', 'double'): 'double',
    ('float', 'double'): 'double',
}
```

---

## 🧪 Testing Strategy

### Test Cases

#### Basic Type Inference
```python
def test_infer_basic_types():
    """Test automatic inference of basic types."""
    spark = MockSparkSession("test")
    data = [
        {"int_col": 1, "float_col": 1.5, "str_col": "test", "bool_col": True}
    ]
    df = spark.createDataFrame(data)  # No schema provided
    
    assert df.schema.fields[0].dataType == LongType()
    assert df.schema.fields[1].dataType == DoubleType()
    assert df.schema.fields[2].dataType == StringType()
    assert df.schema.fields[3].dataType == BooleanType()
```

#### Null Handling
```python
def test_infer_with_nulls():
    """Test type inference with null values."""
    data = [
        {"id": 1, "value": 100},
        {"id": None, "value": None},
        {"id": 3, "value": 150},
    ]
    df = spark.createDataFrame(data)
    
    assert df.schema.fields[0].dataType == LongType()
    assert df.schema.fields[0].nullable == True
    assert df.schema.fields[1].dataType == LongType()
    assert df.schema.fields[1].nullable == True
```

#### Type Conflicts
```python
def test_infer_type_conflicts():
    """Test handling of type conflicts."""
    data = [
        {"value": 100},     # Integer
        {"value": 95.5},    # Float
    ]
    df = spark.createDataFrame(data)
    
    # Should promote to DoubleType
    assert df.schema.fields[0].dataType == DoubleType()
```

#### Complex Types
```python
def test_infer_array_type():
    """Test ArrayType inference."""
    data = [
        {"id": 1, "tags": ["a", "b", "c"]},
        {"id": 2, "tags": ["x", "y"]},
    ]
    df = spark.createDataFrame(data)
    
    assert isinstance(df.schema.fields[1].dataType, ArrayType)
    assert df.schema.fields[1].dataType.elementType == StringType()

def test_infer_map_type():
    """Test MapType inference."""
    data = [
        {"id": 1, "metadata": {"key1": "val1", "key2": "val2"}},
    ]
    df = spark.createDataFrame(data)
    
    assert isinstance(df.schema.fields[1].dataType, MapType)

def test_infer_struct_type():
    """Test nested StructType inference."""
    data = [
        {"id": 1, "address": {"street": "Main St", "city": "NYC"}},
    ]
    df = spark.createDataFrame(data)
    
    assert isinstance(df.schema.fields[1].dataType, StructType)
```

#### Schema Evolution
```python
def test_schema_merging():
    """Test schema merging for union operations."""
    schema1 = StructType([StructField("id", IntegerType())])
    schema2 = StructType([StructField("id", LongType())])
    
    merged = merge_schemas(schema1, schema2)
    assert merged.fields[0].dataType == LongType()

def test_schema_compatibility():
    """Test schema compatibility checking."""
    old_schema = StructType([StructField("id", LongType())])
    new_schema = StructType([StructField("id", IntegerType())])  # Narrowing
    
    compatible, issues = check_schema_compatibility(old_schema, new_schema)
    assert not compatible  # Type narrowing is breaking
    assert "Type narrowing" in issues[0]
```

---

## 📋 Implementation Plan

### Phase 1: Core Infrastructure (Week 1)

**Objective**: Build foundation for schema inference

**Tasks**:
1. Create `mock_spark/core/schema_inference.py`
2. Implement `SchemaInferenceEngine` class
3. Define type hierarchy and promotion rules
4. Add basic type detection from Python values
5. Unit tests for type detection

**Deliverables**:
- `SchemaInferenceEngine.infer_from_data()`
- `SchemaInferenceEngine.infer_field_type()`
- `TypeInferenceRules` with hierarchy and promotions
- 20+ unit tests

**Success Criteria**:
- ✅ Infer Int, Long, Double, String, Boolean from Python data
- ✅ Handle null values correctly
- ✅ Type promotion rules working
- ✅ All tests passing

### Phase 2: Complex Types (Week 2)

**Objective**: Support arrays, maps, and nested structs

**Tasks**:
1. Implement ArrayType inference
2. Implement MapType inference
3. Implement nested StructType inference
4. Handle heterogeneous collections
5. Add comprehensive tests

**Deliverables**:
- `detect_array_type()`
- `detect_map_type()`
- `detect_struct_type()`
- 30+ tests for complex types

**Success Criteria**:
- ✅ Arrays inferred with correct element types
- ✅ Maps inferred with key/value types
- ✅ Nested structs detected and built
- ✅ Compatibility tests pass

### Phase 3: Smart Inference (Week 3)

**Objective**: Intelligent type decisions and optimization

**Tasks**:
1. Sample-based inference (don't scan all rows)
2. Type conflict resolution with warnings
3. Configurable inference behavior
4. Performance optimization
5. Edge case handling

**Deliverables**:
- `infer_from_sample(sample_size=1000)`
- `resolve_type_conflicts()` with logging
- Configuration options for inference strictness
- Performance benchmarks

**Success Criteria**:
- ✅ Fast inference on large datasets
- ✅ Informative warnings for conflicts
- ✅ Configurable behavior
- ✅ No performance regression

### Phase 4: Schema Evolution (Week 4)

**Objective**: Schema merging and compatibility

**Tasks**:
1. Implement `SchemaEvolution` class
2. Schema merging for union operations
3. Compatibility checking
4. Migration suggestions
5. Documentation

**Deliverables**:
- `merge_schemas()`
- `check_compatibility()`
- `suggest_migration()`
- API documentation

**Success Criteria**:
- ✅ Schemas merge correctly
- ✅ Breaking changes detected
- ✅ Helpful migration suggestions
- ✅ Integration tests pass

### Phase 5: File Format Inference (Optional)

**Objective**: Infer schemas from CSV/JSON files

**Tasks**:
1. CSV header and type detection
2. JSON structure analysis
3. Sample-based inference for large files
4. Schema caching
5. Integration with reader

**Deliverables**:
- `spark.read.csv(..., inferSchema=True)`
- `spark.read.json(..., inferSchema=True)`
- Cached schema for repeated reads

**Success Criteria**:
- ✅ CSV schemas inferred correctly
- ✅ JSON schemas inferred correctly
- ✅ Performance acceptable
- ✅ Matches PySpark behavior

---

## 🎯 API Design

### New Public APIs

#### Automatic Inference (Phase 1)
```python
# Already exists, but enhanced
df = spark.createDataFrame(data)  # No schema → auto-infer

# New: Configure inference behavior
spark.conf.set("spark.sql.inferSchema.enabled", "true")  # default
spark.conf.set("spark.sql.inferSchema.sampleRatio", "1.0")  # scan all rows
spark.conf.set("spark.sql.inferSchema.maxRows", "1000")  # sample limit
```

#### Schema Utilities (Phase 4)
```python
from mock_spark.core.schema_inference import merge_schemas, check_schema_compatibility

# Merge two schemas
merged = merge_schemas(df1.schema, df2.schema)

# Check compatibility
compatible, issues = check_schema_compatibility(old_schema, new_schema)
if not compatible:
    for issue in issues:
        print(f"⚠️  {issue}")
```

#### File Inference (Phase 5)
```python
# CSV with schema inference
df = spark.read.option("inferSchema", "true").option("header", "true").csv("data.csv")

# JSON with schema inference
df = spark.read.option("inferSchema", "true").json("data.json")

# Inspect inferred schema
df.printSchema()
```

---

## 🔬 Technical Challenges

### Challenge 1: Type Ambiguity

**Problem**: Python int could be Int, Long, or even String
```python
{"value": 100}  # Int? Long? String?
```

**Solution**:
- Default to LongType for integers (matches PySpark)
- Provide configuration for Int vs Long preference
- Document behavior clearly

### Challenge 2: Mixed-Type Collections

**Problem**: Lists/dicts with heterogeneous types
```python
{"values": [1, "two", 3.0, True]}  # What type?
```

**Solution**:
- Promote to most general common type
- If no common type, use StringType
- Log warning about type coercion

### Challenge 3: Performance

**Problem**: Scanning all rows for type detection is slow

**Solution**:
- Sample-based inference (first N rows)
- Configurable sample size
- Cache inferred schemas
- Lazy validation (validate on write, not read)

### Challenge 4: Null Values

**Problem**: All-null columns have no type information
```python
{"value": None}  # What type?
```

**Solution**:
- Default to StringType for all-null columns
- Set nullable=True
- Allow explicit schema override
- Match PySpark behavior

---

## 📊 Success Metrics

### Functionality Metrics
- ✅ Infer 100% of basic types (Int, Long, Double, String, Boolean)
- ✅ Infer 90%+ of complex types (Array, Map, Struct)
- ✅ Handle nulls correctly in 100% of cases
- ✅ Type promotion matches PySpark in 95%+ of cases

### Performance Metrics
- ✅ Inference adds <10% overhead for small datasets (<1000 rows)
- ✅ Inference adds <5% overhead for large datasets (>10000 rows)
- ✅ Sample-based inference completes in <100ms

### Compatibility Metrics
- ✅ Pass 100% of existing tests (no regressions)
- ✅ Pass 20+ new schema inference tests
- ✅ Match PySpark behavior in 95%+ of cases
- ✅ Compatibility tests with real PySpark pass

### Code Quality Metrics
- ✅ MyPy type checking passes
- ✅ Black formatting applied
- ✅ Test coverage >80% for new code
- ✅ Documentation complete

---

## 🧪 Testing Plan

### Unit Tests (50+ tests)
- Basic type inference (10 tests)
- Null handling (5 tests)
- Type conflicts (8 tests)
- Array inference (7 tests)
- Map inference (5 tests)
- Struct inference (8 tests)
- Schema merging (7 tests)

### Integration Tests (20+ tests)
- End-to-end workflows
- Real data scenarios
- Performance benchmarks
- Error handling

### Compatibility Tests (15+ tests)
- Compare with PySpark inference
- Ensure matching behavior
- Edge case validation

---

## 📝 Documentation Plan

### New Documentation
1. **Schema Inference Guide** (`docs/guides/schema_inference.md`)
   - Overview of automatic inference
   - Type detection rules
   - Configuration options
   - Best practices
   
2. **API Reference Updates**
   - Document new SchemaInferenceEngine
   - Update createDataFrame() docs
   - Add schema evolution examples

3. **Migration Guide Updates**
   - Schema inference differences from PySpark
   - Configuration recommendations
   - Troubleshooting common issues

### Updated Documentation
- README.md - Add schema inference to features
- getting_started.md - Include inference examples
- examples/ - Add schema inference demos

---

## ⚠️ Risks & Mitigation

### Risk 1: Breaking Changes
**Risk**: New inference behavior breaks existing tests  
**Mitigation**: 
- Feature flag for opt-in
- Extensive compatibility testing
- Fallback to explicit schemas

### Risk 2: Performance Regression
**Risk**: Inference slows down DataFrame creation  
**Mitigation**:
- Sample-based inference
- Caching
- Benchmark suite
- Performance tests

### Risk 3: PySpark Incompatibility
**Risk**: Inference differs from PySpark  
**Mitigation**:
- Comprehensive compatibility tests
- Document differences
- Provide PySpark-match mode

---

## 📅 Timeline

### Week 1: Foundation
- Day 1-2: Design and architecture
- Day 3-4: Basic type inference implementation
- Day 5: Testing and refinement

### Week 2: Complex Types
- Day 1-2: ArrayType and MapType
- Day 3-4: StructType nesting
- Day 5: Testing and edge cases

### Week 3: Smart Features
- Day 1-2: Sample-based inference
- Day 3: Configuration and optimization
- Day 4-5: Performance testing

### Week 4: Evolution & Polish
- Day 1-2: Schema evolution features
- Day 3: Documentation
- Day 4-5: Final testing and release prep

**Target Release**: Version 1.1.0 (4 weeks)

---

## 🎯 Success Definition

### Minimum Viable Product (MVP)
- ✅ Automatic inference for basic types from dict data
- ✅ Null handling
- ✅ ArrayType detection
- ✅ Type promotion rules
- ✅ 100% test pass rate

### Full Feature Set
- ✅ All basic types inferred
- ✅ Complex types (Array, Map, Struct)
- ✅ Schema evolution utilities
- ✅ File format inference
- ✅ Configuration options
- ✅ Comprehensive documentation

### Quality Bar
- ✅ No performance regression
- ✅ 95%+ PySpark compatibility
- ✅ MyPy clean
- ✅ Black formatted
- ✅ 100% test coverage for new code

---

## 🤝 Collaboration

### Code Review Checkpoints
1. After Phase 1: Basic inference working
2. After Phase 2: Complex types implemented
3. After Phase 4: Schema evolution complete
4. Before merge: Full review

### Community Feedback
- Share design doc for feedback
- Alpha release for testing
- Gather user input on API design

---

## 📚 References

### PySpark Behavior
- [PySpark Schema Inference](https://spark.apache.org/docs/latest/sql-data-sources-json.html)
- [Type Coercion Rules](https://spark.apache.org/docs/latest/sql-ref-datatypes.html)
- [CSV Schema Inference](https://spark.apache.org/docs/latest/sql-data-sources-csv.html)

### Implementation Examples
- pandas DataFrame type detection
- Arrow schema inference
- DuckDB type detection

---

## 🎬 Getting Started

### Setup
```bash
# Already on feature branch
git branch  # Should show: feature/schema-inference

# Run tests to establish baseline
pytest tests/ -q

# Start implementation
# 1. Create mock_spark/core/schema_inference.py
# 2. Add basic type inference
# 3. Write tests
# 4. Iterate
```

### First Task
Create `mock_spark/core/schema_inference.py` with:
- `SchemaInferenceEngine` class
- `infer_from_data()` method
- Basic type detection for Int, Float, String, Boolean
- Unit tests

---

**Status**: ✅ Plan complete and ready for implementation  
**Next Step**: Begin Phase 1 implementation  
**Questions**: Review plan and provide feedback before starting

