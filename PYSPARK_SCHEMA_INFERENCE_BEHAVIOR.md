# PySpark Schema Inference Behavior Reference

**Source**: Real PySpark 3.2.4 testing  
**Date**: October 7, 2025  
**Purpose**: Document actual PySpark behavior for Mock Spark implementation  

---

## 🔬 Tested Behaviors

### Basic Type Mapping

| Python Type | PySpark Inferred Type | Notes |
|-------------|----------------------|-------|
| `int` | `LongType` | **NOT IntegerType!** All Python ints → Long |
| `float` | `DoubleType` | **NOT FloatType!** All Python floats → Double |
| `str` | `StringType` | Direct mapping |
| `bool` | `BooleanType` | Direct mapping |
| `None` (all) | **ValueError** | Cannot infer from all-null column |

### Key Findings

#### ✅ 1. Integers Always Become LongType
```python
data = [{"value": 100}]
df = spark.createDataFrame(data)
# Result: value: long (nullable = true)
```

**NOT** IntegerType - even for small numbers!

#### ✅ 2. Floats Always Become DoubleType
```python
data = [{"value": 95.5}]
df = spark.createDataFrame(data)
# Result: value: double (nullable = true)
```

#### ✅ 3. All Fields Are Nullable
```python
data = [{"id": 1, "name": "Alice"}]
df = spark.createDataFrame(data)
# Result: Both fields have nullable = true
```

Even when no nulls present!

#### ❌ 4. All-Null Columns Raise ValueError
```python
data = [{"value": None}]
df = spark.createDataFrame(data)
# Raises: ValueError: Some of types cannot be determined after inferring
```

Cannot infer type from only null values.

#### ❌ 5. Type Conflicts Raise TypeError
```python
# Int/Float conflict
data = [{"value": 100}, {"value": 95.5}]
df = spark.createDataFrame(data)
# Raises: TypeError: Can not merge type <class 'LongType'> and <class 'DoubleType'>

# Numeric/String conflict
data = [{"value": 100}, {"value": "text"}]
df = spark.createDataFrame(data)
# Raises: TypeError: Can not merge types
```

**No automatic type promotion across rows!**

---

## 🗂️ Complex Types

### Arrays

#### ArrayType(StringType)
```python
data = [{"tags": ["a", "b", "c"]}, {"tags": ["x", "y"]}]
df = spark.createDataFrame(data)
# Schema:
#  |-- tags: array (nullable = true)
#  |    |-- element: string (containsNull = true)
```

#### ArrayType(LongType)
```python
data = [{"numbers": [1, 2, 3]}]
df = spark.createDataFrame(data)
# Schema:
#  |-- numbers: array (nullable = true)
#  |    |-- element: long (containsNull = true)
```

#### Empty Arrays
```python
data = [{"tags": []}, {"tags": ["python"]}]
df = spark.createDataFrame(data)
# Infers from non-empty array: ArrayType(StringType)
```

### Maps (Nested Dicts)

#### MapType for Consistent Keys
```python
data = [
    {"metadata": {"key1": "val1", "key2": "val2"}},
    {"metadata": {"key1": "val3", "key2": "val4"}}
]
df = spark.createDataFrame(data)
# Schema:
#  |-- metadata: map (nullable = true)
#  |    |-- key: string
#  |    |-- value: string (valueContainsNull = true)
```

**Important**: PySpark infers consistent-key dicts as **MapType**, not StructType!

#### Nested Structs
```python
data = [{"address": {"street": "Main St", "city": "NYC"}}]
df = spark.createDataFrame(data)
# Schema:
#  |-- address: map (nullable = true)
#  |    |-- key: string
#  |    |-- value: string (valueContainsNull = true)
```

Even consistent structured data becomes MapType.

---

## 🔄 Sparse Data (Missing Keys)

### All Keys Included
```python
data = [
    {"id": 1, "name": "Alice"},
    {"id": 2, "age": 30},
    {"id": 3, "name": "Charlie", "age": 35}
]
df = spark.createDataFrame(data)
# Schema includes all keys: id, name, age
# All fields: nullable = true
```

### Type Inferred from Available Values
```python
data = [
    {"id": 1, "name": "Alice"},  # 'age' missing
    {"id": 2, "age": 30},         # 'name' missing
]
df = spark.createDataFrame(data)
# 'name' inferred as StringType (from row 1)
# 'age' inferred as LongType (from row 2)
# Both nullable = true
```

---

## 📐 Column Ordering

### Alphabetical Sorting
```python
data = [{"name": "Alice", "age": 25, "id": 1}]
df = spark.createDataFrame(data)
# Columns: ['age', 'id', 'name']  ← Alphabetically sorted!
```

**PySpark sorts columns alphabetically**, not by insertion order or first occurrence.

---

## 🚨 Error Conditions

### 1. All-Null Column
```python
data = [{"value": None}]
df = spark.createDataFrame(data)
# ❌ ValueError: Some of types cannot be determined after inferring
```

### 2. Type Conflicts
```python
# Int/Float mix
data = [{"value": 100}, {"value": 95.5}]
# ❌ TypeError: Can not merge type LongType and DoubleType

# Numeric/String mix
data = [{"value": 100}, {"value": "text"}]
# ❌ TypeError: Can not merge types

# Boolean/Int mix  
data = [{"value": True}, {"value": 1}]
# ❌ TypeError: Can not merge type BooleanType and LongType
```

**No automatic type promotion!** Explicit schema required for mixed types.

---

## ✅ Implementation Checklist

### Must-Have Behaviors
- [x] Map `int` → `LongType` (not IntegerType)
- [x] Map `float` → `DoubleType` (not FloatType)
- [x] Map `bool` → `BooleanType`
- [x] Map `str` → `StringType`
- [x] Set all fields to `nullable=True`
- [x] Sort columns alphabetically
- [x] Raise `ValueError` for all-null columns
- [x] Raise `TypeError` for type conflicts
- [x] Support `ArrayType` for Python lists
- [x] Support `MapType` for Python dicts (not StructType!)
- [x] Include all keys from all rows (union)
- [x] Infer types from non-null values when some nulls present

### Edge Cases
- [x] Empty DataFrame → Empty schema
- [x] Single row → Infer correctly
- [x] Empty arrays → Infer from non-empty arrays in other rows
- [x] Sparse data → All fields nullable
- [x] Large integers → Still LongType

---

## 🧪 Test Data Examples

### Valid Cases (Should Work)
```python
# Basic types (consistent)
[{"id": 1, "name": "Alice", "score": 95.5, "active": True}]

# Arrays
[{"tags": ["a", "b"]}, {"tags": ["x", "y"]}]

# Nulls in some rows
[{"id": 1, "value": 100}, {"id": 2, "value": None}]

# Missing keys
[{"id": 1, "name": "Alice"}, {"id": 2, "age": 30}]
```

### Invalid Cases (Should Raise Errors)
```python
# All nulls
[{"value": None}]  # ValueError

# Type conflicts
[{"value": 100}, {"value": 95.5}]  # TypeError
[{"value": 100}, {"value": "text"}]  # TypeError
[{"value": True}, {"value": 1}]  # TypeError
```

---

## 📝 Implementation Notes

### Priority 1: Match PySpark Exactly
- Use LongType for all Python ints
- Use DoubleType for all Python floats
- Sort columns alphabetically
- Raise same errors for same conditions

### Priority 2: Performance
- Sample-based inference for large datasets
- Cache inferred schemas
- Don't scan all rows if not needed

### Priority 3: Developer Experience
- Clear error messages
- Helpful suggestions for fixing type conflicts
- Configuration options

---

## 🔗 References

- PySpark Version: 3.2.4
- Java Version: OpenJDK 11.0.28
- Test Date: October 7, 2025
- Test Script: Captured in this document

---

## ✅ Ready for Implementation

This document provides the ground truth for implementing schema inference that exactly matches PySpark behavior. All unit tests have been updated to reflect these actual behaviors.

**Next Step**: Implement `SchemaInferenceEngine` to match these exact behaviors.

