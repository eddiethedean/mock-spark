# SQLAlchemy Refactoring Summary

**Quick Reference Guide** | October 7, 2025

---

## 📊 Current State

| Metric | Value |
|--------|-------|
| Files with Raw SQL | 6 |
| Total Raw SQL Locations | ~50-60 |
| SQLAlchemy Already Used | Partial (1 file) |
| Estimated Refactor Effort | 4-6 weeks |
| Risk Level | Low-Medium |

---

## 🎯 Priority Files

### ✅ Phase 1: Quick Wins (1-2 weeks)

| File | Raw SQL Count | Effort | Impact | Priority |
|------|---------------|--------|--------|----------|
| `sqlmodel_materializer.py` | 5-10 | LOW | HIGH | 🔥 1 |
| `export.py` | 3-5 | LOW | HIGH | 🔥 2 |
| `duckdb_materializer.py` | 5-10 | MEDIUM | HIGH | 🔥 3 |

### ⚠️ Phase 2: Core Infrastructure (2-3 weeks)

| File | Raw SQL Count | Effort | Impact | Priority |
|------|---------------|--------|--------|----------|
| `storage/backends/duckdb.py` | 15-20 | MEDIUM | HIGH | ⚠️ 4 |
| `sql_builder.py` | 20-30 | HIGH | MEDIUM | ⚠️ 5 |

### ℹ️ Phase 3: Optional

| File | Raw SQL Count | Effort | Impact | Priority |
|------|---------------|--------|--------|----------|
| `session/sql/executor.py` | 5-10 | LOW | LOW | ℹ️ 6 |

---

## 🔍 Most Common Raw SQL Patterns

### 1. CREATE TABLE (15+ occurrences)
```python
# ❌ OLD
f"CREATE TABLE {name} ({', '.join(columns)})"

# ✅ NEW
Table(name, metadata, *columns).create(engine)
```

### 2. INSERT INTO (10+ occurrences)
```python
# ❌ OLD
f"INSERT INTO {table} VALUES ({placeholders})"

# ✅ NEW
insert(table).values(**data_dict)
```

### 3. SELECT with WHERE (10+ occurrences)
```python
# ❌ OLD
f"SELECT * FROM {table} WHERE {condition}"

# ✅ NEW
select(table).where(condition)
```

### 4. DROP TABLE (5+ occurrences)
```python
# ❌ OLD
f"DROP TABLE IF EXISTS {table}"

# ✅ NEW
table.drop(engine, checkfirst=True)
```

### 5. Complex SELECT (10+ occurrences)
```python
# ❌ OLD
f"SELECT {cols} FROM {table} WHERE {where} ORDER BY {order}"

# ✅ NEW
select(*cols).where(where).order_by(*order)
```

---

## 💡 Key Benefits

| Benefit | Description | Impact |
|---------|-------------|--------|
| **Security** | SQL injection prevention | 🔒 Critical |
| **Type Safety** | Catch errors at Python level | 🛡️ High |
| **Maintainability** | Less string manipulation | 📝 High |
| **Testability** | Mock database easily | ✅ High |
| **IDE Support** | Autocomplete & refactoring | 🎯 Medium |
| **Performance** | Query caching & optimization | ⚡ Medium |
| **Extensibility** | Support multiple databases | 🔌 Medium |

## ⚠️ When Raw SQL Is Still Needed (5-20% of cases)

| Scenario | Why | Example |
|----------|-----|---------|
| **Database Metadata** | No SQLAlchemy API | `SHOW TABLES`, `DESCRIBE` |
| **Fully Dynamic Schemas** | Schema unknown at runtime | Creating tables from data |
| **SQL Parser Implementation** | Handling arbitrary SQL | `session/sql/executor.py` |
| **DuckDB-Specific Features** | Vendor-specific syntax | `PRAGMA`, extensions |
| **Performance-Critical Bulk Ops** | Raw SQL 2-5x faster | >1000 row inserts |
| **Complex Window Functions** | SQLAlchemy too verbose | Advanced window specs |

**See:** `refactoring_examples/SQLMODEL_LIMITATIONS.md` for detailed analysis

---

## 📁 New Files to Create

1. **`mock_spark/storage/sqlalchemy_utils.py`**
   - Type converters (MockSpark ↔ SQLAlchemy)
   - Table factories
   - Query builders
   - See: `refactoring_examples/sqlalchemy_utils.py`

2. **`mock_spark/storage/query_builder.py`**
   - DuckDBQueryBuilder class
   - Wrapper around SQLAlchemy select()
   - Maintains MockSpark API

3. **`tests/unit/test_sqlalchemy_refactoring.py`**
   - Tests for new utilities
   - Comparison tests (old SQL vs new)

---

## 🚀 Quick Start: Refactoring Your First File

### Step 1: Setup (5 minutes)
```python
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
from sqlalchemy import select, insert, and_, desc
from sqlalchemy.orm import Session

# Create engine
engine = create_engine("duckdb:///:memory:")
metadata = MetaData()
```

### Step 2: Convert CREATE TABLE (10 minutes)
```python
# Before:
columns_str = ", ".join([f"{name} {type}" for name, type in cols])
conn.execute(f"CREATE TABLE {table_name} ({columns_str})")

# After:
table = Table(
    table_name,
    metadata,
    *[Column(name, sql_type) for name, sql_type in cols]
)
table.create(engine, checkfirst=True)
```

### Step 3: Convert INSERT (10 minutes)
```python
# Before:
placeholders = ", ".join(["?" for _ in values])
conn.execute(f"INSERT INTO {table} VALUES ({placeholders})", values)

# After:
with Session(engine) as session:
    stmt = insert(table).values(**data_dict)
    session.execute(stmt)
    session.commit()
```

### Step 4: Convert SELECT (15 minutes)
```python
# Before:
sql = f"SELECT * FROM {table} WHERE {condition}"
results = conn.execute(sql).fetchall()

# After:
stmt = select(table).where(table.c.age > 25)
results = session.execute(stmt).all()
```

### Step 5: Test (10 minutes)
```python
# Write test comparing old vs new
def test_refactored_query():
    old_results = execute_old_way()
    new_results = execute_new_way()
    assert old_results == new_results
```

**Total Time:** ~1 hour for first file

---

## 📋 Refactoring Checklist

### For Each File

- [ ] Read existing code and understand logic
- [ ] Write tests for current behavior
- [ ] Create SQLAlchemy equivalents
- [ ] Replace raw SQL gradually
- [ ] Run tests after each change
- [ ] Compare SQL output (print compiled SQL)
- [ ] Check linter errors
- [ ] Update documentation
- [ ] Code review
- [ ] Merge when all tests pass

### For Each Raw SQL String

- [ ] Identify SQL type (CREATE, INSERT, SELECT, etc.)
- [ ] Find SQLAlchemy equivalent
- [ ] Handle any DuckDB-specific syntax
- [ ] Test with edge cases (empty data, NULL values)
- [ ] Verify performance is equal or better

---

## 🧪 Testing Strategy

### Unit Tests
```python
def test_table_creation_equivalence():
    """Verify new method creates same schema as old."""
    old_table = create_table_old_way(...)
    new_table = create_table_new_way(...)
    assert old_table.columns == new_table.columns
```

### Integration Tests
```python
def test_query_execution_equivalence():
    """Verify queries return same results."""
    old_results = query_old_way(...)
    new_results = query_new_way(...)
    assert old_results == new_results
```

### Performance Tests
```python
def test_performance_not_degraded():
    """Verify refactoring doesn't slow down queries."""
    old_time = time_query(old_implementation)
    new_time = time_query(new_implementation)
    assert new_time <= old_time * 1.1  # Allow 10% margin
```

---

## 🛠️ Useful Commands

### Find Raw SQL
```bash
# Find all SQL strings
grep -r "f\".*SELECT\|INSERT\|CREATE\|UPDATE\|DELETE" mock_spark/

# Count occurrences
grep -r "connection.execute" mock_spark/ | wc -l
```

### Test After Changes
```bash
# Run specific test file
python -m pytest tests/unit/test_dataframe_duckdb_integration.py -v

# Run all tests
python -m pytest tests/ -v

# Check linter
python -m mypy mock_spark/
```

### Verify SQL Output
```python
# Print compiled SQL to compare
from sqlalchemy import select
stmt = select(table).where(table.c.id > 5)
print(stmt.compile(compile_kwargs={"literal_binds": True}))
```

---

## 📚 Resources

### Documentation
- **Full Analysis:** `RAW_SQL_ANALYSIS.md`
- **Code Examples:** `refactoring_examples/sqlalchemy_refactor_demo.py`
- **Utilities:** `refactoring_examples/sqlalchemy_utils.py`

### SQLAlchemy Docs
- Core Tutorial: https://docs.sqlalchemy.org/en/20/core/tutorial.html
- DuckDB Dialect: https://github.com/Mause/duckdb_engine
- SQLModel: https://sqlmodel.tiangolo.com/

### Internal Docs
- Mock Spark: `docs/storage_serialization_guide.md`
- Testing: `docs/guides/pytest_integration.md`

---

## 🎯 Success Metrics

### After Refactoring

- [ ] **Zero** f-string SQL construction
- [ ] **100%** test coverage maintained
- [ ] **No** performance regression
- [ ] **Better** error messages
- [ ] **Easier** to add new database backends
- [ ] **Safer** from SQL injection

---

## 🤝 Getting Help

### Code Review Checklist
1. All raw SQL removed
2. Tests pass
3. No performance regression
4. Error handling improved
5. Documentation updated

### Questions?
- See examples in `refactoring_examples/`
- Check `RAW_SQL_ANALYSIS.md` for detailed analysis
- Review SQLAlchemy docs
- Test with `compare_sql_output()` utility

---

## 📊 Progress Tracking

### Template for Each File

```markdown
## File: mock_spark/dataframe/export.py

**Status:** 🔄 In Progress

**Raw SQL Count:** 3
**Lines Changed:** 25
**Tests Added:** 2
**Time Spent:** 2 hours

**Changes:**
- [x] Line 96: INSERT INTO → insert().values()
- [x] Line 119: CREATE TABLE → Table.create()
- [x] Line 120: Type inference → get_column_type_for_value()

**Tests:**
- [x] test_to_duckdb_creates_table
- [x] test_to_duckdb_inserts_data

**Performance:** ✅ No regression
**Code Review:** ✅ Approved
**Merged:** ✅ 2025-10-15
```

---

## 🎉 Quick Win Example

**File:** `export.py` (Line 119-120)

### Before (2 lines of raw SQL)
```python
columns = [f"{field.name} {duckdb_type}" for field in df.schema.fields]
create_sql = f"CREATE TABLE {table_name} ({', '.join(columns)})"
connection.execute(create_sql)
```

### After (Type-safe SQLAlchemy)
```python
from mock_spark.storage.sqlalchemy_utils import create_table_from_mock_schema

metadata = MetaData()
table = create_table_from_mock_schema(table_name, df.schema, metadata)
table.create(connection, checkfirst=True)
```

**Benefits:**
- ✅ Type-safe
- ✅ No string manipulation
- ✅ Reusable across codebase
- ✅ 3 lines → 3 lines (same readability, better safety)

---

**Updated:** October 7, 2025
**Next Review:** After Phase 1 completion
