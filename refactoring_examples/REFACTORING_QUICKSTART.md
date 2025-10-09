# SQLModel Refactoring: Quick Start Guide

**One-page reference incorporating Inspector findings**

---

## 🎯 The Plan in 30 Seconds

**Goal:** Replace raw SQL with SQLModel + SQLAlchemy
**Coverage:** **85-98%** of code (better than expected!)
**Timeline:** 3-5 weeks
**Key Tool:** SQLAlchemy Inspector (works with DuckDB!)

---

## 📊 What Changed (Inspector Discovery!)

| Before Research | After Research | Impact |
|----------------|----------------|--------|
| Can refactor ~80-95% | Can refactor **85-98%** | 📈 More coverage |
| Metadata needs raw SQL | **Inspector works!** | 🎉 Major win |
| SHOW TABLES → raw SQL | `inspector.get_table_names()` | ✅ Type-safe |
| DESCRIBE → raw SQL | `inspector.get_columns()` | ✅ Better API |
| Manual table building | `Table(..., autoload_with=engine)` | ✅ Automatic |

---

## ✅ Use SQLModel For (85% of code)

```python
from sqlmodel import SQLModel, Field, Session, create_engine, select

class User(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(min_length=1)
    age: int = Field(ge=0)

engine = create_engine('duckdb:///:memory:')
SQLModel.metadata.create_all(engine)

with Session(engine) as session:
    user = User(name="Alice", age=25)
    session.add(user)
    session.commit()
```

**When:** Known schemas, CRUD, type-safe queries

---

## ✅ Use Inspector For (Metadata Operations)

```python
from sqlalchemy import inspect

inspector = inspect(engine)

# List tables (was: SHOW TABLES)
tables = inspector.get_table_names()

# Check exists (was: SELECT 1 FROM...)
exists = inspector.has_table('users')

# Get columns (was: DESCRIBE)
columns = inspector.get_columns('users')
# Returns: [{'name': 'id', 'type': INTEGER, 'nullable': True}, ...]

# Get schemas
schemas = inspector.get_schema_names()
```

**When:** Metadata queries, table introspection

---

## ✅ Use SQLAlchemy Core For (13% of code)

```python
from sqlalchemy import Table, Column, Integer, String, MetaData, select

# Dynamic table from runtime data
metadata = MetaData()
columns = []
for key, value in data[0].items():
    col_type = Integer if isinstance(value, int) else String
    columns.append(Column(key, col_type))

table = Table("dynamic", metadata, *columns)
table.create(engine)

# Or reflect existing table
reflected = Table("existing", metadata, autoload_with=engine)

# Query
stmt = select(table).where(table.c.age > 25)
results = session.execute(stmt).all()
```

**When:** Dynamic schemas, complex queries, reflection

---

## ✋ Keep Raw SQL For (2% of code)

### DuckDB Extensions Only
```python
from sqlalchemy import text

with engine.connect() as conn:
    conn.execute(text("INSTALL parquet"))
    conn.execute(text("LOAD parquet"))
    conn.execute(text("PRAGMA memory_limit='4GB'"))
```

### SQL Parser (Implementing SQL Engine)
```python
def execute_sql(query: str):
    """User provides arbitrary SQL."""
    return connection.execute(text(query))
```

### Performance (Optional, >10k rows)
```python
if len(data) > 10000:
    connection.executemany("INSERT INTO ...", values)
```

---

## 📋 File-by-File Checklist

### Phase 1: Quick Wins (1-2 weeks)
- [ ] `export.py` - CREATE TABLE → SQLAlchemy
- [ ] `sqlmodel_materializer.py` - SELECT → select()
- [ ] `duckdb_materializer.py` - Temp tables → SQLAlchemy

### Phase 2: Core (1-2 weeks)
- [ ] `storage/backends/duckdb.py` - **95% refactor with Inspector!**
  - [ ] SHOW TABLES → inspector.get_table_names()
  - [ ] Table exists → inspector.has_table()
  - [ ] DESCRIBE → inspector.get_columns()
  - [ ] Keep extensions as raw SQL
- [ ] `sql_builder.py` - String building → SQLAlchemy Core

### Phase 3: Testing (1 week)
- [ ] Unit tests
- [ ] Integration tests
- [ ] Performance benchmarks

---

## 🚀 Quick Examples

### Example 1: List Tables
```python
# ❌ OLD
result = connection.execute("SHOW TABLES").fetchall()
tables = [row[0] for row in result]

# ✅ NEW
inspector = inspect(engine)
tables = inspector.get_table_names()
```

### Example 2: Create Table
```python
# ❌ OLD
columns = [f"{name} {type}" for name, type in cols]
sql = f"CREATE TABLE {table} ({', '.join(columns)})"
connection.execute(sql)

# ✅ NEW (Static)
class User(SQLModel, table=True):
    id: int = Field(primary_key=True)
    name: str

SQLModel.metadata.create_all(engine)

# ✅ NEW (Dynamic)
table = Table(name, metadata, *[Column(n, t) for n, t in cols])
table.create(engine)
```

### Example 3: Insert Data
```python
# ❌ OLD
placeholders = ", ".join(["?" for _ in values])
connection.execute(f"INSERT INTO {table} VALUES ({placeholders})", values)

# ✅ NEW
with Session(engine) as session:
    user = User(name="Alice", age=25)
    session.add(user)
    session.commit()
```

### Example 4: Query with Filter
```python
# ❌ OLD
result = connection.execute(f"SELECT * FROM {table} WHERE age > 25")

# ✅ NEW
stmt = select(User).where(User.age > 25)
results = session.exec(stmt).all()
```

---

## 🧪 How to Test

```python
def test_inspector_works():
    """Test Inspector functionality."""
    engine = create_engine('duckdb:///:memory:')

    # Create table
    User.__table__.create(engine)

    # Test Inspector
    inspector = inspect(engine)
    assert 'user' in inspector.get_table_names()
    assert inspector.has_table('user')

    columns = inspector.get_columns('user')
    assert any(c['name'] == 'id' for c in columns)
```

---

## 📚 Key Documents

1. **`SQLMODEL_REFACTORING_PLAN.md`** - Comprehensive plan
2. **`refactoring_examples/SQLMODEL_LIMITATIONS.md`** - Edge cases
3. **`DUCKDB_ENGINE_RESEARCH.md`** - Inspector research
4. **`RESEARCH_FINDINGS_SUMMARY.md`** - Executive summary
5. **`refactoring_examples/sqlmodel_refactor_demo.py`** - Code examples

---

## ⚡ Quick Decision Tree

```
Need to interact with database?
│
├─ Known schema at design time?
│  └─ ✅ Use SQLModel
│
├─ List/inspect tables or columns?
│  └─ ✅ Use Inspector
│
├─ Schema determined at runtime?
│  └─ ✅ Use SQLAlchemy Core
│
├─ DuckDB extension or PRAGMA?
│  └─ ✋ Use raw SQL (text())
│
├─ Implementing SQL parser?
│  └─ ✋ Use raw SQL (text())
│
└─ Very large batch (>10k rows)?
   └─ ⚡ Consider raw SQL for performance
```

---

## ✅ Success Metrics

- [ ] **85-98%** of SQL refactored
- [ ] **All 396 tests** pass
- [ ] **No performance regression**
- [ ] **100% test coverage** maintained
- [ ] **Type safety** throughout

---

## 🎯 Next Steps

1. ✅ Review this guide
2. ✅ Read full plan (`SQLMODEL_REFACTORING_PLAN.md`)
3. ✅ Start with `export.py` (easiest)
4. ✅ Test Inspector integration
5. ✅ Move to Phase 2

---

## 💡 Key Insights

### What We Learned

1. **Inspector Works!** 🎉
   - Can eliminate SHOW TABLES, DESCRIBE, table existence checks
   - Type-safe metadata operations
   - 3-5% more code can be refactored

2. **duckdb-engine is Mature**
   - Already installed: `duckdb-engine>=0.15.0`
   - Full SQLAlchemy support
   - Production-ready

3. **Hybrid Approach is Best**
   - Don't force refactoring where it doesn't fit
   - 2% raw SQL is acceptable (extensions, parser)
   - Use right tool for right job

### What Changed from Original Plan

| Aspect | Original | Updated | Better? |
|--------|----------|---------|---------|
| Coverage | 80-95% | **85-98%** | ✅ Yes |
| Metadata | Raw SQL | **Inspector** | ✅ Yes |
| Risk | Medium | **Low-Medium** | ✅ Yes |
| Timeline | 4-6 weeks | **3-5 weeks** | ✅ Yes |

---

## 🎉 Bottom Line

**The refactoring plan is BETTER than we initially thought!**

- ✅ Can refactor MORE code (Inspector support)
- ✅ LOWER risk (proven libraries)
- ✅ CLEANER API (Inspector vs raw SQL)
- ✅ Type-safe metadata operations
- ✅ Same timeline (3-5 weeks)

**Recommendation:** Proceed with confidence! 🚀

---

**Last Updated:** October 7, 2025
**Status:** Ready to implement
**Confidence:** Very High
