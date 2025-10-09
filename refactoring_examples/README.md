# SQLAlchemy Refactoring Examples

This directory contains examples and utilities for refactoring Mock Spark's raw SQL to use SQLAlchemy/SQLModel.

---

## 🎯 Quick Start Guide

### What You Need to Know

**SQLModel** (recommended) = SQLAlchemy + Pydantic type safety
- ✅ Use for **80%** of refactoring: known schemas, CRUD, type-safe queries
- ✅ Better validation, IDE support, cleaner syntax

**SQLAlchemy Core** (fallback) = Dynamic table operations
- ✅ Use for **15%**: fully dynamic schemas, complex query building

**Raw SQL** (when necessary) = Keep for edge cases
- ⚠️ Use for **5%**: metadata queries, SQL parsing, DuckDB features, bulk operations

**See:** `SQLMODEL_LIMITATIONS.md` for detailed analysis of when each approach is needed.

---

## 📁 Files in This Directory

### 1. `sqlmodel_refactor_demo.py` ⭐ **RECOMMENDED**
**SQLModel examples** - The preferred approach for most refactoring.

**Contains:**
- 10 SQLModel examples with type safety
- Dynamic table creation from MockSpark schemas
- Query builder pattern
- Complete production-ready examples

**Run it:**
```bash
cd refactoring_examples
python sqlmodel_refactor_demo.py
```

### 2. `sqlalchemy_refactor_demo.py`
**Complete before/after examples** showing how to refactor common patterns.

**Contains:**
- 10 detailed examples with explanations
- Side-by-side comparison of old vs new code
- Running examples you can execute
- Complete flow demonstration

**Run it:**
```bash
cd refactoring_examples
python sqlalchemy_refactor_demo.py
```

**Output:**
```
=== SQLAlchemy Refactoring Examples ===

Running complete example...

Users over 25: [(2, 'Bob', 30), (3, 'Charlie', 35)]
Count: 3, Avg Age: 30.0

=== Testing SQL Equivalence ===

Generated SQL: SELECT test.id, test.name FROM test WHERE test.id > 5 ORDER BY test.name DESC
```

---

### 3. `SQLMODEL_LIMITATIONS.md` 📖 **IMPORTANT**
**Analysis of edge cases** where SQLModel/SQLAlchemy won't work.

**Contains:**
- 7 scenarios where raw SQL is still needed
- Why each limitation exists
- Workarounds and solutions
- Hybrid approach recommendations
- Code examples for each edge case

**Key insights:**
- ✋ **Must use raw SQL**: Metadata queries, SQL parsing, DuckDB features
- ⚠️ **Can work around**: Dynamic schemas, performance-critical operations
- ✅ **Hybrid approach**: Use best tool for each job (80% SQLModel, 15% SQLAlchemy Core, 5% raw SQL)

---

### 4. `sqlalchemy_utils.py`
**Production-ready utility functions** for the refactoring.

**Contains:**
- Type converters (MockSpark ↔ SQLAlchemy)
- Table factories
- Column builders
- Query helpers
- Migration utilities
- Testing helpers

**Key Functions:**
- `mock_type_to_sqlalchemy()` - Convert types
- `create_table_from_mock_schema()` - Create tables
- `TableFactory` - Factory pattern for table creation
- `ColumnBuilder` - Fluent API for columns
- `compare_sql_output()` - Test equivalence

**Usage Example:**
```python
from sqlalchemy_utils import create_table_from_mock_schema, TableFactory

# Method 1: Direct function
table = create_table_from_mock_schema("users", mock_schema, metadata)

# Method 2: Factory pattern
factory = TableFactory()
table = factory.from_mock_schema("users", mock_schema)
```

---

## 🚀 Getting Started

### Step 1: Review Examples
```bash
# Read the demo file
cat sqlalchemy_refactor_demo.py

# Or open in your editor
code sqlalchemy_refactor_demo.py
```

### Step 2: Run Examples
```bash
# Make sure dependencies are installed
pip install sqlalchemy duckdb-engine

# Run the demo
python sqlalchemy_refactor_demo.py
```

### Step 3: Copy Utilities
```bash
# Copy utilities to main codebase
cp sqlalchemy_utils.py ../mock_spark/storage/
```

### Step 4: Start Refactoring
1. Choose a file from the priority list (see `../REFACTORING_SUMMARY.md`)
2. Write tests for current behavior
3. Use utilities from `sqlalchemy_utils.py`
4. Follow patterns from `sqlalchemy_refactor_demo.py`
5. Run tests to verify equivalence

---

## 📚 Quick Reference

### Common Patterns

#### Create Table
```python
# Before
create_sql = f"CREATE TABLE {name} ({cols})"
conn.execute(create_sql)

# After
table = create_table_from_mock_schema(name, schema, metadata)
table.create(engine)
```

#### Insert Data
```python
# Before
conn.execute(f"INSERT INTO {table} VALUES ({placeholders})", values)

# After
session.execute(insert(table).values(**data))
session.commit()
```

#### Select Query
```python
# Before
result = conn.execute(f"SELECT * FROM {table} WHERE {condition}")

# After
stmt = select(table).where(condition)
result = session.execute(stmt).all()
```

---

## 🧪 Testing Your Refactoring

### Method 1: Compare SQL Strings
```python
from sqlalchemy_utils import print_compiled_sql

# See what SQL is generated
stmt = select(table).where(table.c.age > 25)
print_compiled_sql(stmt)
```

### Method 2: Compare Results
```python
from sqlalchemy_utils import compare_sql_output

old_sql = "SELECT * FROM users WHERE age > 25"
new_stmt = select(table).where(table.c.age > 25)

match, old_results, new_results = compare_sql_output(old_sql, new_stmt, connection)
assert match, "Results don't match!"
```

### Method 3: Unit Tests
```python
def test_create_table_equivalence():
    """Verify table creation produces same schema."""
    old_table = create_table_old_way(...)
    new_table = create_table_new_way(...)

    assert set(old_table.columns) == set(new_table.columns)
```

---

## 📖 Example Walkthrough

Let's refactor a real function from `export.py`:

### Original Code (Lines 119-120)
```python
def _create_duckdb_table(df: "MockDataFrame", connection, table_name: str) -> None:
    columns = []
    for field in df.schema.fields:
        duckdb_type = DataFrameExporter._get_duckdb_type(field.dataType)
        columns.append(f"{field.name} {duckdb_type}")

    create_sql = f"CREATE TABLE {table_name} ({', '.join(columns)})"
    connection.execute(create_sql)
```

### Refactored Code
```python
from sqlalchemy import MetaData
from mock_spark.storage.sqlalchemy_utils import create_table_from_mock_schema

def _create_duckdb_table(df: "MockDataFrame", engine, table_name: str) -> Table:
    """Create DuckDB table using SQLAlchemy."""
    metadata = MetaData()
    table = create_table_from_mock_schema(table_name, df.schema, metadata)
    table.create(engine, checkfirst=True)
    return table
```

### What Changed?
1. ❌ Removed manual string building
2. ❌ Removed raw SQL execution
3. ✅ Added type-safe table creation
4. ✅ Returns Table object for reuse
5. ✅ Automatic type mapping
6. ✅ SQL injection safe

### Test It
```python
def test_create_duckdb_table():
    """Test table creation equivalence."""
    import duckdb
    from sqlalchemy import create_engine

    # Setup
    connection = duckdb.connect(":memory:")
    engine = create_engine("duckdb:///:memory:")

    # Create test DataFrame
    data = [{"id": 1, "name": "Alice"}]
    df = MockDataFrame(data, infer_schema(data))

    # Create table with new method
    table = _create_duckdb_table(df, engine, "test_table")

    # Verify table exists
    assert "test_table" in engine.table_names()

    # Verify columns
    assert "id" in table.c
    assert "name" in table.c
```

---

## 🎯 Refactoring Checklist

For each function you refactor:

- [ ] Read and understand current implementation
- [ ] Write test for current behavior
- [ ] Identify raw SQL patterns
- [ ] Find matching example in `sqlalchemy_refactor_demo.py`
- [ ] Use utilities from `sqlalchemy_utils.py`
- [ ] Replace raw SQL with SQLAlchemy
- [ ] Run original test - should still pass
- [ ] Add new test for edge cases
- [ ] Check performance (should be same or better)
- [ ] Update docstring
- [ ] Code review
- [ ] Merge

---

## 🛠️ Utilities Quick Reference

### Type Conversion
```python
from sqlalchemy_utils import mock_type_to_sqlalchemy

# Convert single type
sql_type = mock_type_to_sqlalchemy(StringType())  # → String

# Convert schema
for field in schema.fields:
    sql_type = mock_type_to_sqlalchemy(field.dataType)
    columns.append(Column(field.name, sql_type))
```

### Table Creation
```python
from sqlalchemy_utils import TableFactory, create_table_from_mock_schema

# Method 1: Direct function
table = create_table_from_mock_schema(name, mock_schema, metadata)

# Method 2: Factory
factory = TableFactory()
table = factory.from_mock_schema(name, mock_schema)
table = factory.from_data(name, data_list)

# Method 3: Fluent API
from sqlalchemy_utils import ColumnBuilder
table = factory.from_columns(name, [
    ColumnBuilder("id").integer().primary_key().build(),
    ColumnBuilder("name").string(100).not_null().build()
])
```

### Query Building
```python
from sqlalchemy import select, insert, and_, or_, desc

# Select
stmt = select(table).where(table.c.age > 25)

# Insert
stmt = insert(table).values(name="Alice", age=25)

# Complex filter
stmt = select(table).where(
    and_(
        table.c.age > 25,
        table.c.name.like("A%")
    )
).order_by(desc(table.c.age))
```

---

## 💡 Tips & Tricks

### Tip 1: Print SQL to Debug
```python
# See what SQL is generated
print(stmt.compile(compile_kwargs={"literal_binds": True}))
```

### Tip 2: Start Simple
Don't refactor everything at once. Start with one function:
1. CREATE TABLE statements
2. INSERT INTO statements
3. Simple SELECT statements
4. Complex queries

### Tip 3: Keep Old Code Temporarily
```python
def query_data(self, filter_expr=None):
    # New way (preferred)
    if USE_SQLALCHEMY:  # Feature flag
        return self._query_data_sqlalchemy(filter_expr)

    # Old way (fallback)
    return self._query_data_raw_sql(filter_expr)
```

### Tip 4: Test with Edge Cases
```python
# Test with empty data
assert create_table(name, []) works

# Test with NULL values
assert insert(table, {"name": None}) works

# Test with special characters
assert insert(table, {"name": "O'Brien"}) works  # Should NOT break!
```

### Tip 5: Use Type Hints
```python
from sqlalchemy import Table, Engine
from sqlalchemy.orm import Session

def create_table(name: str, schema: MockStructType, engine: Engine) -> Table:
    """Type hints help catch errors early."""
    ...
```

---

## 🐛 Common Issues

### Issue 1: "No such column"
**Problem:** Column name doesn't match table definition

**Solution:**
```python
# Use safe_column_reference
from sqlalchemy_utils import safe_column_reference
col = safe_column_reference(table, "age")  # Raises helpful error
```

### Issue 2: "Table already exists"
**Problem:** Trying to create table that exists

**Solution:**
```python
# Use checkfirst=True
table.create(engine, checkfirst=True)
```

### Issue 3: SQL injection still possible
**Problem:** Using text() with string interpolation

**Solution:**
```python
# ❌ Still vulnerable
stmt = text(f"SELECT * FROM {table} WHERE id = {user_input}")

# ✅ Safe with parameters
stmt = text("SELECT * FROM :table WHERE id = :id")
result = connection.execute(stmt, {"table": table_name, "id": user_id})

# ✅ Better: Use SQLAlchemy
stmt = select(table).where(table.c.id == user_id)
```

---

## 📊 Progress Tracking

Track your refactoring progress:

```markdown
## mock_spark/dataframe/export.py

**Status:** ✅ Complete

**Changes:**
- [x] Line 96: INSERT → insert().values()
- [x] Line 119: CREATE TABLE → Table.create()

**Tests:** 2 added, all passing
**Performance:** No regression
**Code Review:** Approved
**Merged:** 2025-10-15
```

---

## 🤝 Contributing

When adding new examples or utilities:

1. **Add to `sqlalchemy_refactor_demo.py`**
   - Include before/after
   - Add explanatory comments
   - Make it runnable

2. **Add to `sqlalchemy_utils.py`**
   - Include docstring with example
   - Add type hints
   - Include in `if __name__ == "__main__"` demo

3. **Update this README**
   - Add to Quick Reference
   - Update examples if needed

---

## 📞 Getting Help

- 📖 Full analysis: `../RAW_SQL_ANALYSIS.md`
- 📋 Quick reference: `../REFACTORING_SUMMARY.md`
- 💻 Code examples: `sqlalchemy_refactor_demo.py`
- 🛠️ Utilities: `sqlalchemy_utils.py`
- 📚 SQLAlchemy docs: https://docs.sqlalchemy.org/

---

**Created:** October 7, 2025
**Last Updated:** October 7, 2025
**Version:** 1.0
