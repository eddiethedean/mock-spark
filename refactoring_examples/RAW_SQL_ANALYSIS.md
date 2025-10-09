# Raw SQL Usage Analysis and SQLAlchemy/SQLModel Refactoring Opportunities

**Date:** October 7, 2025
**Project:** Mock Spark
**Objective:** Identify raw SQL usage and opportunities to use SQLModel/SQLAlchemy ORM

---

## Executive Summary

The codebase currently uses raw SQL string construction in **4 primary modules**:
1. `sql_builder.py` - Manual SQL query building
2. `duckdb_materializer.py` - DuckDB query execution with string interpolation
3. `storage/backends/duckdb.py` - Table creation and data insertion
4. `dataframe/export.py` - DuckDB table creation
5. `sqlmodel_materializer.py` - Mixed approach (partially uses SQLAlchemy)

**Good News:**
- `sqlmodel_materializer.py` already uses SQLAlchemy Table API for table creation
- SQLModel is already imported and available in the codebase
- Infrastructure is in place for ORM-based approach

**Opportunities:** Significant refactoring can eliminate ~80% of raw SQL strings.

---

## Detailed Analysis

### 1. `mock_spark/dataframe/sql_builder.py` (347 lines)

**Current State:** 100% raw SQL string construction

**Raw SQL Examples:**
```python
# Line 67-68
create_sql = f"CREATE TABLE \"{self.table_name}\" ({', '.join(columns)})"

# Line 316
insert_sql = f'INSERT INTO "{self.table_name}" VALUES '

# Line 228-290: build_sql() method
sql_parts = []
select_clause = "SELECT " + ", ".join(self.select_columns)
from_clause = f'FROM "{self.table_name}"'
where_clause = "WHERE " + " AND ".join(self.where_conditions)
```

**Refactoring Opportunities:**

#### ✅ Recommended: Replace with SQLModel (preferred) or SQLAlchemy Core API
```python
from sqlmodel import SQLModel, Field, Session, select
from typing import Optional

# Define table with SQLModel (type-safe, Pydantic validation)
class User(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    age: int

# Queries use same SQLAlchemy select() but with model classes
stmt = select(User).where(User.age > 25)

# Or use SQLAlchemy Core for dynamic tables:
from sqlalchemy import select, insert, and_, or_, desc, asc
stmt = select(table_obj.c[col] for col in cols).where(and_(*conditions))
```

**Benefits:**
- **Type safety** with Pydantic models
- **SQL injection protection** (automatic parameter binding)
- **Runtime validation** of data
- **Database agnostic** (easier to support other backends)
- **Query composition** and reusability
- **Better error messages** and IDE support
- **Simpler syntax** than raw SQLAlchemy

**Effort:** Medium (2-3 days) - Core infrastructure module

---

### 2. `mock_spark/dataframe/duckdb_materializer.py` (107 lines)

**Current State:** Raw SQL with f-strings

**Raw SQL Examples:**
```python
# Line 78-79
create_step_sql = f'CREATE TEMPORARY TABLE "{next_table}" AS {step_sql}'
self.connection.execute(create_step_sql)

# Line 85
final_query = f'SELECT * FROM "{current_table}"'

# Line 100
self.connection.execute(f'DROP TABLE IF EXISTS "{temp_table}"')
```

**Refactoring Opportunities:**

#### ✅ Recommended: Use SQLModel for type-safe tables or SQLAlchemy Core for dynamic tables
```python
# Option 1: SQLModel for known schemas (preferred)
from sqlmodel import SQLModel, Field, Session, select

class TempData(SQLModel, table=True):
    __tablename__ = name
    id: int = Field(primary_key=True)
    value: float

# Create table
SQLModel.metadata.create_all(engine)

# Insert from select:
with Session(engine) as session:
    results = session.exec(select_stmt).all()
    for row in results:
        session.add(TempData(**row))
    session.commit()

# Option 2: SQLAlchemy Core for dynamic schemas
from sqlalchemy import Table, select, insert
new_table = Table(name, metadata, *columns, prefixes=['TEMPORARY'])
new_table.create(engine)
stmt = insert(new_table).from_select(columns, select_stmt)
session.execute(stmt)
```

**Benefits:**
- **Type safety** and validation with SQLModel
- Proper **transaction handling**
- Better **error handling**
- Reusable table definitions

**Effort:** Low-Medium (1-2 days) - Small focused module

---

### 3. `mock_spark/storage/backends/duckdb.py` (430 lines)

**Current State:** Heavy raw SQL usage for table operations

**Raw SQL Examples:**
```python
# Line 67-68
create_sql = f"CREATE TABLE IF NOT EXISTS {self.name} ({', '.join(columns)})"
self.connection.execute(create_sql)

# Line 101
self.connection.execute(f"DROP TABLE IF EXISTS {self.name}")

# Line 111
self.connection.execute(f"INSERT INTO {self.name} VALUES ({placeholders})", values)

# Line 163-164
create_sql = f"CREATE TABLE {self.name} ({', '.join(columns)})"
self.connection.execute(create_sql)

# Line 172-174
query = f"SELECT * FROM {self.name} WHERE {filter_expr}"
result = self.connection.execute(query).fetchall()

# Line 242
self.connection.execute(f"DROP TABLE IF EXISTS {table}")
```

**Refactoring Opportunities:**

#### ✅ Recommended: Use SQLModel for type-safe ORM + SQLAlchemy Core for dynamic tables
```python
# Option 1: SQLModel for known schemas (PREFERRED)
from sqlmodel import SQLModel, Field, Session, select

class DuckDBTable(SQLModel, table=True):
    """Type-safe table with Pydantic validation."""
    __tablename__ = name
    id: int = Field(primary_key=True)
    name: str = Field(max_length=100)
    age: int = Field(ge=0)  # ge = greater than or equal

# Create table
SQLModel.metadata.create_all(engine)

# Insert with automatic validation
with Session(engine) as session:
    user = DuckDBTable(id=1, name="Alice", age=25)
    session.add(user)
    session.commit()

# Query with type-safe results
with Session(engine) as session:
    stmt = select(DuckDBTable).where(DuckDBTable.age > 18)
    results = session.exec(stmt).all()  # Returns List[DuckDBTable]

# Option 2: SQLAlchemy Core for dynamic schemas
from sqlalchemy import Table, Column, Integer, String, MetaData

metadata = MetaData()
table = Table(name, metadata, *dynamic_columns)
table.create(engine, checkfirst=True)
stmt = insert(table).values(data_dict)
session.execute(stmt)
```

**Additional Benefits:**
- **Pydantic validation**: Line 139-149 `_validate_data()` automatic with SQLModel
- **Type-safe queries**: Line 166-195 `query_data()` returns typed objects
- **Automatic type conversion** and validation
- **Better IDE support** with autocomplete
- **Runtime validation** catches errors early

**Effort:** Medium (2-3 days) - Core storage module, but well-structured

---

### 4. `mock_spark/dataframe/export.py` (151 lines)

**Current State:** Raw SQL for DuckDB table creation

**Raw SQL Examples:**
```python
# Line 96
connection.executemany(f"INSERT INTO {table_name} VALUES ({placeholders})", values_list)

# Line 119-120
create_sql = f"CREATE TABLE {table_name} ({', '.join(columns)})"
connection.execute(create_sql)
```

**Refactoring Opportunities:**

#### ✅ Recommended: Extract table creation to SQLAlchemy helper
```python
# Create reusable table creation utility:
class DuckDBTableFactory:
    @staticmethod
    def create_from_schema(
        name: str,
        schema: MockStructType,
        engine
    ) -> Table:
        columns = [
            Column(field.name, _mock_to_sqlalchemy_type(field.dataType))
            for field in schema.fields
        ]
        table = Table(name, MetaData(), *columns)
        table.create(engine, checkfirst=True)
        return table

# Then use:
table = DuckDBTableFactory.create_from_schema(name, df.schema, engine)
stmt = insert(table).values(data)
session.execute(stmt)
```

**Effort:** Low (0.5-1 day) - Small module with limited scope

---

### 5. `mock_spark/dataframe/sqlmodel_materializer.py` (200+ lines shown)

**Current State:** Mixed - Uses SQLAlchemy Table API but still has raw SQL

**Good Patterns Already in Use:**
```python
# Line 9: Already imports SQLAlchemy properly
from sqlalchemy import Table, Column, Integer, String, Float, Boolean

# Line 131-133: Good use of SQLAlchemy Table
table = Table(table_name, self.metadata, *columns)
table.create(self.engine, checkfirst=True)

# Line 144-147: Good use of insert() API
insert_stmt = table.insert().values(
    dict(zip([col.name for col in columns], values))
)
session.exec(insert_stmt)
```

**Remaining Raw SQL:**
```python
# Line 176-181: Still builds raw SQL for SELECT
sql = f"SELECT {', '.join(column_names)} FROM {source_table}"
if filter_expr is not None:
    filter_sql = str(filter_expr.compile(compile_kwargs={"literal_binds": True}))
    sql += f" WHERE {filter_sql}"
results = session.exec(text(sql)).all()
```

**Refactoring Opportunities:**

#### ✅ Recommended: Replace text() with select() API
```python
# Instead of:
sql = f"SELECT {', '.join(column_names)} FROM {source_table}"
results = session.exec(text(sql)).all()

# Use:
stmt = select(*[source_table_obj.c[col] for col in column_names])
if filter_expr is not None:
    stmt = stmt.where(filter_expr)
results = session.execute(stmt).all()
```

**Effort:** Low (1 day) - Already 70% there, just needs final conversion

---

## Prioritized Refactoring Plan

### Phase 1: Quick Wins (1-2 weeks)
**Impact:** High | **Effort:** Low-Medium

1. **Complete `sqlmodel_materializer.py` refactoring** ✅
   - Remove remaining `text()` calls
   - Use pure SQLAlchemy select/insert
   - **Files:** 1 | **Lines:** ~50 changes

2. **Refactor `export.py`** ✅
   - Extract DuckDBTableFactory
   - Replace raw CREATE/INSERT
   - **Files:** 1 | **Lines:** ~30 changes

3. **Update `duckdb_materializer.py`** ✅
   - Use Table.create() instead of raw CREATE
   - Use insert().from_select() patterns
   - **Files:** 1 | **Lines:** ~40 changes

### Phase 2: Core Infrastructure (2-3 weeks)
**Impact:** High | **Effort:** Medium

4. **Refactor `storage/backends/duckdb.py`** ⚠️
   - Replace all raw SQL in DuckDBTable
   - Use SQLAlchemy Table throughout
   - Add proper transaction management
   - **Files:** 1 | **Lines:** ~150 changes
   - **Note:** Core module, needs comprehensive testing

5. **Refactor `sql_builder.py`** ⚠️
   - Replace string-based building with SQLAlchemy Core
   - Maintain same public API
   - **Files:** 1 | **Lines:** ~200 changes
   - **Note:** Critical path, affects lazy evaluation

### Phase 3: Testing & Validation (1 week)
6. Add comprehensive unit tests for new SQLAlchemy code
7. Integration tests for DuckDB backend
8. Performance benchmarking (should be faster)

---

## Benefits Summary

### Security
- ✅ **SQL Injection Prevention**: Automatic parameter binding
- ✅ **Type Safety**: SQLAlchemy validates types at Python level
- ✅ **Schema Validation**: Catch errors before execution

### Maintainability
- ✅ **Reduced String Manipulation**: 80% less f-string concatenation
- ✅ **Better Error Messages**: SQLAlchemy provides clear stack traces
- ✅ **IDE Support**: Autocomplete for table columns
- ✅ **Refactoring Safety**: Static analysis tools can catch issues

### Performance
- ✅ **Query Optimization**: SQLAlchemy can cache compiled queries
- ✅ **Connection Pooling**: Built-in connection management
- ✅ **Batch Operations**: Better support for bulk inserts

### Extensibility
- ✅ **Database Agnostic**: Easy to support PostgreSQL, MySQL, etc.
- ✅ **Query Composition**: Build complex queries from components
- ✅ **Plugin Support**: SQLAlchemy's event system

---

## Code Examples: Before & After

### Example 1: Table Creation

**Before (sql_builder.py:313):**
```python
create_sql = f"CREATE TABLE \"{self.table_name}\" ({', '.join(columns)})"
self.connection.execute(create_sql)
```

**After:**
```python
from sqlalchemy import Table, Column, MetaData

table = Table(
    self.table_name,
    self.metadata,
    *[Column(name, dtype) for name, dtype in columns]
)
table.create(self.engine, checkfirst=True)
```

### Example 2: Data Insertion

**Before (duckdb.py:111):**
```python
placeholders = ", ".join(["?" for _ in values])
self.connection.execute(
    f"INSERT INTO {self.name} VALUES ({placeholders})",
    values
)
```

**After:**
```python
from sqlalchemy import insert

stmt = insert(self.table).values(
    dict(zip([field.name for field in self.schema.fields], values))
)
session.execute(stmt)
```

### Example 3: Filtered Query

**Before (duckdb.py:172-174):**
```python
if filter_expr:
    query = f"SELECT * FROM {self.name} WHERE {filter_expr}"
else:
    query = f"SELECT * FROM {self.name}"
result = self.connection.execute(query).fetchall()
```

**After:**
```python
from sqlalchemy import select

stmt = select(self.table)
if filter_expr:
    stmt = stmt.where(filter_expr)
result = session.execute(stmt).all()
```

### Example 4: Complex Query Building

**Before (sql_builder.py:227-290):**
```python
def build_sql(self) -> str:
    sql_parts = []
    select_clause = "SELECT " + ", ".join(self.select_columns)
    from_clause = f'FROM "{self.table_name}"'

    sql_parts.append(select_clause)
    sql_parts.append(from_clause)

    if self.where_conditions:
        where_clause = "WHERE " + " AND ".join(self.where_conditions)
        sql_parts.append(where_clause)

    if self.order_by_columns:
        order_clause = "ORDER BY " + ", ".join(self.order_by_columns)
        sql_parts.append(order_clause)

    return " ".join(sql_parts)
```

**After:**
```python
from sqlalchemy import select, and_, desc

def build_query(self) -> Select:
    stmt = select(*[self.table.c[col] for col in self.select_columns])

    if self.where_conditions:
        stmt = stmt.where(and_(*self.where_conditions))

    if self.order_by_columns:
        order_cols = [
            desc(self.table.c[col]) if desc_order
            else self.table.c[col]
            for col, desc_order in self.order_by_columns
        ]
        stmt = stmt.order_by(*order_cols)

    return stmt
```

---

## Risk Assessment

### Low Risk ✅
- `export.py` - Isolated functionality
- `sqlmodel_materializer.py` - Already partially refactored
- `duckdb_materializer.py` - Limited scope

### Medium Risk ⚠️
- `storage/backends/duckdb.py` - Core storage, but good test coverage
- `sql_builder.py` - Critical path, but can maintain API compatibility

### Mitigation Strategy
1. **Comprehensive unit tests** before refactoring
2. **Feature flags** to toggle between old/new implementations
3. **Gradual rollout** - one module at a time
4. **Performance benchmarks** to catch regressions
5. **Integration tests** with real PySpark compatibility tests

---

## Implementation Notes

### Helper Utilities to Create

1. **Type Converter**
```python
# mock_spark/storage/sqlalchemy_utils.py
def mock_type_to_sqlalchemy(mock_type):
    """Convert MockSpark types to SQLAlchemy types."""
    mapping = {
        StringType: String,
        IntegerType: Integer,
        LongType: BigInteger,
        DoubleType: Float,
        BooleanType: Boolean,
        DateType: Date,
        TimestampType: DateTime,
    }
    return mapping.get(type(mock_type), String)
```

2. **Table Factory**
```python
class SQLAlchemyTableFactory:
    @classmethod
    def from_mock_schema(cls, name: str, schema: MockStructType, metadata: MetaData):
        columns = [
            Column(
                field.name,
                mock_type_to_sqlalchemy(field.dataType),
                nullable=field.nullable
            )
            for field in schema.fields
        ]
        return Table(name, metadata, *columns)
```

3. **Query Builder Wrapper**
```python
class DuckDBQueryBuilder:
    """Wraps SQLAlchemy query building with DuckDB-specific optimizations."""

    def __init__(self, table: Table):
        self.table = table
        self._stmt = select(table)

    def filter(self, condition):
        self._stmt = self._stmt.where(condition)
        return self

    def select_columns(self, *cols):
        self._stmt = select(*[self.table.c[col] for col in cols])
        return self

    def build(self):
        return self._stmt
```

---

## Testing Strategy

### Unit Tests
- Test each refactored module in isolation
- Mock SQLAlchemy engine/session
- Verify SQL generation (use `str(stmt.compile())`)

### Integration Tests
- Run against actual DuckDB instance
- Compare results with raw SQL implementation
- Test edge cases (empty tables, NULL values, complex types)

### Compatibility Tests
- Existing PySpark compatibility tests must pass
- No breaking changes to public API
- Performance must be equal or better

### Migration Tests
- Create tests that run both old and new implementations
- Compare results for equality
- Catch any behavioral differences

---

## Conclusion

**Total Effort Estimate:** 4-6 weeks for complete refactoring
**Risk Level:** Low-Medium (with proper testing)
**Benefits:** High (security, maintainability, extensibility)

**Recommendation:** ✅ **Proceed with phased approach**

Start with Phase 1 (quick wins) to gain confidence, then tackle core infrastructure in Phase 2. The codebase already has good structure and partial SQLAlchemy usage, making this refactoring lower risk than a complete rewrite.

---

## Next Steps

1. **Review this analysis** with team
2. **Create JIRA tickets** for each phase
3. **Set up feature flag** for gradual rollout
4. **Write comprehensive tests** for existing behavior
5. **Start with `sqlmodel_materializer.py`** (easiest win)

---

## Appendix: Full File List

Files with raw SQL (ordered by priority for refactoring):

1. ✅ `mock_spark/dataframe/sqlmodel_materializer.py` (Priority: HIGH, Effort: LOW)
2. ✅ `mock_spark/dataframe/export.py` (Priority: HIGH, Effort: LOW)
3. ✅ `mock_spark/dataframe/duckdb_materializer.py` (Priority: HIGH, Effort: MEDIUM)
4. ⚠️ `mock_spark/storage/backends/duckdb.py` (Priority: HIGH, Effort: MEDIUM)
5. ⚠️ `mock_spark/dataframe/sql_builder.py` (Priority: MEDIUM, Effort: HIGH)
6. ℹ️ `mock_spark/session/sql/executor.py` (Priority: LOW, Effort: LOW)
   - Mostly high-level logic, minimal raw SQL

**Total Raw SQL Occurrences:** ~50-60 locations across 6 files

---

**Generated:** October 7, 2025
**Author:** Codebase Analysis Tool
**Version:** 1.0
