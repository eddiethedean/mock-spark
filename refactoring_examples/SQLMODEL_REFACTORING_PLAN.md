# SQLModel Refactoring Plan (Final - Incorporating Inspector Findings)

**Date:** October 7, 2025
**Status:** Ready for Implementation
**Confidence:** Very High

---

## 🎯 Executive Summary

**Goal:** Refactor Mock Spark to use SQLModel + SQLAlchemy instead of raw SQL

**Coverage:** **85-98% of SQL** can be refactored (better than expected!)

**Timeline:** 3-5 weeks

**Risk:** Low-Medium (using proven, mature libraries)

---

## 📊 Final Coverage Breakdown

| Approach | Coverage | Use Cases | Change from Original |
|----------|----------|-----------|---------------------|
| **SQLModel** | **85%** | Type-safe CRUD, known schemas | +5% (Inspector support) |
| **SQLAlchemy Core** | **13%** | Dynamic schemas, complex queries | Same |
| **Raw SQL** | **2%** | DuckDB extensions, SQL parser only | -3% (Inspector eliminates need) |

**Key Finding:** SQLAlchemy Inspector works perfectly with DuckDB via `duckdb-engine>=0.15.0`!

---

## ✅ What We Can Now Refactor (Updated)

### Previously Thought to Need Raw SQL → Now Can Use SQLAlchemy!

1. ✅ **List Tables** → `inspector.get_table_names()`
2. ✅ **Check Table Exists** → `inspector.has_table(name)`
3. ✅ **Get Column Metadata** → `inspector.get_columns(name)`
4. ✅ **Describe Tables** → `inspector.get_columns(name)`
5. ✅ **Reflect Existing Tables** → `Table(..., autoload_with=engine)`
6. ✅ **Get Schema Names** → `inspector.get_schema_names()`

---

## ✋ What Still Requires Raw SQL (Short List!)

### 1. DuckDB Extensions Only (~1% of code)
```python
# ✋ MUST USE RAW SQL: DuckDB-specific
connection.execute(text("INSTALL parquet"))
connection.execute(text("LOAD sqlite"))
connection.execute(text("PRAGMA memory_limit='4GB'"))
```

### 2. SQL Parser Implementation (~1% of code)
```python
# ✋ MUST USE RAW SQL: Implementing SQL execution engine
def execute_sql(query: str):
    # User provides arbitrary SQL
    return connection.execute(text(query))
```

### 3. Performance-Critical Bulk (Optional)
```python
# ⚡ OPTIONAL RAW SQL: 2-5x faster for >10k rows
if len(data) > 10000:
    connection.executemany("INSERT INTO ...", values)
```

**Total: ~2% of codebase must stay as raw SQL**

---

## 📋 File-by-File Refactoring Plan

### Phase 1: Quick Wins (1-2 weeks)

#### File 1: `mock_spark/dataframe/export.py`
**Complexity:** Low
**Effort:** 0.5-1 day
**Refactor:** 100%

**Changes:**
```python
# Current (Lines 119-120): Raw SQL
create_sql = f"CREATE TABLE {table_name} ({', '.join(columns)})"
connection.execute(create_sql)

# New: SQLAlchemy Core
from sqlalchemy import Table, Column, MetaData
from mock_spark.storage.sqlalchemy_helpers import create_table_from_mock_schema

metadata = MetaData()
table = create_table_from_mock_schema(table_name, df.schema, metadata)
table.create(engine, checkfirst=True)
```

**Benefits:**
- Type-safe table creation
- Automatic column type mapping
- No string manipulation

---

#### File 2: `mock_spark/dataframe/sqlmodel_materializer.py`
**Complexity:** Medium
**Effort:** 1-2 days
**Refactor:** 100%

**Changes:**
- Replace remaining `text()` calls with `select()`, `insert()`
- Use SQLAlchemy Core for dynamic operations
- Keep table creation with SQLAlchemy Table API

**Current Issues:**
```python
# Line 176: Raw SQL
sql = f"SELECT {', '.join(column_names)} FROM {source_table}"
results = session.exec(text(sql)).all()
```

**New Approach:**
```python
# Use SQLAlchemy select()
from sqlalchemy import select

stmt = select(*[source_table_obj.c[col] for col in column_names])
results = session.execute(stmt).all()
```

---

#### File 3: `mock_spark/dataframe/duckdb_materializer.py`
**Complexity:** Medium
**Effort:** 1-2 days
**Refactor:** 100%

**Changes:**
- Use SQLAlchemy Core Table() API
- Replace `CREATE TEMPORARY TABLE` with Table creation
- Use `insert().from_select()` for data movement

**Current:**
```python
# Line 78: Raw SQL
create_step_sql = f'CREATE TEMPORARY TABLE "{next_table}" AS {step_sql}'
self.connection.execute(create_step_sql)
```

**New:**
```python
# SQLAlchemy approach
from sqlalchemy import Table, insert

# Create temp table
temp_table = Table(next_table, metadata, *columns, prefixes=['TEMPORARY'])
temp_table.create(engine)

# Insert from select
stmt = insert(temp_table).from_select(columns, select_stmt)
session.execute(stmt)
```

---

### Phase 2: Core Infrastructure (1-2 weeks)

#### File 4: `mock_spark/storage/backends/duckdb.py` ⭐ **MAJOR UPDATE**
**Complexity:** Medium
**Effort:** 2-3 days (was 2-3 weeks!)
**Refactor:** **95%** (was 70%)

**Major Changes from Inspector Support:**

```python
from sqlalchemy import create_engine, inspect, Table, Column, MetaData, select, insert, text
from sqlmodel import Session

class DuckDBStorageManager:
    def __init__(self, db_path=None):
        """Initialize with SQLAlchemy engine."""
        url = f'duckdb:///{db_path}' if db_path else 'duckdb:///:memory:'
        self.engine = create_engine(url)
        self.metadata = MetaData()
        self._enable_extensions()  # Only place we use raw SQL

    # ✅ REFACTOR: Use Inspector (was raw SQL)
    def list_tables(self, schema="default"):
        """List tables using SQLAlchemy Inspector."""
        inspector = inspect(self.engine)
        return inspector.get_table_names()

    # ✅ REFACTOR: Use Inspector (was raw SQL)
    def table_exists(self, schema, table):
        """Check if table exists using Inspector."""
        inspector = inspect(self.engine)
        return inspector.has_table(table)

    # ✅ REFACTOR: Use Inspector (was raw SQL)
    def get_table_schema(self, schema, table):
        """Get table schema using Inspector."""
        inspector = inspect(self.engine)
        columns = inspector.get_columns(table)
        return self._convert_to_mock_schema(columns)

    # ✅ REFACTOR: Use reflection (was manual building)
    def get_table(self, schema, table):
        """Get table object using reflection."""
        return Table(table, self.metadata, autoload_with=self.engine)

    # ✅ REFACTOR: Use SQLAlchemy (was raw SQL)
    def create_table(self, schema, table, fields):
        """Create table using SQLAlchemy."""
        columns = [
            Column(field.name, self._to_sqlalchemy_type(field.dataType))
            for field in fields
        ]
        table_obj = Table(table, self.metadata, *columns)
        table_obj.create(self.engine, checkfirst=True)
        return table_obj

    # ✅ REFACTOR: Use SQLAlchemy with smart batching
    def insert_data(self, schema, table, data, mode="append"):
        """Insert data with automatic batching."""
        table_obj = self.get_table(schema, table)

        if len(data) < 1000:
            # Small batch: Use SQLAlchemy for type safety
            with Session(self.engine) as session:
                stmt = insert(table_obj).values(data)
                session.execute(stmt)
                session.commit()
        else:
            # Large batch: Use bulk insert for performance
            with self.engine.begin() as conn:
                conn.execute(insert(table_obj), data)

    # ✅ REFACTOR: Use SQLAlchemy select()
    def query_table(self, schema, table, filter_expr=None):
        """Query table using SQLAlchemy."""
        table_obj = self.get_table(schema, table)

        with Session(self.engine) as session:
            stmt = select(table_obj)
            if filter_expr is not None:
                stmt = stmt.where(filter_expr)
            return session.execute(stmt).all()

    # ✋ KEEP RAW SQL: DuckDB extensions only
    def _enable_extensions(self):
        """Enable DuckDB extensions (only raw SQL in this file!)."""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("INSTALL sqlite"))
                conn.execute(text("LOAD sqlite"))
        except:
            pass  # Extensions might not be available
```

**Lines of Raw SQL:** ~10 (down from ~150)
**Reduction:** 93%!

---

#### File 5: `mock_spark/dataframe/sql_builder.py`
**Complexity:** Medium-High
**Effort:** 2-3 days
**Refactor:** 100%

**Strategy:** Replace string-based SQL building with SQLAlchemy Core

**Current Architecture:**
```python
class SQLQueryBuilder:
    def build_sql(self) -> str:
        """Returns SQL string."""
        sql_parts = []
        select_clause = "SELECT " + ", ".join(self.select_columns)
        from_clause = f'FROM "{self.table_name}"'
        # ... more string building
        return " ".join(sql_parts)
```

**New Architecture:**
```python
from sqlalchemy import select, and_, desc, func, Table, MetaData

class SQLAlchemyQueryBuilder:
    """Type-safe query builder using SQLAlchemy Core."""

    def __init__(self, table: Table):
        self.table = table
        self._stmt = select(table)
        self._filters = []
        self._order_by = []

    def add_filter(self, condition):
        """Add WHERE condition (SQLAlchemy expression)."""
        self._filters.append(condition)
        return self

    def add_select(self, columns):
        """Add SELECT columns."""
        cols = [self.table.c[col] for col in columns]
        self._stmt = select(*cols)
        return self

    def add_order_by(self, columns):
        """Add ORDER BY."""
        order_cols = [
            desc(self.table.c[col]) if descending
            else self.table.c[col]
            for col in columns
        ]
        self._stmt = self._stmt.order_by(*order_cols)
        return self

    def add_group_by(self, columns):
        """Add GROUP BY."""
        group_cols = [self.table.c[col] for col in columns]
        self._stmt = self._stmt.group_by(*group_cols)
        return self

    def build(self):
        """Build final SQLAlchemy statement."""
        stmt = self._stmt
        if self._filters:
            stmt = stmt.where(and_(*self._filters))
        return stmt

    def execute(self, session):
        """Execute and return results."""
        return session.execute(self.build()).all()
```

**Benefits:**
- Type-safe query building
- Composable operations
- No string manipulation
- Database agnostic
- Better error messages

---

### Phase 3: Optional / Keep As-Is

#### File 6: `mock_spark/session/sql/executor.py`
**Complexity:** N/A
**Effort:** 0 days
**Refactor:** 0% (intentionally kept as raw SQL)

**Reason:** This IS the SQL parser implementation. Raw SQL is appropriate here.

```python
class MockSQLExecutor:
    def execute(self, query: str):
        """
        ✋ KEEP RAW SQL: We're implementing a SQL execution engine.
        User provides arbitrary SQL, we need to parse and execute it.
        """
        ast = self.parser.parse(query)

        # Execute using DuckDB's SQL engine
        with self.engine.connect() as conn:
            result = conn.execute(text(query))
            return self._convert_to_dataframe(result)
```

---

## 🛠️ Helper Utilities to Create

### 1. `mock_spark/storage/sqlalchemy_helpers.py`
```python
"""SQLAlchemy helper functions for Mock Spark."""

from sqlalchemy import Table, Column, Integer, BigInteger, String, Float, Boolean, inspect
from sqlalchemy import Date, DateTime, MetaData
from typing import List, Dict, Any
from mock_spark.spark_types import MockStructType

def create_table_from_mock_schema(
    name: str,
    schema: MockStructType,
    metadata: MetaData
) -> Table:
    """Create SQLAlchemy Table from MockSpark schema."""
    columns = []
    for field in schema.fields:
        col_type = mock_type_to_sqlalchemy(field.dataType)
        columns.append(Column(field.name, col_type, nullable=field.nullable))

    return Table(name, metadata, *columns)

def mock_type_to_sqlalchemy(mock_type):
    """Convert MockSpark type to SQLAlchemy type."""
    type_mapping = {
        "StringType": String,
        "IntegerType": Integer,
        "LongType": BigInteger,
        "DoubleType": Float,
        "FloatType": Float,
        "BooleanType": Boolean,
        "DateType": Date,
        "TimestampType": DateTime,
    }
    type_name = type(mock_type).__name__
    return type_mapping.get(type_name, String)

def list_all_tables(engine) -> List[str]:
    """List all tables using Inspector."""
    inspector = inspect(engine)
    return inspector.get_table_names()

def table_exists(engine, table_name: str) -> bool:
    """Check if table exists using Inspector."""
    inspector = inspect(engine)
    return inspector.has_table(table_name)

def get_table_columns(engine, table_name: str) -> List[Dict[str, Any]]:
    """Get table columns using Inspector."""
    inspector = inspect(engine)
    return inspector.get_columns(table_name)

def reflect_table(engine, table_name: str, metadata: MetaData) -> Table:
    """Reflect existing table into SQLAlchemy Table object."""
    return Table(table_name, metadata, autoload_with=engine)
```

### 2. `mock_spark/storage/performance_config.py`
```python
"""Performance configuration for SQL operations."""

class SQLPerformanceConfig:
    """Configure when to use raw SQL for performance."""

    # Batch size thresholds
    SMALL_BATCH_THRESHOLD = 100
    MEDIUM_BATCH_THRESHOLD = 1000
    LARGE_BATCH_THRESHOLD = 10000

    # Use SQLAlchemy for small/medium batches
    USE_SQLALCHEMY_FOR_SMALL = True
    USE_SQLALCHEMY_FOR_MEDIUM = True

    # Use raw SQL for very large batches (performance)
    USE_RAW_SQL_FOR_LARGE = True

    @classmethod
    def should_use_raw_sql(cls, batch_size: int) -> bool:
        """Determine if raw SQL should be used for this batch size."""
        return batch_size > cls.LARGE_BATCH_THRESHOLD and cls.USE_RAW_SQL_FOR_LARGE
```

---

## 📅 Implementation Timeline

### Week 1-2: Phase 1 (Quick Wins)
- ✅ Day 1-2: `export.py` (0.5-1 day)
- ✅ Day 3-5: `sqlmodel_materializer.py` (1-2 days)
- ✅ Day 6-8: `duckdb_materializer.py` (1-2 days)
- ✅ Day 9-10: Create helper utilities

**Deliverable:** 3 files refactored, utilities ready

### Week 3-4: Phase 2 (Core Infrastructure)
- ✅ Day 11-15: `storage/backends/duckdb.py` (2-3 days)
- ✅ Day 16-20: `sql_builder.py` (2-3 days)

**Deliverable:** Core infrastructure refactored

### Week 5: Phase 3 (Testing & Documentation)
- ✅ Day 21-22: Unit tests for all refactored code
- ✅ Day 23-24: Integration tests
- ✅ Day 25: Performance benchmarks
- ✅ Day 26-27: Documentation updates
- ✅ Day 28: Code review and cleanup

**Total:** 3-5 weeks (20-28 days)

---

## ✅ Success Criteria

### Technical Metrics
- [ ] **85-98% of SQL** uses SQLModel/SQLAlchemy
- [ ] **100% test coverage** maintained
- [ ] **No performance regression** (or improvements with bulk ops)
- [ ] **All 396 tests** pass

### Code Quality Metrics
- [ ] **Type hints** on all new code
- [ ] **Docstrings** with examples
- [ ] **Error handling** improved
- [ ] **SQL injection** vulnerabilities eliminated

### Documentation Metrics
- [ ] **All raw SQL documented** with justification
- [ ] **Migration guide** for contributors
- [ ] **API docs** updated
- [ ] **Examples** updated

---

## 🧪 Testing Strategy

### 1. Unit Tests (Per File)
```python
def test_inspector_list_tables():
    """Test Inspector can list tables."""
    engine = create_engine('duckdb:///:memory:')
    manager = DuckDBStorageManager()

    # Create test tables
    manager.create_table("default", "users", schema)
    manager.create_table("default", "orders", schema)

    # Test Inspector
    tables = manager.list_tables()
    assert "users" in tables
    assert "orders" in tables

def test_sqlalchemy_insert_vs_raw_sql():
    """Verify SQLAlchemy produces same results as raw SQL."""
    # Setup
    engine = create_engine('duckdb:///:memory:')

    # Test with SQLAlchemy
    sqlalchemy_result = insert_with_sqlalchemy(engine, data)

    # Test with raw SQL (reference)
    raw_sql_result = insert_with_raw_sql(engine, data)

    # Compare
    assert sqlalchemy_result == raw_sql_result
```

### 2. Integration Tests
- Test full DataFrame operations with new backend
- Verify lazy evaluation works correctly
- Test window functions with new query builder

### 3. Performance Tests
```python
@pytest.mark.performance
def test_bulk_insert_performance():
    """Verify bulk insert performance."""
    sizes = [100, 1000, 10000]

    for size in sizes:
        data = generate_test_data(size)

        # Time SQLAlchemy
        sqlalchemy_time = time_operation(lambda: insert_sqlalchemy(data))

        # Time raw SQL
        raw_sql_time = time_operation(lambda: insert_raw_sql(data))

        # Acceptable overhead: 20% for small batches, 0% for large
        if size < 1000:
            assert sqlalchemy_time < raw_sql_time * 1.2
        else:
            # Should use raw SQL for large batches
            assert raw_sql_time < sqlalchemy_time * 1.2
```

### 4. Compatibility Tests
- All existing PySpark compatibility tests must pass
- No breaking changes to public API

---

## 📚 Documentation Updates

### New Documentation
1. **`docs/sqlalchemy_migration.md`** - Migration guide for contributors
2. **`docs/sqlmodel_best_practices.md`** - When to use SQLModel vs Core
3. **`docs/inspector_usage.md`** - Using Inspector for metadata

### Updated Documentation
1. `docs/storage_serialization_guide.md` - Update with SQLAlchemy examples
2. `docs/api_reference.md` - Update storage backend API
3. `README.md` - Note SQLModel/SQLAlchemy usage

---

## 🎯 Risk Mitigation

### Risk 1: Breaking Changes
**Mitigation:**
- Comprehensive unit tests before refactoring
- Feature flag for gradual rollout
- Keep old implementation temporarily for comparison

### Risk 2: Performance Regression
**Mitigation:**
- Benchmark before/after for each file
- Use raw SQL for bulk operations if needed
- Performance tests in CI/CD

### Risk 3: Team Learning Curve
**Mitigation:**
- Comprehensive documentation
- Code examples for common patterns
- Pair programming for complex refactoring

---

## 🎉 Expected Benefits

### Immediate Benefits (Week 1-2)
- ✅ Better type safety in 3 files
- ✅ SQL injection protection
- ✅ Cleaner, more maintainable code

### Medium-term Benefits (Week 3-5)
- ✅ 85-98% of SQL is type-safe
- ✅ Database-agnostic core (easier to support other backends)
- ✅ Better error messages
- ✅ Easier testing with mocked engines

### Long-term Benefits (Post-Release)
- ✅ Easier to add new features
- ✅ Better contributor experience
- ✅ More reliable codebase
- ✅ Potential performance improvements

---

## 📞 Key Decisions

### Decision 1: SQLModel vs SQLAlchemy Core
**Decision:** Use SQLModel for known schemas (85%), SQLAlchemy Core for dynamic schemas (13%)

**Rationale:**
- SQLModel provides better type safety
- Pydantic validation catches errors early
- SQLAlchemy Core is flexible for dynamic needs

### Decision 2: Inspector for Metadata
**Decision:** Use SQLAlchemy Inspector instead of raw SQL

**Rationale:**
- Works perfectly with DuckDB
- Type-safe and database-agnostic
- Better error messages
- Easier to test

### Decision 3: Keep Some Raw SQL
**Decision:** Keep raw SQL for DuckDB extensions and SQL parser only (~2%)

**Rationale:**
- No SQLAlchemy equivalent for extensions
- SQL parser SHOULD use raw SQL (it's implementing SQL)
- Pragmatic approach - don't force refactoring where it doesn't fit

---

## ✅ Approval Checklist

- [ ] Team reviewed and approved plan
- [ ] Timeline acceptable (3-5 weeks)
- [ ] Risk mitigation strategies agreed upon
- [ ] Success criteria defined
- [ ] Testing strategy approved
- [ ] Documentation plan reviewed
- [ ] Ready to start Phase 1

---

**Last Updated:** October 7, 2025
**Status:** Ready for Implementation
**Next Step:** Start Phase 1 with `export.py`
