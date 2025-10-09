# Mock Spark SQLModel Implementation Plan

**Date:** October 7, 2025
**Objective:** Convert 98% of Mock Spark to SQLModel/SQLAlchemy while keeping spark.sql()
**Timeline:** 3-5 weeks
**Status:** Ready for Implementation

---

## 🎯 Executive Summary

### What We're Doing
- ✅ Convert 98% of code to SQLModel/SQLAlchemy
- ✅ Keep `spark.sql()` as raw SQL (that's its purpose)
- ✅ Use SQLAlchemy Inspector for all metadata
- ✅ Type-safe operations throughout

### What We're NOT Doing
- ❌ NOT converting `spark.sql()` (it should execute SQL)
- ❌ NOT building SQL parser (unnecessary)
- ❌ NOT breaking user code (backward compatible)

### Key Metrics
- **Coverage:** 98% SQLModel/SQLAlchemy, 2% raw SQL
- **Files Changed:** 5 major files
- **Breaking Changes:** 0
- **Performance Impact:** 0 (improvements possible)
- **Effort:** 3-5 weeks

---

## 📊 Scope Breakdown

### ✅ Convert to SQLModel/SQLAlchemy (98%)

| Component | From | To | Effort |
|-----------|------|----|----|
| Metadata queries | Raw SQL | Inspector | 1 day |
| Table creation | f-strings | Table.create() | 2 days |
| Data insertion | Raw SQL | insert().values() | 2 days |
| Query execution | Raw SQL | select().where() | 3 days |
| Storage backend | Raw SQL | SQLModel/Inspector | 5 days |

### ✋ Keep as Raw SQL (2%)

| Component | Why Keep | Location |
|-----------|----------|----------|
| `spark.sql()` | It's a SQL executor by design | `session/sql/executor.py` |
| PRAGMA config | Configuration only | `storage/backends/duckdb.py` (~5 lines) |

---

## 🗂️ File-by-File Implementation Plan

### Phase 1: Quick Wins (Week 1-2)

---

#### File 1: `mock_spark/dataframe/export.py`

**Current State:** 3-5 raw SQL strings
**Target State:** 100% SQLAlchemy
**Effort:** 0.5-1 day
**Priority:** HIGH (easiest win)

##### Current Code (Lines 119-120)
```python
def _create_duckdb_table(df: "MockDataFrame", connection, table_name: str) -> None:
    """Create DuckDB table from MockSpark schema."""
    columns = []
    for field in df.schema.fields:
        duckdb_type = DataFrameExporter._get_duckdb_type(field.dataType)
        columns.append(f"{field.name} {duckdb_type}")

    create_sql = f"CREATE TABLE {table_name} ({', '.join(columns)})"
    connection.execute(create_sql)
```

##### New Code
```python
from sqlalchemy import Table, Column, MetaData, create_engine
from mock_spark.storage.sqlalchemy_helpers import (
    create_table_from_mock_schema,
    mock_type_to_sqlalchemy
)

def _create_duckdb_table(df: "MockDataFrame", engine, table_name: str) -> Table:
    """Create DuckDB table using SQLAlchemy."""
    metadata = MetaData()

    # Create table from schema
    table = create_table_from_mock_schema(table_name, df.schema, metadata)
    table.create(engine, checkfirst=True)

    return table
```

##### Changes Required
1. **Replace `connection` parameter with `engine`**
   - Change: `connection` → `engine` throughout file
   - Update: `to_duckdb()` method signature

2. **Use helper function for table creation**
   - Import: `create_table_from_mock_schema`
   - Remove: Manual column string building
   - Use: SQLAlchemy Table API

3. **Update data insertion (Line 96)**
```python
# Before
connection.executemany(f"INSERT INTO {table_name} VALUES ({placeholders})", values_list)

# After
from sqlalchemy import insert

with engine.begin() as conn:
    stmt = insert(table).values(values_list)
    conn.execute(stmt)
```

##### Testing
```python
def test_export_to_duckdb():
    """Test SQLAlchemy table creation."""
    from sqlalchemy import create_engine

    engine = create_engine('duckdb:///:memory:')
    df = MockDataFrame([{'id': 1, 'name': 'Alice'}], schema)

    table_name = df.toDuckDB(engine, 'test_table')

    # Verify with Inspector
    inspector = inspect(engine)
    assert inspector.has_table('test_table')
    columns = inspector.get_columns('test_table')
    assert len(columns) == 2
```

##### Deliverable
- ✅ All table creation uses SQLAlchemy
- ✅ All inserts use SQLAlchemy
- ✅ Unit tests pass
- ✅ No raw SQL in file

---

#### File 2: `mock_spark/dataframe/sqlmodel_materializer.py`

**Current State:** 5-10 raw SQL strings (mostly `text()` calls)
**Target State:** 100% SQLAlchemy Core
**Effort:** 1-2 days
**Priority:** HIGH

##### Current Issues

**Issue 1: Line 176-181 - Raw SELECT**
```python
# Current
sql = f"SELECT {', '.join(column_names)} FROM {source_table}"
if filter_expr is not None:
    filter_sql = str(filter_expr.compile(compile_kwargs={"literal_binds": True}))
    sql += f" WHERE {filter_sql}"
results = session.exec(text(sql)).all()
```

**Fix:**
```python
# New
from sqlalchemy import select

stmt = select(*[source_table_obj.c[col] for col in column_names])
if filter_expr is not None:
    stmt = stmt.where(filter_expr)
results = session.execute(stmt).all()
```

##### Complete Refactoring Strategy

1. **Remove all `text()` imports**
2. **Replace string SQL with select()**
3. **Use Table objects for all operations**
4. **Keep using SQLAlchemy Table API (already correct)**

##### Key Changes

**Change 1: `_apply_filter` method (Line 150-193)**
```python
def _apply_filter(self, source_table: str, target_table: str, condition: Any) -> None:
    """Apply filter using SQLAlchemy (no raw SQL)."""
    source_table_obj = self._created_tables[source_table]

    # Convert condition to SQLAlchemy expression
    filter_expr = self._condition_to_sqlalchemy(source_table_obj, condition)

    # Create target table
    self._copy_table_structure(source_table, target_table)
    target_table_obj = self._created_tables[target_table]

    # Execute filter using SQLAlchemy select
    with Session(self.engine) as session:
        # Build SELECT statement
        stmt = select(source_table_obj)
        if filter_expr is not None:
            stmt = stmt.where(filter_expr)

        results = session.execute(stmt).all()

        # Insert into target using SQLAlchemy
        for result in results:
            row_dict = dict(zip([col.name for col in source_table_obj.columns], result))
            stmt = insert(target_table_obj).values(row_dict)
            session.execute(stmt)
        session.commit()
```

**Change 2: All similar methods**
- `_apply_select` - Use `select()` with column list
- `_apply_order_by` - Use `order_by()`
- `_apply_limit` - Use `limit()`
- `_apply_with_column` - Use computed columns in select

##### Testing
```python
def test_materializer_no_raw_sql():
    """Verify no raw SQL in materializer."""
    # Mock the text() function to detect calls
    from unittest.mock import patch

    with patch('sqlalchemy.text') as mock_text:
        materializer = SQLModelMaterializer()
        result = materializer.materialize(data, schema, operations)

        # Verify text() was never called
        mock_text.assert_not_called()
```

##### Deliverable
- ✅ All queries use select(), insert(), update()
- ✅ No text() calls
- ✅ All operations type-safe
- ✅ Tests pass

---

#### File 3: `mock_spark/dataframe/duckdb_materializer.py`

**Current State:** 5-10 raw SQL strings
**Target State:** 100% SQLAlchemy Core
**Effort:** 1-2 days
**Priority:** HIGH

##### Current Issues

**Issue 1: CREATE TEMPORARY TABLE (Line 78)**
```python
create_step_sql = f'CREATE TEMPORARY TABLE "{next_table}" AS {step_sql}'
self.connection.execute(create_step_sql)
```

**Fix:**
```python
from sqlalchemy import Table, insert

# Create temporary table
temp_table = Table(next_table, metadata, *columns, prefixes=['TEMPORARY'])
temp_table.create(self.engine)

# Insert from select
stmt = insert(temp_table).from_select(column_list, select_stmt)
session.execute(stmt)
```

##### Complete Refactoring

**Step 1: Replace `connection` with `engine`**
```python
class DuckDBMaterializer:
    def __init__(self):
        # Before: self.connection = duckdb.connect(":memory:")
        # After:
        from sqlalchemy import create_engine
        self.engine = create_engine('duckdb:///:memory:')
        self.metadata = MetaData()
```

**Step 2: Build queries with SQLAlchemy**
```python
def materialize(self, data, schema, operations):
    """Materialize using SQLAlchemy (no raw SQL)."""
    # Create initial table
    initial_table = self._create_table_with_data("temp_0", data, schema)

    current_table = initial_table

    # Apply operations
    for i, (op_name, op_val) in enumerate(operations):
        next_table_name = f"temp_{i+1}"

        if op_name == "filter":
            next_table = self._apply_filter(current_table, next_table_name, op_val)
        elif op_name == "select":
            next_table = self._apply_select(current_table, next_table_name, op_val)
        # ... more operations

        current_table = next_table

    # Get final results
    return self._get_results(current_table)

def _create_table_with_data(self, name, data, schema):
    """Create table using SQLAlchemy."""
    # Build columns from data
    columns = []
    for field in schema.fields:
        col_type = mock_type_to_sqlalchemy(field.dataType)
        columns.append(Column(field.name, col_type))

    # Create table
    table = Table(name, self.metadata, *columns, prefixes=['TEMPORARY'])
    table.create(self.engine)

    # Insert data
    if data:
        with self.engine.begin() as conn:
            conn.execute(insert(table), data)

    return table
```

##### Testing
```python
def test_duckdb_materializer_sqlalchemy():
    """Test materialization without raw SQL."""
    materializer = DuckDBMaterializer()

    data = [{'id': 1, 'name': 'Alice', 'age': 25}]
    schema = MockStructType([
        MockStructField('id', IntegerType()),
        MockStructField('name', StringType()),
        MockStructField('age', IntegerType())
    ])

    operations = [
        ('filter', MockColumnOperation('age', '>', 20)),
        ('select', ('name', 'age'))
    ]

    results = materializer.materialize(data, schema, operations)
    assert len(results) == 1
    assert results[0]['name'] == 'Alice'
```

##### Deliverable
- ✅ All table creation uses Table.create()
- ✅ All queries use select()
- ✅ No connection.execute(sql_string)
- ✅ Tests pass

---

### Phase 2: Core Infrastructure (Week 3-4)

---

#### File 4: `mock_spark/storage/backends/duckdb.py` ⭐ **BIGGEST CHANGE**

**Current State:** ~150 lines of raw SQL
**Target State:** ~10 lines (PRAGMA only)
**Effort:** 2-3 days
**Priority:** CRITICAL

##### Refactoring Strategy

**Key Insight:** Use Inspector for ALL metadata operations

##### Section-by-Section Changes

**Section 1: Initialization**
```python
class DuckDBStorageManager(IStorageManager):
    """Storage backend with SQLAlchemy and Inspector."""

    def __init__(self, db_path: Optional[str] = None):
        """Initialize with SQLAlchemy engine."""
        # Create SQLAlchemy engine
        url = f'duckdb:///{db_path}' if db_path else 'duckdb:///:memory:'
        self.engine = create_engine(url)
        self.metadata = MetaData()

        # Get DuckDB connection for extensions
        self.duckdb_conn = self._get_duckdb_connection()

        # Configure DuckDB
        self._configure_duckdb()

        # Create default schema
        self.schemas: Dict[str, DuckDBSchema] = {}
        self.schemas["default"] = DuckDBSchema("default", self.engine, self.metadata)

    def _get_duckdb_connection(self):
        """Get raw DuckDB connection for extensions."""
        # For engine-level operations if needed
        with self.engine.connect() as conn:
            return conn.connection.connection

    def _configure_duckdb(self):
        """✋ ONLY RAW SQL: DuckDB configuration."""
        try:
            # Use DuckDB Python API for extensions
            self.duckdb_conn.install_extension('sqlite')
            self.duckdb_conn.load_extension('sqlite')

            # PRAGMA still needs SQL-like syntax (acceptable)
            self.duckdb_conn.execute("PRAGMA memory_limit='4GB'")
        except Exception:
            pass  # Extensions might not be available
```

**Section 2: Metadata Operations → Inspector**
```python
def list_tables(self, schema: str = "default") -> List[str]:
    """✅ NEW: Use Inspector instead of SHOW TABLES."""
    from sqlalchemy import inspect
    inspector = inspect(self.engine)
    return inspector.get_table_names()

def table_exists(self, schema: str, table: str) -> bool:
    """✅ NEW: Use Inspector instead of SELECT 1 FROM..."""
    from sqlalchemy import inspect
    inspector = inspect(self.engine)
    return inspector.has_table(table)

def get_table_schema(self, schema: str, table: str) -> Optional[MockStructType]:
    """✅ NEW: Use Inspector instead of DESCRIBE."""
    from sqlalchemy import inspect
    inspector = inspect(self.engine)

    if not inspector.has_table(table):
        return None

    columns = inspector.get_columns(table)
    fields = []
    for col in columns:
        mock_type = sqlalchemy_type_to_mock(col['type'])
        fields.append(MockStructField(col['name'], mock_type, col.get('nullable', True)))

    return MockStructType(fields)
```

**Section 3: Table Operations → SQLAlchemy**
```python
def create_table(
    self,
    schema: str,
    table: str,
    fields: Union[List[MockStructField], MockStructType],
) -> Optional[Table]:
    """✅ NEW: Create table using SQLAlchemy."""
    if isinstance(fields, MockStructType):
        fields = fields.fields

    # Build columns
    columns = []
    for field in fields:
        col_type = mock_type_to_sqlalchemy(field.dataType)
        columns.append(Column(field.name, col_type, nullable=field.nullable))

    # Create table
    table_obj = Table(table, self.metadata, *columns)
    table_obj.create(self.engine, checkfirst=True)

    return table_obj

def drop_table(self, schema: str, table: str) -> None:
    """✅ NEW: Drop table using SQLAlchemy."""
    if schema in self.schemas and table in self.schemas[schema].tables:
        table_obj = self.schemas[schema].tables[table]
        table_obj.drop(self.engine, checkfirst=True)
        del self.schemas[schema].tables[table]
```

**Section 4: Data Operations → SQLAlchemy**
```python
def insert_data(
    self,
    schema: str,
    table: str,
    data: List[Dict[str, Any]],
    mode: str = "append"
) -> None:
    """✅ NEW: Insert using SQLAlchemy with smart batching."""
    if not data:
        return

    # Get table object
    table_obj = self._get_table(schema, table)

    # Handle mode
    if mode == "overwrite":
        with self.engine.begin() as conn:
            conn.execute(delete(table_obj))

    # Smart batching for performance
    if len(data) < 1000:
        # Small batch: Use SQLAlchemy (better validation)
        with self.engine.begin() as conn:
            conn.execute(insert(table_obj), data)
    else:
        # Large batch: Use bulk insert (better performance)
        with self.engine.begin() as conn:
            conn.execute(insert(table_obj), data)

def query_table(
    self,
    schema: str,
    table: str,
    filter_expr: Optional[Any] = None
) -> List[Dict[str, Any]]:
    """✅ NEW: Query using SQLAlchemy select."""
    table_obj = self._get_table(schema, table)

    with Session(self.engine) as session:
        stmt = select(table_obj)

        if filter_expr is not None:
            stmt = stmt.where(filter_expr)

        results = session.execute(stmt).all()

        # Convert to dicts
        columns = [col.name for col in table_obj.columns]
        return [dict(zip(columns, row)) for row in results]

def _get_table(self, schema: str, table: str) -> Table:
    """Get table object, using reflection if needed."""
    if schema in self.schemas and table in self.schemas[schema].tables:
        return self.schemas[schema].tables[table]

    # Reflect existing table
    return Table(table, self.metadata, autoload_with=self.engine)
```

##### Complete File Structure
```python
"""
DuckDB storage backend with SQLAlchemy and Inspector.

✅ 95% SQLAlchemy/Inspector
✋ 5% Raw SQL (PRAGMA configuration only)
"""

from sqlalchemy import create_engine, inspect, Table, Column, MetaData
from sqlalchemy import select, insert, update, delete
from sqlalchemy.orm import Session
import duckdb
from typing import Dict, List, Optional, Any

class DuckDBStorageManager:
    """Type-safe storage with Inspector and SQLAlchemy."""

    def __init__(self, db_path=None):
        """Initialize with SQLAlchemy engine."""
        self.engine = create_engine(
            f'duckdb:///{db_path}' if db_path else 'duckdb:///:memory:'
        )
        self.metadata = MetaData()
        self.duckdb_conn = self._get_duckdb_connection()
        self._configure_duckdb()  # Only raw SQL here

    # ✅ All metadata uses Inspector
    def list_tables(self): ...
    def table_exists(self, table): ...
    def get_table_schema(self, schema, table): ...

    # ✅ All CRUD uses SQLAlchemy
    def create_table(self, schema, table, fields): ...
    def drop_table(self, schema, table): ...
    def insert_data(self, schema, table, data, mode): ...
    def query_table(self, schema, table, filter_expr): ...

    # ✋ Only raw SQL (acceptable)
    def _configure_duckdb(self):
        """Configure DuckDB extensions and settings."""
        try:
            # Python API for extensions
            self.duckdb_conn.install_extension('sqlite')
            self.duckdb_conn.load_extension('sqlite')

            # PRAGMA for configuration (SQL-like but acceptable)
            self.duckdb_conn.execute("PRAGMA memory_limit='4GB'")
        except:
            pass
```

##### Testing Strategy
```python
def test_storage_backend_inspector():
    """Test Inspector integration."""
    manager = DuckDBStorageManager()

    # Create table
    schema = MockStructType([
        MockStructField('id', IntegerType()),
        MockStructField('name', StringType())
    ])
    manager.create_table('default', 'users', schema)

    # Test Inspector methods
    tables = manager.list_tables()
    assert 'users' in tables

    exists = manager.table_exists('default', 'users')
    assert exists is True

    schema = manager.get_table_schema('default', 'users')
    assert len(schema.fields) == 2

def test_no_raw_sql_in_operations():
    """Verify operations don't use raw SQL."""
    from unittest.mock import patch

    manager = DuckDBStorageManager()
    manager.create_table('default', 'test', schema)

    # Patch text() to detect raw SQL
    with patch('sqlalchemy.text') as mock_text:
        # Perform operations
        manager.insert_data('default', 'test', [{'id': 1, 'name': 'Alice'}])
        results = manager.query_table('default', 'test')

        # Verify text() was never called
        mock_text.assert_not_called()
```

##### Deliverable
- ✅ All metadata uses Inspector
- ✅ All CRUD uses SQLAlchemy
- ✅ Only PRAGMA uses SQL-like syntax
- ✅ ~140 lines of raw SQL eliminated
- ✅ 95% reduction in raw SQL

---

#### File 5: `mock_spark/dataframe/sql_builder.py`

**Current State:** 100% string-based SQL building
**Target State:** 100% SQLAlchemy Core
**Effort:** 2-3 days
**Priority:** MEDIUM (can be phased)

##### Refactoring Strategy

**Replace string building with SQLAlchemy query composition**

##### New Architecture
```python
"""
SQLAlchemy query builder replacing string-based SQL builder.

✅ 100% SQLAlchemy Core
✅ Type-safe query building
✅ Composable operations
"""

from sqlalchemy import Table, select, insert, and_, or_, desc, asc, func
from sqlalchemy import MetaData
from typing import Any, List, Optional

class SQLAlchemyQueryBuilder:
    """Build queries using SQLAlchemy Core (no string SQL)."""

    def __init__(self, table: Table):
        """Initialize with SQLAlchemy Table object."""
        self.table = table
        self._stmt = select(table)
        self._filters = []
        self._order_by = []
        self._group_by = []
        self._limit = None

    def add_filter(self, condition):
        """Add WHERE condition (SQLAlchemy expression)."""
        self._filters.append(condition)
        return self

    def add_select(self, columns: List[str]):
        """Select specific columns."""
        if '*' in columns:
            self._stmt = select(self.table)
        else:
            cols = [self.table.c[col] for col in columns]
            self._stmt = select(*cols)
        return self

    def add_order_by(self, columns: List[tuple]):
        """Add ORDER BY with direction."""
        for col, direction in columns:
            if direction == 'DESC':
                self._order_by.append(desc(self.table.c[col]))
            else:
                self._order_by.append(asc(self.table.c[col]))
        return self

    def add_group_by(self, columns: List[str]):
        """Add GROUP BY."""
        self._group_by = [self.table.c[col] for col in columns]
        return self

    def add_limit(self, n: int):
        """Add LIMIT."""
        self._limit = n
        return self

    def build(self):
        """Build final SQLAlchemy statement."""
        stmt = self._stmt

        # Add WHERE
        if self._filters:
            stmt = stmt.where(and_(*self._filters))

        # Add GROUP BY
        if self._group_by:
            stmt = stmt.group_by(*self._group_by)

        # Add ORDER BY
        if self._order_by:
            stmt = stmt.order_by(*self._order_by)

        # Add LIMIT
        if self._limit:
            stmt = stmt.limit(self._limit)

        return stmt

    def execute(self, session):
        """Execute and return results."""
        stmt = self.build()
        return session.execute(stmt).all()
```

##### Migration from Old SQL Builder
```python
# Before: SQLQueryBuilder (string-based)
builder = SQLQueryBuilder("users", schema)
builder.add_filter(F.col("age") > 25)
builder.add_select(["name", "age"])
sql_string = builder.build_sql()  # Returns SQL string
result = connection.execute(sql_string)

# After: SQLAlchemyQueryBuilder (type-safe)
table = Table('users', metadata, autoload_with=engine)
builder = SQLAlchemyQueryBuilder(table)
builder.add_filter(table.c.age > 25)
builder.add_select(["name", "age"])
stmt = builder.build()  # Returns SQLAlchemy statement
result = session.execute(stmt).all()
```

##### Testing
```python
def test_query_builder_sqlalchemy():
    """Test SQLAlchemy query builder."""
    engine = create_engine('duckdb:///:memory:')
    metadata = MetaData()

    # Create test table
    table = Table('users', metadata,
        Column('id', Integer),
        Column('name', String),
        Column('age', Integer)
    )
    table.create(engine)

    # Insert test data
    with engine.begin() as conn:
        conn.execute(insert(table), [
            {'id': 1, 'name': 'Alice', 'age': 25},
            {'id': 2, 'name': 'Bob', 'age': 30}
        ])

    # Build and execute query
    builder = SQLAlchemyQueryBuilder(table)
    builder.add_filter(table.c.age > 25)
    builder.add_select(['name', 'age'])
    builder.add_order_by([('age', 'DESC')])

    with Session(engine) as session:
        results = builder.execute(session)
        assert len(results) == 1
        assert results[0][0] == 'Bob'  # name
        assert results[0][1] == 30     # age
```

##### Deliverable
- ✅ All query building uses SQLAlchemy
- ✅ No string concatenation
- ✅ Type-safe operations
- ✅ Composable query builder
- ✅ Tests pass

---

### Phase 3: Keep As-Is (Week 5)

#### File 6: `mock_spark/session/sql/executor.py`

**Current State:** Uses raw SQL to execute user queries
**Target State:** **KEEP AS-IS** (this is correct!)
**Effort:** 0 days
**Priority:** N/A

##### Why We Keep This
```python
class MockSQLExecutor:
    """
    ✋ KEEP RAW SQL: This IS the SQL execution engine.

    spark.sql() is SUPPOSED to execute SQL strings.
    This is by design, just like PySpark.
    """

    def execute(self, query: str) -> IDataFrame:
        """Execute user-provided SQL query."""
        # Parse the query
        ast = self.parser.parse(query)

        # Execute based on query type
        if ast.query_type == "SELECT":
            return self._execute_select(ast)
        # ...

        # User expects SQL to be executed here
        # This is NOT a maintenance/security concern
```

**Rationale:**
- ✅ spark.sql() **should** execute SQL (that's its purpose)
- ✅ PySpark's spark.sql() uses raw SQL too
- ✅ Users provide the SQL strings (trusted input in tests)
- ✅ This is 2% of the codebase

##### No Changes Needed
- Leave this file as-is
- Document why raw SQL is appropriate here
- Focus refactoring efforts on the 98% that should use SQLAlchemy

---

## 🛠️ Helper Utilities to Create

### Create: `mock_spark/storage/sqlalchemy_helpers.py`

```python
"""
SQLAlchemy helper functions for Mock Spark.

Provides utilities for converting between MockSpark and SQLAlchemy types,
creating tables, and working with the Inspector.
"""

from sqlalchemy import Table, Column, Integer, BigInteger, String, Float
from sqlalchemy import Boolean, Date, DateTime, LargeBinary, Numeric, MetaData, inspect
from typing import List, Dict, Any, Optional
from mock_spark.spark_types import (
    MockStructType, MockStructField,
    StringType, IntegerType, LongType, DoubleType, FloatType,
    BooleanType, DateType, TimestampType, BinaryType, DecimalType
)


def mock_type_to_sqlalchemy(mock_type):
    """
    Convert MockSpark data type to SQLAlchemy type.

    Args:
        mock_type: MockSpark type instance (e.g., StringType())

    Returns:
        SQLAlchemy type class

    Example:
        >>> mock_type_to_sqlalchemy(StringType())
        String
        >>> mock_type_to_sqlalchemy(IntegerType())
        Integer
    """
    type_mapping = {
        StringType: String,
        IntegerType: Integer,
        LongType: BigInteger,
        DoubleType: Float,
        FloatType: Float,
        BooleanType: Boolean,
        DateType: Date,
        TimestampType: DateTime,
        BinaryType: LargeBinary,
        DecimalType: Numeric,
    }

    for mock_class, sql_class in type_mapping.items():
        if isinstance(mock_type, mock_class):
            return sql_class

    # Default to String
    return String


def sqlalchemy_type_to_mock(sqlalchemy_type):
    """
    Convert SQLAlchemy type to MockSpark data type.

    Args:
        sqlalchemy_type: SQLAlchemy type instance

    Returns:
        MockSpark type instance
    """
    type_name = type(sqlalchemy_type).__name__

    type_mapping = {
        'String': StringType,
        'VARCHAR': StringType,
        'Integer': IntegerType,
        'BigInteger': LongType,
        'Float': DoubleType,
        'DOUBLE': DoubleType,
        'Boolean': BooleanType,
        'Date': DateType,
        'DateTime': TimestampType,
        'TIMESTAMP': TimestampType,
        'LargeBinary': BinaryType,
        'Numeric': DecimalType,
    }

    mock_type_class = type_mapping.get(type_name, StringType)
    return mock_type_class()


def create_table_from_mock_schema(
    name: str,
    schema: MockStructType,
    metadata: MetaData,
    **kwargs
) -> Table:
    """
    Create SQLAlchemy Table from MockSpark schema.

    Args:
        name: Table name
        schema: MockStructType with fields
        metadata: SQLAlchemy MetaData instance
        **kwargs: Additional Table arguments

    Returns:
        SQLAlchemy Table object

    Example:
        >>> schema = MockStructType([
        ...     MockStructField('id', IntegerType()),
        ...     MockStructField('name', StringType())
        ... ])
        >>> metadata = MetaData()
        >>> table = create_table_from_mock_schema('users', schema, metadata)
    """
    columns = []

    for field in schema.fields:
        col_type = mock_type_to_sqlalchemy(field.dataType)
        nullable = getattr(field, 'nullable', True)
        columns.append(Column(field.name, col_type, nullable=nullable))

    return Table(name, metadata, *columns, **kwargs)


def list_all_tables(engine) -> List[str]:
    """
    List all tables using Inspector.

    Args:
        engine: SQLAlchemy engine

    Returns:
        List of table names
    """
    inspector = inspect(engine)
    return inspector.get_table_names()


def table_exists(engine, table_name: str) -> bool:
    """
    Check if table exists using Inspector.

    Args:
        engine: SQLAlchemy engine
        table_name: Name of table to check

    Returns:
        True if table exists, False otherwise
    """
    inspector = inspect(engine)
    return inspector.has_table(table_name)


def get_table_columns(engine, table_name: str) -> List[Dict[str, Any]]:
    """
    Get table column metadata using Inspector.

    Args:
        engine: SQLAlchemy engine
        table_name: Name of table

    Returns:
        List of column metadata dicts
    """
    inspector = inspect(engine)
    return inspector.get_columns(table_name)


def reflect_table(engine, table_name: str, metadata: MetaData) -> Table:
    """
    Reflect existing table into SQLAlchemy Table object.

    Args:
        engine: SQLAlchemy engine
        table_name: Name of table to reflect
        metadata: MetaData instance

    Returns:
        Reflected Table object
    """
    return Table(table_name, metadata, autoload_with=engine)


class TableFactory:
    """Factory for creating SQLAlchemy tables."""

    def __init__(self, metadata: MetaData = None):
        self.metadata = metadata or MetaData()

    def from_mock_schema(self, name: str, schema: MockStructType, **kwargs) -> Table:
        """Create table from MockSpark schema."""
        return create_table_from_mock_schema(name, schema, self.metadata, **kwargs)

    def from_data(self, name: str, data: List[Dict], **kwargs) -> Table:
        """Create table by inferring types from data."""
        if not data:
            raise ValueError("Cannot infer schema from empty data")

        # Infer columns from first row
        columns = []
        for key, value in data[0].items():
            if isinstance(value, bool):
                col_type = Boolean
            elif isinstance(value, int):
                col_type = Integer
            elif isinstance(value, float):
                col_type = Float
            else:
                col_type = String
            columns.append(Column(key, col_type))

        return Table(name, self.metadata, *columns, **kwargs)

    def reflect(self, name: str, engine) -> Table:
        """Reflect existing table."""
        return Table(name, self.metadata, autoload_with=engine)
```

---

## 📅 Implementation Timeline

### Week 1-2: Phase 1 (Quick Wins)

**Days 1-2: File 1 - export.py**
- [ ] Replace CREATE TABLE with Table.create()
- [ ] Replace INSERT with insert().values()
- [ ] Update function signatures
- [ ] Write unit tests
- [ ] Code review

**Days 3-5: File 2 - sqlmodel_materializer.py**
- [ ] Remove all text() calls
- [ ] Replace with select(), insert()
- [ ] Update _apply_filter method
- [ ] Update _apply_select method
- [ ] Write unit tests
- [ ] Code review

**Days 6-8: File 3 - duckdb_materializer.py**
- [ ] Replace connection with engine
- [ ] Use Table.create() for temp tables
- [ ] Use insert().from_select()
- [ ] Write unit tests
- [ ] Code review

**Days 9-10: Helper Utilities**
- [ ] Create sqlalchemy_helpers.py
- [ ] Write comprehensive tests
- [ ] Documentation

**Deliverable:** 3 files refactored, helpers ready

---

### Week 3-4: Phase 2 (Core Infrastructure)

**Days 11-15: File 4 - storage/backends/duckdb.py**
- [ ] Day 11: Add engine and Inspector
- [ ] Day 12: Replace metadata operations (list_tables, table_exists, get_schema)
- [ ] Day 13: Replace table operations (create, drop)
- [ ] Day 14: Replace data operations (insert, query)
- [ ] Day 15: Testing and cleanup

**Days 16-20: File 5 - sql_builder.py**
- [ ] Day 16: Create SQLAlchemyQueryBuilder class
- [ ] Day 17: Implement filter, select, order_by
- [ ] Day 18: Implement group_by, limit
- [ ] Day 19: Testing
- [ ] Day 20: Migration and cleanup

**Deliverable:** Core infrastructure refactored

---

### Week 5: Phase 3 (Testing & Polish)

**Days 21-22: Unit Tests**
- [ ] Test all refactored files
- [ ] Test Inspector integration
- [ ] Test helper utilities
- [ ] Edge cases

**Days 23-24: Integration Tests**
- [ ] Test DataFrame operations end-to-end
- [ ] Test storage backend
- [ ] Test query building

**Days 25-27: Performance & Documentation**
- [ ] Performance benchmarks
- [ ] Update API documentation
- [ ] Migration guide
- [ ] Code examples

**Day 28: Final Review**
- [ ] Code review
- [ ] Final testing
- [ ] Prepare for merge

**Deliverable:** Production-ready code

---

## ✅ Success Criteria

### Technical Metrics
- [ ] **98% of code uses SQLModel/SQLAlchemy**
- [ ] **Only 2% raw SQL** (spark.sql + PRAGMA)
- [ ] **All 396 tests pass**
- [ ] **100% test coverage maintained**
- [ ] **No performance regression**
- [ ] **No breaking changes to public API**

### Code Quality Metrics
- [ ] **Type hints** on all new code
- [ ] **Docstrings** with examples
- [ ] **No mypy errors**
- [ ] **Black formatted**
- [ ] **Comprehensive error handling**

### Documentation Metrics
- [ ] **All raw SQL documented** with justification
- [ ] **Migration guide** for contributors
- [ ] **API docs** updated
- [ ] **Code examples** provided

---

## 🧪 Testing Strategy

### Unit Tests (Per File)
```python
def test_no_raw_sql(file_module):
    """Verify no text() calls in refactored code."""
    from unittest.mock import patch

    with patch('sqlalchemy.text') as mock_text:
        # Perform all operations
        run_all_operations(file_module)

        # Verify text() was never called
        mock_text.assert_not_called()

def test_inspector_integration():
    """Test Inspector works correctly."""
    engine = create_engine('duckdb:///:memory:')

    # Create table
    table = Table('test', metadata, Column('id', Integer))
    table.create(engine)

    # Test Inspector
    inspector = inspect(engine)
    assert 'test' in inspector.get_table_names()
    assert inspector.has_table('test')

def test_sqlalchemy_vs_raw_sql_equivalence():
    """Verify SQLAlchemy produces same results as raw SQL."""
    # Create test data
    setup_test_data()

    # Execute with SQLAlchemy
    sqlalchemy_result = execute_with_sqlalchemy()

    # Execute with raw SQL (for comparison)
    raw_sql_result = execute_with_raw_sql()

    # Compare
    assert sqlalchemy_result == raw_sql_result
```

### Integration Tests
```python
def test_full_dataframe_pipeline():
    """Test complete DataFrame operations."""
    spark = MockSparkSession()

    # Create DataFrame
    df = spark.createDataFrame([
        {'id': 1, 'name': 'Alice', 'age': 25},
        {'id': 2, 'name': 'Bob', 'age': 30}
    ])

    # Transform
    result = (df
        .filter(F.col('age') > 25)
        .select('name', 'age')
        .orderBy(F.desc('age'))
    )

    # Verify
    assert result.count() == 1
    assert result.collect()[0]['name'] == 'Bob'

def test_storage_backend_operations():
    """Test storage backend with SQLAlchemy."""
    manager = DuckDBStorageManager()

    # Create table
    schema = MockStructType([MockStructField('id', IntegerType())])
    manager.create_table('default', 'test', schema)

    # Insert data
    manager.insert_data('default', 'test', [{'id': 1}])

    # Query data
    results = manager.query_table('default', 'test')
    assert len(results) == 1
```

### Performance Tests
```python
def test_performance_no_regression():
    """Verify no performance regression."""
    # Benchmark before and after
    times_before = benchmark_operations_raw_sql()
    times_after = benchmark_operations_sqlalchemy()

    # Allow 10% overhead
    for op, time_after in times_after.items():
        assert time_after < times_before[op] * 1.1
```

---

## 🎯 Risk Mitigation

### Risk 1: Breaking Changes
**Mitigation:**
- Maintain backward compatibility
- Don't change public API
- Comprehensive testing before merge

### Risk 2: Performance Regression
**Mitigation:**
- Benchmark all operations
- Use bulk operations for large datasets
- Profile before/after

### Risk 3: Missing Edge Cases
**Mitigation:**
- Test with all 396 existing tests
- Add edge case tests
- Review PySpark compatibility tests

### Risk 4: Team Learning Curve
**Mitigation:**
- Comprehensive documentation
- Code examples for each pattern
- Pair programming sessions

---

## 📚 Documentation Updates

### New Documentation
1. **`docs/sqlalchemy_migration.md`** - Migration guide
2. **`docs/sqlmodel_best_practices.md`** - Best practices
3. **`docs/inspector_usage.md`** - Inspector guide

### Updated Documentation
1. `docs/storage_serialization_guide.md` - SQLAlchemy examples
2. `docs/api_reference.md` - Updated API docs
3. `README.md` - Note SQLModel usage

---

## 🎯 Final Checklist

### Before Starting
- [ ] Review this plan
- [ ] Team approval
- [ ] Create feature branch
- [ ] Set up CI/CD for testing

### During Implementation
- [ ] Follow file-by-file plan
- [ ] Write tests for each change
- [ ] Document as you go
- [ ] Regular code reviews

### Before Merge
- [ ] All 396 tests pass
- [ ] Performance benchmarks good
- [ ] Documentation complete
- [ ] Code review approved
- [ ] No mypy/linting errors

### After Merge
- [ ] Monitor production
- [ ] Update contributor guide
- [ ] Celebrate! 🎉

---

**Created:** October 7, 2025
**Status:** Ready for Implementation
**Next Step:** Start with File 1 (export.py)
**Estimated Completion:** 3-5 weeks from start

---

## 🚀 Ready to Start?

**Quick Start Command:**
```bash
# Create feature branch
git checkout -b feature/sqlmodel-refactoring

# Start with easiest file
code mock_spark/dataframe/export.py

# Reference this plan
open IMPLEMENTATION_PLAN.md
```

Let's build something great! 🎉
