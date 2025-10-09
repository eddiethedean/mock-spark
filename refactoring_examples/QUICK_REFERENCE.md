# SQLModel Refactoring: Quick Decision Guide

**One-page reference** for deciding when to use SQLModel vs raw SQL

---

## 🎯 Decision Tree

```
Is this a...

┌─ Database Metadata Query? (SHOW TABLES, DESCRIBE)
│  └─→ ✋ USE RAW SQL (no SQLAlchemy equivalent)

├─ SQL Parser/Executor? (handling arbitrary SQL)
│  └─→ ✋ USE RAW SQL (you're implementing the SQL engine)

├─ DuckDB-Specific Feature? (PRAGMA, extensions)
│  └─→ ✋ USE RAW SQL (vendor-specific)

├─ Bulk Operation? (>1000 rows)
│  └─→ ⚡ USE RAW SQL (2-5x faster)

├─ Fully Dynamic Schema? (structure unknown until runtime)
│  └─→ ✅ USE SQLAlchemy Core (Table API)

├─ Complex Window Function?
│  └─→ ⚠️ YOUR CHOICE (SQLAlchemy works but verbose)

└─ Everything Else?
   └─→ ✅ USE SQLModel (80% of cases)
```

---

## 📊 Coverage Breakdown

| Approach | % of Code | Use For | Example |
|----------|-----------|---------|---------|
| **SQLModel** | **80%** | Known schemas, CRUD, type-safe queries | User tables, CRUD operations |
| **SQLAlchemy Core** | **15%** | Dynamic schemas, complex queries | Tables from runtime data |
| **Raw SQL** | **5%** | Metadata, parsing, vendor features, bulk ops | SHOW TABLES, >1000 row inserts |

---

## ✅ Use SQLModel When...

✅ Schema is known at design time
✅ You want Pydantic validation
✅ Type safety is important
✅ < 1000 rows per operation
✅ Standard SQL operations (SELECT, INSERT, UPDATE, DELETE)
✅ You want IDE autocomplete

**Example:**
```python
class User(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(min_length=1, max_length=100)
    age: int = Field(ge=0, le=150)

with Session(engine) as session:
    user = User(name="Alice", age=25)  # Validated!
    session.add(user)
    session.commit()
```

---

## ✅ Use SQLAlchemy Core When...

✅ Schema is determined at runtime
✅ Table structure comes from data
✅ Need dynamic column selection
✅ Building complex query builders

**Example:**
```python
from sqlalchemy import Table, Column, Integer, String, MetaData

# Dynamic table from runtime data
columns = []
for key, value in data[0].items():
    col_type = Integer if isinstance(value, int) else String
    columns.append(Column(key, col_type))

table = Table("dynamic", MetaData(), *columns)
table.create(engine)
```

---

## ✋ Use Raw SQL When...

✋ **Database metadata queries**
```python
# No SQLAlchemy equivalent
connection.execute("SHOW TABLES")
connection.execute("DESCRIBE table_name")
```

✋ **SQL parser implementation**
```python
# You're implementing the SQL engine
def execute_sql(query: str):
    return connection.execute(text(query))
```

✋ **DuckDB-specific features**
```python
# Vendor-specific syntax
connection.execute("PRAGMA memory_limit='4GB'")
connection.execute("INSTALL parquet")
```

⚡ **Performance-critical bulk operations**
```python
# 2-5x faster for >1000 rows
values = [(name, age) for name, age in data]
connection.executemany(
    "INSERT INTO users (name, age) VALUES (?, ?)",
    values
)
```

---

## 🚦 Examples from Mock Spark Codebase

### ✅ REFACTOR THIS (use SQLModel)

#### `mock_spark/dataframe/export.py:119`
```python
# ❌ OLD: Raw SQL
create_sql = f"CREATE TABLE {table_name} ({', '.join(columns)})"
connection.execute(create_sql)

# ✅ NEW: SQLModel/SQLAlchemy
table = create_table_from_mock_schema(table_name, df.schema, metadata)
table.create(engine, checkfirst=True)
```

#### `mock_spark/storage/backends/duckdb.py:111`
```python
# ❌ OLD: Raw SQL
placeholders = ", ".join(["?" for _ in values])
self.connection.execute(
    f"INSERT INTO {self.name} VALUES ({placeholders})", values
)

# ✅ NEW: SQLModel
with Session(engine) as session:
    obj = DuckDBTable(**data_dict)
    session.add(obj)
    session.commit()
```

---

### ✋ KEEP RAW SQL

#### `mock_spark/storage/backends/duckdb.py:253`
```python
# ✋ KEEP: No SQLAlchemy equivalent
result = self.connection.execute("SHOW TABLES").fetchall()
return [row[0] for row in result]
```

#### `mock_spark/session/sql/executor.py:59`
```python
# ✋ KEEP: You're implementing SQL parser
def execute(self, query: str):
    # User provides arbitrary SQL
    result = self.connection.execute(text(query))
    return result.fetchall()
```

#### Bulk Insert (>1000 rows)
```python
# ✋ KEEP: Much faster for large batches
if len(data) > 1000:
    values = [(row['name'], row['age']) for row in data]
    connection.executemany(
        "INSERT INTO users (name, age) VALUES (?, ?)",
        values
    )
```

---

## 🔄 Hybrid Approach Template

```python
class SmartDataManager:
    """Uses best tool for each job."""

    def __init__(self, engine):
        self.engine = engine
        self.metadata = MetaData()

    def create_static_table(self):
        """Known schema → SQLModel"""
        class User(SQLModel, table=True):
            id: Optional[int] = Field(default=None, primary_key=True)
            name: str

        SQLModel.metadata.create_all(self.engine)
        return User

    def create_dynamic_table(self, name, data):
        """Unknown schema → SQLAlchemy Core"""
        columns = [Column(k, String) for k in data[0].keys()]
        table = Table(name, self.metadata, *columns)
        table.create(self.engine)
        return table

    def list_tables(self):
        """Metadata → Raw SQL"""
        with self.engine.connect() as conn:
            return conn.execute(text("SHOW TABLES")).fetchall()

    def insert_data(self, model, data):
        """Smart: choose based on size"""
        if len(data) < 1000:
            # Small: Use SQLModel for validation
            with Session(self.engine) as session:
                session.add_all([model(**row) for row in data])
                session.commit()
        else:
            # Large: Use raw SQL for performance
            self._bulk_insert(model.__tablename__, data)

    def _bulk_insert(self, table, data):
        """Performance-critical → Raw SQL"""
        columns = list(data[0].keys())
        placeholders = ", ".join(["?" for _ in columns])
        sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"

        with self.engine.connect() as conn:
            conn.executemany(sql, [tuple(row.values()) for row in data])
```

---

## 📝 Checklist: Should I Refactor This?

Before refactoring, ask:

- [ ] Is this standard CRUD? → ✅ **Use SQLModel**
- [ ] Is the schema known at design time? → ✅ **Use SQLModel**
- [ ] Is this a metadata query (SHOW, DESCRIBE)? → ✋ **Keep raw SQL**
- [ ] Is this DuckDB-specific? → ✋ **Keep raw SQL**
- [ ] Is this a bulk operation (>1000 rows)? → ✋ **Keep raw SQL or benchmark**
- [ ] Is the schema fully dynamic? → ✅ **Use SQLAlchemy Core**
- [ ] Am I implementing a SQL parser? → ✋ **Keep raw SQL**
- [ ] Is this complex window function? → ⚠️ **Your choice** (both work)

---

## 🎯 Quick Examples

### 1-Minute Examples for Each Scenario

#### SQLModel (Most Common)
```python
class User(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str

SQLModel.metadata.create_all(engine)

with Session(engine) as session:
    user = User(name="Alice")
    session.add(user)
    session.commit()

    results = session.exec(select(User).where(User.name == "Alice")).all()
```

#### SQLAlchemy Core (Dynamic)
```python
from sqlalchemy import Table, Column, String, MetaData

metadata = MetaData()
table = Table("dynamic", metadata, Column("name", String))
table.create(engine)

with engine.connect() as conn:
    conn.execute(insert(table).values(name="Alice"))
    results = conn.execute(select(table)).all()
```

#### Raw SQL (Edge Cases)
```python
from sqlalchemy import text

with engine.connect() as conn:
    # Metadata
    tables = conn.execute(text("SHOW TABLES")).fetchall()

    # DuckDB-specific
    conn.execute(text("PRAGMA memory_limit='4GB'"))

    # Bulk insert
    conn.executemany(
        "INSERT INTO users VALUES (?, ?)",
        [(f"User{i}", i) for i in range(10000)]
    )
```

---

## 📚 Full Documentation

- **Detailed Analysis**: `SQLMODEL_LIMITATIONS.md`
- **Code Examples**: `sqlmodel_refactor_demo.py`
- **Utilities**: `sqlalchemy_utils.py`
- **Full Guide**: `README.md`

---

## 💡 Key Takeaway

**Use the right tool for the job:**
- **80% SQLModel** (type-safe, validated, clean)
- **15% SQLAlchemy Core** (dynamic, flexible)
- **5% Raw SQL** (metadata, vendor features, performance)

**Don't try to force everything into one approach!**

---

**Last Updated:** October 7, 2025
