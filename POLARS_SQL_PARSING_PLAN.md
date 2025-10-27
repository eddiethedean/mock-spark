# SQL Parsing Implementation Plan for Polars Architecture

## Executive Summary

This document outlines the implementation strategy for implementing `spark.sql()` functionality in the new Polars-based architecture. Since Polars already includes native SQL support, this feature requires **zero additional dependencies** and provides full backward compatibility.

**Key Strategy:**
- **Primary Approach**: Use Polars' native `pl.SQLContext()` for direct SQL execution (zero additional dependencies)
- **Secondary Approach**: Optional sqlglot library for complex query translation (only if needed)
- **Implementation**: Integrated into main Polars migration plan (Phase 3)
- **Backward Compatibility**: Full API compatibility with existing `spark.sql()` usage

---

## Research Summary: Available Python Packages

### 1. **Polars Native SQL Support** (Primary - No Additional Dependencies)

**Package**: Built into Polars (`polars>=0.20.0`)
- **Size**: ~50-70MB (already included)
- **Capabilities**: 
  - Native SQL execution via `pl.SQLContext()`
  - Supports SELECT, JOIN, WHERE, GROUP BY, ORDER BY, LIMIT
  - Registered DataFrame tables
  - Window functions support
  - Complex aggregations

**Usage Example:**
```python
import polars as pl

# Register DataFrames
ctx = pl.SQLContext()
ctx.register("users", df1)
ctx.register("orders", df2)

# Execute SQL
result = ctx.execute("""
    SELECT u.name, SUM(o.amount) as total
    FROM users u
    JOIN orders o ON u.id = o.user_id
    GROUP BY u.name
    ORDER BY total DESC
""").collect()
```

**Pros:**
- ✅ Zero additional dependencies
- ✅ Native performance (Rust backend)
- ✅ Full integration with Polars
- ✅ Window functions supported
- ✅ Active development and maintenance

**Cons:**
- ❌ Limited SQL dialect support (not all PostgreSQL/SQLite features)
- ❌ No CTE (WITH) support yet
- ❌ No stored procedures or custom functions

---

### 2. **sqlparse** (Optional - Lightweight)

**Package**: `sqlparse`
- **Size**: ~200KB
- **Purpose**: Non-validating SQL parser for Python
- **Capabilities**:
  - Parse SQL into abstract syntax tree
  - Format and split SQL statements
  - Extract tokens and keywords
  - Validate syntax

**Usage Example:**
```python
import sqlparse

sql = "SELECT * FROM users WHERE age > 18"
parsed = sqlparse.parse(sql)
print(parsed)
```

**Pros:**
- ✅ Very lightweight (~200KB)
- ✅ Fast parsing
- ✅ Good for simple queries
- ✅ No dependencies

**Cons:**
- ❌ Not a full SQL translator
- ❌ Cannot execute SQL
- ❌ Limited semantic analysis

---

### 3. **sqlglot** (Optional - Recommended)

**Package**: `sqlglot`
- **Size**: ~3-5MB (with transpilation support)
- **Purpose**: SQL parser, transpiler, and optimizer
- **Capabilities**:
  - Parse SQL into AST
  - Transpile between SQL dialects
  - Optimize queries
  - Generate SQL from AST
  - Support multiple dialects (SQLite, MySQL, PostgreSQL, etc.)

**Usage Example:**
```python
from sqlglot import parse_one, transpile

# Parse SQL
sql = "SELECT name, age FROM users WHERE age > 18"
ast = parse_one(sql)

# Transpile to another dialect
mysql_sql = transpile(sql, write="mysql")

# Extract columns
columns = ast.find_all("Identifier")
```

**Pros:**
- ✅ Comprehensive SQL dialect support
- ✅ Transpile between dialects
- ✅ Optimize queries
- ✅ AST manipulation
- ✅ Active development

**Cons:**
- ❌ Larger dependency (~3-5MB)
- ❌ More complex API
- ❌ Overkill for simple queries

---

### 4. **DuckDB** (Optional - For Complex Queries)

**Package**: `duckdb`
- **Size**: ~15-25MB
- **Purpose**: In-process analytical database
- **Capabilities**:
  - Execute SQL on DataFrames
  - Query Polars DataFrames directly
  - Full SQL support
  - Performance optimizations

**Usage Example:**
```python
import duckdb
import polars as pl

# Create connection
con = duckdb.connect()

# Register Polars DataFrame
con.register("users", df.to_polars())

# Execute SQL
result = con.execute("SELECT * FROM users WHERE age > 18").df()
```

**Pros:**
- ✅ Full SQL support
- ✅ Can query Polars DataFrames
- ✅ High performance
- ✅ Excellent for complex queries

**Cons:**
- ❌ Large dependency (~15-25MB)
- ❌ Additional process overhead
- ❌ May not be necessary if Polars SQL is sufficient

---

## Recommended Approach: Hybrid Strategy

### Phase 1: Polars Native SQL (Primary)

**Implementation:**
```python
class PolarsSQLContext:
    """SQL context for executing SQL queries on Polars DataFrames."""
    
    def __init__(self):
        self.ctx = pl.SQLContext()
        self._registered_tables = {}
    
    def register(self, name: str, df: "MockDataFrame"):
        """Register a MockDataFrame as a table."""
        # Convert MockDataFrame to Polars
        pl_df = self._convert_to_polars(df)
        self.ctx.register(name, pl_df)
        self._registered_tables[name] = df
    
    def execute(self, query: str) -> "MockDataFrame":
        """Execute SQL query and return MockDataFrame."""
        try:
            # Execute on Polars
            result = self.ctx.execute(query).collect()
            
            # Convert back to MockDataFrame
            return self._convert_to_mock_df(result)
        except Exception as e:
            # Fallback to parsing approach
            return self._parse_and_translate(query)
    
    def _parse_and_translate(self, query: str) -> "MockDataFrame":
        """Parse SQL and translate to Polars operations."""
        # This would use sqlglot or sqlparse for complex queries
        pass
```

**Coverage:**
- ✅ 80-90% of SQL queries can be handled directly
- ✅ No additional dependencies
- ✅ Native performance

---

### Phase 2: Optional sqlglot for Complex Queries (Fallback)

**Usage:**
```python
try:
    from sqlglot import parse_one, transpile
    SQLGLOT_AVAILABLE = True
except ImportError:
    SQLGLOT_AVAILABLE = False

class SQLTranslator:
    """Translates SQL to Polars operations."""
    
    def translate_to_polars(self, sql: str) -> "MockDataFrame":
        """Translate SQL to Polars DataFrame operations."""
        if not SQLGLOT_AVAILABLE:
            raise ImportError("sqlglot is required for complex SQL queries")
        
        # Parse SQL into AST
        ast = parse_one(sql)
        
        # Convert AST to Polars operations
        return self._ast_to_polars(ast)
```

**Coverage:**
- ✅ Handles remaining 10-20% complex queries
- ✅ CTEs, nested queries, advanced features
- ✅ Optional dependency

---

## Implementation Plan (Integrated into Main Migration)

This SQL support is **integrated into the main Polars migration plan** (Phase 3). Since Polars already includes native SQL support, no separate implementation timeline is needed.

### Implementation During Main Migration

**Phase 3 (Weeks 5-6): Advanced Operations**
- Implement `PolarsSQLContext` wrapper
- Register DataFrames as tables for SQL execution
- Convert MockDataFrame ↔ Polars DataFrame
- Execute SQL queries via `pl.SQLContext()`
- Return results as MockDataFrame
- Test with existing SQL test suite

**Phase 4 (Weeks 7-8): Optional Enhancements (If Needed)**
- Integrate sqlglot as optional dependency (only if complex queries fail)
- Implement fallback for advanced SQL features
- Detect query complexity and route appropriately
- SQL dialect translation (if needed)

### Key Implementation Details

Since Polars is already a dependency, SQL support requires:
1. **Zero additional packages** (Polars native SQL included)
2. **Wrapper implementation** around `pl.SQLContext()`
3. **DataFrame registration** for table access
4. **Result conversion** back to MockDataFrame format
5. **Optional sqlglot** only if complex queries need translation

---

## Optional Dependency Management

### Installation Strategy

**Important:** Since Polars already includes native SQL support, **no additional dependencies are required** for basic SQL functionality.

```toml
[project]
dependencies = [
    "polars>=1.0.0",  # Already includes pl.SQLContext() - no extra dependency needed
    "pyarrow>=10.0.0",
]

[project.optional-dependencies]
sql-advanced = [
    "sqlglot>=20.0.0",  # Only for complex query translation (rarely needed)
]
```

### User Installation

```bash
# Basic installation (includes SQL support via Polars!)
pip install mock-spark
# spark.sql() works out of the box with zero additional dependencies!

# Only needed for very complex SQL queries
pip install mock-spark[sql-advanced]
```

### Runtime Detection

```python
class MockSparkSession:
    def __init__(self):
        self._has_sqlglot = self._check_dependency("sqlglot")
        self._has_duckdb = self._check_dependency("duckdb")
    
    def sql(self, query: str) -> "MockDataFrame":
        """Execute SQL with automatic fallback."""
        try:
            # Try Polars native SQL first
            return self._execute_polars_sql(query)
        except Exception:
            if self._has_sqlglot:
                # Fallback to sqlglot translation
                return self._translate_with_sqlglot(query)
            else:
                raise ImportError(
                    "Complex SQL requires sqlglot. Install with: pip install mock-spark[sql]"
                )
```

---

## SQL Query Routing Strategy

```
User calls spark.sql("SELECT ...")
           ↓
    Route to Executor
           ↓
    ┌──────────────────────┐
    │ Parse Query Complexity │
    └──────────────────────┘
           ↓
    ┌──────────────────────┐
    │ Simple Query?        │
    └──────────────────────┘
           ↓           ↓
         YES          NO
           ↓           ↓
    Polars Native  sqlglot
    SQL Context    Parser
           ↓           ↓
    Convert to   Translate to
    Polars DF    Polars Operations
           ↓           ↓
         MockDataFrame
           ↓
         Return
```

---

## Supported SQL Features

### Phase 1: Basic SQL (Polars Native)

✅ **SELECT Statements**
- Simple columns: `SELECT col1, col2 FROM table`
- Expressions: `SELECT col1 + col2 AS sum FROM table`
- Wildcards: `SELECT * FROM table`
- Aliases: `SELECT name AS full_name FROM users`

✅ **WHERE Clauses**
- Comparisons: `WHERE age > 18`
- Logical operators: `WHERE age > 18 AND status = 'active'`
- IN, LIKE, BETWEEN: `WHERE id IN (1, 2, 3)`

✅ **JOINs**
- INNER JOIN, LEFT JOIN, RIGHT JOIN
- Multiple table joins
- Join conditions

✅ **GROUP BY**
- Basic aggregations: `SELECT dept, COUNT(*) FROM users GROUP BY dept`
- Multiple grouping columns

✅ **ORDER BY**
- Single/multiple columns
- ASC/DESC

✅ **LIMIT**
- Simple limit: `SELECT * FROM table LIMIT 10`

✅ **Aggregations**
- COUNT, SUM, AVG, MIN, MAX
- DISTINCT: `SELECT COUNT(DISTINCT id) FROM users`

---

### Phase 2: Advanced SQL (sqlglot Optional)

✅ **Window Functions**
- ROW_NUMBER, RANK, DENSE_RANK
- LAG, LEAD
- SUM/AVG OVER PARTITION BY

✅ **Subqueries**
- Correlated subqueries
- EXISTS, IN subqueries

✅ **CTEs (WITH)**
- Common table expressions
- Multiple CTEs

✅ **UNION/INTERSECT/EXCEPT**
- Set operations

✅ **CASE WHEN**
- Conditional expressions

---

## Performance Considerations

### Polars Native SQL
- **Speed**: Native Rust execution, very fast
- **Memory**: Efficient columnar storage
- **Limitations**: SQL dialect support

### sqlglot Parsing
- **Speed**: ~5-10ms parsing overhead
- **Memory**: Additional AST in memory
- **Benefits**: Handles complex queries

### Hybrid Approach
- **Strategy**: Route simple queries to Polars, complex to sqlglot
- **Performance**: Best of both worlds
- **Flexibility**: User choice

---

## Migration Path for Users

### Current Usage (Works Immediately - No Changes Required!)
```python
# Current code works unchanged with Polars native SQL
df = spark.sql("SELECT * FROM users WHERE age > 18")
# Works with zero additional dependencies!

# More complex queries also work out of the box
df = spark.sql("""
    SELECT u.name, SUM(o.amount) as total
    FROM users u
    JOIN orders o ON u.id = o.user_id
    GROUP BY u.name
    ORDER BY total DESC
""")
# All via Polars native SQL - no extra dependencies needed!
```

### Optional Advanced Dependencies
```python
# For very advanced SQL (CTEs, complex subqueries), install optional dependency
pip install mock-spark[sql-advanced]

# Now supports advanced SQL features via sqlglot
df = spark.sql("""
    WITH cte AS (
        SELECT user_id, COUNT(*) as order_count
        FROM orders
        GROUP BY user_id
    )
    SELECT u.name, c.order_count
    FROM users u
    JOIN cte c ON u.id = c.user_id
    WHERE c.order_count > 10
""")
```

---

## Testing Strategy

### Unit Tests
```python
def test_basic_select():
    df = spark.sql("SELECT * FROM users")
    assert df.count() == 10

def test_where_clause():
    df = spark.sql("SELECT * FROM users WHERE age > 18")
    assert all(row['age'] > 18 for row in df.collect())

def test_join():
    df = spark.sql("""
        SELECT u.name, o.amount
        FROM users u
        JOIN orders o ON u.id = o.user_id
    """)
```

### Integration Tests
```python
def test_complex_query():
    # Test complex SQL with optional dependencies
    df = spark.sql("""
        SELECT 
            dept,
            AVG(salary) OVER (PARTITION BY dept) as avg_dept_salary
        FROM employees
        ORDER BY avg_dept_salary DESC
    """)
    assert df is not None
```

### Performance Tests
```python
def test_sql_performance():
    import time
    
    start = time.time()
    df = spark.sql("SELECT * FROM large_table WHERE id > 1000")
    duration = time.time() - start
    
    assert duration < 1.0  # Should be fast with Polars
```

---

## Success Metrics

### Coverage Targets
- ✅ 80-90% of SQL queries work with Polars native SQL
- ✅ 95-98% of SQL queries work with optional sqlglot
- ✅ 100% of existing `spark.sql()` tests pass

### Performance Targets
- ✅ Simple queries: <50ms
- ✅ Complex queries: <500ms
- ✅ No regression vs current implementation

### User Experience
- ✅ Zero changes to existing user code
- ✅ Optional dependencies are truly optional
- ✅ Clear error messages for missing features

---

## Risk Assessment

### High Risk
1. **Query Complexity Detection**
   - **Risk**: Mis-routing queries
   - **Mitigation**: Comprehensive routing logic

2. **Performance Regression**
   - **Risk**: sqlglot parsing overhead
   - **Mitigation**: Smart routing, Polars native for simple queries

### Medium Risk
1. **SQL Dialect Differences**
   - **Risk**: Polars SQL vs PySpark SQL differences
   - **Mitigation**: Translation layer, documentation

2. **Backward Compatibility**
   - **Risk**: Breaking existing SQL usage
   - **Mitigation**: Extensive testing, gradual rollout

### Low Risk
1. **Optional Dependency Installation**
   - **Risk**: User confusion
   - **Mitigation**: Clear documentation, helpful error messages

---

## Conclusion

The implementation strategy uses **Polars native SQL** as the primary approach:

1. **Primary**: Polars native SQL via `pl.SQLContext()` (80-90% coverage, **zero additional dependencies**)
2. **Fallback**: sqlglot for very complex queries (10-20% coverage, **optional**)
3. **Integration**: Built into main Polars migration plan (Phase 3)

This provides:
- ✅ **Zero additional dependencies** for 80-90% of SQL queries (Polars already included)
- ✅ **Full backward compatibility** - existing `spark.sql()` code works unchanged
- ✅ **Excellent performance** - Native Rust SQL execution
- ✅ **Lightweight implementation** - No extra dependencies needed
- ✅ **Optional enhancement** - sqlglot available for complex queries

---

*This plan enables full `spark.sql()` compatibility in the Polars architecture while maintaining a lightweight core and providing optional advanced SQL capabilities.*
