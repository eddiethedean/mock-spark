"""
SQLAlchemy Refactoring Examples
Demonstrates how to replace raw SQL with SQLAlchemy ORM patterns

This file shows concrete before/after examples for the Mock Spark codebase.
"""

from sqlalchemy import (
    create_engine,
    Table,
    Column,
    Integer,
    String,
    Float,
    Boolean,
    MetaData,
    select,
    insert,
    and_,
    or_,
    desc,
    asc,
    func,
    text,
)
from sqlalchemy.orm import Session


# ============================================================================
# Example 1: Table Creation (from sql_builder.py and duckdb.py)
# ============================================================================


def create_table_old_way(connection, table_name: str, columns: list):
    """
    OLD WAY: Raw SQL with string interpolation
    Issues:
    - SQL injection risk if table_name is user input
    - No type checking
    - Hard to test
    - Database-specific syntax
    """
    column_defs = []
    for col_name, col_type in columns:
        column_defs.append(f"{col_name} {col_type}")

    create_sql = f'CREATE TABLE "{table_name}" ({", ".join(column_defs)})'
    connection.execute(create_sql)


def create_table_new_way(engine, table_name: str, columns: list):
    """
    NEW WAY: SQLAlchemy Table API
    Benefits:
    - Automatic parameter binding (SQL injection safe)
    - Type checking at Python level
    - Database agnostic
    - Better error messages
    - Testable without database
    """
    metadata = MetaData()

    # Map string types to SQLAlchemy types
    type_mapping = {
        "INTEGER": Integer,
        "VARCHAR": String,
        "DOUBLE": Float,
        "BOOLEAN": Boolean,
    }

    # Build columns
    column_objects = []
    for col_name, col_type in columns:
        sql_type = type_mapping.get(col_type, String)
        column_objects.append(Column(col_name, sql_type))

    # Create table
    table = Table(table_name, metadata, *column_objects)
    table.create(engine, checkfirst=True)

    return table  # Return for further use


# ============================================================================
# Example 2: Data Insertion (from duckdb.py)
# ============================================================================


def insert_data_old_way(connection, table_name: str, rows: list):
    """
    OLD WAY: Manual value placeholders
    Issues:
    - Manual placeholder generation
    - No validation
    - Error-prone for complex types
    """
    for row in rows:
        values = list(row.values())
        placeholders = ", ".join(["?" for _ in values])
        connection.execute(f"INSERT INTO {table_name} VALUES ({placeholders})", values)


def insert_data_new_way(session: Session, table: Table, rows: list):
    """
    NEW WAY: SQLAlchemy insert()
    Benefits:
    - Automatic type conversion
    - Batch insert support
    - Transaction management
    - Validation
    """
    # Single insert
    for row_dict in rows:
        stmt = insert(table).values(**row_dict)
        session.execute(stmt)

    # Or better - batch insert:
    session.execute(insert(table), rows)

    session.commit()


# ============================================================================
# Example 3: SELECT Query with Filter (from duckdb.py)
# ============================================================================


def query_with_filter_old_way(connection, table_name: str, filter_expr: str):
    """
    OLD WAY: String concatenation for WHERE clause
    Issues:
    - SQL injection if filter_expr is user input
    - No validation of column names
    - Hard to compose complex conditions
    """
    if filter_expr:
        query = f"SELECT * FROM {table_name} WHERE {filter_expr}"
    else:
        query = f"SELECT * FROM {table_name}"

    result = connection.execute(query).fetchall()
    return result


def query_with_filter_new_way(session: Session, table: Table, conditions: list = None):
    """
    NEW WAY: SQLAlchemy select() with where()
    Benefits:
    - Type-safe column references
    - Composable queries
    - Automatic parameter binding
    - Better error messages
    """
    stmt = select(table)

    if conditions:
        # Combine multiple conditions with AND
        stmt = stmt.where(and_(*conditions))

    result = session.execute(stmt).all()
    return result


# ============================================================================
# Example 4: Complex Query Building (from sql_builder.py)
# ============================================================================


def build_complex_query_old_way(
    table_name: str, select_cols: list, where_conditions: list, order_by: list
):
    """
    OLD WAY: Manual SQL string building
    Issues:
    - Many string operations
    - Easy to make syntax errors
    - Hard to modify or extend
    - No query validation until execution
    """
    sql_parts = []

    # SELECT
    if select_cols:
        select_clause = "SELECT " + ", ".join(f'"{col}"' for col in select_cols)
    else:
        select_clause = "SELECT *"
    sql_parts.append(select_clause)

    # FROM
    from_clause = f'FROM "{table_name}"'
    sql_parts.append(from_clause)

    # WHERE
    if where_conditions:
        where_clause = "WHERE " + " AND ".join(where_conditions)
        sql_parts.append(where_clause)

    # ORDER BY
    if order_by:
        order_clause = "ORDER BY " + ", ".join(order_by)
        sql_parts.append(order_clause)

    return " ".join(sql_parts)


def build_complex_query_new_way(
    table: Table, select_cols: list = None, where_conditions: list = None, order_by: list = None
):
    """
    NEW WAY: SQLAlchemy query composition
    Benefits:
    - Chainable API
    - Type checking
    - Query validation
    - Can inspect/modify before execution
    """
    # SELECT columns
    if select_cols:
        stmt = select(*[table.c[col] for col in select_cols])
    else:
        stmt = select(table)

    # WHERE conditions
    if where_conditions:
        stmt = stmt.where(and_(*where_conditions))

    # ORDER BY
    if order_by:
        order_cols = []
        for col_spec in order_by:
            if isinstance(col_spec, tuple):  # (column, direction)
                col_name, direction = col_spec
                if direction == "DESC":
                    order_cols.append(desc(table.c[col_name]))
                else:
                    order_cols.append(asc(table.c[col_name]))
            else:
                order_cols.append(table.c[col_spec])
        stmt = stmt.order_by(*order_cols)

    return stmt


# ============================================================================
# Example 5: Window Functions (from sql_builder.py)
# ============================================================================


def window_function_old_way(table_name: str, partition_cols: list, order_cols: list):
    """
    OLD WAY: Manual window function SQL
    Issues:
    - Complex string building
    - Error-prone
    - Hard to maintain
    """
    partition_by = ", ".join(f'"{col}"' for col in partition_cols)
    order_by = ", ".join(f'"{col}"' for col in order_cols)

    sql = f"""
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY {partition_by} ORDER BY {order_by}) as rank
        FROM "{table_name}"
    """
    return sql


def window_function_new_way(table: Table, partition_cols: list, order_cols: list):
    """
    NEW WAY: SQLAlchemy window functions
    Benefits:
    - Clear, readable code
    - Type-safe
    - Reusable window specs
    """
    from sqlalchemy import over

    # Create window specification
    window_spec = over(
        partition_by=[table.c[col] for col in partition_cols],
        order_by=[table.c[col] for col in order_cols],
    )

    # Apply window function
    rank_column = func.row_number().over(window_spec).label("rank")

    # Build query
    stmt = select(table, rank_column)

    return stmt


# ============================================================================
# Example 6: JOIN Operations (from sql_builder.py)
# ============================================================================


def join_tables_old_way(table1: str, table2: str, join_col: str, join_type: str = "INNER"):
    """
    OLD WAY: String-based JOIN
    Issues:
    - Manual join syntax
    - Easy to make mistakes
    - No validation of join conditions
    """
    sql = f"""
        SELECT *
        FROM "{table1}" t1
        {join_type} JOIN "{table2}" t2 ON t1."{join_col}" = t2."{join_col}"
    """
    return sql


def join_tables_new_way(table1: Table, table2: Table, join_col: str):
    """
    NEW WAY: SQLAlchemy join()
    Benefits:
    - Type-safe joins
    - Clear join conditions
    - Supports complex join logic
    """
    # Inner join (default)
    stmt = select(table1, table2).select_from(
        table1.join(table2, table1.c[join_col] == table2.c[join_col])
    )

    # Or for left join:
    # stmt = select(table1, table2).select_from(
    #     table1.outerjoin(table2, table1.c[join_col] == table2.c[join_col])
    # )

    return stmt


# ============================================================================
# Example 7: Aggregate Functions (from sql_builder.py)
# ============================================================================


def aggregate_old_way(table_name: str, group_cols: list, agg_col: str):
    """
    OLD WAY: Raw aggregate SQL
    Issues:
    - Manual GROUP BY syntax
    - No aggregate validation
    """
    group_by = ", ".join(f'"{col}"' for col in group_cols)
    sql = f"""
        SELECT {group_by},
               COUNT(*) as count,
               AVG("{agg_col}") as avg_value,
               MAX("{agg_col}") as max_value
        FROM "{table_name}"
        GROUP BY {group_by}
    """
    return sql


def aggregate_new_way(table: Table, group_cols: list, agg_col: str):
    """
    NEW WAY: SQLAlchemy aggregate functions
    Benefits:
    - Built-in functions (count, avg, max, etc.)
    - Type checking
    - Composable
    """
    stmt = select(
        *[table.c[col] for col in group_cols],
        func.count().label("count"),
        func.avg(table.c[agg_col]).label("avg_value"),
        func.max(table.c[agg_col]).label("max_value"),
    ).group_by(*[table.c[col] for col in group_cols])

    return stmt


# ============================================================================
# Example 8: CREATE TABLE AS SELECT (from duckdb_materializer.py)
# ============================================================================


def create_table_as_select_old_way(
    connection, new_table: str, source_table: str, where_clause: str
):
    """
    OLD WAY: Raw CREATE TABLE AS
    Issues:
    - String concatenation
    - No validation
    """
    if where_clause:
        sql = f'CREATE TABLE "{new_table}" AS SELECT * FROM "{source_table}" WHERE {where_clause}'
    else:
        sql = f'CREATE TABLE "{new_table}" AS SELECT * FROM "{source_table}"'

    connection.execute(sql)


def create_table_as_select_new_way(
    engine, new_table: str, source_table: Table, where_conditions: list = None
):
    """
    NEW WAY: SQLAlchemy CREATE TABLE AS with insert().from_select()
    Benefits:
    - Two-step process (create, then insert)
    - Better error handling
    - Can inspect query before execution
    """
    metadata = MetaData()

    # Create target table with same structure
    target_columns = [col.copy() for col in source_table.columns]
    target_table = Table(new_table, metadata, *target_columns)
    target_table.create(engine)

    # Build select query
    stmt = select(source_table)
    if where_conditions:
        stmt = stmt.where(and_(*where_conditions))

    # Insert from select
    with Session(engine) as session:
        insert_stmt = insert(target_table).from_select(
            [col.name for col in source_table.columns], stmt
        )
        session.execute(insert_stmt)
        session.commit()


# ============================================================================
# Example 9: Dynamic Column Types (Utility Function)
# ============================================================================


def mock_spark_type_to_sqlalchemy(mock_type):
    """
    Convert MockSpark types to SQLAlchemy types.
    This helper function should be added to a new utility module.
    """
    from mock_spark.spark_types import (
        StringType,
        IntegerType,
        LongType,
        DoubleType,
        FloatType,
        BooleanType,
        DateType,
        TimestampType,
    )
    from sqlalchemy import BigInteger, Date, DateTime

    type_mapping = {
        StringType: String,
        IntegerType: Integer,
        LongType: BigInteger,
        DoubleType: Float,
        FloatType: Float,
        BooleanType: Boolean,
        DateType: Date,
        TimestampType: DateTime,
    }

    return type_mapping.get(type(mock_type), String)


# ============================================================================
# Example 10: Table Factory Pattern (Recommended Utility)
# ============================================================================


class DuckDBTableFactory:
    """
    Factory for creating SQLAlchemy tables from MockSpark schemas.
    Add this to: mock_spark/storage/sqlalchemy_utils.py
    """

    @staticmethod
    def from_mock_schema(table_name: str, mock_schema, metadata: MetaData) -> Table:
        """
        Create SQLAlchemy Table from MockSpark schema.

        Args:
            table_name: Name for the table
            mock_schema: MockStructType instance
            metadata: SQLAlchemy MetaData instance

        Returns:
            SQLAlchemy Table object
        """
        columns = []
        for field in mock_schema.fields:
            col_type = mock_spark_type_to_sqlalchemy(field.dataType)
            columns.append(Column(field.name, col_type, nullable=field.nullable))

        return Table(table_name, metadata, *columns)


# ============================================================================
# Usage Example: Complete Flow
# ============================================================================


def complete_refactoring_example():
    """
    Complete example showing how to refactor a typical Mock Spark flow.
    """
    # Setup
    engine = create_engine("duckdb:///:memory:")
    metadata = MetaData()

    # 1. Create table (OLD WAY)
    # create_table_old_way(engine.raw_connection(), "users", [("id", "INTEGER"), ("name", "VARCHAR")])

    # 1. Create table (NEW WAY)
    users_table = Table(
        "users", metadata, Column("id", Integer), Column("name", String), Column("age", Integer)
    )
    users_table.create(engine)

    # 2. Insert data (NEW WAY)
    with Session(engine) as session:
        data = [
            {"id": 1, "name": "Alice", "age": 25},
            {"id": 2, "name": "Bob", "age": 30},
            {"id": 3, "name": "Charlie", "age": 35},
        ]
        session.execute(insert(users_table), data)
        session.commit()

    # 3. Query with filter (NEW WAY)
    with Session(engine) as session:
        stmt = select(users_table).where(users_table.c.age > 25)
        results = session.execute(stmt).all()
        print("Users over 25:", results)

    # 4. Complex query with aggregation (NEW WAY)
    with Session(engine) as session:
        stmt = select(
            func.count(users_table.c.id).label("count"),
            func.avg(users_table.c.age).label("avg_age"),
        )
        result = session.execute(stmt).first()
        print(f"Count: {result.count}, Avg Age: {result.avg_age}")


# ============================================================================
# Testing Helper: Compare Old vs New
# ============================================================================


def test_sql_equivalence():
    """
    Helper to verify that refactored code produces equivalent SQL.
    """
    from sqlalchemy.dialects import sqlite

    engine = create_engine("duckdb:///:memory:")
    metadata = MetaData()

    table = Table("test", metadata, Column("id", Integer), Column("name", String))

    # Build query with new way
    stmt = select(table).where(table.c.id > 5).order_by(desc(table.c.name))

    # Compile to SQL string for comparison
    compiled = stmt.compile(dialect=sqlite.dialect(), compile_kwargs={"literal_binds": True})
    print("Generated SQL:", compiled)

    # Expected SQL (old way)
    # SELECT * FROM test WHERE id > 5 ORDER BY name DESC


if __name__ == "__main__":
    print("=== SQLAlchemy Refactoring Examples ===\n")
    print("See function implementations for before/after comparisons\n")
    print("Running complete example...\n")

    complete_refactoring_example()

    print("\n=== Testing SQL Equivalence ===\n")
    test_sql_equivalence()
