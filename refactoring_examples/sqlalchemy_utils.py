"""
SQLAlchemy Utility Functions for Mock Spark

This module provides helper functions to convert between MockSpark types
and SQLAlchemy types, and utilities for creating tables programmatically.

Recommended location: mock_spark/storage/sqlalchemy_utils.py
"""

from sqlalchemy import (
    Table,
    Column,
    Integer,
    BigInteger,
    String,
    Float,
    Boolean,
    Date,
    DateTime,
    MetaData,
    LargeBinary,
    Numeric,
)
from typing import Any


def mock_type_to_sqlalchemy(mock_type: Any):
    """
    Convert MockSpark data type to SQLAlchemy type.

    Args:
        mock_type: MockSpark data type instance (e.g., StringType(), IntegerType())

    Returns:
        SQLAlchemy type class

    Example:
        >>> from mock_spark.spark_types import StringType, IntegerType
        >>> mock_type_to_sqlalchemy(StringType())
        <class 'sqlalchemy.sql.sqltypes.String'>
    """
    # Import MockSpark types (avoid circular import)
    try:
        from mock_spark.spark_types import (
            StringType,
            IntegerType,
            LongType,
            DoubleType,
            FloatType,
            BooleanType,
            DateType,
            TimestampType,
            BinaryType,
            DecimalType,
            ShortType,
            ByteType,
            NullType,
            ArrayType,
            MapType,
            StructType,
        )
    except ImportError:
        # Fallback if types can't be imported
        return String

    # Type mapping
    type_mapping = {
        "StringType": String,
        "IntegerType": Integer,
        "LongType": BigInteger,
        "DoubleType": Float,
        "FloatType": Float,
        "BooleanType": Boolean,
        "DateType": Date,
        "TimestampType": DateTime,
        "BinaryType": LargeBinary,
        "DecimalType": Numeric,
        "ShortType": Integer,  # SQLite doesn't have SMALLINT
        "ByteType": Integer,  # SQLite doesn't have TINYINT
        "NullType": String,  # Default to VARCHAR for NULL type
        "ArrayType": LargeBinary,  # Store as BLOB
        "MapType": LargeBinary,  # Store as BLOB
        "StructType": LargeBinary,  # Store as BLOB
    }

    # Get type name
    type_name = type(mock_type).__name__

    return type_mapping.get(type_name, String)


def sqlalchemy_type_to_mock(sqlalchemy_type: Any):
    """
    Convert SQLAlchemy type to MockSpark data type.

    Args:
        sqlalchemy_type: SQLAlchemy type instance

    Returns:
        MockSpark type instance

    Example:
        >>> from sqlalchemy import String
        >>> sqlalchemy_type_to_mock(String())
        StringType()
    """
    try:
        from mock_spark.spark_types import (
            StringType,
            IntegerType,
            LongType,
            DoubleType,
            FloatType,
            BooleanType,
            DateType,
            TimestampType,
            BinaryType,
            DecimalType,
        )
    except ImportError:
        # Fallback
        from mock_spark.spark_types import StringType

        return StringType()

    # Reverse type mapping
    type_mapping = {
        "String": StringType,
        "VARCHAR": StringType,
        "Integer": IntegerType,
        "BigInteger": LongType,
        "Float": DoubleType,
        "DOUBLE": DoubleType,
        "Boolean": BooleanType,
        "Date": DateType,
        "DateTime": TimestampType,
        "TIMESTAMP": TimestampType,
        "LargeBinary": BinaryType,
        "BLOB": BinaryType,
        "Numeric": DecimalType,
    }

    type_name = type(sqlalchemy_type).__name__
    mock_type_class = type_mapping.get(type_name, StringType)

    return mock_type_class()


def create_table_from_mock_schema(
    table_name: str, mock_schema: Any, metadata: MetaData, **kwargs
) -> Table:
    """
    Create SQLAlchemy Table from MockSpark schema.

    Args:
        table_name: Name for the table
        mock_schema: MockStructType instance with fields
        metadata: SQLAlchemy MetaData instance
        **kwargs: Additional arguments for Table() (e.g., prefixes=['TEMPORARY'])

    Returns:
        SQLAlchemy Table object

    Example:
        >>> from mock_spark.spark_types import MockStructType, MockStructField, StringType, IntegerType
        >>> from sqlalchemy import MetaData
        >>> schema = MockStructType([
        ...     MockStructField("id", IntegerType()),
        ...     MockStructField("name", StringType())
        ... ])
        >>> metadata = MetaData()
        >>> table = create_table_from_mock_schema("users", schema, metadata)
        >>> table.c.id.type
        <class 'sqlalchemy.sql.sqltypes.Integer'>
    """
    columns = []

    for field in mock_schema.fields:
        sql_type = mock_type_to_sqlalchemy(field.dataType)
        nullable = getattr(field, "nullable", True)

        columns.append(Column(field.name, sql_type, nullable=nullable))

    return Table(table_name, metadata, *columns, **kwargs)


def get_column_type_for_value(value: Any):
    """
    Infer SQLAlchemy column type from a Python value.

    Args:
        value: Python value to infer type from

    Returns:
        SQLAlchemy type class

    Example:
        >>> get_column_type_for_value(42)
        <class 'sqlalchemy.sql.sqltypes.Integer'>
        >>> get_column_type_for_value("hello")
        <class 'sqlalchemy.sql.sqltypes.String'>
    """
    if isinstance(value, bool):
        # Check bool before int (bool is subclass of int)
        return Boolean
    elif isinstance(value, int):
        return Integer
    elif isinstance(value, float):
        return Float
    elif isinstance(value, str):
        return String
    elif isinstance(value, bytes):
        return LargeBinary
    elif value is None:
        return String  # Default for NULL
    else:
        return String  # Default fallback


def create_table_from_data(table_name: str, data: list, metadata: MetaData, **kwargs) -> Table:
    """
    Create SQLAlchemy Table by inferring types from data.

    Args:
        table_name: Name for the table
        data: List of dicts with data (uses first row for type inference)
        metadata: SQLAlchemy MetaData instance
        **kwargs: Additional arguments for Table()

    Returns:
        SQLAlchemy Table object

    Example:
        >>> data = [
        ...     {"id": 1, "name": "Alice", "age": 25},
        ...     {"id": 2, "name": "Bob", "age": 30}
        ... ]
        >>> metadata = MetaData()
        >>> table = create_table_from_data("users", data, metadata)
    """
    if not data:
        raise ValueError("Cannot infer schema from empty data")

    # Infer types from first row
    first_row = data[0]
    columns = []

    for key, value in first_row.items():
        col_type = get_column_type_for_value(value)
        columns.append(Column(key, col_type))

    return Table(table_name, metadata, *columns, **kwargs)


class TableFactory:
    """
    Factory class for creating SQLAlchemy tables in various ways.

    This provides a clean API for table creation across the codebase.
    """

    def __init__(self, metadata: MetaData = None):
        """
        Initialize TableFactory.

        Args:
            metadata: SQLAlchemy MetaData instance (creates new if None)
        """
        self.metadata = metadata or MetaData()

    def from_mock_schema(self, table_name: str, mock_schema: Any, **kwargs) -> Table:
        """Create table from MockSpark schema."""
        return create_table_from_mock_schema(table_name, mock_schema, self.metadata, **kwargs)

    def from_data(self, table_name: str, data: list, **kwargs) -> Table:
        """Create table by inferring types from data."""
        return create_table_from_data(table_name, data, self.metadata, **kwargs)

    def from_columns(self, table_name: str, columns: list, **kwargs) -> Table:
        """Create table from list of Column objects."""
        return Table(table_name, self.metadata, *columns, **kwargs)


class ColumnBuilder:
    """
    Helper for building SQLAlchemy columns with fluent API.

    Example:
        >>> col = ColumnBuilder("id").integer().primary_key().build()
        >>> col = ColumnBuilder("name").string(100).not_null().build()
    """

    def __init__(self, name: str):
        self.name = name
        self._type = String
        self._kwargs = {}

    def string(self, length: int = None):
        """Set type to String."""
        self._type = String(length) if length else String
        return self

    def integer(self):
        """Set type to Integer."""
        self._type = Integer
        return self

    def bigint(self):
        """Set type to BigInteger."""
        self._type = BigInteger
        return self

    def float(self):
        """Set type to Float."""
        self._type = Float
        return self

    def boolean(self):
        """Set type to Boolean."""
        self._type = Boolean
        return self

    def date(self):
        """Set type to Date."""
        self._type = Date
        return self

    def datetime(self):
        """Set type to DateTime."""
        self._type = DateTime
        return self

    def primary_key(self):
        """Set as primary key."""
        self._kwargs["primary_key"] = True
        return self

    def not_null(self):
        """Set as NOT NULL."""
        self._kwargs["nullable"] = False
        return self

    def nullable(self):
        """Set as nullable."""
        self._kwargs["nullable"] = True
        return self

    def default(self, value):
        """Set default value."""
        self._kwargs["default"] = value
        return self

    def build(self) -> Column:
        """Build the Column object."""
        return Column(self.name, self._type, **self._kwargs)


# ============================================================================
# Query Building Helpers
# ============================================================================


def safe_column_reference(table: Table, column_name: str):
    """
    Safely reference a column, raising helpful error if not found.

    Args:
        table: SQLAlchemy Table
        column_name: Name of column to reference

    Returns:
        Column object

    Raises:
        ValueError: If column doesn't exist
    """
    if column_name not in table.c:
        available = ", ".join(table.c.keys())
        raise ValueError(
            f"Column '{column_name}' not found in table '{table.name}'. "
            f"Available columns: {available}"
        )

    return table.c[column_name]


def build_where_clause(table: Table, conditions: dict):
    """
    Build WHERE clause from dict of conditions.

    Args:
        table: SQLAlchemy Table
        conditions: Dict of column_name: value

    Returns:
        SQLAlchemy BinaryExpression for WHERE clause

    Example:
        >>> conditions = {"age": 25, "name": "Alice"}
        >>> where_clause = build_where_clause(users_table, conditions)
        >>> stmt = select(users_table).where(where_clause)
    """
    from sqlalchemy import and_

    clauses = []
    for col_name, value in conditions.items():
        col = safe_column_reference(table, col_name)
        clauses.append(col == value)

    return and_(*clauses) if clauses else None


# ============================================================================
# Migration Helpers
# ============================================================================


def wrap_raw_sql_safely(connection, sql: str, params: dict = None):
    """
    Wrapper for executing raw SQL with proper error handling.
    Use this temporarily during migration period.

    Args:
        connection: SQLAlchemy connection
        sql: Raw SQL string
        params: Parameters to bind (optional)

    Returns:
        Result of query

    Example:
        >>> # During migration, wrap raw SQL:
        >>> result = wrap_raw_sql_safely(conn, "SELECT * FROM users WHERE id = :id", {"id": 1})
    """
    from sqlalchemy import text

    try:
        if params:
            return connection.execute(text(sql), params)
        else:
            return connection.execute(text(sql))
    except Exception as e:
        raise ValueError(f"SQL execution failed: {e}\nSQL: {sql}\nParams: {params}") from e


def compare_sql_output(old_sql: str, new_stmt, connection):
    """
    Helper to verify that refactored queries produce same results.

    Args:
        old_sql: Old raw SQL string
        new_stmt: New SQLAlchemy statement
        connection: Database connection

    Returns:
        tuple: (results_match: bool, old_results, new_results)
    """
    from sqlalchemy import text

    # Execute old SQL
    old_results = connection.execute(text(old_sql)).fetchall()

    # Execute new statement
    new_results = connection.execute(new_stmt).fetchall()

    # Compare
    results_match = old_results == new_results

    return results_match, old_results, new_results


# ============================================================================
# Testing Utilities
# ============================================================================


def print_compiled_sql(stmt, dialect=None):
    """
    Print compiled SQL for debugging.

    Args:
        stmt: SQLAlchemy statement
        dialect: SQLAlchemy dialect (uses default if None)

    Example:
        >>> from sqlalchemy import select
        >>> stmt = select(users_table).where(users_table.c.age > 25)
        >>> print_compiled_sql(stmt)
    """
    compiled = stmt.compile(compile_kwargs={"literal_binds": True})
    print(f"Compiled SQL:\n{compiled}\n")
    return str(compiled)


# ============================================================================
# Example Usage
# ============================================================================


if __name__ == "__main__":
    print("=== SQLAlchemy Utilities for Mock Spark ===\n")

    # Example 1: Create table from MockSpark schema
    print("1. Creating table from MockSpark schema...")
    from mock_spark.spark_types import MockStructType, MockStructField, StringType, IntegerType

    schema = MockStructType(
        [MockStructField("id", IntegerType()), MockStructField("name", StringType(), nullable=True)]
    )

    metadata = MetaData()
    table = create_table_from_mock_schema("users", schema, metadata)
    print(f"   Created table: {table.name}")
    print(f"   Columns: {list(table.c.keys())}")

    # Example 2: Create table from data
    print("\n2. Creating table from data...")
    data = [{"id": 1, "name": "Alice", "age": 25}, {"id": 2, "name": "Bob", "age": 30}]

    metadata2 = MetaData()
    table2 = create_table_from_data("employees", data, metadata2)
    print(f"   Created table: {table2.name}")
    print(f"   Columns: {[(col.name, type(col.type).__name__) for col in table2.columns]}")

    # Example 3: Using TableFactory
    print("\n3. Using TableFactory...")
    factory = TableFactory()
    table3 = factory.from_columns(
        "products",
        [
            ColumnBuilder("id").integer().primary_key().build(),
            ColumnBuilder("name").string(100).not_null().build(),
            ColumnBuilder("price").float().build(),
        ],
    )
    print(f"   Created table: {table3.name}")
    print(
        f"   Columns: {[(col.name, type(col.type).__name__, col.nullable) for col in table3.columns]}"
    )

    print("\n✅ All examples completed!")
