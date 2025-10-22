"""
SQLAlchemy Helper Functions for Mock Spark.

Provides utilities for converting between MockSpark types and SQLAlchemy types,
creating tables programmatically, and working with SQLAlchemy Inspector.
"""

from typing import Any, List, Dict, Optional
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
    inspect,
)
from sqlalchemy.engine import Engine


def mock_type_to_sqlalchemy(mock_type: Any) -> Any:
    """
    Convert MockSpark data type to SQLAlchemy type.

    Args:
        mock_type: MockSpark data type instance (e.g., StringType(), IntegerType())

    Returns:
        SQLAlchemy type class
    """
    from sqlalchemy import ARRAY, VARCHAR
    from sqlalchemy.types import TypeDecorator

    # Get type name
    type_name = type(mock_type).__name__

    # Handle ArrayType with element type
    if "ArrayType" in type_name or "Array" in type_name:
        # Check if the array has an element type specified
        if hasattr(mock_type, "elementType"):
            element_sql_type = mock_type_to_sqlalchemy(mock_type.elementType)
            return ARRAY(element_sql_type)
        else:
            # Fallback to VARCHAR for untyped arrays
            return ARRAY(VARCHAR)

    # Handle MapType - use custom type that will be handled with raw SQL
    if "MapType" in type_name or "Map" in type_name:
        # Return a marker type that we can detect later
        class DuckDBMapType(TypeDecorator):
            impl = String
            cache_ok = True

            def __init__(self) -> None:
                super().__init__()
                self.is_duckdb_map = True  # Marker attribute

        return DuckDBMapType

    # Type mapping from MockSpark type names to SQLAlchemy types
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
        "ShortType": Integer,
        "ByteType": Integer,
    }

    return type_mapping.get(type_name, String)


def sqlalchemy_type_to_mock(sqlalchemy_type: Any) -> Any:
    """
    Convert SQLAlchemy type to MockSpark data type.

    Args:
        sqlalchemy_type: SQLAlchemy type instance

    Returns:
        MockSpark type instance
    """
    from mock_spark.spark_types import (
        StringType,
        IntegerType,
        LongType,
        DoubleType,
        BooleanType,
        DateType,
        TimestampType,
        BinaryType,
        DecimalType,
    )

    # Reverse type mapping from SQLAlchemy type names to MockSpark types
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
    table_name: str, mock_schema: Any, metadata: MetaData, **kwargs: Any
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
    """
    columns: List[Any] = []

    for field in mock_schema.fields:
        sql_type = mock_type_to_sqlalchemy(field.dataType)
        nullable = getattr(field, "nullable", True)

        columns.append(Column(field.name, sql_type, nullable=nullable))

    return Table(table_name, metadata, *columns, **kwargs)


def get_column_type_for_value(value: Any) -> Any:
    """
    Infer SQLAlchemy column type from a Python value.

    Args:
        value: Python value to infer type from

    Returns:
        SQLAlchemy type class
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


def create_table_from_data(
    table_name: str, data: List[Dict[str, Any]], metadata: MetaData, **kwargs: Any
) -> Table:
    """
    Create SQLAlchemy Table by inferring types from data.

    Args:
        table_name: Name for the table
        data: List of dicts with data (uses first row for type inference)
        metadata: SQLAlchemy MetaData instance
        **kwargs: Additional arguments for Table()

    Returns:
        SQLAlchemy Table object
    """
    if not data:
        raise ValueError("Cannot infer schema from empty data")

    # Infer types from first row
    first_row = data[0]
    columns: List[Any] = []

    for key, value in first_row.items():
        col_type = get_column_type_for_value(value)
        columns.append(Column(key, col_type))

    return Table(table_name, metadata, *columns, **kwargs)


def list_all_tables(engine: Engine) -> List[str]:
    """
    List all tables using SQLAlchemy Inspector.

    Args:
        engine: SQLAlchemy engine

    Returns:
        List of table names
    """
    inspector = inspect(engine)
    return inspector.get_table_names()


def table_exists(engine: Engine, table_name: str) -> bool:
    """
    Check if table exists using SQLAlchemy Inspector.

    Args:
        engine: SQLAlchemy engine
        table_name: Name of table to check

    Returns:
        True if table exists, False otherwise
    """
    inspector = inspect(engine)
    return inspector.has_table(table_name)


def get_table_columns(engine: Engine, table_name: str) -> List[Any]:
    """
    Get table column metadata using SQLAlchemy Inspector.

    Args:
        engine: SQLAlchemy engine
        table_name: Name of table

    Returns:
        List of column metadata (ReflectedColumn objects that act like dicts)
    """
    inspector = inspect(engine)
    return inspector.get_columns(table_name)


def reflect_table(engine: Engine, table_name: str, metadata: MetaData) -> Table:
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
    """
    Factory class for creating SQLAlchemy tables in various ways.

    Provides a clean API for table creation across the codebase.
    """

    def __init__(self, metadata: Optional[MetaData] = None):
        """
        Initialize TableFactory.

        Args:
            metadata: SQLAlchemy MetaData instance (creates new if None)
        """
        self.metadata = metadata or MetaData()

    def from_mock_schema(
        self, table_name: str, mock_schema: Any, **kwargs: Any
    ) -> Table:
        """Create table from MockSpark schema."""
        return create_table_from_mock_schema(
            table_name, mock_schema, self.metadata, **kwargs
        )

    def from_data(
        self, table_name: str, data: List[Dict[str, Any]], **kwargs: Any
    ) -> Table:
        """Create table by inferring types from data."""
        return create_table_from_data(table_name, data, self.metadata, **kwargs)

    def from_columns(
        self, table_name: str, columns: List[Column], **kwargs: Any
    ) -> Table:
        """Create table from list of Column objects."""
        return Table(table_name, self.metadata, *columns, **kwargs)
