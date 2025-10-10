"""
Mock data types and schema system for Mock Spark.

This module provides comprehensive mock implementations of PySpark data types
and schema structures that behave identically to the real PySpark types.
Includes primitive types, complex types, schema definitions, and Row objects
for complete type system compatibility.

Key Features:
    - Complete PySpark data type hierarchy
    - Primitive types (String, Integer, Long, Double, Boolean)
    - Complex types (Array, Map, Struct)
    - Schema definition with MockStructType and MockStructField
    - Row objects with PySpark-compatible interface
    - Type inference and conversion utilities

Example:
    >>> from mock_spark.spark_types import StringType, IntegerType, MockStructType, MockStructField
    >>> schema = MockStructType([
    ...     MockStructField("name", StringType()),
    ...     MockStructField("age", IntegerType())
    ... ])
    >>> df = spark.createDataFrame(data, schema)
"""

from typing import (
    Any,
    Dict,
    List,
    Optional,
    Iterator,
    KeysView,
    ValuesView,
    ItemsView,
)
from dataclasses import dataclass


class MockDataType:
    """Base class for mock data types.

    Provides the foundation for all data types in the Mock Spark type system.
    Supports nullable/non-nullable semantics and PySpark-compatible type names.

    Attributes:
        nullable: Whether the data type allows null values.

    Example:
        >>> StringType()
        StringType(nullable=True)
        >>> IntegerType(nullable=False)
        IntegerType(nullable=False)
    """

    def __init__(self, nullable: bool = True):
        self.nullable = nullable

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, self.__class__) and self.nullable == other.nullable

    def __hash__(self) -> int:
        """Hash method to make MockDataType hashable."""
        return hash((self.__class__.__name__, self.nullable))

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(nullable={self.nullable})"

    def typeName(self) -> str:
        """Get PySpark-compatible type name."""
        type_mapping = {
            "StringType": "string",
            "IntegerType": "int",
            "LongType": "bigint",
            "DoubleType": "double",
            "BooleanType": "boolean",
            "DateType": "date",
            "TimestampType": "timestamp",
            "FloatType": "float",
            "ShortType": "smallint",
            "ByteType": "tinyint",
            "DecimalType": "decimal",
            "BinaryType": "binary",
            "NullType": "null",
            "ArrayType": "array",
            "MapType": "map",
            "StructType": "struct",
        }
        return type_mapping.get(self.__class__.__name__, self.__class__.__name__.lower())


class StringType(MockDataType):
    """Mock StringType."""

    pass


class IntegerType(MockDataType):
    """Mock IntegerType."""

    pass


class LongType(MockDataType):
    """Mock LongType."""

    pass


class DoubleType(MockDataType):
    """Mock DoubleType."""

    pass


class BooleanType(MockDataType):
    """Mock BooleanType."""

    pass


class DateType(MockDataType):
    """Mock DateType."""

    pass


class TimestampType(MockDataType):
    """Mock TimestampType."""

    pass


class DecimalType(MockDataType):
    """Mock decimal type."""

    def __init__(self, precision: int = 10, scale: int = 0, nullable: bool = True):
        """Initialize DecimalType."""
        super().__init__(nullable)
        self.precision = precision
        self.scale = scale

    def __repr__(self) -> str:
        """String representation."""
        return f"DecimalType({self.precision}, {self.scale})"


class ArrayType(MockDataType):
    """Mock array type."""

    def __init__(self, element_type: MockDataType, nullable: bool = True):
        """Initialize ArrayType."""
        super().__init__(nullable)
        self.element_type = element_type

    def __repr__(self) -> str:
        """String representation."""
        return f"ArrayType({self.element_type})"


class MapType(MockDataType):
    """Mock map type."""

    def __init__(self, key_type: MockDataType, value_type: MockDataType, nullable: bool = True):
        """Initialize MapType."""
        super().__init__(nullable)
        self.key_type = key_type
        self.value_type = value_type

    def __repr__(self) -> str:
        """String representation."""
        return f"MapType({self.key_type}, {self.value_type})"


class BinaryType(MockDataType):
    """Mock BinaryType for binary data."""

    pass


class NullType(MockDataType):
    """Mock NullType for null values."""

    pass


class FloatType(MockDataType):
    """Mock FloatType for single precision floating point numbers."""

    pass


class ShortType(MockDataType):
    """Mock ShortType for short integers."""

    pass


class ByteType(MockDataType):
    """Mock ByteType for byte values."""

    pass


@dataclass
class MockStructField:
    """Mock StructField for schema definition."""

    name: str
    dataType: MockDataType
    nullable: bool = True
    metadata: Optional[Dict[str, Any]] = None

    def __post_init__(self) -> None:
        if self.metadata is None:
            self.metadata = {}
        # Add field_type attribute for compatibility
        self.field_type = self.dataType

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, MockStructField)
            and self.name == other.name
            and self.dataType == other.dataType
            and self.nullable == other.nullable
        )

    def __repr__(self) -> str:
        return f"MockStructField(name='{self.name}', dataType={self.dataType}, nullable={self.nullable})"


class StructType(MockDataType):
    """Mock struct type."""

    def __init__(self, fields: Optional[List[MockStructField]] = None, nullable: bool = True):
        """Initialize StructType."""
        super().__init__(nullable)
        self.fields = fields or []

    def __repr__(self) -> str:
        """String representation."""
        return f"StructType({self.fields})"


class MockStructType(StructType):
    """Mock StructType for schema definition."""

    def __init__(self, fields: Optional[List[MockStructField]] = None):
        super().__init__(fields or [])
        self.fields = fields or []
        self._field_map = {field.name: field for field in self.fields}

    def __getitem__(self, index: int) -> MockStructField:
        return self.fields[index]

    def __len__(self) -> int:
        return len(self.fields)

    def __iter__(self) -> Iterator[MockStructField]:
        return iter(self.fields)

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, MockStructType) and self.fields == other.fields

    def __repr__(self) -> str:
        fields_str = ", ".join(repr(field) for field in self.fields)
        return f"MockStructType([{fields_str}])"

    def merge_with(self, other: "MockStructType") -> "MockStructType":
        """Merge this schema with another, adding new fields from other.

        Args:
            other: Schema to merge with

        Returns:
            New schema with fields from both schemas
        """
        # Create dict of existing fields by name
        existing_fields = {f.name: f for f in self.fields}

        # Add fields from other that don't exist
        merged_fields = list(self.fields)  # Start with current fields
        for field in other.fields:
            if field.name not in existing_fields:
                merged_fields.append(field)

        return MockStructType(merged_fields)

    def has_same_columns(self, other: "MockStructType") -> bool:
        """Check if two schemas have the same column names.

        Args:
            other: Schema to compare with

        Returns:
            True if column names match, False otherwise
        """
        self_cols = {f.name for f in self.fields}
        other_cols = {f.name for f in other.fields}
        return self_cols == other_cols

    def fieldNames(self) -> List[str]:
        """Get list of field names."""
        return [field.name for field in self.fields]

    def getFieldIndex(self, name: str) -> int:
        """Get index of field by name."""
        if name not in self._field_map:
            raise ValueError(f"Field '{name}' not found in schema")
        return self.fields.index(self._field_map[name])

    def contains(self, name: str) -> bool:
        """Check if field exists in schema."""
        return name in self._field_map

    def add_field(self, field: MockStructField) -> None:
        """Add a field to the struct type."""
        self.fields.append(field)
        self._field_map[field.name] = field

    def get_field_by_name(self, name: str) -> Optional[MockStructField]:
        """Get field by name."""
        return self._field_map.get(name)

    def has_field(self, name: str) -> bool:
        """Check if field exists in schema."""
        return name in self._field_map


@dataclass
class MockDatabase:
    """Mock database representation."""

    name: str
    description: Optional[str] = None
    locationUri: Optional[str] = None

    def __repr__(self) -> str:
        return f"MockDatabase(name='{self.name}')"


@dataclass
class MockTable:
    """Mock table representation."""

    name: str
    database: str
    tableType: str = "MANAGED"
    isTemporary: bool = False

    def __repr__(self) -> str:
        return f"MockTable(name='{self.name}', database='{self.database}')"


# Type conversion utilities
def convert_python_type_to_mock_type(python_type: type) -> MockDataType:
    """Convert Python type to MockDataType."""
    type_mapping = {
        str: StringType(),
        int: LongType(),  # Use LongType for integers to match PySpark
        float: DoubleType(),
        bool: BooleanType(),
        bytes: BinaryType(),
        type(None): NullType(),
    }

    return type_mapping.get(python_type, StringType())


def infer_schema_from_data(data: List[Dict[str, Any]]) -> MockStructType:
    """Infer schema from data."""
    if not data:
        return MockStructType([])

    # Get field names and types from first row
    first_row = data[0]
    fields = []

    for name, value in first_row.items():
        if value is None:
            data_type: MockDataType = StringType()
        else:
            data_type = convert_python_type_to_mock_type(type(value))

        fields.append(MockStructField(name=name, dataType=data_type))

    return MockStructType(fields)


def create_schema_from_columns(columns: List[str]) -> MockStructType:
    """Create schema from column names (all StringType)."""
    fields = [MockStructField(name=col, dataType=StringType()) for col in columns]
    return MockStructType(fields)


class MockRow:
    """Mock Row object providing PySpark-compatible row interface.

    Represents a single row in a DataFrame with PySpark-compatible methods
    for accessing data by index, key, or attribute.

    Attributes:
        data: Dictionary containing row data.

    Example:
        >>> row = MockRow({"name": "Alice", "age": 25})
        >>> row.name
        'Alice'
        >>> row[0]
        'Alice'
        >>> row.asDict()
        {'name': 'Alice', 'age': 25}
    """

    def __init__(self, data: Any, schema: Optional["MockStructType"] = None):
        """Initialize MockRow.

        Args:
            data: Row data. Accepts dict-like (name -> value) or sequence-like (values).
            schema: Optional schema providing ordered field names for index access.
        """
        self._schema = schema
        # Normalize to dict storage while preserving order based on schema if provided
        if isinstance(data, dict):
            if schema is not None and getattr(schema, "fields", None):
                # Reorder dict according to schema field order
                ordered_items = [(f.name, data.get(f.name)) for f in schema.fields]
                self.data = {k: v for k, v in ordered_items}
            else:
                self.data = dict(data)
        else:
            # sequence-like data paired with schema
            if schema is None or not getattr(schema, "fields", None):
                raise ValueError("Sequence row data requires a schema with fields")
            values = list(data)
            names = [f.name for f in schema.fields]
            # If values shorter/longer, pad/truncate to schema length
            if len(values) < len(names):
                values = values + [None] * (len(names) - len(values))
            if len(values) > len(names):
                values = values[: len(names)]
            self.data = {name: values[idx] for idx, name in enumerate(names)}

    def __getitem__(self, key: Any) -> Any:
        """Get item by column name or index (PySpark-compatible)."""
        if isinstance(key, str):
            if key not in self.data:
                raise KeyError(f"Key '{key}' not found in row")
            return self.data[key]
        # Support integer index access using schema order
        if isinstance(key, int):
            field_names = self._get_field_names_ordered()
            try:
                name = field_names[key]
            except IndexError:
                raise IndexError("Row index out of range")
            return self.data.get(name)
        raise TypeError("Row indices must be integers or strings")

    def __contains__(self, key: str) -> bool:
        """Check if key exists."""
        return key in self.data

    def keys(self) -> KeysView[str]:
        """Get keys."""
        return self.data.keys()

    def values(self) -> ValuesView[Any]:
        """Get values."""
        return self.data.values()

    def items(self) -> ItemsView[str, Any]:
        """Get items."""
        return self.data.items()

    def __len__(self) -> int:
        """Get length."""
        return len(self.data)

    def __eq__(self, other: Any) -> bool:
        """Compare with another row object."""
        if hasattr(other, "data"):
            # Compare with another MockRow
            result: bool = self.data == other.data
            return result
        elif hasattr(other, "__dict__"):
            # Compare with PySpark Row object
            # PySpark Row objects have attributes for each column
            try:
                for key, value in self.data.items():
                    if not hasattr(other, key) or getattr(other, key) != value:
                        return False
                return True
            except:
                return False
        else:
            return False

    def asDict(self) -> Dict[str, Any]:
        """Convert to dictionary (PySpark compatibility)."""
        # Ensure order follows schema if provided
        if self._schema is not None:
            return {name: self.data.get(name) for name in self._get_field_names_ordered()}
        return self.data.copy()

    def __getattr__(self, name: str) -> Any:
        """Get value by attribute name (PySpark compatibility)."""
        if name in self.data:
            return self.data[name]
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")

    def __iter__(self) -> Iterator[Any]:
        """Iterate values in schema order if available, else dict order."""
        for name in self._get_field_names_ordered():
            yield self.data.get(name)

    def __repr__(self) -> str:
        """String representation matching PySpark format."""
        values_str = ", ".join(f"{k}={self.data.get(k)}" for k in self._get_field_names_ordered())
        return f"Row({values_str})"

    def get(self, key: str, default: Any = None) -> Any:
        """Get value by key with default."""
        return self.data.get(key, default)

    def _get_field_names_ordered(self) -> List[str]:
        if self._schema is not None and getattr(self._schema, "fields", None):
            return [f.name for f in self._schema.fields]
        # fallback to dict insertion order
        return list(self.data.keys())
