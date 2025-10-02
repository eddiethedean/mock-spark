"""
Mock data types and schema system for Mock Spark.

This module provides mock implementations of PySpark data types and schema
structures that behave identically to the real PySpark types.
"""

from typing import Any, Dict, List, Optional, Union, Iterator, KeysView, ValuesView, ItemsView
from dataclasses import dataclass


class MockDataType:
    """Base class for mock data types."""
    
    def __init__(self, nullable: bool = True):
        self.nullable = nullable
    
    def __eq__(self, other: Any) -> bool:
        return isinstance(other, self.__class__) and self.nullable == other.nullable
    
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(nullable={self.nullable})"


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
            isinstance(other, MockStructField) and
            self.name == other.name and
            self.dataType == other.dataType and
            self.nullable == other.nullable
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
        int: IntegerType(),
        float: DoubleType(),
        bool: BooleanType(),
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
    """Mock row for DataFrame operations."""
    
    def __init__(self, data: Dict[str, Any]):
        """Initialize MockRow."""
        self.data = data
    
    def __getitem__(self, key: str) -> Any:
        """Get item by key."""
        if key not in self.data:
            raise KeyError(f"Key '{key}' not found in row")
        return self.data[key]
    
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
    
    def __repr__(self) -> str:
        """String representation."""
        return f"MockRow({self.data})"
