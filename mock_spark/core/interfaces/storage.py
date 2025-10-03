"""
Storage interface definitions.

This module defines the abstract interfaces for storage operations,
table management, and data persistence.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union, Iterator
from ..types.schema import ISchema, IStructField


class IStorageManager(ABC):
    """Abstract interface for storage management."""

    @abstractmethod
    def create_schema(self, schema_name: str) -> None:
        """Create a new schema."""
        pass

    @abstractmethod
    def drop_schema(self, schema_name: str, cascade: bool = False) -> None:
        """Drop a schema."""
        pass

    @abstractmethod
    def schema_exists(self, schema_name: str) -> bool:
        """Check if schema exists."""
        pass

    @abstractmethod
    def list_schemas(self) -> List[str]:
        """List all schemas."""
        pass

    @abstractmethod
    def create_table(self, schema_name: str, table_name: str, fields: List[IStructField]) -> None:
        """Create a new table."""
        pass

    @abstractmethod
    def drop_table(self, schema_name: str, table_name: str) -> None:
        """Drop a table."""
        pass

    @abstractmethod
    def table_exists(self, schema_name: str, table_name: str) -> bool:
        """Check if table exists."""
        pass

    @abstractmethod
    def list_tables(self, schema_name: Optional[str] = None) -> List[str]:
        """List tables in schema."""
        pass

    @abstractmethod
    def get_table_schema(self, schema_name: str, table_name: str) -> ISchema:
        """Get table schema."""
        pass

    @abstractmethod
    def insert_data(self, schema_name: str, table_name: str, data: List[Dict[str, Any]]) -> None:
        """Insert data into table."""
        pass

    @abstractmethod
    def query_data(self, schema_name: str, table_name: str, **filters: Any) -> List[Dict[str, Any]]:
        """Query data from table."""
        pass

    @abstractmethod
    def get_table_metadata(self, schema_name: str, table_name: str) -> "ITableMetadata":
        """Get table metadata."""
        pass


class ITable(ABC):
    """Abstract interface for table operations."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Get table name."""
        pass

    @property
    @abstractmethod
    def schema(self) -> ISchema:
        """Get table schema."""
        pass

    @property
    @abstractmethod
    def metadata(self) -> "ITableMetadata":
        """Get table metadata."""
        pass

    @abstractmethod
    def insert(self, data: List[Dict[str, Any]]) -> None:
        """Insert data into table."""
        pass

    @abstractmethod
    def query(self, **filters: Any) -> List[Dict[str, Any]]:
        """Query data from table."""
        pass

    @abstractmethod
    def count(self) -> int:
        """Count rows in table."""
        pass

    @abstractmethod
    def truncate(self) -> None:
        """Truncate table."""
        pass

    @abstractmethod
    def drop(self) -> None:
        """Drop table."""
        pass


class ISchema(ABC):
    """Abstract interface for schema operations."""

    @property
    @abstractmethod
    def fields(self) -> List[IStructField]:
        """Get schema fields."""
        pass

    @abstractmethod
    def add_field(self, field: IStructField) -> None:
        """Add field to schema."""
        pass

    @abstractmethod
    def remove_field(self, field_name: str) -> None:
        """Remove field from schema."""
        pass

    @abstractmethod
    def get_field(self, field_name: str) -> Optional[IStructField]:
        """Get field by name."""
        pass

    @abstractmethod
    def field_names(self) -> List[str]:
        """Get field names."""
        pass

    @abstractmethod
    def field_types(self) -> Dict[str, Any]:
        """Get field types."""
        pass


class IStructField(ABC):
    """Abstract interface for struct field operations."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Get field name."""
        pass

    @property
    @abstractmethod
    def data_type(self) -> Any:
        """Get field data type."""
        pass

    @property
    @abstractmethod
    def nullable(self) -> bool:
        """Check if field is nullable."""
        pass

    @property
    @abstractmethod
    def metadata(self) -> Dict[str, Any]:
        """Get field metadata."""
        pass


class ITableMetadata(ABC):
    """Abstract interface for table metadata."""

    @property
    @abstractmethod
    def schema_name(self) -> str:
        """Get schema name."""
        pass

    @property
    @abstractmethod
    def table_name(self) -> str:
        """Get table name."""
        pass

    @property
    @abstractmethod
    def created_at(self) -> float:
        """Get creation timestamp."""
        pass

    @property
    @abstractmethod
    def row_count(self) -> int:
        """Get row count."""
        pass

    @property
    @abstractmethod
    def size_bytes(self) -> int:
        """Get size in bytes."""
        pass

    @abstractmethod
    def update_row_count(self, count: int) -> None:
        """Update row count."""
        pass

    @abstractmethod
    def update_size(self, size_bytes: int) -> None:
        """Update size."""
        pass


class IDataSerializer(ABC):
    """Abstract interface for data serialization."""

    @abstractmethod
    def serialize(self, data: Any) -> bytes:
        """Serialize data to bytes."""
        pass

    @abstractmethod
    def deserialize(self, data: bytes) -> Any:
        """Deserialize data from bytes."""
        pass

    @abstractmethod
    def get_format(self) -> str:
        """Get serialization format."""
        pass


class IDataDeserializer(ABC):
    """Abstract interface for data deserialization."""

    @abstractmethod
    def deserialize(self, data: bytes) -> Any:
        """Deserialize data from bytes."""
        pass

    @abstractmethod
    def get_format(self) -> str:
        """Get deserialization format."""
        pass
