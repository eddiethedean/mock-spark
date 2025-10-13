"""
Memory storage backend.

This module provides an in-memory storage implementation.

Refactored to use BaseStorageManager to eliminate code duplication
and follow the SOLID Open/Closed principle.
"""

from typing import List, Dict, Any, Optional, Union
from ..interfaces import ITable, ISchema
from .base import BaseStorageManager
from mock_spark.spark_types import MockStructType, MockStructField


class MemoryTable(ITable):
    """In-memory table implementation."""

    def __init__(self, name: str, schema: MockStructType):
        """Initialize memory table.

        Args:
            name: Table name.
            schema: Table schema.
        """
        self.name = name
        self.schema = schema
        self.data: List[Dict[str, Any]] = []
        self.metadata = {
            "created_at": "2024-01-01T00:00:00Z",
            "row_count": 0,
            "schema_version": "1.0",
        }

    def insert_data(self, data: List[Dict[str, Any]], mode: str = "append") -> None:
        """Insert data into table.

        Args:
            data: Data to insert.
            mode: Insert mode ("append", "overwrite", "ignore").
        """
        if mode == "overwrite":
            self.data = data.copy()
        elif mode == "append":
            self.data.extend(data)
        elif mode == "ignore":
            # Only insert if table is empty
            if not self.data:
                self.data.extend(data)

        self.metadata["row_count"] = len(self.data)

    def query_data(self, filter_expr: Optional[str] = None) -> List[Dict[str, Any]]:
        """Query data from table.

        Args:
            filter_expr: Optional filter expression.

        Returns:
            List of data rows.
        """
        if filter_expr is None:
            return self.data.copy()

        # Simple filter implementation
        # In a real implementation, this would parse and evaluate the filter expression
        return self.data.copy()

    def get_schema(self) -> MockStructType:
        """Get table schema.

        Returns:
            Table schema.
        """
        return self.schema

    def get_metadata(self) -> Dict[str, Any]:
        """Get table metadata.

        Returns:
            Table metadata.
        """
        return self.metadata.copy()


class MemorySchema(ISchema):
    """In-memory schema implementation."""

    def __init__(self, name: str):
        """Initialize memory schema.

        Args:
            name: Schema name.
        """
        self.name = name
        self.tables: Dict[str, MemoryTable] = {}

    def create_table(
        self, table: str, columns: Union[List[MockStructField], MockStructType]
    ) -> None:
        """Create a new table in this schema.

        Args:
            table: Name of the table.
            columns: Table columns definition.
        """
        if isinstance(columns, list):
            schema = MockStructType(columns)
        else:
            schema = columns

        self.tables[table] = MemoryTable(table, schema)

    def table_exists(self, table: str) -> bool:
        """Check if table exists in this schema.

        Args:
            table: Name of the table.

        Returns:
            True if table exists, False otherwise.
        """
        return table in self.tables

    def drop_table(self, table: str) -> None:
        """Drop a table from this schema.

        Args:
            table: Name of the table.
        """
        if table in self.tables:
            del self.tables[table]

    def list_tables(self) -> List[str]:
        """List all tables in this schema.

        Returns:
            List of table names.
        """
        return list(self.tables.keys())


class MemoryStorageManager(BaseStorageManager):
    """In-memory storage manager implementation.

    Inherits shared logic from BaseStorageManager, eliminating code duplication
    following the SOLID Template Method pattern.
    """

    def __init__(self) -> None:
        """Initialize memory storage manager."""
        super().__init__()
        self._initialize_default_schema()

    def _create_schema_instance(self, name: str) -> ISchema:
        """Create a MemorySchema instance.

        Args:
            name: Schema name

        Returns:
            MemorySchema instance
        """
        return MemorySchema(name)

    def drop_schema(self, schema: str) -> None:
        """Drop a schema (overrides base to prevent dropping default).

        Args:
            schema: Name of the schema to drop.
        """
        if schema in self.schemas and schema != "default":
            del self.schemas[schema]

    def close(self) -> None:
        """Close storage backend and clean up resources.

        For in-memory storage, this is a no-op as there are no external resources.
        """
        pass
