"""
File-based storage backend.

This module provides a file-based storage implementation using JSON files.

Refactored to use BaseStorageManager to eliminate code duplication
and follow the SOLID Open/Closed principle.
"""

import json
import os
import shutil
from typing import List, Dict, Any, Optional, Union
from ..interfaces import ITable, ISchema
from .base import BaseStorageManager
from mock_spark.spark_types import MockStructType, MockStructField


class FileTable(ITable):
    """File-based table implementation."""

    def __init__(self, name: str, schema: MockStructType, file_path: str):
        """Initialize file table.

        Args:
            name: Table name.
            schema: Table schema.
            file_path: Path to table data file.
        """
        self.name = name
        self.schema = schema
        self.file_path = file_path
        self.metadata = {
            "created_at": "2024-01-01T00:00:00Z",
            "row_count": 0,
            "schema_version": "1.0",
        }
        self._ensure_file_exists()

    def _ensure_file_exists(self) -> None:
        """Ensure the table data file exists."""
        os.makedirs(os.path.dirname(self.file_path), exist_ok=True)
        if not os.path.exists(self.file_path):
            with open(self.file_path, "w") as f:
                json.dump([], f)

    def _load_data(self) -> List[Dict[str, Any]]:
        """Load data from file.

        Returns:
            List of data rows.
        """
        try:
            with open(self.file_path, "r") as f:
                data = json.load(f)
                return data if isinstance(data, list) else []
        except (FileNotFoundError, json.JSONDecodeError):
            return []

    def _save_data(self, data: List[Dict[str, Any]]) -> None:
        """Save data to file.

        Args:
            data: Data to save.
        """
        with open(self.file_path, "w") as f:
            json.dump(data, f, indent=2)

    def insert_data(self, data: List[Dict[str, Any]], mode: str = "append") -> None:
        """Insert data into table.

        Args:
            data: Data to insert.
            mode: Insert mode ("append", "overwrite", "ignore").
        """
        if not data:
            return

        current_data = self._load_data()

        if mode == "overwrite":
            current_data = data
        elif mode == "append":
            current_data.extend(data)
        elif mode == "ignore":
            # Only insert if table is empty
            if not current_data:
                current_data = data

        self._save_data(current_data)
        self.metadata["row_count"] = len(current_data)

    def query_data(self, filter_expr: Optional[str] = None) -> List[Dict[str, Any]]:
        """Query data from table.

        Args:
            filter_expr: Optional filter expression.

        Returns:
            List of data rows.
        """
        data = self._load_data()

        if filter_expr is None:
            return data

        # Simple filter implementation
        # In a real implementation, this would parse and evaluate the filter expression
        return data

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
        data = self._load_data()
        metadata = self.metadata.copy()
        metadata["row_count"] = len(data)
        return metadata


class FileSchema(ISchema):
    """File-based schema implementation."""

    def __init__(self, name: str, base_path: str):
        """Initialize file schema.

        Args:
            name: Schema name.
            base_path: Base path for schema files.
        """
        self.name = name
        self.base_path = os.path.join(base_path, name)
        self.tables: Dict[str, FileTable] = {}
        os.makedirs(self.base_path, exist_ok=True)

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

        table_path = os.path.join(self.base_path, f"{table}.json")
        self.tables[table] = FileTable(table, schema, table_path)

    def table_exists(self, table: str) -> bool:
        """Check if table exists in this schema.

        Args:
            table: Name of the table.

        Returns:
            True if table exists, False otherwise.
        """
        table_path = os.path.join(self.base_path, f"{table}.json")
        return os.path.exists(table_path)

    def drop_table(self, table: str) -> None:
        """Drop a table from this schema.

        Args:
            table: Name of the table.
        """
        table_path = os.path.join(self.base_path, f"{table}.json")
        if os.path.exists(table_path):
            os.remove(table_path)

        if table in self.tables:
            del self.tables[table]

    def list_tables(self) -> List[str]:
        """List all tables in this schema.

        Returns:
            List of table names.
        """
        if not os.path.exists(self.base_path):
            return []

        tables = []
        for filename in os.listdir(self.base_path):
            if filename.endswith(".json"):
                tables.append(filename[:-5])  # Remove .json extension

        return tables


class FileStorageManager(BaseStorageManager):
    """File-based storage manager implementation.

    Inherits shared logic from BaseStorageManager, eliminating code duplication
    following the SOLID Template Method pattern.
    """

    def __init__(self, base_path: str = "mock_spark_storage"):
        """Initialize file storage manager.

        Args:
            base_path: Base path for storage files.
        """
        super().__init__()
        self.base_path = base_path
        self._initialize_default_schema()

    def _create_schema_instance(self, name: str) -> ISchema:
        """Create a FileSchema instance.

        Args:
            name: Schema name

        Returns:
            FileSchema instance
        """
        return FileSchema(name, self.base_path)

    def drop_schema(self, schema: str) -> None:
        """Drop a schema (overrides base to remove filesystem directory).

        Args:
            schema: Name of the schema to drop.
        """
        if schema in self.schemas and schema != "default":
            # Remove schema directory
            schema_path = os.path.join(self.base_path, schema)
            if os.path.exists(schema_path):
                shutil.rmtree(schema_path)
            del self.schemas[schema]

    def close(self) -> None:
        """Close storage backend and clean up resources.

        For file-based storage, this is a no-op as files are managed per operation.
        """
        pass
