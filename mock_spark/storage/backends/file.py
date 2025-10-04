"""
File-based storage backend.

This module provides a file-based storage implementation using JSON files.
"""

import json
import os
from typing import List, Dict, Any, Optional, Union
from ..interfaces import IStorageManager, ITable, ISchema
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

    def _ensure_file_exists(self):
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


class FileStorageManager(IStorageManager):
    """File-based storage manager implementation."""

    def __init__(self, base_path: str = "mock_spark_storage"):
        """Initialize file storage manager.

        Args:
            base_path: Base path for storage files.
        """
        self.base_path = base_path
        self.schemas: Dict[str, FileSchema] = {}
        # Create default schema
        self.schemas["default"] = FileSchema("default", base_path)

    def create_schema(self, schema: str) -> None:
        """Create a new schema.

        Args:
            schema: Name of the schema to create.
        """
        if schema not in self.schemas:
            self.schemas[schema] = FileSchema(schema, self.base_path)

    def schema_exists(self, schema: str) -> bool:
        """Check if schema exists.

        Args:
            schema: Name of the schema to check.

        Returns:
            True if schema exists, False otherwise.
        """
        return schema in self.schemas

    def drop_schema(self, schema: str) -> None:
        """Drop a schema.

        Args:
            schema: Name of the schema to drop.
        """
        if schema in self.schemas and schema != "default":
            # Remove schema directory
            schema_path = os.path.join(self.base_path, schema)
            if os.path.exists(schema_path):
                import shutil

                shutil.rmtree(schema_path)
            del self.schemas[schema]

    def list_schemas(self) -> List[str]:
        """List all schemas.

        Returns:
            List of schema names.
        """
        return list(self.schemas.keys())

    def table_exists(self, schema: str, table: str) -> bool:
        """Check if table exists.

        Args:
            schema: Name of the schema.
            table: Name of the table.

        Returns:
            True if table exists, False otherwise.
        """
        if schema not in self.schemas:
            return False
        return self.schemas[schema].table_exists(table)

    def create_table(
        self, schema: str, table: str, columns: Union[List[MockStructField], MockStructType]
    ) -> None:
        """Create a new table.

        Args:
            schema: Name of the schema.
            table: Name of the table.
            columns: Table columns definition.
        """
        if schema not in self.schemas:
            self.create_schema(schema)

        self.schemas[schema].create_table(table, columns)

    def drop_table(self, schema: str, table: str) -> None:
        """Drop a table.

        Args:
            schema: Name of the schema.
            table: Name of the table.
        """
        if schema in self.schemas:
            self.schemas[schema].drop_table(table)

    def insert_data(
        self, schema: str, table: str, data: List[Dict[str, Any]], mode: str = "append"
    ) -> None:
        """Insert data into table.

        Args:
            schema: Name of the schema.
            table: Name of the table.
            data: Data to insert.
            mode: Insert mode ("append", "overwrite", "ignore").
        """
        if schema in self.schemas and table in self.schemas[schema].tables:
            self.schemas[schema].tables[table].insert_data(data, mode)

    def query_table(
        self, schema: str, table: str, filter_expr: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Query data from table.

        Args:
            schema: Name of the schema.
            table: Name of the table.
            filter_expr: Optional filter expression.

        Returns:
            List of data rows.
        """
        if schema in self.schemas and table in self.schemas[schema].tables:
            return self.schemas[schema].tables[table].query_data(filter_expr)
        return []

    def get_table_schema(self, schema: str, table: str) -> Optional[MockStructType]:
        """Get table schema.

        Args:
            schema: Name of the schema.
            table: Name of the table.

        Returns:
            Table schema or None if table doesn't exist.
        """
        if schema in self.schemas and table in self.schemas[schema].tables:
            return self.schemas[schema].tables[table].get_schema()
        return None

    def get_data(self, schema: str, table: str) -> List[Dict[str, Any]]:
        """Get all data from table.

        Args:
            schema: Name of the schema.
            table: Name of the table.

        Returns:
            List of data rows.
        """
        return self.query_table(schema, table)

    def create_temp_view(self, name: str, dataframe) -> None:
        """Create a temporary view from a DataFrame.

        Args:
            name: Name of the temporary view.
            dataframe: DataFrame to create view from.
        """
        # Create a schema and table for the temporary view
        schema = "default"
        self.create_schema(schema)

        # Convert DataFrame data to table format
        data = dataframe.data
        schema_obj = dataframe.schema

        # Create the table
        self.create_table(schema, name, schema_obj)

        # Insert the data
        self.insert_data(schema, name, data, mode="overwrite")

    def list_tables(self, schema: str) -> List[str]:
        """List tables in schema.

        Args:
            schema: Name of the schema.

        Returns:
            List of table names.
        """
        if schema not in self.schemas:
            return []
        return self.schemas[schema].list_tables()
