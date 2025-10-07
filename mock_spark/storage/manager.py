"""
Storage manager module.

This module provides a unified storage manager that can use different backends.
"""

from typing import List, Dict, Any, Optional, Union, Type
from .interfaces import IStorageManager
from .backends.memory import MemoryStorageManager
from .backends.file import FileStorageManager
from .backends.duckdb import DuckDBStorageManager
from mock_spark.spark_types import MockStructType, MockStructField


class StorageManagerFactory:
    """Factory for creating storage managers."""

    @staticmethod
    def create_memory_manager() -> IStorageManager:
        """Create a memory storage manager.

        Returns:
            Memory storage manager instance.
        """
        return MemoryStorageManager()

    @staticmethod
    def create_file_manager(base_path: str = "mock_spark_storage") -> IStorageManager:
        """Create a file storage manager.

        Args:
            base_path: Base path for storage files.

        Returns:
            File storage manager instance.
        """
        return FileStorageManager(base_path)

    @staticmethod
    def create_duckdb_manager(db_path: Optional[str] = None) -> IStorageManager:
        """Create a DuckDB storage manager with in-memory storage by default.

        Args:
            db_path: Optional path to DuckDB database file. If None, uses in-memory storage.

        Returns:
            DuckDB storage manager instance.
        """
        return DuckDBStorageManager(db_path)


class UnifiedStorageManager(IStorageManager):
    """Unified storage manager that can switch between backends."""

    def __init__(self, backend: IStorageManager):
        """Initialize unified storage manager.

        Args:
            backend: Storage backend to use.
        """
        self.backend = backend

    def create_schema(self, schema: str) -> None:
        """Create a new schema.

        Args:
            schema: Name of the schema to create.
        """
        self.backend.create_schema(schema)

    def schema_exists(self, schema: str) -> bool:
        """Check if schema exists.

        Args:
            schema: Name of the schema to check.

        Returns:
            True if schema exists, False otherwise.
        """
        return self.backend.schema_exists(schema)

    def drop_schema(self, schema: str) -> None:
        """Drop a schema.

        Args:
            schema: Name of the schema to drop.
        """
        self.backend.drop_schema(schema)

    def list_schemas(self) -> List[str]:
        """List all schemas.

        Returns:
            List of schema names.
        """
        return self.backend.list_schemas()

    def table_exists(self, schema: str, table: str) -> bool:
        """Check if table exists.

        Args:
            schema: Name of the schema.
            table: Name of the table.

        Returns:
            True if table exists, False otherwise.
        """
        return self.backend.table_exists(schema, table)

    def create_table(
        self,
        schema: str,
        table: str,
        columns: Union[List[MockStructField], MockStructType],
    ) -> None:
        """Create a new table.

        Args:
            schema: Name of the schema.
            table: Name of the table.
            columns: Table columns definition.
        """
        self.backend.create_table(schema, table, columns)

    def drop_table(self, schema: str, table: str) -> None:
        """Drop a table.

        Args:
            schema: Name of the schema.
            table: Name of the table.
        """
        self.backend.drop_table(schema, table)

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
        self.backend.insert_data(schema, table, data, mode)

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
        return self.backend.query_table(schema, table, filter_expr)

    def get_table_schema(self, schema: str, table: str) -> Optional[MockStructType]:
        """Get table schema.

        Args:
            schema: Name of the schema.
            table: Name of the table.

        Returns:
            Table schema or None if table doesn't exist.
        """
        return self.backend.get_table_schema(schema, table)

    def get_data(self, schema: str, table: str) -> List[Dict[str, Any]]:
        """Get all data from table.

        Args:
            schema: Name of the schema.
            table: Name of the table.

        Returns:
            List of data rows.
        """
        return self.backend.get_data(schema, table)

    def create_temp_view(self, name: str, dataframe) -> None:
        """Create a temporary view from a DataFrame.

        Args:
            name: Name of the temporary view.
            dataframe: DataFrame to create view from.
        """
        self.backend.create_temp_view(name, dataframe)

    def list_tables(self, schema: str) -> List[str]:
        """List tables in schema.

        Args:
            schema: Name of the schema.

        Returns:
            List of table names.
        """
        return self.backend.list_tables(schema)

    def switch_backend(self, backend: IStorageManager) -> None:
        """Switch to a different storage backend.

        Args:
            backend: New storage backend to use.
        """
        self.backend = backend
