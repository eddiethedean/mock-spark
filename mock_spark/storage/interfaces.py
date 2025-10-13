"""
Storage interfaces module.

This module contains the core interfaces for storage operations, execution engines,
and export backends. Consolidates protocols from the old backend/protocols.py.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Union, Tuple, Protocol
from mock_spark.spark_types import MockStructType, MockStructField, MockRow


class IStorageManager(ABC):
    """Interface for storage management operations."""

    @abstractmethod
    def create_schema(self, schema: str) -> None:
        """Create a new schema.

        Args:
            schema: Name of the schema to create.
        """
        pass

    @abstractmethod
    def schema_exists(self, schema: str) -> bool:
        """Check if schema exists.

        Args:
            schema: Name of the schema to check.

        Returns:
            True if schema exists, False otherwise.
        """
        pass

    @abstractmethod
    def drop_schema(self, schema: str) -> None:
        """Drop a schema.

        Args:
            schema: Name of the schema to drop.
        """
        pass

    @abstractmethod
    def list_schemas(self) -> List[str]:
        """List all schemas.

        Returns:
            List of schema names.
        """
        pass

    @abstractmethod
    def table_exists(self, schema: str, table: str) -> bool:
        """Check if table exists.

        Args:
            schema: Name of the schema.
            table: Name of the table.

        Returns:
            True if table exists, False otherwise.
        """
        pass

    @abstractmethod
    def create_table(
        self,
        schema: str,
        table: str,
        columns: Union[List[MockStructField], MockStructType],
    ) -> Optional[Any]:
        """Create a new table.

        Args:
            schema: Name of the schema.
            table: Name of the table.
            columns: Table columns definition.
        """
        pass

    @abstractmethod
    def drop_table(self, schema: str, table: str) -> None:
        """Drop a table.

        Args:
            schema: Name of the schema.
            table: Name of the table.
        """
        pass

    @abstractmethod
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
        pass

    @abstractmethod
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
        pass

    @abstractmethod
    def get_table_schema(self, schema: str, table: str) -> Optional[MockStructType]:
        """Get table schema.

        Args:
            schema: Name of the schema.
            table: Name of the table.

        Returns:
            Table schema or None if table doesn't exist.
        """
        pass

    @abstractmethod
    def get_data(self, schema: str, table: str) -> List[Dict[str, Any]]:
        """Get all data from table.

        Args:
            schema: Name of the schema.
            table: Name of the table.

        Returns:
            List of data rows.
        """
        pass

    @abstractmethod
    def create_temp_view(self, name: str, dataframe: Any) -> None:
        """Create a temporary view from a DataFrame.

        Args:
            name: Name of the temporary view.
            dataframe: DataFrame to create view from.
        """
        pass

    @abstractmethod
    def list_tables(self, schema: str) -> List[str]:
        """List tables in schema.

        Args:
            schema: Name of the schema.

        Returns:
            List of table names.
        """
        pass

    @abstractmethod
    def get_table_metadata(self, schema: str, table: str) -> Optional[Dict[str, Any]]:
        """Get table metadata including Delta-specific fields.

        Args:
            schema: Name of the schema.
            table: Name of the table.

        Returns:
            Table metadata dictionary or None if table doesn't exist.
        """
        pass

    @abstractmethod
    def update_table_metadata(
        self, schema: str, table: str, metadata_updates: Dict[str, Any]
    ) -> None:
        """Update table metadata fields.

        Args:
            schema: Name of the schema.
            table: Name of the table.
            metadata_updates: Dictionary of metadata fields to update.
        """
        pass


class ITable(ABC):
    """Interface for table operations."""

    @abstractmethod
    def insert_data(self, data: List[Dict[str, Any]], mode: str = "append") -> None:
        """Insert data into table.

        Args:
            data: Data to insert.
            mode: Insert mode ("append", "overwrite", "ignore").
        """
        pass

    @abstractmethod
    def query_data(self, filter_expr: Optional[str] = None) -> List[Dict[str, Any]]:
        """Query data from table.

        Args:
            filter_expr: Optional filter expression.

        Returns:
            List of data rows.
        """
        pass

    @abstractmethod
    def get_schema(self) -> MockStructType:
        """Get table schema.

        Returns:
            Table schema.
        """
        pass

    @abstractmethod
    def get_metadata(self) -> Dict[str, Any]:
        """Get table metadata.

        Returns:
            Table metadata.
        """
        pass


class ISchema(ABC):
    """Interface for schema operations."""

    @abstractmethod
    def create_table(
        self, table: str, columns: Union[List[MockStructField], MockStructType]
    ) -> Optional[Any]:
        """Create a new table in this schema.

        Args:
            table: Name of the table.
            columns: Table columns definition.
        """
        pass

    @abstractmethod
    def table_exists(self, table: str) -> bool:
        """Check if table exists in this schema.

        Args:
            table: Name of the table.

        Returns:
            True if table exists, False otherwise.
        """
        pass

    @abstractmethod
    def drop_table(self, table: str) -> None:
        """Drop a table from this schema.

        Args:
            table: Name of the table.
        """
        pass

    @abstractmethod
    def list_tables(self) -> List[str]:
        """List all tables in this schema.

        Returns:
            List of table names.
        """
        pass


# Additional protocols from backend/ consolidation


class DataMaterializer(Protocol):
    """Protocol for materializing lazy DataFrame operations.

    This protocol defines the interface for materializing queued operations
    on DataFrames. Implementations can use different execution engines.
    """

    def materialize(
        self,
        data: List[Dict[str, Any]],
        schema: MockStructType,
        operations: List[Tuple[str, Any]],
    ) -> List[MockRow]:
        """Materialize lazy operations into actual data.

        Args:
            data: Initial data
            schema: DataFrame schema
            operations: List of queued operations (operation_name, payload)

        Returns:
            List of result rows
        """
        ...

    def close(self) -> None:
        """Close the materializer and clean up resources."""
        ...


class QueryExecutor(Protocol):
    """Protocol for executing queries on data.

    This protocol defines the interface for query execution backends.
    Implementations can use different engines (DuckDB, SQLite, etc.).
    """

    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute a SQL query and return results.

        Args:
            query: SQL query string

        Returns:
            List of result rows as dictionaries
        """
        ...

    def create_table(self, name: str, schema: MockStructType, data: List[Dict[str, Any]]) -> None:
        """Create a table with the given schema and data.

        Args:
            name: Table name
            schema: Table schema
            data: Initial data for the table
        """
        ...

    def close(self) -> None:
        """Close the query executor and clean up resources."""
        ...


class ExportBackend(Protocol):
    """Protocol for DataFrame export operations.

    This protocol defines the interface for exporting DataFrames to
    different formats and systems (DuckDB, pandas, etc.).
    """

    def to_duckdb(self, df: Any, connection: Any = None, table_name: Optional[str] = None) -> str:
        """Export DataFrame to DuckDB.

        Args:
            df: Source DataFrame
            connection: DuckDB connection (creates new if None)
            table_name: Table name (auto-generated if None)

        Returns:
            Table name in DuckDB
        """
        ...

    def create_duckdb_table(self, df: Any, connection: Any, table_name: str) -> Any:
        """Create a DuckDB table from DataFrame schema.

        Args:
            df: Source DataFrame
            connection: DuckDB connection
            table_name: Table name

        Returns:
            Table object
        """
        ...


# Alias for backward compatibility
StorageBackend = IStorageManager
