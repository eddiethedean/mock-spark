"""
Protocol definitions for backend interfaces.

This module defines the protocols (interfaces) that backend implementations
must satisfy. Using protocols enables dependency injection and makes modules
testable independently.
"""

from typing import Protocol, List, Dict, Any, Optional, Union, Tuple
from mock_spark.spark_types import MockStructType, MockStructField, MockRow


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


class StorageBackend(Protocol):
    """Protocol for storage operations.

    This protocol defines the interface for storage backends that handle
    data persistence, table management, and schema operations.
    """

    def create_schema(self, schema: str) -> None:
        """Create a new schema/database.

        Args:
            schema: Schema name
        """
        ...

    def schema_exists(self, schema: str) -> bool:
        """Check if schema exists.

        Args:
            schema: Schema name

        Returns:
            True if schema exists
        """
        ...

    def drop_schema(self, schema: str) -> None:
        """Drop a schema/database.

        Args:
            schema: Schema name
        """
        ...

    def list_schemas(self) -> List[str]:
        """List all schemas/databases.

        Returns:
            List of schema names
        """
        ...

    def table_exists(self, schema: str, table: str) -> bool:
        """Check if table exists.

        Args:
            schema: Schema name
            table: Table name

        Returns:
            True if table exists
        """
        ...

    def create_table(
        self,
        schema: str,
        table: str,
        columns: Union[List[MockStructField], MockStructType],
    ) -> Optional[Any]:
        """Create a new table.

        Args:
            schema: Schema name
            table: Table name
            columns: Column definitions

        Returns:
            Table object (implementation-specific)
        """
        ...

    def drop_table(self, schema: str, table: str) -> None:
        """Drop a table.

        Args:
            schema: Schema name
            table: Table name
        """
        ...

    def insert_data(
        self,
        schema: str,
        table: str,
        data: List[Dict[str, Any]],
        mode: str = "append",
    ) -> None:
        """Insert data into a table.

        Args:
            schema: Schema name
            table: Table name
            data: Data to insert
            mode: Insert mode ("append", "overwrite", "ignore")
        """
        ...

    def query_table(
        self, schema: str, table: str, filter_expr: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Query data from a table.

        Args:
            schema: Schema name
            table: Table name
            filter_expr: Optional filter expression

        Returns:
            List of result rows
        """
        ...

    def get_table_schema(self, schema: str, table: str) -> Optional[MockStructType]:
        """Get table schema.

        Args:
            schema: Schema name
            table: Table name

        Returns:
            Table schema or None
        """
        ...

    def get_data(self, schema: str, table: str) -> List[Dict[str, Any]]:
        """Get all data from a table.

        Args:
            schema: Schema name
            table: Table name

        Returns:
            List of all rows
        """
        ...

    def create_temp_view(self, name: str, dataframe: Any) -> None:
        """Create a temporary view from a DataFrame.

        Args:
            name: View name
            dataframe: Source DataFrame
        """
        ...

    def list_tables(self, schema: str) -> List[str]:
        """List tables in a schema.

        Args:
            schema: Schema name

        Returns:
            List of table names
        """
        ...

    def get_table_metadata(self, schema: str, table: str) -> Optional[Dict[str, Any]]:
        """Get table metadata.

        Args:
            schema: Schema name
            table: Table name

        Returns:
            Metadata dictionary or None
        """
        ...

    def update_table_metadata(
        self, schema: str, table: str, metadata_updates: Dict[str, Any]
    ) -> None:
        """Update table metadata.

        Args:
            schema: Schema name
            table: Table name
            metadata_updates: Metadata fields to update
        """
        ...

    def close(self) -> None:
        """Close storage backend and clean up resources."""
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
