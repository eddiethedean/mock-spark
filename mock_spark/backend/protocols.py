"""
Protocol definitions for backend interfaces.

This module defines the protocols (interfaces) that backend implementations
must satisfy. Using protocols enables dependency injection and makes modules
testable independently.
"""

from typing import Protocol, List, Dict, Any, Optional, Tuple
from mock_spark.spark_types import MockStructType, MockRow
from mock_spark.core.interfaces.storage import IStorageManager


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

    def create_table(
        self, name: str, schema: MockStructType, data: List[Dict[str, Any]]
    ) -> None:
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


# StorageBackend protocol is now an alias for IStorageManager
# Import the canonical interface to avoid duplication
StorageBackend = IStorageManager


class ExportBackend(Protocol):
    """Protocol for DataFrame export operations.

    This protocol defines the interface for exporting DataFrames to
    different formats and systems (DuckDB, pandas, etc.).
    """

    def to_duckdb(
        self, df: Any, connection: Any = None, table_name: Optional[str] = None
    ) -> str:
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
