"""
Protocol definitions for backend interfaces.

This module defines the protocols (interfaces) that backend implementations
must satisfy. Using protocols enables dependency injection and makes modules
testable independently.
"""

from typing import Protocol, List, Dict, Any, Tuple
from mock_spark.spark_types import StructType, Row
from mock_spark.core.interfaces.storage import IStorageManager


class QueryExecutor(Protocol):
    """Protocol for executing queries on data.

    This protocol defines the interface for query execution backends.
    Implementations can use different engines.
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
        self, name: str, schema: StructType, data: List[Dict[str, Any]]
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
        schema: StructType,
        operations: List[Tuple[str, Any]],
    ) -> List[Row]:
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
    different formats and systems. Backend implementations should provide
    methods for exporting to their specific target systems.
    """

    # Protocol intentionally minimal - specific export methods
    # are implemented directly in backend implementations
    ...
