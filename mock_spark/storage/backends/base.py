"""
Base Storage Backend Implementation

This module provides a base class for storage managers, implementing the
Template Method pattern to eliminate code duplication between FileStorageManager
and MemoryStorageManager.

Follows the SOLID principles:
- Single Responsibility: Storage management only
- Open/Closed: Open for extension via subclassing, closed for modification
- Liskov Substitution: Subclasses can replace base without breaking behavior
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from ..interfaces import ISchema

from ..interfaces import IStorageManager
from mock_spark.spark_types import MockStructType, MockStructField


class BaseStorageManager(IStorageManager, ABC):
    """Base storage manager with shared logic for all storage backends.

    This class implements the Template Method pattern, providing concrete
    implementations for common storage operations while allowing subclasses
    to customize schema creation via the factory method pattern.

    Subclasses must implement:
    - _create_schema_instance(name, *args) -> ISchema
    """

    def __init__(self) -> None:
        """Initialize base storage manager.

        Subclasses should call super().__init__() and then set up any
        backend-specific configuration before calling _initialize_default_schema().
        """
        self.schemas: Dict[str, Any] = {}

    @abstractmethod
    def _create_schema_instance(self, name: str) -> "ISchema":
        """Factory method for creating schema instances.

        This method must be implemented by subclasses to create the
        appropriate schema type (MemorySchema, FileSchema, etc.).

        Args:
            name: Schema name

        Returns:
            Schema instance of the appropriate type
        """
        pass

    def _initialize_default_schema(self) -> None:
        """Initialize the default schema.

        Should be called by subclasses after their __init__ setup is complete.
        """
        self.schemas["default"] = self._create_schema_instance("default")

    # Shared implementations (previously duplicated in MemoryStorageManager and FileStorageManager)

    def create_schema(self, schema: str) -> None:
        """Create a new schema.

        Args:
            schema: Name of the schema to create.
        """
        if schema not in self.schemas:
            self.schemas[schema] = self._create_schema_instance(schema)

    def schema_exists(self, schema: str) -> bool:
        """Check if schema exists.

        Args:
            schema: Name of the schema to check.

        Returns:
            True if schema exists, False otherwise.
        """
        return schema in self.schemas

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

    def create_temp_view(self, name: str, dataframe: Any) -> None:
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

    def get_table_metadata(self, schema: str, table: str) -> Optional[Dict[str, Any]]:
        """Get table metadata including Delta-specific fields.

        Args:
            schema: Name of the schema.
            table: Name of the table.

        Returns:
            Table metadata dictionary or None if table doesn't exist.
        """
        if schema not in self.schemas:
            return None
        if table not in self.schemas[schema].tables:
            return None
        return self.schemas[schema].tables[table].get_metadata()

    def update_table_metadata(
        self, schema: str, table: str, metadata_updates: Dict[str, Any]
    ) -> None:
        """Update table metadata fields.

        Args:
            schema: Name of the schema.
            table: Name of the table.
            metadata_updates: Dictionary of metadata fields to update.
        """
        if schema in self.schemas and table in self.schemas[schema].tables:
            table_obj = self.schemas[schema].tables[table]
            table_obj.metadata.update(metadata_updates)
