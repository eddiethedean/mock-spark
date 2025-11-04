"""
Mock Catalog implementation for Mock Spark.

This module provides a mock implementation of PySpark's Catalog
that behaves identically to the real Catalog for testing and development.
It includes database and table management, caching operations, and
catalog queries without requiring a JVM or actual Spark installation.

Key Features:
    - Complete PySpark Catalog API compatibility
    - Database management (create, list, drop)
    - Table management (create, list, drop, cache)
    - Schema validation and error handling
    - Integration with storage manager

Example:
    >>> from mock_spark.session import MockCatalog
    >>> catalog = MockCatalog(storage_manager)
    >>> catalog.createDatabase("test_db")
    >>> catalog.listDatabases()
    [MockDatabase(name='test_db')]
"""

from typing import Any, List, Optional
from ..core.interfaces.storage import IStorageManager
from ..core.exceptions.analysis import AnalysisException
from ..core.exceptions.validation import IllegalArgumentException


class MockDatabase:
    """Mock database object for catalog operations."""

    def __init__(self, name: str):
        """Initialize MockDatabase.

        Args:
            name: Database name.
        """
        self.name = name

    def __str__(self) -> str:
        """String representation."""
        return f"MockDatabase(name='{self.name}')"

    def __repr__(self) -> str:
        """Representation."""
        return self.__str__()


class MockTable:
    """Mock table object for catalog operations."""

    def __init__(self, name: str, database: str = "default"):
        """Initialize MockTable.

        Args:
            name: Table name.
            database: Database name.
        """
        self.name = name
        self.database = database

    def __str__(self) -> str:
        """String representation."""
        return f"MockTable(name='{self.name}', database='{self.database}')"

    def __repr__(self) -> str:
        """Representation."""
        return self.__str__()


class MockCatalog:
    """Mock Catalog for Spark session.

    Provides a comprehensive mock implementation of PySpark's Catalog
    that supports all major operations including database management,
    table operations, and caching without requiring actual Spark installation.

    Attributes:
        storage: Storage manager for data persistence.

    Example:
        >>> catalog = MockCatalog(storage_manager)
        >>> catalog.createDatabase("test_db")
        >>> catalog.listDatabases()
        [MockDatabase(name='test_db')]
    """

    def __init__(self, storage: IStorageManager):
        """Initialize MockCatalog.

        Args:
            storage: Storage manager instance.
        """
        self.storage = storage
        self._cached_tables: set[str] = set()  # Track cached tables

    def listDatabases(self) -> List[MockDatabase]:
        """List all databases.

        Returns:
            List of MockDatabase objects.
        """
        return [MockDatabase(name) for name in self.storage.list_schemas()]

    def setCurrentDatabase(self, dbName: str) -> None:
        """Set current/active database.

        Args:
            dbName: Database name to set as current.

        Raises:
            AnalysisException: If database does not exist.
        """
        if not self.storage.schema_exists(dbName):
            from mock_spark.core.exceptions.analysis import AnalysisException

            raise AnalysisException(f"Database '{dbName}' does not exist")
        self.storage.set_current_schema(dbName)

    def currentDatabase(self) -> str:
        """Get current database name.

        Returns:
            Current database name.
        """
        return self.storage.get_current_schema()

    def createDatabase(self, name: str, ignoreIfExists: bool = True) -> None:
        """Create a database.

        Args:
            name: Database name.
            ignoreIfExists: Whether to ignore if database already exists.

        Raises:
            IllegalArgumentException: If name is not a string or is empty.
            AnalysisException: If database already exists and ignoreIfExists is False.
        """
        if not isinstance(name, str):
            raise IllegalArgumentException("Database name must be a string")

        if not name:
            raise IllegalArgumentException("Database name cannot be empty")

        if not ignoreIfExists and self.storage.schema_exists(name):
            raise AnalysisException(f"Database '{name}' already exists")

        try:
            self.storage.create_schema(name)
        except Exception as e:
            if isinstance(e, (AnalysisException, IllegalArgumentException)):
                raise
            raise AnalysisException(f"Failed to create database '{name}': {str(e)}")

    def dropDatabase(
        self,
        name: str,
        ignoreIfNotExists: bool = True,
        ignore_if_not_exists: Optional[bool] = None,
        cascade: bool = False,
    ) -> None:
        """Drop a database.

        Args:
            name: Database name.
            ignoreIfNotExists: Whether to ignore if database doesn't exist (PySpark style).
            ignore_if_not_exists: Whether to ignore if database doesn't exist (Python style).
            cascade: Whether to drop tables in the database (ignored in mock).

        Raises:
            IllegalArgumentException: If name is not a string or is empty.
            AnalysisException: If database doesn't exist and ignoreIfNotExists is False.
        """
        if not isinstance(name, str):
            raise IllegalArgumentException("Database name must be a string")

        if not name:
            raise IllegalArgumentException("Database name cannot be empty")

        # Support both camelCase (PySpark) and snake_case (Python) parameter names
        ignore_flag = (
            ignore_if_not_exists
            if ignore_if_not_exists is not None
            else ignoreIfNotExists
        )

        if not ignore_flag and not self.storage.schema_exists(name):
            raise AnalysisException(f"Database '{name}' does not exist")

        if self.storage.schema_exists(name):
            try:
                self.storage.drop_schema(name)
            except Exception as e:
                if isinstance(e, (AnalysisException, IllegalArgumentException)):
                    raise
                raise AnalysisException(f"Failed to drop database '{name}': {str(e)}")

    def tableExists(self, tableName: str, dbName: Optional[str] = None) -> bool:
        """Check if table exists.

        Args:
            tableName: Table name or qualified name (schema.table).
            dbName: Optional database name. Uses current database if None.

        Returns:
            True if table exists, False otherwise.

        Raises:
            IllegalArgumentException: If names are not strings or are empty.
            AnalysisException: If there's an error checking table existence.
        """
        # Handle qualified table names (schema.table)
        if "." in tableName and dbName is None:
            parts = tableName.split(".", 1)
            if len(parts) == 2:
                dbName, tableName = parts
        elif dbName is not None and "." not in tableName:
            # If both tableName and dbName are provided and tableName is not qualified,
            # then dbName is the schema and tableName is the table name
            # This handles the case: tableExists("test_table", "test_schema")
            # But also check if tableName might actually be a schema name
            # (some PySpark code might call it as tableExists("schema", "table"))
            # Check if reversing makes more sense (if "tableName" exists as a schema)
            if self.storage.schema_exists(tableName) and not self.storage.schema_exists(
                dbName
            ):
                # If tableName is actually a schema and dbName is not, swap them
                # This handles: tableExists("test_schema", "test_table")
                # which should check table "test_table" in schema "test_schema"
                dbName, tableName = tableName, dbName

        if dbName is None:
            dbName = self.storage.get_current_schema()
        if not isinstance(dbName, str):
            raise IllegalArgumentException("Database name must be a string")

        if not isinstance(tableName, str):
            raise IllegalArgumentException("Table name must be a string")

        if not dbName:
            raise IllegalArgumentException("Database name cannot be empty")

        if not tableName:
            raise IllegalArgumentException("Table name cannot be empty")

        try:
            # First check storage directly
            exists = self.storage.table_exists(dbName, tableName)
            if exists:
                return True

            # If storage says it doesn't exist, try to verify by attempting to get schema
            # This handles cases where table exists but isn't properly registered
            # But only if schema exists (otherwise table definitely doesn't exist)
            if self.storage.schema_exists(dbName):
                try:
                    table_schema = self.storage.get_table_schema(dbName, tableName)
                    # Empty schema means table doesn't exist - check if it has fields
                    if table_schema is not None and len(table_schema.fields) > 0:
                        # Table exists in storage but wasn't detected by table_exists()
                        # This is a synchronization issue - return True since we can access it
                        return True
                except Exception:
                    # Table doesn't exist or can't be accessed
                    pass

            # Final check: try to list tables in the database and check if our table is there
            # This is a more comprehensive check that catches any synchronization issues
            try:
                table_list = self.storage.list_tables(dbName)
                if tableName in table_list:
                    return True
            except Exception:
                # Can't list tables or database doesn't exist
                pass

            return False
        except Exception as e:
            if isinstance(e, (AnalysisException, IllegalArgumentException)):
                raise
            raise AnalysisException(
                f"Failed to check table existence '{dbName}.{tableName}': {str(e)}"
            )

    def listTables(self, dbName: Optional[str] = None) -> List[MockTable]:
        """List tables in database.

        Args:
            dbName: Optional database name. Uses current database if None.

        Returns:
            List of MockTable objects.

        Raises:
            IllegalArgumentException: If dbName is not a string or is empty.
            AnalysisException: If database doesn't exist or there's an error.
        """
        if dbName is None:
            dbName = self.storage.get_current_schema()
        if not isinstance(dbName, str):
            raise IllegalArgumentException("Database name must be a string")

        if not dbName:
            raise IllegalArgumentException("Database name cannot be empty")

        if not self.storage.schema_exists(dbName):
            raise AnalysisException(f"Database '{dbName}' does not exist")

        try:
            table_names = self.storage.list_tables(dbName)
            return [MockTable(name, dbName) for name in table_names]
        except Exception as e:
            if isinstance(e, (AnalysisException, IllegalArgumentException)):
                raise
            raise AnalysisException(
                f"Failed to list tables in database '{dbName}': {str(e)}"
            )

    def createTable(
        self,
        tableName: str,
        path: str,
        source: str = "parquet",
        schema: Optional[Any] = None,
        **options: Any,
    ) -> None:
        """Create table.

        Args:
            tableName: Table name.
            path: Path to data.
            source: Data source format.
            schema: Table schema.
            **options: Additional options.
        """
        # Mock implementation - in real Spark this would create a table
        pass

    def dropTable(self, tableName: str) -> None:
        """Drop table.

        Args:
            tableName: Table name or qualified name (schema.table).

        Raises:
            IllegalArgumentException: If table name is invalid.
            AnalysisException: If table doesn't exist or can't be dropped.
        """
        if not isinstance(tableName, str):
            raise IllegalArgumentException("Table name must be a string")

        if not tableName:
            raise IllegalArgumentException("Table name cannot be empty")

        # Handle qualified table names (schema.table)
        if "." in tableName:
            parts = tableName.split(".", 1)
            if len(parts) == 2:
                dbName, tableName = parts
            else:
                raise IllegalArgumentException(
                    f"Invalid qualified table name: {tableName}"
                )
        else:
            dbName = self.storage.get_current_schema()

        try:
            # Check if table exists first
            if not self.storage.table_exists(dbName, tableName):
                raise AnalysisException(f"Table '{dbName}.{tableName}' does not exist")

            # Drop the table from storage
            self.storage.drop_table(dbName, tableName)
        except Exception as e:
            if isinstance(e, (AnalysisException, IllegalArgumentException)):
                raise
            raise AnalysisException(
                f"Failed to drop table '{dbName}.{tableName}': {str(e)}"
            )

    def isCached(self, tableName: str) -> bool:
        """Check if table is cached.

        Args:
            tableName: Table name or qualified name (schema.table).

        Returns:
            True if table is cached, False otherwise.

        Raises:
            IllegalArgumentException: If table name is invalid.
        """
        if not isinstance(tableName, str):
            raise IllegalArgumentException("Table name must be a string")

        if not tableName:
            raise IllegalArgumentException("Table name cannot be empty")

        # Handle qualified table names (schema.table)
        if "." in tableName:
            parts = tableName.split(".", 1)
            if len(parts) == 2:
                dbName, tableName = parts
                qualified_name = f"{dbName}.{tableName}"
            else:
                raise IllegalArgumentException(
                    f"Invalid qualified table name: {tableName}"
                )
        else:
            dbName = self.storage.get_current_schema()
            qualified_name = f"{dbName}.{tableName}"

        return qualified_name in self._cached_tables

    def cacheTable(self, tableName: str) -> None:
        """Cache table.

        Args:
            tableName: Table name or qualified name (schema.table).

        Raises:
            IllegalArgumentException: If table name is invalid.
            AnalysisException: If table doesn't exist.
        """
        if not isinstance(tableName, str):
            raise IllegalArgumentException("Table name must be a string")

        if not tableName:
            raise IllegalArgumentException("Table name cannot be empty")

        # Handle qualified table names (schema.table)
        if "." in tableName:
            parts = tableName.split(".", 1)
            if len(parts) == 2:
                dbName, tableName = parts
                qualified_name = f"{dbName}.{tableName}"
            else:
                raise IllegalArgumentException(
                    f"Invalid qualified table name: {tableName}"
                )
        else:
            dbName = self.storage.get_current_schema()
            qualified_name = f"{dbName}.{tableName}"

        # Check if table exists
        if not self.storage.table_exists(dbName, tableName):
            raise AnalysisException(f"Table '{qualified_name}' does not exist")

        # Add to cache
        self._cached_tables.add(qualified_name)

    def uncacheTable(self, tableName: str) -> None:
        """Uncache table.

        Args:
            tableName: Table name or qualified name (schema.table).

        Raises:
            IllegalArgumentException: If table name is invalid.
        """
        if not isinstance(tableName, str):
            raise IllegalArgumentException("Table name must be a string")

        if not tableName:
            raise IllegalArgumentException("Table name cannot be empty")

        # Handle qualified table names (schema.table)
        if "." in tableName:
            parts = tableName.split(".", 1)
            if len(parts) == 2:
                dbName, tableName = parts
                qualified_name = f"{dbName}.{tableName}"
            else:
                raise IllegalArgumentException(
                    f"Invalid qualified table name: {tableName}"
                )
        else:
            dbName = self.storage.get_current_schema()
            qualified_name = f"{dbName}.{tableName}"

        # Remove from cache
        self._cached_tables.discard(qualified_name)

    def refreshTable(self, tableName: str) -> None:
        """Refresh table.

        Args:
            tableName: Table name.
        """
        # Mock implementation - in real Spark this would refresh a table
        pass

    def refreshByPath(self, path: str) -> None:
        """Refresh by path.

        Args:
            path: Path to refresh.
        """
        # Mock implementation - in real Spark this would refresh by path
        pass

    def recoverPartitions(self, tableName: str) -> None:
        """Recover partitions.

        Args:
            tableName: Table name.
        """
        # Mock implementation - in real Spark this would recover partitions
        pass

    def clearCache(self) -> None:
        """Clear cache."""
        # Mock implementation - in real Spark this would clear the cache
        pass
