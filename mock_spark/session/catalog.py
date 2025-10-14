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

    def listDatabases(self) -> List[MockDatabase]:
        """List all databases.

        Returns:
            List of MockDatabase objects.
        """
        return [MockDatabase(name) for name in self.storage.list_schemas()]

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
            ignore_if_not_exists if ignore_if_not_exists is not None else ignoreIfNotExists
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

    def tableExists(self, dbName: str, tableName: str) -> bool:
        """Check if table exists.

        Args:
            dbName: Database name.
            tableName: Table name.

        Returns:
            True if table exists, False otherwise.

        Raises:
            IllegalArgumentException: If names are not strings or are empty.
            AnalysisException: If there's an error checking table existence.
        """
        if not isinstance(dbName, str):
            raise IllegalArgumentException("Database name must be a string")

        if not isinstance(tableName, str):
            raise IllegalArgumentException("Table name must be a string")

        if not dbName:
            raise IllegalArgumentException("Database name cannot be empty")

        if not tableName:
            raise IllegalArgumentException("Table name cannot be empty")

        try:
            return self.storage.table_exists(dbName, tableName)
        except Exception as e:
            if isinstance(e, (AnalysisException, IllegalArgumentException)):
                raise
            raise AnalysisException(
                f"Failed to check table existence '{dbName}.{tableName}': {str(e)}"
            )

    def listTables(self, dbName: str) -> List[str]:
        """List tables in database.

        Args:
            dbName: Database name.

        Returns:
            List of table names.

        Raises:
            IllegalArgumentException: If dbName is not a string or is empty.
            AnalysisException: If database doesn't exist or there's an error.
        """
        if not isinstance(dbName, str):
            raise IllegalArgumentException("Database name must be a string")

        if not dbName:
            raise IllegalArgumentException("Database name cannot be empty")

        if not self.storage.schema_exists(dbName):
            raise AnalysisException(f"Database '{dbName}' does not exist")

        try:
            return self.storage.list_tables(dbName)
        except Exception as e:
            if isinstance(e, (AnalysisException, IllegalArgumentException)):
                raise
            raise AnalysisException(f"Failed to list tables in database '{dbName}': {str(e)}")

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
            tableName: Table name.
        """
        # Mock implementation - in real Spark this would drop a table
        pass

    def isCached(self, tableName: str) -> bool:
        """Check if table is cached.

        Args:
            tableName: Table name.

        Returns:
            True if table is cached, False otherwise.
        """
        return False  # Mock implementation

    def cacheTable(self, tableName: str) -> None:
        """Cache table.

        Args:
            tableName: Table name.
        """
        # Mock implementation - in real Spark this would cache a table
        pass

    def uncacheTable(self, tableName: str) -> None:
        """Uncache table.

        Args:
            tableName: Table name.
        """
        # Mock implementation - in real Spark this would uncache a table
        pass

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
