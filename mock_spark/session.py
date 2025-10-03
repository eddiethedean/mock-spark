"""
Mock SparkSession implementation for Mock Spark.

This module provides a complete mock implementation of PySpark's SparkSession
that behaves identically to the real SparkSession for testing and development.
It includes session management, DataFrame creation, SQL operations, and catalog
management without requiring a JVM or actual Spark installation.

Key Features:
    - Complete PySpark SparkSession API compatibility
    - DataFrame creation from various data sources
    - SQL query parsing and execution
    - Catalog operations (databases, tables)
    - Configuration management
    - Session lifecycle management

Example:
    >>> from mock_spark import MockSparkSession
    >>> spark = MockSparkSession("MyApp")
    >>> data = [{"name": "Alice", "age": 25}]
    >>> df = spark.createDataFrame(data)
    >>> df.show()
    >>> spark.sql("CREATE DATABASE test")
"""

from typing import Any, Dict, List, Optional, Union
from .storage import MockStorageManager
from .dataframe import MockDataFrame
from .spark_types import MockStructType
from .functions import MockFunctions
from .errors import (
    raise_table_not_found,
    raise_schema_not_found,
    raise_invalid_argument,
    raise_value_error,
    AnalysisException,
    IllegalArgumentException,
)


class MockDatabase:
    """Mock database object for catalog operations."""

    def __init__(self, name: str):
        self.name = name


class MockJVMContext:
    """Mock JVM context for testing."""

    def __init__(self) -> None:
        self.functions = MockFunctions()

    def __getattr__(self, name: str) -> Any:
        """Return mock functions for any attribute access."""
        if name == "read":
            raise AttributeError(
                f"MockJVMContext does not support '{name}'. "
                f"Use MockSparkSession.createDataFrame() to create DataFrames instead. "
                f"Available methods: {list(self.functions.__dict__.keys())}"
            )
        return getattr(self.functions, name, None)


class MockSparkContext:
    """Mock SparkContext for testing without PySpark."""

    def __init__(self, app_name: str = "MockSparkApp"):
        """Initialize MockSparkContext."""
        self.app_name = app_name
        self._jvm = MockJVMContext()

    def setLogLevel(self, level: str) -> None:
        """Set log level."""
        pass  # Mock implementation

    @property
    def appName(self) -> str:
        """Get application name."""
        return self.app_name


class MockSparkSession:
    """Mock SparkSession providing complete PySpark API compatibility.

    Provides a comprehensive mock implementation of PySpark's SparkSession
    that supports all major operations including DataFrame creation, SQL
    queries, catalog management, and configuration without requiring JVM.

    Attributes:
        app_name: Application name for the Spark session.
        sparkContext: MockSparkContext instance for session context.
        catalog: MockCatalog instance for database and table operations.
        conf: Configuration object for session settings.
        storage: MockStorageManager for data persistence.

    Example:
        >>> spark = MockSparkSession("MyApp")
        >>> df = spark.createDataFrame([{"name": "Alice", "age": 25}])
        >>> df.select("name").show()
        >>> spark.sql("CREATE DATABASE test")
        >>> spark.stop()
    """

    def __init__(self, app_name: str = "MockSparkApp"):
        """Initialize MockSparkSession.

        Args:
            app_name: Application name for the Spark session.
        """
        self.app_name = app_name
        self.storage = MockStorageManager()
        self._catalog = MockCatalog(self.storage)
        self.sparkContext = MockSparkContext(app_name)
        self._conf = MockSparkConf()
        self._udf = MockUDF()
        self._version = "3.4.0"  # Mock version

        # Mockable method implementations
        self._createDataFrame_impl = self._real_createDataFrame
        self._table_impl = self._real_table
        self._sql_impl = self._real_sql

    @property
    def appName(self) -> str:
        """Get application name."""
        return self.app_name

    @property
    def version(self) -> str:
        """Get Spark version."""
        return self._version

    @property
    def catalog(self) -> "MockCatalog":
        """Get the catalog."""
        return self._catalog

    @property
    def conf(self) -> "MockSparkConf":
        """Get configuration."""
        return self._conf

    @property
    def udf(self) -> "MockUDF":
        """Get UDF registration."""
        return self._udf

    def createDataFrame(
        self,
        data: List[Union[Dict[str, Any], tuple]],
        schema: Optional[Union[MockStructType, List[str]]] = None,
    ) -> MockDataFrame:
        """Create a DataFrame from data (mockable version)."""
        return self._createDataFrame_impl(data, schema)

    def _real_createDataFrame(
        self,
        data: List[Union[Dict[str, Any], tuple]],
        schema: Optional[Union[MockStructType, List[str]]] = None,
    ) -> MockDataFrame:
        """Create a DataFrame from data.

        Args:
            data: List of dictionaries or tuples representing rows.
            schema: Optional schema definition (MockStructType or list of column names).

        Returns:
            MockDataFrame instance with the specified data and schema.

        Raises:
            PySparkValueError: If data is not in the expected format.

        Example:
            >>> data = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
            >>> df = spark.createDataFrame(data)
            >>> df = spark.createDataFrame(data, ["name", "age"])
        """
        if not isinstance(data, list):
            raise_value_error("Data must be a list of dictionaries or tuples")  # type: ignore[unreachable]

        # Handle list of column names as schema
        if isinstance(schema, list):
            from .spark_types import MockStructType, MockStructField, StringType

            fields = [MockStructField(name, StringType()) for name in schema]
            schema = MockStructType(fields)

        if schema is None:
            # Infer schema from data
            if not data:
                # For empty dataset, create empty schema
                from .spark_types import MockStructType

                schema = MockStructType([])
            else:
                # Simple schema inference
                sample_row = data[0]
                if not isinstance(sample_row, (dict, tuple)):
                    raise_value_error("Data must be a list of dictionaries or tuples")  # type: ignore[unreachable]

                fields = []
                if isinstance(sample_row, dict):
                    # Dictionary format - sort keys alphabetically to match PySpark behavior
                    for key in sorted(sample_row.keys()):
                        value = sample_row[key]
                        from .spark_types import (
                            StringType,
                            LongType,
                            DoubleType,
                            BooleanType,
                            MockDataType,
                        )

                        field_type: MockDataType
                        if isinstance(value, bool):
                            field_type = BooleanType()
                        elif isinstance(value, int):
                            field_type = LongType()  # Use LongType to match PySpark
                        elif isinstance(value, float):
                            field_type = DoubleType()
                        elif isinstance(value, list):
                            # ArrayType - infer element type from first non-null element
                            element_type: MockDataType = (
                                StringType()
                            )  # Default to StringType
                            for item in value:
                                if item is not None:
                                    if isinstance(item, str):
                                        element_type = StringType()
                                    elif isinstance(item, int):
                                        element_type = LongType()
                                    elif isinstance(item, float):
                                        element_type = DoubleType()
                                    elif isinstance(item, bool):
                                        element_type = BooleanType()
                                    break

                            # Validate that all elements have the same type (PySpark limitation)
                            for item in value:
                                if item is not None:
                                    if isinstance(item, str) and not isinstance(
                                        element_type, StringType
                                    ):
                                        raise_value_error(
                                            f"Array element type mismatch: expected {element_type.__class__.__name__}, got str"
                                        )
                                    elif isinstance(item, int) and not isinstance(
                                        element_type, LongType
                                    ):
                                        raise_value_error(
                                            f"Array element type mismatch: expected {element_type.__class__.__name__}, got int"
                                        )
                                    elif isinstance(item, float) and not isinstance(
                                        element_type, DoubleType
                                    ):
                                        raise_value_error(
                                            f"Array element type mismatch: expected {element_type.__class__.__name__}, got float"
                                        )
                                    elif isinstance(item, bool) and not isinstance(
                                        element_type, BooleanType
                                    ):
                                        raise_value_error(
                                            f"Array element type mismatch: expected {element_type.__class__.__name__}, got bool"
                                        )

                            from .spark_types import ArrayType

                            field_type = ArrayType(element_type)
                        elif isinstance(value, dict):
                            # MapType - assume string keys and string values for simplicity
                            from .spark_types import MapType

                            field_type = MapType(StringType(), StringType())
                        else:
                            field_type = StringType()

                        from .spark_types import MockStructField

                        fields.append(MockStructField(key, field_type))
                else:
                    # Tuple format - need schema to convert
                    raise_value_error(
                        "Cannot infer schema from tuples without explicit schema"
                    )

                from .spark_types import MockStructType

                schema = MockStructType(fields)

        # Convert tuples to dictionaries if schema is provided
        if data and isinstance(data[0], tuple) and schema:
            converted_data: List[Dict[str, Any]] = []
            field_names = [field.name for field in schema.fields]
            for row in data:
                if len(row) != len(field_names):
                    raise_value_error(
                        f"Row length {len(row)} doesn't match schema field count {len(field_names)}"
                    )
                converted_data.append(dict(zip(field_names, row)))
            data = converted_data  # type: ignore[assignment]

        # Sort data rows to match schema column order
        if data and isinstance(data[0], dict):
            field_names = [field.name for field in schema.fields]
            sorted_data = []
            for row in data:
                if isinstance(row, dict):
                    sorted_row = {key: row[key] for key in field_names if key in row}
                else:
                    # Handle tuple data
                    sorted_row = {
                        field_names[i]: row[i]
                        for i in range(min(len(row), len(field_names)))
                    }
                sorted_data.append(sorted_row)
            data = sorted_data  # type: ignore[assignment]

        return MockDataFrame(data, schema, self.storage)  # type: ignore[arg-type]

    def table(self, table_name: str) -> MockDataFrame:
        """Get a table as DataFrame (mockable version)."""
        return self._table_impl(table_name)

    def _real_table(self, table_name: str) -> MockDataFrame:
        """Get a table as DataFrame."""
        if not isinstance(table_name, str):
            raise_value_error("Table name must be a string")  # type: ignore[unreachable]  # type: ignore[unreachable]

        if "." in table_name:
            schema_name, table_name = table_name.split(".", 1)
        else:
            schema_name = "default"

        if not self.storage.table_exists(schema_name, table_name):
            raise_table_not_found(f"{schema_name}.{table_name}")

        table = self.storage.get_table(schema_name, table_name)
        if table is None:
            raise_table_not_found(f"{schema_name}.{table_name}")

        # At this point, table is guaranteed to be not None
        assert table is not None
        return MockDataFrame(table.data, table.schema, self.storage)

    def sql(self, query: str) -> MockDataFrame:
        """Execute SQL query (mockable version)."""
        return self._sql_impl(query)

    def _real_sql(self, query: str) -> MockDataFrame:  # type: ignore[return]
        """Execute SQL query."""
        if not isinstance(query, str):
            raise_value_error("Query must be a string")  # type: ignore[unreachable]

        # Simple SQL parsing for basic operations
        query = query.strip().upper()

        if query.startswith("CREATE DATABASE"):
            # Extract database name
            parts = query.split()
            if len(parts) >= 3:
                db_name = parts[2].strip("`\"'")
                self.catalog.createDatabase(db_name)
                return MockDataFrame([], MockStructType([]), self.storage)

        elif query.startswith("DROP DATABASE"):
            # Extract database name
            parts = query.split()
            if len(parts) >= 3:
                db_name = parts[2].strip("`\"'")
                if self.storage.schema_exists(db_name):
                    self.storage.drop_schema(db_name)
                return MockDataFrame([], MockStructType([]), self.storage)

        elif query.startswith("SHOW DATABASES"):
            databases = self.catalog.listDatabases()
            data = [{"databaseName": db.name} for db in databases]
            from .spark_types import MockStructType, MockStructField, StringType

            schema = MockStructType([MockStructField("databaseName", StringType())])
            return MockDataFrame(data, schema, self.storage)

        else:
            # For other queries, return empty DataFrame
            from .spark_types import MockStructType

            return MockDataFrame([], MockStructType([]), self.storage)

    def stop(self) -> None:
        """Stop the Spark session."""
        self.storage.clear_all()

    def __enter__(self) -> "MockSparkSession":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.stop()

    def createGlobalTempView(self, name: str, replace: bool = False) -> None:
        """Create global temporary view."""
        # Mock implementation - same as regular temp view
        self.createTempView(name)

    def createOrReplaceGlobalTempView(self, name: str) -> None:
        """Create or replace global temporary view."""
        self.createGlobalTempView(name, replace=True)

    def createOrReplaceTempView(self, name: str) -> None:
        """Create or replace temporary view."""
        self.createTempView(name)

    def newSession(self) -> "MockSparkSession":
        """Create new session."""
        return MockSparkSession(self.app_name)

    def getActiveSession(self) -> "MockSparkSession":
        """Get active session."""
        return self

    def clearCache(self) -> None:
        """Clear cache."""
        pass  # Mock implementation

    def clearActiveSession(self) -> None:
        """Clear active session."""
        pass  # Mock implementation

    # Mocking methods for error testing
    def mock_createDataFrame(self, side_effect=None, return_value=None):
        """Mock createDataFrame method for error testing.

        Args:
            side_effect: Exception to raise when createDataFrame is called
            return_value: Value to return when createDataFrame is called
        """
        if side_effect:

            def raise_exception(*args, **kwargs):
                raise side_effect

            self._createDataFrame_impl = raise_exception
        elif return_value:

            def return_value_func(*args, **kwargs):
                return return_value

            self._createDataFrame_impl = return_value_func
        else:
            self._createDataFrame_impl = self._real_createDataFrame

    def mock_table(self, side_effect=None, return_value=None):
        """Mock table method for error testing.

        Args:
            side_effect: Exception to raise when table is called
            return_value: Value to return when table is called
        """
        if side_effect:

            def raise_exception(*args, **kwargs):
                raise side_effect

            self._table_impl = raise_exception
        elif return_value:

            def return_value_func(*args, **kwargs):
                return return_value

            self._table_impl = return_value_func
        else:
            self._table_impl = self._real_table

    def mock_sql(self, side_effect=None, return_value=None):
        """Mock sql method for error testing.

        Args:
            side_effect: Exception to raise when sql is called
            return_value: Value to return when sql is called
        """
        if side_effect:

            def raise_exception(*args, **kwargs):
                raise side_effect

            self._sql_impl = raise_exception
        elif return_value:

            def return_value_func(*args, **kwargs):
                return return_value

            self._sql_impl = return_value_func
        else:
            self._sql_impl = self._real_sql

    def reset_mocks(self):
        """Reset all mocked methods to their real implementations."""
        self._createDataFrame_impl = self._real_createDataFrame
        self._table_impl = self._real_table
        self._sql_impl = self._real_sql

    def __getattr__(self, name: str) -> Any:
        """Handle attribute access with helpful error messages."""
        if name == "read":
            raise AttributeError(
                f"MockSparkSession does not support '{name}'. "
                f"Use createDataFrame() to create DataFrames instead. "
                f"Available methods: createDataFrame, table, sql, catalog, conf, udf"
            )
        elif name == "streams":
            raise AttributeError(
                f"MockSparkSession does not support '{name}'. "
                f"Streaming operations are not supported in mock mode. "
                f"Use createDataFrame() for batch operations instead."
            )
        elif name == "sparkContext":
            return self.sparkContext
        else:
            raise AttributeError(
                f"MockSparkSession has no attribute '{name}'. "
                f"Available methods: createDataFrame, table, sql, catalog, conf, udf, "
                f"createTempView, createGlobalTempView, newSession, stop"
            )


class MockCatalog:
    """Mock Catalog for Spark session."""

    def __init__(self, storage: MockStorageManager):
        """Initialize MockCatalog."""
        self.storage = storage

    def listDatabases(self) -> List[MockDatabase]:
        """List all databases."""
        return [MockDatabase(name) for name in self.storage.list_schemas()]

    def createDatabase(self, name: str, ignoreIfExists: bool = True) -> None:
        """Create a database."""
        if not isinstance(name, str):
            raise_value_error("Database name must be a string")  # type: ignore[unreachable]

        if not name:
            raise_value_error("Database name cannot be empty")

        if not ignoreIfExists and self.storage.schema_exists(name):
            raise AnalysisException(f"Database '{name}' already exists", None)

        try:
            self.storage.create_schema(name)
        except Exception as e:
            if isinstance(e, (AnalysisException, IllegalArgumentException)):
                raise
            raise AnalysisException(
                f"Failed to create database '{name}': {str(e)}", None
            )

    def tableExists(self, dbName: str, tableName: str) -> bool:
        """Check if table exists."""
        if not isinstance(dbName, str):
            raise_value_error("Database name must be a string")  # type: ignore[unreachable]

        if not isinstance(tableName, str):
            raise_value_error("Table name must be a string")  # type: ignore[unreachable]

        if not dbName:
            raise_value_error("Database name cannot be empty")

        if not tableName:
            raise_value_error("Table name cannot be empty")

        try:
            return self.storage.table_exists(dbName, tableName)
        except Exception as e:
            if isinstance(e, (AnalysisException, IllegalArgumentException)):
                raise
            raise AnalysisException(
                f"Failed to check table existence '{dbName}.{tableName}': {str(e)}",
                None,
            )

    def listTables(self, dbName: str) -> List[str]:
        """List tables in database."""
        if not isinstance(dbName, str):
            raise_value_error("Database name must be a string")  # type: ignore[unreachable]

        if not dbName:
            raise_value_error("Database name cannot be empty")

        if not self.storage.schema_exists(dbName):
            raise_schema_not_found(dbName)

        try:
            return self.storage.list_tables(dbName)
        except Exception as e:
            if isinstance(e, (AnalysisException, IllegalArgumentException)):
                raise
            raise AnalysisException(
                f"Failed to list tables in database '{dbName}': {str(e)}", None
            )

    def createTable(
        self,
        tableName: str,
        path: str,
        source: str = "parquet",
        schema: Optional[Any] = None,
        **options: Any,
    ) -> None:
        """Create table."""
        # Mock implementation
        pass

    def dropTable(self, tableName: str) -> None:
        """Drop table."""
        # Mock implementation
        pass

    def isCached(self, tableName: str) -> bool:
        """Check if table is cached."""
        return False  # Mock implementation

    def cacheTable(self, tableName: str) -> None:
        """Cache table."""
        pass  # Mock implementation

    def uncacheTable(self, tableName: str) -> None:
        """Uncache table."""
        pass  # Mock implementation

    def refreshTable(self, tableName: str) -> None:
        """Refresh table."""
        pass  # Mock implementation

    def refreshByPath(self, path: str) -> None:
        """Refresh by path."""
        pass  # Mock implementation

    def recoverPartitions(self, tableName: str) -> None:
        """Recover partitions."""
        pass  # Mock implementation

    def clearCache(self) -> None:
        """Clear cache."""
        pass  # Mock implementation


class MockSparkConf:
    """Mock SparkConf for configuration."""

    def __init__(self):
        """Initialize MockSparkConf."""
        self._config = {
            "spark.app.name": "MockSparkApp",
            "spark.master": "local[*]",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
        }

    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value."""
        return self._config.get(key, default)

    def set(self, key: str, value: Any) -> None:
        """Set configuration value."""
        self._config[key] = value

    def setAll(self, pairs: Dict[str, Any]) -> None:
        """Set multiple configuration values."""
        self._config.update(pairs)

    def setMaster(self, master: str) -> None:
        """Set master URL."""
        self._config["spark.master"] = master

    def setAppName(self, name: str) -> None:
        """Set application name."""
        self._config["spark.app.name"] = name

    def setIfMissing(self, key: str, value: Any) -> None:
        """Set configuration value if missing."""
        if key not in self._config:
            self._config[key] = value

    def remove(self, key: str) -> None:
        """Remove configuration value."""
        self._config.pop(key, None)

    def contains(self, key: str) -> bool:
        """Check if configuration contains key."""
        return key in self._config

    def getAll(self) -> Dict[str, Any]:
        """Get all configuration values."""
        return self._config.copy()


class MockUDF:
    """Mock UDF registration."""

    def register(self, name: str, func: Any, returnType: Any = None) -> None:
        """Register UDF."""
        # Mock implementation - just store the function
        pass

    def registerJavaFunction(
        self, name: str, className: str, returnType: Any = None
    ) -> None:
        """Register Java UDF."""
        pass  # Mock implementation

    def registerPython(self, name: str, func: Any, returnType: Any = None) -> None:
        """Register Python UDF."""
        pass  # Mock implementation


class MockSparkSessionBuilder:
    """Mock SparkSession builder."""

    def __init__(self):
        """Initialize builder."""
        self._app_name = "MockSparkApp"

    def appName(self, name: str) -> "MockSparkSessionBuilder":
        """Set app name."""
        self._app_name = name
        return self

    def master(self, master: str) -> "MockSparkSessionBuilder":
        """Set master URL."""
        return self

    def config(
        self, key_or_pairs: Union[str, Dict[str, Any]], value: Any = None
    ) -> "MockSparkSessionBuilder":
        """Set configuration."""
        return self

    def enableHiveSupport(self) -> "MockSparkSessionBuilder":
        """Enable Hive support."""
        return self


# Add builder to MockSparkSession
setattr(MockSparkSession, "builder", MockSparkSessionBuilder())
