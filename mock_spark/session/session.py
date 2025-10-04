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
    >>> from mock_spark.session import MockSparkSession
    >>> spark = MockSparkSession("MyApp")
    >>> data = [{"name": "Alice", "age": 25}]
    >>> df = spark.createDataFrame(data)
    >>> df.show()
    >>> spark.sql("CREATE DATABASE test")
"""

from typing import Any, Dict, List, Optional, Union
from ..core.interfaces.session import ISession
from ..core.interfaces.dataframe import IDataFrame, IDataFrameReader
from ..core.interfaces.storage import IStorageManager
from ..core.exceptions.validation import IllegalArgumentException
from .context import MockSparkContext
from .catalog import MockCatalog
from .config import MockConfiguration
from .sql.executor import MockSQLExecutor
from mock_spark.storage import MemoryStorageManager
from mock_spark.dataframe import MockDataFrame
from ..spark_types import MockStructType


class MockDataFrameReader(IDataFrameReader):
    """Mock DataFrameReader for reading data from various sources."""

    def __init__(self, session: ISession):
        """Initialize MockDataFrameReader.
        
        Args:
            session: Mock Spark session instance.
        """
        self.session = session
        self._format = "parquet"
        self._options = {}

    def format(self, source: str) -> "MockDataFrameReader":
        """Set input format.
        
        Args:
            source: Data source format.
            
        Returns:
            Self for method chaining.
        """
        self._format = source
        return self

    def option(self, key: str, value: Any) -> "MockDataFrameReader":
        """Set option.
        
        Args:
            key: Option key.
            value: Option value.
            
        Returns:
            Self for method chaining.
        """
        self._options[key] = value
        return self

    def options(self, **options: Any) -> "MockDataFrameReader":
        """Set multiple options.
        
        Args:
            **options: Option key-value pairs.
            
        Returns:
            Self for method chaining.
        """
        self._options.update(options)
        return self

    def schema(self, schema: Union[MockStructType, str]) -> "MockDataFrameReader":
        """Set schema.
        
        Args:
            schema: Schema definition.
            
        Returns:
            Self for method chaining.
        """
        # Mock implementation
        return self

    def load(self, path: Optional[str] = None, format: Optional[str] = None, **options: Any) -> IDataFrame:
        """Load data.
        
        Args:
            path: Path to data.
            format: Data format.
            **options: Additional options.
            
        Returns:
            DataFrame with loaded data.
        """
        # Mock implementation - return empty DataFrame
        return MockDataFrame([], [])

    def table(self, table_name: str) -> IDataFrame:
        """Load table.
        
        Args:
            table_name: Table name.
            
        Returns:
            DataFrame with table data.
        """
        return self.session.table(table_name)

    def json(self, path: str, **options: Any) -> IDataFrame:
        """Load JSON data.
        
        Args:
            path: Path to JSON file.
            **options: Additional options.
            
        Returns:
            DataFrame with JSON data.
        """
        # Mock implementation
        return MockDataFrame([], [])

    def csv(self, path: str, **options: Any) -> IDataFrame:
        """Load CSV data.
        
        Args:
            path: Path to CSV file.
            **options: Additional options.
            
        Returns:
            DataFrame with CSV data.
        """
        # Mock implementation
        return MockDataFrame([], [])

    def parquet(self, path: str, **options: Any) -> IDataFrame:
        """Load Parquet data.
        
        Args:
            path: Path to Parquet file.
            **options: Additional options.
            
        Returns:
            DataFrame with Parquet data.
        """
        # Mock implementation
        return MockDataFrame([], [])


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
        storage: MemoryStorageManager for data persistence.

    Example:
        >>> spark = MockSparkSession("MyApp")
        >>> df = spark.createDataFrame([{"name": "Alice", "age": 25}])
        >>> df.select("name").show()
        >>> spark.sql("CREATE DATABASE test")
        >>> spark.stop()
    """
    
    # Class attribute for builder pattern
    builder = None

    def __init__(self, app_name: str = "MockSparkApp"):
        """Initialize MockSparkSession.

        Args:
            app_name: Application name for the Spark session.
        """
        self.app_name = app_name
        self.storage = MemoryStorageManager()
        self._catalog = MockCatalog(self.storage)
        self.sparkContext = MockSparkContext(app_name)
        self._conf = MockConfiguration()
        self._version = "3.4.0"  # Mock version
        self._sql_executor = MockSQLExecutor(self)
        
        # Mockable method implementations
        self._createDataFrame_impl = self._real_createDataFrame
        self._table_impl = self._real_table
        self._sql_impl = self._real_sql
        
        # Error simulation
        self._error_rules = {}

    @property
    def appName(self) -> str:
        """Get application name."""
        return self.app_name

    @property
    def version(self) -> str:
        """Get Spark version."""
        return self._version

    @property
    def catalog(self) -> MockCatalog:
        """Get the catalog."""
        return self._catalog

    @property
    def conf(self) -> MockConfiguration:
        """Get configuration."""
        return self._conf

    @property
    def read(self) -> MockDataFrameReader:
        """Get DataFrame reader."""
        return MockDataFrameReader(self)

    def createDataFrame(
        self,
        data: List[Union[Dict[str, Any], tuple]],
        schema: Optional[Union[MockStructType, List[str]]] = None,
    ) -> IDataFrame:
        """Create a DataFrame from data (mockable version)."""
        return self._createDataFrame_impl(data, schema)

    def _real_createDataFrame(
        self,
        data: List[Union[Dict[str, Any], tuple]],
        schema: Optional[Union[MockStructType, List[str]]] = None,
    ) -> IDataFrame:
        """Create a DataFrame from data.

        Args:
            data: List of dictionaries or tuples representing rows.
            schema: Optional schema definition (MockStructType or list of column names).

        Returns:
            MockDataFrame instance with the specified data and schema.

        Raises:
            IllegalArgumentException: If data is not in the expected format.

        Example:
            >>> data = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
            >>> df = spark.createDataFrame(data)
            >>> df = spark.createDataFrame(data, ["name", "age"])
        """
        if not isinstance(data, list):
            raise IllegalArgumentException("Data must be a list of dictionaries or tuples")

        # Handle list of column names as schema
        if isinstance(schema, list):
            from ..spark_types import MockStructType, MockStructField, StringType

            fields = [MockStructField(name, StringType()) for name in schema]
            schema = MockStructType(fields)

        if schema is None:
            # Infer schema from data
            if not data:
                # For empty dataset, create empty schema
                from ..spark_types import MockStructType
                schema = MockStructType([])
            else:
                # Simple schema inference
                sample_row = data[0]
                if not isinstance(sample_row, (dict, tuple)):
                    raise IllegalArgumentException("Data must be a list of dictionaries or tuples")

                fields = []
                if isinstance(sample_row, dict):
                    # Dictionary format - sort keys alphabetically to match PySpark behavior
                    sorted_keys = sorted(sample_row.keys())
                    for key in sorted_keys:
                        value = sample_row[key]
                        from ..spark_types import (
                            StringType,
                            LongType,
                            DoubleType,
                            BooleanType,
                        )

                        field_type = self._infer_type(value)
                        from ..spark_types import MockStructField
                        fields.append(MockStructField(key, field_type))

                from ..spark_types import MockStructType
                schema = MockStructType(fields)
                
                # Reorder data rows to match schema (alphabetical key order)
                if isinstance(sample_row, dict):
                    reordered_data = []
                    for row in data:
                        if isinstance(row, dict):
                            reordered_row = {key: row[key] for key in sorted_keys}
                            reordered_data.append(reordered_row)
                        else:
                            reordered_data.append(row)
                    data = reordered_data

        return MockDataFrame(data, schema, self.storage)

    def _infer_type(self, value: Any) -> Any:
        """Infer data type from value.
        
        Args:
            value: Value to infer type from.
            
        Returns:
            Inferred data type.
        """
        from ..spark_types import (
            StringType,
            LongType,
            DoubleType,
            BooleanType,
            ArrayType,
            MapType,
        )

        if isinstance(value, bool):
            return BooleanType()
        elif isinstance(value, int):
            return LongType()
        elif isinstance(value, float):
            return DoubleType()
        elif isinstance(value, list):
            # ArrayType - infer element type from first non-null element
            element_type = StringType()  # Default to StringType
            for item in value:
                if item is not None:
                    element_type = self._infer_type(item)
                    break
            return ArrayType(element_type)
        elif isinstance(value, dict):
            # MapType - assume string keys and string values for simplicity
            return MapType(StringType(), StringType())
        else:
            return StringType()

    def sql(self, query: str) -> IDataFrame:
        """Execute SQL query (mockable version)."""
        return self._sql_impl(query)

    def _real_sql(self, query: str) -> IDataFrame:
        """Execute SQL query.

        Args:
            query: SQL query string.

        Returns:
            DataFrame with query results.

        Example:
            >>> df = spark.sql("SELECT * FROM users WHERE age > 18")
        """
        return self._sql_executor.execute(query)

    def table(self, table_name: str) -> IDataFrame:
        """Get table as DataFrame (mockable version)."""
        self._check_error_rules("table", table_name)
        return self._table_impl(table_name)

    def _real_table(self, table_name: str) -> IDataFrame:
        """Get table as DataFrame.

        Args:
            table_name: Table name.

        Returns:
            DataFrame with table data.

        Example:
            >>> df = spark.table("users")
        """
        # Parse table name
        schema, table = (
            table_name.split(".", 1) if "." in table_name else ("default", table_name)
        )
        
        # Check if table exists
        if not self.storage.table_exists(schema, table):
            from mock_spark.errors import AnalysisException
            raise AnalysisException(f"Table or view not found: {table_name}")
        
        # Get table data and schema
        table_data = self.storage.get_data(schema, table)
        table_schema = self.storage.get_table_schema(schema, table)
        
        return MockDataFrame(table_data, table_schema, self.storage)

    def range(self, start: int, end: int, step: int = 1, numPartitions: Optional[int] = None) -> IDataFrame:
        """Create DataFrame with range of numbers.

        Args:
            start: Start value (inclusive).
            end: End value (exclusive).
            step: Step size.
            numPartitions: Number of partitions (ignored in mock).

        Returns:
            DataFrame with range data.

        Example:
            >>> df = spark.range(0, 10, 2)
        """
        data = [{"id": i} for i in range(start, end, step)]
        return self.createDataFrame(data, ["id"])

    def stop(self) -> None:
        """Stop the session."""
        # Mock implementation - in real Spark this would stop the session
        pass

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop()

    def newSession(self) -> "MockSparkSession":
        """Create new session.

        Returns:
            New MockSparkSession instance.
        """
        return MockSparkSession(self.app_name)

    # Mockable methods for testing
    def mock_createDataFrame(self, side_effect=None, return_value=None):
        """Mock createDataFrame method for testing."""
        if side_effect:
            def mock_impl(*args, **kwargs):
                raise side_effect
            self._createDataFrame_impl = mock_impl
        elif return_value:
            def mock_impl(*args, **kwargs):
                return return_value
            self._createDataFrame_impl = mock_impl

    def mock_table(self, side_effect=None, return_value=None):
        """Mock table method for testing."""
        if side_effect:
            def mock_impl(*args, **kwargs):
                raise side_effect
            self._table_impl = mock_impl
        elif return_value:
            def mock_impl(*args, **kwargs):
                return return_value
            self._table_impl = mock_impl

    def mock_sql(self, side_effect=None, return_value=None):
        """Mock sql method for testing."""
        if side_effect:
            def mock_impl(*args, **kwargs):
                raise side_effect
            self._sql_impl = mock_impl
        elif return_value:
            def mock_impl(*args, **kwargs):
                return return_value
            self._sql_impl = mock_impl

    # Error simulation methods
    def add_error_rule(self, method_name: str, error_condition, error_exception):
        """Add error simulation rule."""
        self._error_rules[method_name] = (error_condition, error_exception)

    def clear_error_rules(self):
        """Clear all error simulation rules."""
        self._error_rules.clear()

    def reset_mocks(self):
        """Reset all mocks to original implementations."""
        self._createDataFrame_impl = self._real_createDataFrame
        self._table_impl = self._real_table
        self._sql_impl = self._real_sql
        self.clear_error_rules()

    def _check_error_rules(self, method_name: str, *args, **kwargs):
        """Check if error should be raised for method."""
        if method_name in self._error_rules:
            for condition, exception in self._error_rules[method_name]:
                if condition(*args, **kwargs):
                    raise exception

    # Integration with MockErrorSimulator
    def _add_error_rule(self, method_name: str, condition, exception):
        """Add error rule (used by MockErrorSimulator)."""
        if method_name not in self._error_rules:
            self._error_rules[method_name] = []
        self._error_rules[method_name].append((condition, exception))

    def _remove_error_rule(self, method_name: str, condition=None):
        """Remove error rule (used by MockErrorSimulator)."""
        if method_name in self._error_rules:
            if condition is None:
                self._error_rules[method_name] = []
            else:
                self._error_rules[method_name] = [
                    (c, e) for c, e in self._error_rules[method_name] if c != condition
                ]

    def _should_raise_error(self, method_name: str, *args, **kwargs):
        """Check if error should be raised (used by MockErrorSimulator)."""
        if method_name in self._error_rules:
            for condition, exception in self._error_rules[method_name]:
                if condition(*args, **kwargs):
                    return exception
        return None


class MockSparkSessionBuilder:
    """Mock SparkSession builder."""

    def __init__(self):
        """Initialize builder."""
        self._app_name = "MockSparkApp"
        self._config = {}

    def appName(self, name: str) -> "MockSparkSessionBuilder":
        """Set app name.
        
        Args:
            name: Application name.
            
        Returns:
            Self for method chaining.
        """
        self._app_name = name
        return self

    def master(self, master: str) -> "MockSparkSessionBuilder":
        """Set master URL.
        
        Args:
            master: Master URL.
            
        Returns:
            Self for method chaining.
        """
        return self

    def config(
        self, key_or_pairs: Union[str, Dict[str, Any]], value: Any = None
    ) -> "MockSparkSessionBuilder":
        """Set configuration.
        
        Args:
            key_or_pairs: Configuration key or dictionary of key-value pairs.
            value: Configuration value (if key_or_pairs is a string).
            
        Returns:
            Self for method chaining.
        """
        if isinstance(key_or_pairs, str):
            self._config[key_or_pairs] = value
        else:
            self._config.update(key_or_pairs)
        return self

    def getOrCreate(self) -> MockSparkSession:
        """Get or create session.
        
        Returns:
            MockSparkSession instance.
        """
        session = MockSparkSession(self._app_name)
        for key, value in self._config.items():
            session.conf.set(key, value)
        return session


# Set the builder attribute on MockSparkSession
MockSparkSession.builder = MockSparkSessionBuilder()
