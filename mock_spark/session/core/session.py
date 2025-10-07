"""
Core session implementation for Mock Spark.

This module provides the core MockSparkSession class for session management,
maintaining compatibility with PySpark's SparkSession interface.
"""

from typing import Any, Dict, List, Optional, Union, cast
from ...core.interfaces.session import ISession
from ...core.interfaces.dataframe import IDataFrame, IDataFrameReader
from ...core.interfaces.storage import IStorageManager
from ...core.exceptions.validation import IllegalArgumentException
from ..context import MockSparkContext
from ..catalog import MockCatalog
from ..config import MockConfiguration, MockSparkConfig
from ..sql.executor import MockSQLExecutor
from mock_spark.storage import DuckDBStorageManager
from mock_spark.dataframe import MockDataFrame, MockDataFrameReader
from ...spark_types import (
    MockStructType,
    MockStructField,
    StringType,
    LongType,
    DoubleType,
    BooleanType,
    ArrayType,
    MapType,
)


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
    builder: Optional["MockSparkSessionBuilder"] = None
    _singleton_session: Optional["MockSparkSession"] = None

    def __init__(
        self,
        app_name: str = "MockSparkApp",
        validation_mode: str = "relaxed",
        enable_type_coercion: bool = True,
        enable_lazy_evaluation: bool = True,
    ):
        """Initialize MockSparkSession.

        Args:
            app_name: Application name for the Spark session.
            validation_mode: "strict", "relaxed", or "minimal" validation behavior.
            enable_type_coercion: Whether to coerce basic types during DataFrame creation.
        """
        self.app_name = app_name
        self.storage = DuckDBStorageManager()
        from typing import cast
        from ...core.interfaces.storage import IStorageManager

        self._catalog = MockCatalog(cast(IStorageManager, self.storage))
        self.sparkContext = MockSparkContext(app_name)
        self._conf = MockConfiguration()
        self._version = "3.4.0"  # Mock version
        from ...core.interfaces.session import ISession

        self._sql_executor = MockSQLExecutor(cast(ISession, self))

        # Mockable method implementations
        self._createDataFrame_impl = self._real_createDataFrame
        self._original_createDataFrame_impl = (
            self._real_createDataFrame
        )  # Store original for mock detection
        self._table_impl = self._real_table
        self._sql_impl = self._real_sql
        # Plugins (Phase 4)
        self._plugins: List[Any] = []

        # Error simulation
        self._error_rules: Dict[str, Any] = {}

        # Validation settings (Phase 2 plumbing)
        self._engine_config = MockSparkConfig(
            validation_mode=validation_mode,
            enable_type_coercion=enable_type_coercion,
            enable_lazy_evaluation=enable_lazy_evaluation,
        )

        # Performance and memory tracking (delegated to SessionPerformanceTracker)
        from ..performance_tracker import SessionPerformanceTracker

        self._performance_tracker = SessionPerformanceTracker()

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
        return MockDataFrameReader(cast(ISession, self))

    def createDataFrame(
        self,
        data: Union[List[Dict[str, Any]], List[Any]],
        schema: Optional[Union[MockStructType, List[str]]] = None,
    ) -> IDataFrame:
        """Create a DataFrame from data (mockable version)."""
        # Plugin hook: before_create_dataframe
        for plugin in getattr(self, "_plugins", []):
            if hasattr(plugin, "before_create_dataframe"):
                try:
                    data, schema = plugin.before_create_dataframe(self, data, schema)
                except Exception:
                    pass
        df = self._createDataFrame_impl(data, schema)
        # Apply lazy/eager mode based on session config (but not for mocked returns)
        # Check if this is a mocked return by seeing if _createDataFrame_impl is not the original
        is_mocked = self._createDataFrame_impl != self._original_createDataFrame_impl
        try:
            if not is_mocked and hasattr(df, "withLazy"):
                lazy_enabled = getattr(self._engine_config, "enable_lazy_evaluation", True)
                df = df.withLazy(lazy_enabled)
        except Exception:
            pass
        # Plugin hook: after_create_dataframe
        for plugin in getattr(self, "_plugins", []):
            if hasattr(plugin, "after_create_dataframe"):
                try:
                    df = plugin.after_create_dataframe(self, df)
                except Exception:
                    pass
        return df

    def _real_createDataFrame(
        self,
        data: Union[List[Dict[str, Any]], List[Any]],
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
            fields = [MockStructField(name, StringType()) for name in schema]
            schema = MockStructType(fields)

            # Convert tuples to dictionaries using provided column names
            if data and isinstance(data[0], tuple):
                reordered_data = []
                column_names = [field.name for field in schema.fields]
                for row in data:
                    if isinstance(row, tuple):
                        row_dict = {column_names[i]: row[i] for i in range(len(row))}
                        reordered_data.append(row_dict)
                    else:
                        reordered_data.append(row)
                data = reordered_data

        if schema is None:
            # Infer schema from data using SchemaInferenceEngine
            if not data:
                # For empty dataset, create empty schema
                schema = MockStructType([])
            else:
                # Check if data is in expected format
                sample_row = data[0]
                if not isinstance(sample_row, (dict, tuple)):
                    raise IllegalArgumentException("Data must be a list of dictionaries or tuples")

                if isinstance(sample_row, dict):
                    # Use SchemaInferenceEngine for dictionary data
                    from ...core.schema_inference import SchemaInferenceEngine

                    schema, data = SchemaInferenceEngine.infer_from_data(data)
                elif isinstance(sample_row, tuple):
                    # For tuples, we need column names - this should have been handled earlier
                    # If we get here, it's an error
                    raise IllegalArgumentException(
                        "Cannot infer schema from tuples without column names. "
                        "Please provide schema or use list of column names."
                    )

        # Apply validation and optional type coercion per mode
        if isinstance(schema, MockStructType) and data:
            from ...core.data_validation import DataValidator

            validator = DataValidator(
                schema,
                validation_mode=self._engine_config.validation_mode,
                enable_coercion=self._engine_config.enable_type_coercion,
            )

            # Validate if in strict mode
            if self._engine_config.validation_mode == "strict":
                validator.validate(data)

            # Coerce if enabled
            if self._engine_config.enable_type_coercion:
                data = validator.coerce(data)

        df = MockDataFrame(data, schema, self.storage)  # type: ignore[return-value]
        # Track memory usage for newly created DataFrame
        try:
            self._track_dataframe(df)
        except Exception:
            pass
        return df

    def _track_dataframe(self, df: MockDataFrame) -> None:
        """Track DataFrame for approximate memory accounting."""
        self._performance_tracker.track_dataframe(df)

    def get_memory_usage(self) -> int:
        """Return approximate memory usage in bytes for tracked DataFrames."""
        return self._performance_tracker.get_memory_usage()

    def clear_cache(self) -> None:
        """Clear tracked DataFrames to free memory accounting."""
        self._performance_tracker.clear_cache()

    def benchmark_operation(self, operation_name: str, func: Any, *args: Any, **kwargs: Any) -> Any:
        """Benchmark an operation and record simple telemetry.

        Returns the function result. Records duration (s), memory_used (bytes),
        and result_size when possible.
        """
        return self._performance_tracker.benchmark_operation(operation_name, func, *args, **kwargs)

    def get_benchmark_results(self) -> Dict[str, Dict[str, Any]]:
        """Return a copy of the latest benchmark results."""
        return self._performance_tracker.get_benchmark_results()

    def _infer_type(self, value: Any) -> Any:
        """Infer data type from value.

        Delegates to SchemaInferenceEngine for consistency.

        Args:
            value: Value to infer type from.

        Returns:
            Inferred data type.
        """
        from ...core.schema_inference import SchemaInferenceEngine

        return SchemaInferenceEngine._infer_type(value)

    # ---------------------------
    # Validation and Coercion
    # ---------------------------
    # NOTE: Validation and coercion logic extracted to DataValidator
    # See: mock_spark/core/data_validation.py

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

    # ---------------------------
    # Plugin registration (Phase 4)
    # ---------------------------
    def register_plugin(self, plugin: Any) -> None:
        self._plugins.append(plugin)

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
        schema, table = table_name.split(".", 1) if "." in table_name else ("default", table_name)
        # Handle global temp views using Spark's convention 'global_temp'
        if schema == "global_temp":
            schema = "global_temp"

        # Check if table exists
        if not self.storage.table_exists(schema, table):
            from mock_spark.errors import AnalysisException

            raise AnalysisException(f"Table or view not found: {table_name}")

        # Get table data and schema
        table_data = self.storage.get_data(schema, table)
        table_schema = self.storage.get_table_schema(schema, table)

        # Ensure schema is not None
        if table_schema is None:
            from ...spark_types import MockStructType

            table_schema = MockStructType([])

        return MockDataFrame(table_data, table_schema, self.storage)  # type: ignore[return-value]

    def range(
        self, start: int, end: int, step: int = 1, numPartitions: Optional[int] = None
    ) -> IDataFrame:
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


# Set the builder attribute on MockSparkSession
from .builder import MockSparkSessionBuilder

MockSparkSession.builder = MockSparkSessionBuilder()
