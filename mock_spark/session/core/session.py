"""
Core session implementation for Mock Spark.

This module provides the core MockSparkSession class for session management,
maintaining compatibility with PySpark's SparkSession interface.
"""

from typing import Any, Dict, List, Optional, Union, Tuple, cast
from ...core.interfaces.session import ISession
from ...core.interfaces.dataframe import IDataFrame
from ..context import MockSparkContext
from ..catalog import MockCatalog
from ..config import MockConfiguration, MockSparkConfig
from ..sql.executor import MockSQLExecutor
from mock_spark.backend.factory import BackendFactory
from mock_spark.backend.protocols import StorageBackend
from mock_spark.dataframe import MockDataFrame, MockDataFrameReader
from ...spark_types import (
    MockStructType,
)
from ..services.protocols import (
    IDataFrameFactory,
    ISQLParameterBinder,
    ISessionLifecycleManager,
    IMockingCoordinator,
)
from ..services.dataframe_factory import DataFrameFactory
from ..services.sql_parameter_binder import SQLParameterBinder
from ..services.lifecycle_manager import SessionLifecycleManager
from ..services.mocking_coordinator import MockingCoordinator


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
        max_memory: str = "1GB",
        allow_disk_spillover: bool = False,
        storage_backend: Optional[StorageBackend] = None,
    ):
        """Initialize MockSparkSession.

        Args:
            app_name: Application name for the Spark session.
            validation_mode: "strict", "relaxed", or "minimal" validation behavior.
            enable_type_coercion: Whether to coerce basic types during DataFrame creation.
            enable_lazy_evaluation: Whether to enable lazy evaluation (default True).
            max_memory: Maximum memory for DuckDB to use (e.g., '1GB', '4GB', '8GB').
                       Default is '1GB' for test isolation.
            allow_disk_spillover: If True, allows DuckDB to spill to disk when memory is full.
                                 If False (default), disables spillover for test isolation.
            storage_backend: Optional storage backend instance. If None, creates DuckDB backend.
        """
        self.app_name = app_name
        # Use dependency injection for storage backend
        if storage_backend is None:
            self.storage = BackendFactory.create_storage_backend(
                backend_type="duckdb",
                max_memory=max_memory,
                allow_disk_spillover=allow_disk_spillover,
            )
        else:
            self.storage = storage_backend
        from typing import cast

        self._catalog = MockCatalog(self.storage)
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

        # Validation settings (Phase 2 plumbing)
        self._engine_config = MockSparkConfig(
            validation_mode=validation_mode,
            enable_type_coercion=enable_type_coercion,
            enable_lazy_evaluation=enable_lazy_evaluation,
        )

        # Performance and memory tracking (delegated to SessionPerformanceTracker)
        from ..performance_tracker import SessionPerformanceTracker

        self._performance_tracker = SessionPerformanceTracker()

        # Service dependencies (injected, typed with Protocols)
        self._dataframe_factory: IDataFrameFactory = DataFrameFactory()
        self._sql_parameter_binder: ISQLParameterBinder = SQLParameterBinder()
        self._lifecycle_manager: ISessionLifecycleManager = SessionLifecycleManager()
        self._mocking_coordinator: IMockingCoordinator = MockingCoordinator()

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
    ) -> "MockDataFrame":
        """Create a DataFrame from data (mockable version)."""
        # Use the mock implementation if set, otherwise use the real implementation
        return self._createDataFrame_impl(data, schema)

    def _real_createDataFrame(
        self,
        data: Union[List[Dict[str, Any]], List[Any]],
        schema: Optional[Union[MockStructType, List[str], str]] = None,
    ) -> "MockDataFrame":
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
        # Plugin hook: before_create_dataframe
        for plugin in getattr(self, "_plugins", []):
            if hasattr(plugin, "before_create_dataframe"):
                try:
                    data, schema = plugin.before_create_dataframe(self, data, schema)
                except Exception:
                    pass

        # Delegate to DataFrameFactory service
        df = self._dataframe_factory.create_dataframe(
            data, schema, self._engine_config, self.storage
        )

        # Apply lazy/eager mode based on session config
        try:
            if hasattr(df, "withLazy"):
                lazy_enabled = getattr(
                    self._engine_config, "enable_lazy_evaluation", True
                )
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

    def benchmark_operation(
        self, operation_name: str, func: Any, *args: Any, **kwargs: Any
    ) -> Any:
        """Benchmark an operation and record simple telemetry.

        Returns the function result. Records duration (s), memory_used (bytes),
        and result_size when possible.
        """
        return self._performance_tracker.benchmark_operation(
            operation_name, func, *args, **kwargs
        )

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

    def sql(self, query: str, *args: Any, **kwargs: Any) -> IDataFrame:
        """Execute SQL query with optional parameters (mockable version).

        Args:
            query: SQL query string with optional placeholders.
            *args: Positional parameters for ? placeholders.
            **kwargs: Named parameters for :name placeholders.

        Returns:
            DataFrame with query results.

        Example:
            >>> spark.sql("SELECT * FROM users WHERE age > ?", 18)
            >>> spark.sql("SELECT * FROM users WHERE age > :min_age", min_age=18)
        """
        return self._sql_impl(query, *args, **kwargs)

    def _real_sql(self, query: str, *args: Any, **kwargs: Any) -> IDataFrame:
        """Execute SQL query with optional parameters.

        Args:
            query: SQL query string with optional placeholders.
            *args: Positional parameters for ? placeholders.
            **kwargs: Named parameters for :name placeholders.

        Returns:
            DataFrame with query results.

        Example:
            >>> df = spark.sql("SELECT * FROM users WHERE age > ?", 18)
            >>> df = spark.sql("SELECT * FROM users WHERE name = :name", name="Alice")
        """
        # Process parameters if provided
        if args or kwargs:
            query = self._sql_parameter_binder.bind_parameters(query, args, kwargs)

        return self._sql_executor.execute(query)

    def _bind_parameters(
        self, query: str, args: Tuple[Any, ...], kwargs: Dict[str, Any]
    ) -> str:
        """Bind parameters to SQL query safely.

        Args:
            query: SQL query with placeholders.
            args: Positional parameters.
            kwargs: Named parameters.

        Returns:
            Query with parameters bound.
        """
        # Delegate to SQLParameterBinder service
        return self._sql_parameter_binder.bind_parameters(query, args, kwargs)

    def _format_param(self, value: Any) -> str:
        """Format a parameter value for SQL safely.

        Args:
            value: Parameter value.

        Returns:
            Formatted parameter string.
        """
        # Delegate to SQLParameterBinder service
        return self._sql_parameter_binder._format_param(value)

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
        if "." in table_name:
            schema, table = table_name.split(".", 1)
        else:
            # Use current schema instead of hardcoded "default"
            schema = self.storage.get_current_schema()
            table = table_name
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
        else:
            # When reading from a table, reset all fields to nullable=True to match PySpark behavior
            # Storage formats (Parquet/Delta) typically make columns nullable by default
            from ...spark_types import MockStructField, MockStructType
            from ...spark_types import (
                BooleanType,
                IntegerType,
                LongType,
                DoubleType,
                StringType,
                MockDataType,
            )

            updated_fields = []
            for field in table_schema.fields:
                # Create new data type with nullable=True
                data_type: MockDataType
                field_type = getattr(field, "dataType", None) or getattr(
                    field, "data_type", None
                )
                if isinstance(field_type, BooleanType):
                    data_type = BooleanType(nullable=True)
                elif isinstance(field_type, IntegerType):
                    data_type = IntegerType(nullable=True)
                elif isinstance(field_type, LongType):
                    data_type = LongType(nullable=True)
                elif isinstance(field_type, DoubleType):
                    data_type = DoubleType(nullable=True)
                elif isinstance(field_type, StringType):
                    data_type = StringType(nullable=True)
                else:
                    # For other types, create with nullable=True
                    if field_type is None:
                        data_type = StringType(nullable=True)  # fallback
                    else:
                        data_type = field_type.__class__(nullable=True)

                # Create new field with nullable=True
                updated_field = MockStructField(field.name, data_type, nullable=True)
                updated_fields.append(updated_field)

            table_schema = MockStructType(updated_fields)

        return MockDataFrame(table_data, table_schema, self.storage)  # type: ignore[return-value]

    def range(
        self, start: int, end: int, step: int = 1, numPartitions: Optional[int] = None
    ) -> "MockDataFrame":
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
        """Stop the session and clean up resources."""
        # Delegate to SessionLifecycleManager service
        self._lifecycle_manager.stop_session(self.storage, self._performance_tracker)

    def __enter__(self) -> "MockSparkSession":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        # Delegate to SessionLifecycleManager service
        self._lifecycle_manager.stop_session(self.storage, self._performance_tracker)

    def newSession(self) -> "MockSparkSession":
        """Create new session.

        Returns:
            New MockSparkSession instance.
        """
        return MockSparkSession(self.app_name)

    # Mockable methods for testing
    def mock_createDataFrame(
        self, side_effect: Any = None, return_value: Any = None
    ) -> None:
        """Mock createDataFrame method for testing."""
        # Delegate to MockingCoordinator service
        self._createDataFrame_impl = self._mocking_coordinator.setup_mock_impl(
            "createDataFrame", side_effect, return_value
        )

    def mock_table(self, side_effect: Any = None, return_value: Any = None) -> None:
        """Mock table method for testing."""
        # Delegate to MockingCoordinator service
        self._table_impl = self._mocking_coordinator.setup_mock_impl(
            "table", side_effect, return_value
        )

    def mock_sql(self, side_effect: Any = None, return_value: Any = None) -> None:
        """Mock sql method for testing."""
        # Delegate to MockingCoordinator service
        self._sql_impl = self._mocking_coordinator.setup_mock_impl(
            "sql", side_effect, return_value
        )

    # Error simulation methods
    def add_error_rule(
        self, method_name: str, error_condition: Any, error_exception: Any
    ) -> None:
        """Add error simulation rule."""
        # Delegate to MockingCoordinator service
        self._mocking_coordinator.add_error_rule(
            method_name, error_condition, error_exception
        )

    def clear_error_rules(self) -> None:
        """Clear all error simulation rules."""
        # Delegate to MockingCoordinator service
        self._mocking_coordinator.clear_error_rules()

    def reset_mocks(self) -> None:
        """Reset all mocks to original implementations."""
        # Delegate to MockingCoordinator service
        original_impls = {
            "createDataFrame": self._real_createDataFrame,
            "table": self._real_table,
            "sql": self._real_sql,
        }
        self._mocking_coordinator.reset_all_mocks(original_impls)

        # Reset implementations to original
        self._createDataFrame_impl = self._real_createDataFrame
        self._table_impl = self._real_table
        self._sql_impl = self._real_sql

    def _check_error_rules(self, method_name: str, *args: Any, **kwargs: Any) -> None:
        """Check if error should be raised for method."""
        # Delegate to MockingCoordinator service
        exception = self._mocking_coordinator.check_error_rules(
            method_name, *args, **kwargs
        )
        if exception:
            raise exception

    # Integration with MockErrorSimulator
    def _add_error_rule(self, method_name: str, condition: Any, exception: Any) -> None:
        """Add error rule (used by MockErrorSimulator)."""
        # Delegate to MockingCoordinator service
        self._mocking_coordinator.add_error_rule(method_name, condition, exception)

    def _remove_error_rule(self, method_name: str, condition: Any = None) -> None:
        """Remove error rule (used by MockErrorSimulator)."""
        # Note: MockingCoordinator doesn't support removal yet, but we can clear all rules
        if condition is None:
            self._mocking_coordinator.clear_error_rules()

    def _should_raise_error(self, method_name: str, *args: Any, **kwargs: Any) -> Any:
        """Check if error should be raised (used by MockErrorSimulator)."""
        # Delegate to MockingCoordinator service
        return self._mocking_coordinator.check_error_rules(method_name, *args, **kwargs)


# Set the builder attribute on MockSparkSession
from .builder import MockSparkSessionBuilder  # noqa: E402

MockSparkSession.builder = MockSparkSessionBuilder()
