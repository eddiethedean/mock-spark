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

    def __init__(self, app_name: str = "MockSparkApp", validation_mode: str = "relaxed", enable_type_coercion: bool = True, enable_lazy_evaluation: bool = True):
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

        # Memory tracking (Phase 3)
        self._tracked_dataframes: List[MockDataFrame] = []
        self._approx_memory_usage_bytes: int = 0
        self._benchmark_results: Dict[str, Dict[str, Any]] = {}

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
        # Apply lazy/eager mode based on session config
        try:
            if hasattr(df, "withLazy"):
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
            # Infer schema from data
            if not data:
                # For empty dataset, create empty schema
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
                        field_type = self._infer_type(value)
                        fields.append(MockStructField(key, field_type))

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
                elif isinstance(sample_row, tuple):
                    # Convert tuples to dictionaries using schema field names
                    reordered_data = []
                    field_names = [field.name for field in schema.fields]
                    for row in data:
                        if isinstance(row, tuple):
                            row_dict = {field_names[i]: row[i] for i in range(len(row))}
                            reordered_data.append(row_dict)
                        else:
                            reordered_data.append(row)
                    data = reordered_data

        # Apply validation and optional type coercion per mode
        if isinstance(schema, MockStructType) and data:
            if self._engine_config.validation_mode == "strict":
                self._validate_data_matches_schema(data, schema)
            # In relaxed/minimal modes, perform optional light coercion
            if self._engine_config.enable_type_coercion:
                data = self._coerce_data_to_schema(data, schema)

        df = MockDataFrame(data, schema, self.storage)  # type: ignore[return-value]
        # Track memory usage for newly created DataFrame
        try:
            self._track_dataframe(df)
        except Exception:
            pass
        return df

    def _track_dataframe(self, df: MockDataFrame) -> None:
        """Track DataFrame for approximate memory accounting."""
        self._tracked_dataframes.append(df)
        self._approx_memory_usage_bytes += self._estimate_dataframe_size(df)

    def _estimate_dataframe_size(self, df: MockDataFrame) -> int:
        """Very rough size estimate based on rows, columns, and value sizes."""
        num_rows = len(df.data)
        num_cols = len(df.schema.fields)
        # assume ~32 bytes per cell average (key+value overhead), adjustable
        return num_rows * num_cols * 32

    def get_memory_usage(self) -> int:
        """Return approximate memory usage in bytes for tracked DataFrames."""
        return self._approx_memory_usage_bytes

    def clear_cache(self) -> None:
        """Clear tracked DataFrames to free memory accounting."""
        self._tracked_dataframes.clear()
        self._approx_memory_usage_bytes = 0

    # ---------------------------
    # Benchmarking API (Phase 3)
    # ---------------------------
    def benchmark_operation(self, operation_name: str, func: Any, *args: Any, **kwargs: Any) -> Any:
        """Benchmark an operation and record simple telemetry.

        Returns the function result. Records duration (s), memory_used (bytes),
        and result_size when possible.
        """
        import time

        start_mem = self.get_memory_usage()
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        end_mem = self.get_memory_usage()

        size: int = 1
        try:
            if hasattr(result, "count"):
                size = int(result.count())
            elif hasattr(result, "collect"):
                size = len(result.collect())
            elif hasattr(result, "__len__"):
                size = len(result)
        except Exception:
            size = 1

        self._benchmark_results[operation_name] = {
            "duration_s": max(end_time - start_time, 0.0),
            "memory_used_bytes": max(end_mem - start_mem, 0),
            "result_size": size,
        }
        return result

    def get_benchmark_results(self) -> Dict[str, Dict[str, Any]]:
        """Return a copy of the latest benchmark results."""
        return dict(self._benchmark_results)

    def _infer_type(self, value: Any) -> Any:
        """Infer data type from value.

        Args:
            value: Value to infer type from.

        Returns:
            Inferred data type.
        """
        from ...spark_types import (
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

    # ---------------------------
    # Validation and Coercion
    # ---------------------------
    def _validate_data_matches_schema(self, data: List[Dict[str, Any]], schema: MockStructType) -> None:
        """Validate that data rows conform to the provided schema.

        Raises IllegalArgumentException on mismatches in strict mode.
        """
        field_types = {f.name: f.dataType.__class__.__name__ for f in schema.fields}
        for row in data:
            if not isinstance(row, dict):
                raise IllegalArgumentException("Strict mode requires dict rows after normalization")
            # Ensure all schema fields present
            for name in field_types.keys():
                if name not in row:
                    raise IllegalArgumentException(f"Missing required field '{name}' in row")
            # Type check best-effort
            for name, value in row.items():
                if name not in field_types:
                    raise IllegalArgumentException(f"Unexpected field '{name}' in row")
                expected = field_types[name]
                if value is None:
                    continue
                actual_py = type(value).__name__
                # Accept numeric widenings (int->LongType, float->DoubleType)
                if expected in ("LongType", "IntegerType") and isinstance(value, int):
                    continue
                if expected in ("DoubleType", "FloatType") and isinstance(value, (int, float)):
                    continue
                if expected == "StringType" and isinstance(value, str):
                    continue
                if expected == "BooleanType" and isinstance(value, bool):
                    continue
                # For complex types, skip deep validation for now
                # Otherwise raise
                raise IllegalArgumentException(
                    f"Type mismatch for field '{name}': expected {expected}, got {actual_py}"
                )

    def _coerce_data_to_schema(self, data: List[Dict[str, Any]], schema: MockStructType) -> List[Dict[str, Any]]:
        """Coerce data types to match schema when possible (best-effort)."""
        coerced: List[Dict[str, Any]] = []
        field_types = {f.name: f.dataType.__class__.__name__ for f in schema.fields}
        for row in data:
            if not isinstance(row, dict):
                coerced.append(row)  # leave as-is if not normalized
                continue
            new_row: Dict[str, Any] = {}
            for name in field_types.keys():
                value = row.get(name)
                expected = field_types[name]
                new_row[name] = self._coerce_value(value, expected)
            coerced.append(new_row)
        return coerced

    def _coerce_value(self, value: Any, expected_type_name: str) -> Any:
        if value is None:
            return None
        try:
            if expected_type_name in ("LongType", "IntegerType"):
                return int(value)
            if expected_type_name in ("DoubleType", "FloatType"):
                return float(value)
            if expected_type_name == "StringType":
                return str(value)
            if expected_type_name == "BooleanType":
                if isinstance(value, bool):
                    return value
                if str(value).lower() in ("true", "1"):  # simple coercion
                    return True
                if str(value).lower() in ("false", "0"):
                    return False
        except Exception:
            return value
        return value

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
