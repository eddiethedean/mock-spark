"""
Mock DataFrame implementation for Mock Spark.

This module provides a complete mock implementation of PySpark DataFrame
that behaves identically to the real PySpark DataFrame for testing and
development purposes. It supports all major DataFrame operations including
selection, filtering, grouping, joining, and window functions.

Key Features:
    - Complete PySpark API compatibility
    - 100% type-safe operations with mypy compliance
    - Window function support with partitioning and ordering
    - Comprehensive error handling matching PySpark exceptions
    - In-memory storage for fast test execution
    - Mockable methods for error testing scenarios
    - Enhanced DataFrameWriter with all save modes
    - Advanced data type support (15+ types including complex types)

Example:
    >>> from mock_spark import MockSparkSession, F
    >>> spark = MockSparkSession("test")
    >>> data = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
    >>> df = spark.createDataFrame(data)
    >>> df.select("name", "age").filter(F.col("age") > 25).show()
    +----+---+
    |name|age|
    +----+---+
    | Bob| 30|
    +----+---+
"""

from typing import Any, Dict, List, Optional, Union, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from .lazy import LazyEvaluationEngine
    from .window_handler import WindowFunctionHandler
    from .collection_handler import CollectionHandler
    from .validation_handler import ValidationHandler

from ..spark_types import (
    MockStructType,
    MockStructField,
    MockRow,
    StringType,
    LongType,
    DoubleType,
    MockDataType,
    IntegerType,
    ArrayType,
    MapType,
)
from ..functions import MockColumn, MockColumnOperation, MockLiteral
from ..storage import MemoryStorageManager
from .grouped import (
    MockGroupedData,
    MockRollupGroupedData,
    MockCubeGroupedData,
)
from .rdd import MockRDD
from ..core.exceptions import (
    IllegalArgumentException,
)
from ..core.exceptions.analysis import ColumnNotFoundException, AnalysisException
from ..core.exceptions.operation import MockSparkColumnNotFoundError
from .writer import MockDataFrameWriter
from .evaluation.expression_evaluator import ExpressionEvaluator


class MockDataFrame:
    """Mock DataFrame implementation with complete PySpark API compatibility.

    Provides a comprehensive mock implementation of PySpark DataFrame that supports
    all major operations including selection, filtering, grouping, joining, and
    window functions. Designed for testing and development without requiring JVM.

    Attributes:
        data: List of dictionaries representing DataFrame rows.
        schema: MockStructType defining the DataFrame schema.
        storage: Optional storage manager for persistence operations.

    Example:
        >>> from mock_spark import MockSparkSession, F
        >>> spark = MockSparkSession("test")
        >>> data = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
        >>> df = spark.createDataFrame(data)
        >>> df.select("name").filter(F.col("age") > 25).show()
        +----+
        |name|
        +----+
        | Bob|
        +----+
    """

    def __init__(
        self,
        data: List[Dict[str, Any]],
        schema: MockStructType,
        storage: Any = None,  # Can be MemoryStorageManager, DuckDBStorageManager, or None
        operations: Optional[List[Any]] = None,
    ):
        """Initialize MockDataFrame.

        Args:
            data: List of dictionaries representing DataFrame rows.
            schema: MockStructType defining the DataFrame schema.
            storage: Optional storage manager for persistence operations.
                    Defaults to a new MemoryStorageManager instance.
        """
        self.data = data
        self.schema = schema
        self.storage = storage or MemoryStorageManager()
        self._cached_count: Optional[int] = None
        self._operations_queue: List[Any] = operations or []
        # Lazy evaluation engine (lazy-initialized)
        self._lazy_engine: Optional["LazyEvaluationEngine"] = None
        # Expression evaluator for column expressions
        self._expression_evaluator = ExpressionEvaluator()
        # Window function handler (lazy-initialized)
        self._window_handler: Optional["WindowFunctionHandler"] = None
        # Collection handler (lazy-initialized)
        self._collection_handler: Optional["CollectionHandler"] = None
        # Validation handler (lazy-initialized)
        self._validation_handler: Optional["ValidationHandler"] = None

    def _get_lazy_engine(self) -> "LazyEvaluationEngine":
        """Get or create the lazy evaluation engine."""
        if self._lazy_engine is None:
            from .lazy import LazyEvaluationEngine

            self._lazy_engine = LazyEvaluationEngine()
        return self._lazy_engine

    def _get_window_handler(self) -> "WindowFunctionHandler":
        """Get or create the window function handler."""
        if self._window_handler is None:
            from .window_handler import WindowFunctionHandler

            self._window_handler = WindowFunctionHandler(self)
        return self._window_handler

    def _get_collection_handler(self) -> "CollectionHandler":
        """Get or create the collection handler."""
        if self._collection_handler is None:
            from .collection_handler import CollectionHandler

            self._collection_handler = CollectionHandler()
        return self._collection_handler

    def _get_validation_handler(self) -> "ValidationHandler":
        """Get or create the validation handler."""
        if self._validation_handler is None:
            from .validation_handler import ValidationHandler

            self._validation_handler = ValidationHandler()
        return self._validation_handler

    def _queue_op(self, op_name: str, payload: Any) -> "MockDataFrame":
        """Queue an operation for lazy evaluation."""
        new_ops = self._operations_queue + [(op_name, payload)]
        return MockDataFrame(
            data=self.data,
            schema=self.schema,
            storage=self.storage,
            operations=new_ops,
        )

    def _materialize_if_lazy(self) -> "MockDataFrame":
        """Materialize lazy operations if any are queued."""
        if self._operations_queue:
            lazy_engine = self._get_lazy_engine()
            return lazy_engine.materialize(self)
        return self

    def __repr__(self) -> str:
        return (
            f"MockDataFrame[{len(self.data)} rows, {len(self.schema.fields)} columns]"
        )

    def __getattribute__(self, name: str) -> Any:
        """
        Custom attribute access to enforce PySpark version compatibility for DataFrame methods.

        This intercepts all attribute access and checks if public methods (non-underscore)
        are available in the current PySpark compatibility mode.

        Args:
            name: Name of the attribute/method being accessed

        Returns:
            The requested attribute/method

        Raises:
            AttributeError: If method not available in current version mode
        """
        # Always allow access to private/protected attributes and core attributes
        if name.startswith("_") or name in ["data", "schema", "storage"]:
            return super().__getattribute__(name)

        # For public methods, check version compatibility
        try:
            attr = super().__getattribute__(name)

            # Only check callable methods (not properties/data)
            if callable(attr):
                from mock_spark._version_compat import is_available, get_pyspark_version

                if not is_available(name, "dataframe_method"):
                    version = get_pyspark_version()
                    raise AttributeError(
                        f"'DataFrame' object has no attribute '{name}' "
                        f"(PySpark {version} compatibility mode)"
                    )

            return attr
        except AttributeError:
            # Re-raise if attribute truly doesn't exist
            raise

    def __getattr__(self, name: str) -> MockColumn:
        """Enable df.column_name syntax for column access (PySpark compatibility)."""
        # Avoid infinite recursion - access object.__getattribute__ directly
        try:
            columns = object.__getattribute__(self, "columns")
            if name in columns:
                # Use F.col to create MockColumn
                from mock_spark.functions import F

                return F.col(name)
        except AttributeError:
            pass

        # If not a column, raise MockSparkColumnNotFoundError for better error messages
        available_cols = getattr(self, "columns", [])
        from mock_spark.core.exceptions.operation import MockSparkColumnNotFoundError

        raise MockSparkColumnNotFoundError(name, available_cols)

    def _validate_column_exists(self, column_name: str, operation: str) -> None:
        """Validate that a column exists in the DataFrame."""
        self._get_validation_handler().validate_column_exists(
            self.schema, column_name, operation
        )

    def _validate_columns_exist(self, column_names: List[str], operation: str) -> None:
        """Validate that multiple columns exist in the DataFrame."""
        self._get_validation_handler().validate_columns_exist(
            self.schema, column_names, operation
        )

    def _validate_filter_expression(self, condition: Any, operation: str) -> None:
        """Validate filter expression before execution."""
        # Check if there are pending joins (columns might come from other DF)
        has_pending_joins = any(op[0] == "join" for op in self._operations_queue)
        self._get_validation_handler().validate_filter_expression(
            self.schema, condition, operation, has_pending_joins
        )

    def _validate_expression_columns(self, expression: Any, operation: str) -> None:
        """Validate column references in complex expressions."""
        # Check if we're in lazy materialization mode by looking at the call stack
        import inspect

        frame = inspect.currentframe()
        in_lazy_materialization = False
        try:
            # Walk up the call stack to see if we're in lazy materialization
            while frame:
                if frame.f_code.co_name == "_materialize_manual":
                    in_lazy_materialization = True
                    break
                frame = frame.f_back
        finally:
            del frame

        self._get_validation_handler().validate_expression_columns(
            self.schema, expression, operation, in_lazy_materialization
        )

    def _execute_with_debug(self, operation: str, sql: str) -> None:
        """Execute with optional debug logging."""
        if hasattr(self.storage, "get_config") and self.storage.get_config(
            "debug_mode", False
        ):
            print(f"[DEBUG] Operation: {operation}")
            print(f"[DEBUG] SQL: {sql}")
        # Execute...

    def show(self, n: int = 20, truncate: bool = True) -> None:
        """Display DataFrame content in a clean table format.

        Args:
            n: Number of rows to display (default: 20).
            truncate: Whether to truncate long values (default: True).

        Example:
            >>> df.show(5)
            MockDataFrame[3 rows, 3 columns]
            name    age  salary
            Alice   25   50000
            Bob     30   60000
            Charlie 35   70000
        """
        print(
            f"MockDataFrame[{len(self.data)} rows, {len(self.schema.fields)} columns]"
        )
        if not self.data:
            print("(empty)")
            return

        # Show first n rows
        display_data = self.data[:n]

        # Get column names
        columns = (
            list(display_data[0].keys()) if display_data else self.schema.fieldNames()
        )

        # Calculate column widths
        col_widths = {}
        for col in columns:
            # Start with column name width
            col_widths[col] = len(col)
            # Check data widths
            for row in display_data:
                value = str(row.get(col, "null"))
                if truncate and len(value) > 20:
                    value = value[:17] + "..."
                col_widths[col] = max(col_widths[col], len(value))

        # Print header (no extra padding) - add blank line for separation
        print()  # Add blank line between metadata and headers
        header_parts = []
        for col in columns:
            header_parts.append(col.ljust(col_widths[col]))
        print(" ".join(header_parts))

        # Print data rows (with padding for alignment)
        for row in display_data:
            row_parts = []
            for col in columns:
                value = str(row.get(col, "null"))
                if truncate and len(value) > 20:
                    value = value[:17] + "..."
                # Add padding to data but not headers
                padded_width = col_widths[col] + 2
                row_parts.append(value.ljust(padded_width))
            print(" ".join(row_parts))

        if len(self.data) > n:
            print(f"\n... ({len(self.data) - n} more rows)")

    def to_markdown(
        self, n: int = 20, truncate: bool = True, underline_headers: bool = True
    ) -> str:
        """
        Return DataFrame as a markdown table string.

        Args:
            n: Number of rows to show
            truncate: Whether to truncate long strings
            underline_headers: Whether to underline headers with = symbols

        Returns:
            String representation of DataFrame as markdown table
        """
        if not self.data:
            return f"MockDataFrame[{len(self.data)} rows, {len(self.schema.fields)} columns]\n\n(empty)"

        # Show first n rows
        display_data = self.data[:n]

        # Get column names
        columns = (
            list(display_data[0].keys()) if display_data else self.schema.fieldNames()
        )

        # Build markdown table
        lines = []
        lines.append(
            f"MockDataFrame[{len(self.data)} rows, {len(self.schema.fields)} columns]"
        )
        lines.append("")  # Blank line

        # Header row
        header_row = "| " + " | ".join(columns) + " |"
        lines.append(header_row)

        # Separator row - use underlines for better visual distinction
        if underline_headers:
            separator_row = (
                "| " + " | ".join(["=" * len(col) for col in columns]) + " |"
            )
        else:
            separator_row = "| " + " | ".join(["---" for _ in columns]) + " |"
        lines.append(separator_row)

        # Data rows
        for row in display_data:
            row_values = []
            for col in columns:
                value = str(row.get(col, "null"))
                if truncate and len(value) > 20:
                    value = value[:17] + "..."
                row_values.append(value)
            data_row = "| " + " | ".join(row_values) + " |"
            lines.append(data_row)

        if len(self.data) > n:
            lines.append(f"\n... ({len(self.data) - n} more rows)")

        return "\n".join(lines)

    def collect(self) -> List[MockRow]:
        """Collect all data as list of Row objects."""
        if self._operations_queue:
            materialized = self._materialize_if_lazy()
            return self._get_collection_handler().collect(
                materialized.data, materialized.schema
            )
        return self._get_collection_handler().collect(self.data, self.schema)

    def toPandas(self) -> Any:
        """Convert to pandas DataFrame (requires pandas as optional dependency)."""
        from .export import DataFrameExporter

        return DataFrameExporter.to_pandas(self)

    def toDuckDB(self, connection: Any = None, table_name: Optional[str] = None) -> str:
        """Convert to DuckDB table for analytical operations.

        Args:
            connection: DuckDB connection or SQLAlchemy Engine (creates temporary if None)
            table_name: Name for the table (auto-generated if None)

        Returns:
            Table name in DuckDB
        """
        from .export import DataFrameExporter

        return DataFrameExporter.to_duckdb(self, connection, table_name)

    def _get_duckdb_type(self, data_type: Any) -> str:
        """Map MockSpark data type to DuckDB type (backwards compatibility).

        This method is kept for backwards compatibility with existing tests.
        Implementation delegated to DataFrameExporter.
        """
        from .export import DataFrameExporter

        return DataFrameExporter._get_duckdb_type(data_type)

    def count(self) -> int:
        """Count number of rows."""
        # Materialize lazy operations if needed
        if self._operations_queue:
            materialized = self._materialize_if_lazy()
            # Don't call count() recursively - just return the length of materialized data
            return len(materialized.data)

        if self._cached_count is None:
            self._cached_count = len(self.data)
        return self._cached_count

    @property
    def columns(self) -> List[str]:
        """Get column names."""
        return [field.name for field in self.schema.fields]

    @property
    def schema(self) -> MockStructType:
        """Get DataFrame schema.

        If lazy with queued operations, project the resulting schema without materializing data.
        """
        if self._operations_queue:
            return self._project_schema_with_operations()
        return self._schema

    @schema.setter
    def schema(self, value: MockStructType) -> None:
        """Set DataFrame schema."""
        self._schema = value

    def printSchema(self) -> None:
        """Print DataFrame schema."""
        print("MockDataFrame Schema:")
        for field in self.schema.fields:
            nullable = "nullable" if field.nullable else "not nullable"
            print(
                f" |-- {field.name}: {field.dataType.__class__.__name__} ({nullable})"
            )

    # ---------------------------
    # Test Helpers (Phase 4)
    # ---------------------------
    def assert_has_columns(self, expected_columns: List[str]) -> None:
        """Assert that DataFrame has the expected columns."""
        from .assertions import DataFrameAssertions

        return DataFrameAssertions.assert_has_columns(self, expected_columns)

    def assert_row_count(self, expected_count: int) -> None:
        """Assert that DataFrame has the expected row count."""
        from .assertions import DataFrameAssertions

        return DataFrameAssertions.assert_row_count(self, expected_count)

    def assert_schema_matches(self, expected_schema: "MockStructType") -> None:
        """Assert that DataFrame schema matches the expected schema."""
        from .assertions import DataFrameAssertions

        return DataFrameAssertions.assert_schema_matches(self, expected_schema)

    def assert_data_equals(self, expected_data: List[Dict[str, Any]]) -> None:
        """Assert that DataFrame data equals the expected data."""
        from .assertions import DataFrameAssertions

        return DataFrameAssertions.assert_data_equals(self, expected_data)

    def _project_schema_with_operations(self) -> MockStructType:
        """Compute schema after applying queued lazy operations."""
        from ..spark_types import (
            MockStructType,
            MockStructField,
            BooleanType,
            LongType,
            StringType,
            DoubleType,
            IntegerType,
        )

        fields_map = {f.name: f for f in self._schema.fields}
        for op_name, op_val in self._operations_queue:
            if op_name == "filter":
                # no schema change
                continue
            if op_name == "select":
                # Schema changes to only include selected columns
                columns = op_val
                new_fields_map = {}
                for col in columns:
                    if isinstance(col, str):
                        if col == "*":
                            # Add all existing fields
                            new_fields_map.update(fields_map)
                        elif col in fields_map:
                            new_fields_map[col] = fields_map[col]
                    elif hasattr(col, "name"):
                        col_name = col.name
                        if col_name == "*":
                            # Add all existing fields
                            new_fields_map.update(fields_map)
                        elif col_name in fields_map:
                            new_fields_map[col_name] = fields_map[col_name]
                        elif hasattr(col, "value") and hasattr(col, "column_type"):
                            # For MockLiteral objects - literals are never nullable
                            # Create a new instance of the data type with nullable=False
                            from ..spark_types import (
                                BooleanType,
                                IntegerType,
                                LongType,
                                DoubleType,
                                StringType,
                            )

                            col_type = col.column_type
                            data_type3: Any
                            if isinstance(col_type, BooleanType):
                                data_type3 = BooleanType(nullable=False)
                            elif isinstance(col_type, IntegerType):
                                data_type3 = IntegerType(nullable=False)
                            elif isinstance(col_type, LongType):
                                data_type3 = LongType(nullable=False)
                            elif isinstance(col_type, DoubleType):
                                data_type3 = DoubleType(nullable=False)
                            elif isinstance(col_type, StringType):
                                data_type3 = StringType(nullable=False)
                            else:
                                # For other types, create a new instance with nullable=False
                                data_type3 = col_type.__class__(nullable=False)

                            field = MockStructField(
                                col_name, data_type3, nullable=False
                            )
                            new_fields_map[col_name] = field
                        else:
                            # New column from expression - infer type based on operation
                            if hasattr(col, "operation"):
                                operation = getattr(col, "operation", None)
                                if operation == "datediff":
                                    new_fields_map[col_name] = MockStructField(
                                        col_name, IntegerType()
                                    )
                                elif operation == "months_between":
                                    new_fields_map[col_name] = MockStructField(
                                        col_name, DoubleType()
                                    )
                                elif operation in [
                                    "hour",
                                    "minute",
                                    "second",
                                    "day",
                                    "dayofmonth",
                                    "month",
                                    "year",
                                    "quarter",
                                    "dayofweek",
                                    "dayofyear",
                                    "weekofyear",
                                ]:
                                    new_fields_map[col_name] = MockStructField(
                                        col_name, IntegerType()
                                    )
                                else:
                                    # Default to StringType for unknown operations
                                    new_fields_map[col_name] = MockStructField(
                                        col_name, StringType()
                                    )
                            else:
                                # No operation attribute - default to StringType
                                new_fields_map[col_name] = MockStructField(
                                    col_name, StringType()
                                )
                fields_map = new_fields_map
                continue
            if op_name == "withColumn":
                col_name, col = op_val
                # Determine type similar to eager withColumn
                if hasattr(col, "operation") and hasattr(col, "column"):
                    if getattr(col, "operation", None) == "cast":
                        # Cast operation - use the target data type from col.value
                        cast_type = col.value
                        if isinstance(cast_type, str):
                            # String type name, convert to actual type
                            if cast_type.lower() in ["double", "float"]:
                                fields_map[col_name] = MockStructField(
                                    col_name, DoubleType()
                                )
                            elif cast_type.lower() in ["int", "integer"]:
                                fields_map[col_name] = MockStructField(
                                    col_name, IntegerType()
                                )
                            elif cast_type.lower() in ["long", "bigint"]:
                                fields_map[col_name] = MockStructField(
                                    col_name, LongType()
                                )
                            elif cast_type.lower() in ["string", "varchar"]:
                                fields_map[col_name] = MockStructField(
                                    col_name, StringType()
                                )
                            elif cast_type.lower() in ["boolean", "bool"]:
                                fields_map[col_name] = MockStructField(
                                    col_name, BooleanType()
                                )
                            elif cast_type.lower() in ["date"]:
                                from ..spark_types import DateType

                                fields_map[col_name] = MockStructField(
                                    col_name, DateType()
                                )
                            elif cast_type.lower() in ["timestamp"]:
                                from ..spark_types import TimestampType

                                fields_map[col_name] = MockStructField(
                                    col_name, TimestampType()
                                )
                            elif cast_type.lower().startswith("decimal"):
                                # Parse decimal(10,2) format
                                import re
                                from ..spark_types import DecimalType

                                match = re.match(
                                    r"decimal\((\d+),(\d+)\)", cast_type.lower()
                                )
                                if match:
                                    precision, scale = (
                                        int(match.group(1)),
                                        int(match.group(2)),
                                    )
                                    fields_map[col_name] = MockStructField(
                                        col_name, DecimalType(precision, scale)
                                    )
                                else:
                                    fields_map[col_name] = MockStructField(
                                        col_name, DecimalType(10, 2)
                                    )
                            elif cast_type.lower().startswith("array<"):
                                # Parse array<element_type> format
                                from ..spark_types import ArrayType

                                element_type_str = cast_type[
                                    6:-1
                                ]  # Extract between "array<" and ">"
                                element_type = self._parse_cast_type_string(
                                    element_type_str
                                )
                                fields_map[col_name] = MockStructField(
                                    col_name, ArrayType(element_type)
                                )
                            elif cast_type.lower().startswith("map<"):
                                # Parse map<key_type,value_type> format
                                from ..spark_types import MapType

                                types = cast_type[4:-1].split(",", 1)
                                key_type = self._parse_cast_type_string(
                                    types[0].strip()
                                )
                                value_type = self._parse_cast_type_string(
                                    types[1].strip()
                                )
                                fields_map[col_name] = MockStructField(
                                    col_name, MapType(key_type, value_type)
                                )
                            else:
                                # Default to StringType for unknown types
                                fields_map[col_name] = MockStructField(
                                    col_name, StringType()
                                )
                        else:
                            # Already a MockDataType object
                            if hasattr(cast_type, "__class__"):
                                fields_map[col_name] = MockStructField(
                                    col_name, cast_type.__class__(nullable=True)
                                )
                            else:
                                fields_map[col_name] = MockStructField(
                                    col_name, cast_type
                                )
                    elif getattr(col, "operation", None) in ["+", "-", "*", "/", "%"]:
                        # Arithmetic operations - infer type from operands
                        left_type = None
                        right_type = None

                        # Get left operand type
                        if hasattr(col.column, "name"):
                            for field in self._schema.fields:
                                if field.name == col.column.name:
                                    left_type = field.dataType
                                    break

                        # Get right operand type
                        if (
                            hasattr(col, "value")
                            and col.value is not None
                            and hasattr(col.value, "name")
                        ):
                            for field in self._schema.fields:
                                if field.name == col.value.name:
                                    right_type = field.dataType
                                    break

                        # If either operand is DoubleType, result is DoubleType
                        if (left_type and isinstance(left_type, DoubleType)) or (
                            right_type and isinstance(right_type, DoubleType)
                        ):
                            fields_map[col_name] = MockStructField(
                                col_name, DoubleType()
                            )
                        else:
                            fields_map[col_name] = MockStructField(col_name, LongType())
                    elif getattr(col, "operation", None) in ["abs"]:
                        fields_map[col_name] = MockStructField(col_name, LongType())
                    elif getattr(col, "operation", None) in ["length"]:
                        fields_map[col_name] = MockStructField(col_name, IntegerType())
                    elif getattr(col, "operation", None) in ["round"]:
                        # round() should return the same type as its input
                        # If input is integer, return LongType; if double, return DoubleType
                        if (
                            hasattr(col.column, "operation")
                            and col.column.operation == "cast"
                        ):
                            # If the input is a cast operation, check the target type
                            cast_type = getattr(col.column, "value", "string")
                            if isinstance(cast_type, str) and cast_type.lower() in [
                                "int",
                                "integer",
                            ]:
                                fields_map[col_name] = MockStructField(
                                    col_name, LongType()
                                )
                            else:
                                fields_map[col_name] = MockStructField(
                                    col_name, DoubleType()
                                )
                        else:
                            # Default to DoubleType for other cases
                            fields_map[col_name] = MockStructField(
                                col_name, DoubleType()
                            )
                    elif getattr(col, "operation", None) in ["upper", "lower"]:
                        fields_map[col_name] = MockStructField(col_name, StringType())
                    elif getattr(col, "operation", None) == "datediff":
                        fields_map[col_name] = MockStructField(col_name, IntegerType())
                    elif getattr(col, "operation", None) == "months_between":
                        fields_map[col_name] = MockStructField(col_name, DoubleType())
                    elif getattr(col, "operation", None) in [
                        "hour",
                        "minute",
                        "second",
                        "day",
                        "dayofmonth",
                        "month",
                        "year",
                        "quarter",
                        "dayofweek",
                        "dayofyear",
                        "weekofyear",
                    ]:
                        fields_map[col_name] = MockStructField(col_name, IntegerType())
                    else:
                        fields_map[col_name] = MockStructField(col_name, StringType())
                elif hasattr(col, "value") and hasattr(col, "column_type"):
                    # For MockLiteral objects - literals are never nullable
                    # Create a new instance of the data type with nullable=False
                    from ..spark_types import (
                        BooleanType,
                        IntegerType,
                        LongType,
                        DoubleType,
                        StringType,
                    )

                    col_type = col.column_type
                    data_type2: MockDataType
                    if isinstance(col_type, BooleanType):
                        data_type2 = BooleanType(nullable=False)
                    elif isinstance(col_type, IntegerType):
                        data_type2 = IntegerType(nullable=False)
                    elif isinstance(col_type, LongType):
                        data_type2 = LongType(nullable=False)
                    elif isinstance(col_type, DoubleType):
                        data_type2 = DoubleType(nullable=False)
                    elif isinstance(col_type, StringType):
                        data_type2 = StringType(nullable=False)
                    else:
                        # For other types, create a new instance with nullable=False
                        data_type2 = col_type.__class__(nullable=False)

                    fields_map[col_name] = MockStructField(
                        col_name, data_type2, nullable=False
                    )
                else:
                    # fallback literal inference
                    if isinstance(col, (int, float)):
                        if isinstance(col, float):
                            fields_map[col_name] = MockStructField(
                                col_name, DoubleType()
                            )
                        else:
                            fields_map[col_name] = MockStructField(col_name, LongType())
                    else:
                        fields_map[col_name] = MockStructField(col_name, StringType())
            elif op_name == "join":
                other_df, on, how = op_val
                # Add fields from the other DataFrame to the schema
                for field in other_df.schema.fields:
                    # Avoid duplicate field names
                    if field.name not in fields_map:
                        fields_map[field.name] = field
                    else:
                        # Handle field name conflicts by prefixing
                        new_field = MockStructField(
                            f"right_{field.name}", field.dataType, field.nullable
                        )
                        fields_map[f"right_{field.name}"] = new_field

        return MockStructType(list(fields_map.values()))

    def select(
        self, *columns: Union[str, MockColumn, MockLiteral, Any]
    ) -> "MockDataFrame":
        """Select columns from the DataFrame.

        Args:
            *columns: Column names, MockColumn objects, or expressions to select.
                     Use "*" to select all columns.

        Returns:
            New MockDataFrame with selected columns.

        Raises:
            AnalysisException: If specified columns don't exist.

        Example:
            >>> df.select("name", "age")
            >>> df.select("*")
            >>> df.select(F.col("name"), F.col("age") * 2)
        """

        if not columns:
            return self

        # Validate column names eagerly (even in lazy mode) to match PySpark behavior
        # But skip validation if there are pending join operations (columns might come from other DF)
        has_pending_joins = any(op[0] == "join" for op in self._operations_queue)

        if not has_pending_joins:
            for col in columns:
                if isinstance(col, str) and col != "*":
                    # Check if column exists
                    if col not in self.columns:
                        from ..core.exceptions.operation import (
                            MockSparkColumnNotFoundError,
                        )

                        raise MockSparkColumnNotFoundError(col, self.columns)
                elif isinstance(col, MockColumn):
                    if hasattr(col, "operation"):
                        # Complex expression - validate column references
                        self._validate_expression_columns(col, "select")
                    else:
                        # Simple column reference - validate
                        if col.name not in self.columns:
                            from ..core.exceptions.operation import (
                                MockSparkColumnNotFoundError,
                            )

                            raise MockSparkColumnNotFoundError(col.name, self.columns)
                elif isinstance(col, MockColumnOperation):
                    # Complex expression - validate column references
                    # Skip validation for function operations that will be evaluated later
                    if not (
                        hasattr(col, "operation")
                        and col.operation in ["months_between", "datediff"]
                    ):
                        self._validate_expression_columns(col, "select")

            # Always use lazy evaluation
            return self._queue_op("select", columns)

        # If there are pending joins, skip validation and go directly to lazy evaluation
        return self._queue_op("select", columns)

    def filter(
        self, condition: Union[MockColumnOperation, MockColumn, "MockLiteral"]
    ) -> "MockDataFrame":
        """Filter rows based on condition."""
        # Pre-validation: validate filter expression
        self._validate_filter_expression(condition, "filter")

        return self._queue_op("filter", condition)

    def withColumn(
        self,
        col_name: str,
        col: Union[MockColumn, MockColumnOperation, MockLiteral, Any],
    ) -> "MockDataFrame":
        """Add or replace column."""
        # Validate column references in expressions
        if isinstance(col, MockColumn) and not hasattr(col, "operation"):
            # Simple column reference - validate
            self._validate_column_exists(col.name, "withColumn")
        elif isinstance(col, MockColumnOperation):
            # Complex expression - validate column references
            self._validate_expression_columns(col, "withColumn")
        # For MockLiteral and other cases, skip validation

        return self._queue_op("withColumn", (col_name, col))

    def orderBy(self, *columns: Union[str, MockColumn]) -> "MockDataFrame":
        """Order by columns."""
        return self._queue_op("orderBy", columns)

    def limit(self, n: int) -> "MockDataFrame":
        """Limit number of rows."""
        return self._queue_op("limit", n)

    def groupBy(self, *columns: Union[str, MockColumn]) -> "MockGroupedData":
        """Group DataFrame by columns for aggregation operations.

        Args:
            *columns: Column names or MockColumn objects to group by.

        Returns:
            MockGroupedData for aggregation operations.

        Example:
            >>> df.groupBy("category").count()
            >>> df.groupBy("dept", "year").avg("salary")
        """
        col_names = []
        for col in columns:
            if isinstance(col, MockColumn):
                col_names.append(col.name)
            else:
                col_names.append(col)

        # Validate that all columns exist
        for col_name in col_names:
            if col_name not in [field.name for field in self.schema.fields]:
                available_columns = [field.name for field in self.schema.fields]
                raise MockSparkColumnNotFoundError(col_name, available_columns)

        return MockGroupedData(self, col_names)

    def rollup(self, *columns: Union[str, MockColumn]) -> "MockRollupGroupedData":
        """Create rollup grouped data for hierarchical grouping.

        Args:
            *columns: Columns to rollup.

        Returns:
            MockRollupGroupedData for hierarchical grouping.

        Example:
            >>> df.rollup("country", "state").sum("sales")
        """
        col_names = []
        for col in columns:
            if isinstance(col, MockColumn):
                col_names.append(col.name)
            else:
                col_names.append(col)

        # Validate that all columns exist
        for col_name in col_names:
            if col_name not in [field.name for field in self.schema.fields]:
                available_columns = [field.name for field in self.schema.fields]
                raise MockSparkColumnNotFoundError(col_name, available_columns)

        return MockRollupGroupedData(self, col_names)

    def cube(self, *columns: Union[str, MockColumn]) -> "MockCubeGroupedData":
        """Create cube grouped data for multi-dimensional grouping.

        Args:
            *columns: Columns to cube.

        Returns:
            MockCubeGroupedData for multi-dimensional grouping.

        Example:
            >>> df.cube("year", "month").sum("revenue")
        """
        col_names = []
        for col in columns:
            if isinstance(col, MockColumn):
                col_names.append(col.name)
            else:
                col_names.append(col)

        # Validate that all columns exist
        for col_name in col_names:
            if col_name not in [field.name for field in self.schema.fields]:
                available_columns = [field.name for field in self.schema.fields]
                raise MockSparkColumnNotFoundError(col_name, available_columns)

        return MockCubeGroupedData(self, col_names)

    def agg(
        self, *exprs: Union[str, MockColumn, MockColumnOperation]
    ) -> "MockDataFrame":
        """Aggregate DataFrame without grouping (global aggregation).

        Args:
            *exprs: Aggregation expressions or column names.

        Returns:
            DataFrame with aggregated results.

        Example:
            >>> df.agg(F.max("age"), F.min("age"))
            >>> df.agg({"age": "max", "salary": "avg"})
        """
        # Create a grouped data object with empty group columns for global aggregation
        grouped = MockGroupedData(self, [])
        return grouped.agg(*exprs)

    def take(self, n: int) -> List[MockRow]:
        """Take first n rows as list of Row objects."""
        if self._operations_queue:
            materialized = self._materialize_if_lazy()
            return self._get_collection_handler().take(
                materialized.data, materialized.schema, n
            )
        return self._get_collection_handler().take(self.data, self.schema, n)

    @property
    def dtypes(self) -> List[Tuple[str, str]]:
        """Get column names and their data types."""
        return [(field.name, field.dataType.typeName()) for field in self.schema.fields]

    def union(self, other: "MockDataFrame") -> "MockDataFrame":
        """Union with another DataFrame."""
        return self._queue_op("union", other)

    def unionByName(
        self, other: "MockDataFrame", allowMissingColumns: bool = False
    ) -> "MockDataFrame":
        """Union with another DataFrame by column names.

        Args:
            other: Another DataFrame to union with.
            allowMissingColumns: If True, allows missing columns (fills with null).

        Returns:
            New MockDataFrame with combined data.
        """
        # Get column names from both DataFrames
        self_cols = set(field.name for field in self.schema.fields)
        other_cols = set(field.name for field in other.schema.fields)

        # Check for missing columns
        missing_in_other = self_cols - other_cols
        missing_in_self = other_cols - self_cols

        if not allowMissingColumns and (missing_in_other or missing_in_self):
            raise AnalysisException(
                f"Union by name failed: missing columns in one of the DataFrames. "
                f"Missing in other: {missing_in_other}, Missing in self: {missing_in_self}"
            )

        # Get all unique column names in order
        all_cols = list(self_cols.union(other_cols))

        # Create combined data with all columns
        combined_data = []

        # Add rows from self DataFrame
        for row in self.data:
            new_row = {}
            for col in all_cols:
                if col in row:
                    new_row[col] = row[col]
                else:
                    new_row[col] = None  # Missing column filled with null
            combined_data.append(new_row)

        # Add rows from other DataFrame
        for row in other.data:
            new_row = {}
            for col in all_cols:
                if col in row:
                    new_row[col] = row[col]
                else:
                    new_row[col] = None  # Missing column filled with null
            combined_data.append(new_row)

        # Create new schema with all columns
        from ..spark_types import (
            MockStructType,
            MockStructField,
            MockDataType,
        )

        new_fields = []
        for col in all_cols:
            # Try to get the data type from the original schema, default to StringType
            field_type: MockDataType = StringType()
            for field in self.schema.fields:
                if field.name == col:
                    field_type = field.dataType
                    break
            # If not found in self schema, check other schema
            if isinstance(field_type, StringType):
                for field in other.schema.fields:
                    if field.name == col:
                        field_type = field.dataType
                        break
            new_fields.append(MockStructField(col, field_type))

        new_schema = MockStructType(new_fields)
        return MockDataFrame(combined_data, new_schema, self.storage)

    def intersect(self, other: "MockDataFrame") -> "MockDataFrame":
        """Intersect with another DataFrame.

        Args:
            other: Another DataFrame to intersect with.

        Returns:
            New MockDataFrame with common rows.
        """
        # Convert rows to tuples for comparison
        self_rows = [
            tuple(row.get(field.name) for field in self.schema.fields)
            for row in self.data
        ]
        other_rows = [
            tuple(row.get(field.name) for field in other.schema.fields)
            for row in other.data
        ]

        # Find common rows
        self_row_set = set(self_rows)
        other_row_set = set(other_rows)
        common_rows = self_row_set.intersection(other_row_set)

        # Convert back to dictionaries
        result_data = []
        for row_tuple in common_rows:
            row_dict = {}
            for i, field in enumerate(self.schema.fields):
                row_dict[field.name] = row_tuple[i]
            result_data.append(row_dict)

        return MockDataFrame(result_data, self.schema, self.storage)

    def exceptAll(self, other: "MockDataFrame") -> "MockDataFrame":
        """Except all with another DataFrame (set difference with duplicates).

        Args:
            other: Another DataFrame to except from this one.

        Returns:
            New MockDataFrame with rows from self not in other, preserving duplicates.
        """
        # Convert rows to tuples for comparison
        self_rows = [
            tuple(row.get(field.name) for field in self.schema.fields)
            for row in self.data
        ]
        other_rows = [
            tuple(row.get(field.name) for field in other.schema.fields)
            for row in other.data
        ]

        # Count occurrences in other DataFrame
        from typing import Dict, List, Tuple

        other_row_counts: Dict[Tuple[Any, ...], int] = {}
        for row_tuple in other_rows:
            other_row_counts[row_tuple] = other_row_counts.get(row_tuple, 0) + 1

        # Count occurrences in self DataFrame
        self_row_counts: Dict[Tuple[Any, ...], int] = {}
        for row_tuple in self_rows:
            self_row_counts[row_tuple] = self_row_counts.get(row_tuple, 0) + 1

        # Calculate the difference preserving duplicates
        result_rows: List[Tuple[Any, ...]] = []
        for row_tuple in self_rows:
            # Count how many times this row appears in other
            other_count = other_row_counts.get(row_tuple, 0)
            # Count how many times this row appears in self so far
            self_count_so_far = result_rows.count(row_tuple)
            # If we haven't exceeded the difference, include this row
            if self_count_so_far < (self_row_counts[row_tuple] - other_count):
                result_rows.append(row_tuple)

        # Convert back to dictionaries
        result_data = []
        for row_tuple in result_rows:
            row_dict = {}
            for i, field in enumerate(self.schema.fields):
                row_dict[field.name] = row_tuple[i]
            result_data.append(row_dict)

        return MockDataFrame(result_data, self.schema, self.storage)

    def crossJoin(self, other: "MockDataFrame") -> "MockDataFrame":
        """Cross join (Cartesian product) with another DataFrame.

        Args:
            other: Another DataFrame to cross join with.

        Returns:
            New MockDataFrame with Cartesian product of rows.
        """
        # Create new schema combining both DataFrames
        from ..spark_types import MockStructType, MockStructField

        # Combine field names, handling duplicates
        new_fields = []
        field_names = set()

        # Add fields from self DataFrame
        for field in self.schema.fields:
            new_fields.append(field)
            field_names.add(field.name)

        # Add fields from other DataFrame, handling name conflicts
        for field in other.schema.fields:
            if field.name in field_names:
                # Create a unique name for the conflict
                new_name = f"{field.name}_right"
                counter = 1
                while new_name in field_names:
                    new_name = f"{field.name}_right_{counter}"
                    counter += 1
                new_fields.append(MockStructField(new_name, field.dataType))
                field_names.add(new_name)
            else:
                new_fields.append(field)
                field_names.add(field.name)

        new_schema = MockStructType(new_fields)

        # Create Cartesian product
        result_data = []
        for left_row in self.data:
            for right_row in other.data:
                new_row = {}

                # Add fields from left DataFrame
                for field in self.schema.fields:
                    new_row[field.name] = left_row.get(field.name)

                # Add fields from right DataFrame, handling name conflicts
                for field in other.schema.fields:
                    if field.name in [f.name for f in self.schema.fields]:
                        # Find the renamed field
                        from typing import Optional

                        renamed: Optional[str] = None
                        for new_field in new_fields:
                            if new_field.name.endswith(
                                "_right"
                            ) and new_field.name.startswith(field.name):
                                renamed = new_field.name
                                break
                    if renamed is not None:
                        new_row[renamed] = right_row.get(field.name)
                    else:
                        new_row[field.name] = right_row.get(field.name)

                result_data.append(new_row)

        return MockDataFrame(result_data, new_schema, self.storage)

    def join(
        self,
        other: "MockDataFrame",
        on: Union[str, List[str], "MockColumnOperation"],
        how: str = "inner",
    ) -> "MockDataFrame":
        """Join with another DataFrame."""
        if isinstance(on, str):
            on = [on]

        return self._queue_op("join", (other, on, how))

    def distinct(self) -> "MockDataFrame":
        """Return distinct rows."""
        seen = set()
        distinct_data = []
        for row in self.data:
            row_tuple = tuple(sorted(row.items()))
            if row_tuple not in seen:
                seen.add(row_tuple)
                distinct_data.append(row)
        return MockDataFrame(distinct_data, self.schema, self.storage)

    def dropDuplicates(self, subset: Optional[List[str]] = None) -> "MockDataFrame":
        """Drop duplicate rows."""
        if subset is None:
            return self.distinct()

        seen = set()
        distinct_data = []
        for row in self.data:
            row_tuple = tuple(sorted((k, v) for k, v in row.items() if k in subset))
            if row_tuple not in seen:
                seen.add(row_tuple)
                distinct_data.append(row)
        return MockDataFrame(distinct_data, self.schema, self.storage)

    def drop(self, *cols: str) -> "MockDataFrame":
        """Drop columns."""
        new_data = []
        for row in self.data:
            new_row = {k: v for k, v in row.items() if k not in cols}
            new_data.append(new_row)

        # Update schema
        new_fields = [field for field in self.schema.fields if field.name not in cols]
        new_schema = MockStructType(new_fields)
        return MockDataFrame(new_data, new_schema, self.storage)

    def withColumnRenamed(self, existing: str, new: str) -> "MockDataFrame":
        """Rename a column."""
        new_data = []
        for row in self.data:
            new_row = {}
            for k, v in row.items():
                if k == existing:
                    new_row[new] = v
                else:
                    new_row[k] = v
            new_data.append(new_row)

        # Update schema
        new_fields = []
        for field in self.schema.fields:
            if field.name == existing:
                new_fields.append(MockStructField(new, field.dataType))
            else:
                new_fields.append(field)
        new_schema = MockStructType(new_fields)
        return MockDataFrame(new_data, new_schema, self.storage)

    def dropna(
        self,
        how: str = "any",
        thresh: Optional[int] = None,
        subset: Optional[List[str]] = None,
    ) -> "MockDataFrame":
        """Drop rows with null values."""
        filtered_data = []
        for row in self.data:
            if subset:
                # Check only specified columns
                null_count = sum(1 for col in subset if row.get(col) is None)
            else:
                # Check all columns
                null_count = sum(1 for v in row.values() if v is None)

            if how == "any" and null_count == 0:
                filtered_data.append(row)
            elif how == "all" and null_count < len(row):
                filtered_data.append(row)
            elif thresh is not None and null_count <= len(row) - thresh:
                filtered_data.append(row)

        return MockDataFrame(filtered_data, self.schema, self.storage)

    def fillna(self, value: Union[Any, Dict[str, Any]]) -> "MockDataFrame":
        """Fill null values."""
        new_data = []
        for row in self.data:
            new_row = row.copy()
            if isinstance(value, dict):
                for col, fill_value in value.items():
                    if new_row.get(col) is None:
                        new_row[col] = fill_value
            else:
                for col in new_row:
                    if new_row[col] is None:
                        new_row[col] = value
            new_data.append(new_row)

        return MockDataFrame(new_data, self.schema, self.storage)

    def explain(self) -> None:
        """Explain execution plan."""
        print("MockDataFrame Execution Plan:")
        print("  MockDataFrame")
        print("    MockDataSource")

    @property
    def rdd(self) -> "MockRDD":
        """Get RDD representation."""
        return MockRDD(self.data)

    def registerTempTable(self, name: str) -> None:
        """Register as temporary table."""
        # Store in storage
        # Create table with schema first
        self.storage.create_table("default", name, self.schema.fields)
        # Then insert data
        dict_data = [
            row.asDict() if hasattr(row, "asDict") else row for row in self.data
        ]
        self.storage.insert_data("default", name, dict_data)

    def createTempView(self, name: str) -> None:
        """Create temporary view."""
        self.registerTempTable(name)

    def _apply_condition(
        self, data: List[Dict[str, Any]], condition: MockColumnOperation
    ) -> List[Dict[str, Any]]:
        """Apply condition to filter data."""
        filtered_data = []

        for row in data:
            if self._evaluate_condition(row, condition):
                filtered_data.append(row)

        return filtered_data

    def _evaluate_condition(
        self, row: Dict[str, Any], condition: Union[MockColumnOperation, MockColumn]
    ) -> bool:
        """Evaluate condition for a single row.

        Delegates to ExpressionEvaluator for consistency.
        """
        return self._expression_evaluator.evaluate_condition(row, condition)

    def _evaluate_column_expression(
        self, row: Dict[str, Any], column_expression: Any
    ) -> Any:
        """Evaluate a column expression for a single row."""
        return self._expression_evaluator.evaluate_expression(row, column_expression)

    def _evaluate_window_functions(
        self, data: List[Dict[str, Any]], window_functions: List[Tuple[Any, ...]]
    ) -> List[Dict[str, Any]]:
        """Evaluate window functions across all rows."""
        return self._get_window_handler().evaluate_window_functions(
            data, window_functions
        )

    def _evaluate_lag_lead(
        self, data: List[Dict[str, Any]], window_func: Any, col_name: str, is_lead: bool
    ) -> None:
        """Evaluate lag or lead window function."""
        return self._get_window_handler()._evaluate_lag_lead(
            data, window_func, col_name, is_lead
        )

    def _apply_ordering_to_indices(
        self, data: List[Dict[str, Any]], indices: List[int], order_by_cols: List[Any]
    ) -> List[int]:
        """Apply ordering to a list of indices based on order by columns."""
        return self._get_window_handler()._apply_ordering_to_indices(
            data, indices, order_by_cols
        )

    def _apply_lag_lead_to_partition(
        self,
        data: List[Dict[str, Any]],
        indices: List[int],
        source_col: str,
        target_col: str,
        offset: int,
        default_value: Any,
        is_lead: bool,
    ) -> None:
        """Apply lag or lead to a specific partition."""
        return self._get_window_handler()._apply_lag_lead_to_partition(
            data, indices, source_col, target_col, offset, default_value, is_lead
        )

    def _evaluate_rank_functions(
        self, data: List[Dict[str, Any]], window_func: Any, col_name: str
    ) -> None:
        """Evaluate rank or dense_rank window function."""
        return self._get_window_handler()._evaluate_rank_functions(
            data, window_func, col_name
        )

    def _apply_rank_to_partition(
        self,
        data: List[Dict[str, Any]],
        indices: List[int],
        order_by_cols: List[Any],
        col_name: str,
        is_dense: bool,
    ) -> None:
        """Apply rank or dense_rank to a specific partition."""
        return self._get_window_handler()._apply_rank_to_partition(
            data, indices, order_by_cols, col_name, is_dense
        )

    def _evaluate_aggregate_window_functions(
        self, data: List[Dict[str, Any]], window_func: Any, col_name: str
    ) -> None:
        """Evaluate aggregate window functions like avg, sum, count, etc."""
        return self._get_window_handler()._evaluate_aggregate_window_functions(
            data, window_func, col_name
        )

    def _apply_aggregate_to_partition(
        self,
        data: List[Dict[str, Any]],
        indices: List[int],
        window_func: Any,
        col_name: str,
    ) -> None:
        """Apply aggregate function to a specific partition."""
        return self._get_window_handler()._apply_aggregate_to_partition(
            data, indices, window_func, col_name
        )

    def _evaluate_case_when(self, row: Dict[str, Any], case_when_obj: Any) -> Any:
        """Evaluate CASE WHEN expression for a row."""
        # Use the MockCaseWhen's own evaluate method
        if hasattr(case_when_obj, "evaluate"):
            return case_when_obj.evaluate(row)

        # Fallback to manual evaluation
        for condition, value in case_when_obj.conditions:
            if self._evaluate_case_when_condition(row, condition):
                return self._expression_evaluator.evaluate_expression(row, value)

        if case_when_obj.else_value is not None:
            return self._expression_evaluator.evaluate_expression(
                row, case_when_obj.else_value
            )

        return None

    def _evaluate_case_when_condition(
        self, row: Dict[str, Any], condition: Any
    ) -> bool:
        """Evaluate a CASE WHEN condition for a row."""
        if hasattr(condition, "operation") and hasattr(condition, "column"):
            # Handle MockColumnOperation conditions
            if condition.operation == ">":
                col_value = (
                    row.get(condition.column.name)
                    if hasattr(condition.column, "name")
                    else row.get(str(condition.column))
                )
                return col_value is not None and col_value > condition.value
            elif condition.operation == ">=":
                col_value = (
                    row.get(condition.column.name)
                    if hasattr(condition.column, "name")
                    else row.get(str(condition.column))
                )
                return col_value is not None and col_value >= condition.value
            elif condition.operation == "<":
                col_value = (
                    row.get(condition.column.name)
                    if hasattr(condition.column, "name")
                    else row.get(str(condition.column))
                )
                return col_value is not None and col_value < condition.value
            elif condition.operation == "<=":
                col_value = (
                    row.get(condition.column.name)
                    if hasattr(condition.column, "name")
                    else row.get(str(condition.column))
                )
                return col_value is not None and col_value <= condition.value
            elif condition.operation == "==":
                col_value = (
                    row.get(condition.column.name)
                    if hasattr(condition.column, "name")
                    else row.get(str(condition.column))
                )
                return bool(col_value == condition.value)
            elif condition.operation == "!=":
                col_value = (
                    row.get(condition.column.name)
                    if hasattr(condition.column, "name")
                    else row.get(str(condition.column))
                )
                return bool(col_value != condition.value)
        return False

    def createOrReplaceTempView(self, name: str) -> None:
        """Create or replace a temporary view of this DataFrame."""
        # Store the DataFrame as a temporary view in the storage manager
        self.storage.create_temp_view(name, self)

    def createGlobalTempView(self, name: str) -> None:
        """Create a global temporary view (session-independent)."""
        # Use the global_temp schema to mimic Spark's behavior
        if not self.storage.schema_exists("global_temp"):
            self.storage.create_schema("global_temp")
        # Create/overwrite the table in global_temp
        data = self.data
        schema_obj = self.schema
        self.storage.create_table("global_temp", name, schema_obj.fields)
        self.storage.insert_data("global_temp", name, [row for row in data])

    def createOrReplaceGlobalTempView(self, name: str) -> None:
        """Create or replace a global temporary view (all PySpark versions).

        Unlike createGlobalTempView, this method does not raise an error if the view already exists.

        Args:
            name: Name of the global temp view

        Example:
            >>> df.createOrReplaceGlobalTempView("my_global_view")
            >>> spark.sql("SELECT * FROM global_temp.my_global_view")
        """
        # Use the global_temp schema to mimic Spark's behavior
        if not self.storage.schema_exists("global_temp"):
            self.storage.create_schema("global_temp")

        # Check if table exists and drop it first
        if self.storage.table_exists("global_temp", name):
            self.storage.drop_table("global_temp", name)

        # Create the table in global_temp
        data = self.data
        schema_obj = self.schema
        self.storage.create_table("global_temp", name, schema_obj.fields)
        self.storage.insert_data("global_temp", name, [row for row in data])

    def colRegex(self, colName: str) -> MockColumn:
        """Select columns matching a regex pattern (all PySpark versions).

        The regex pattern should be wrapped in backticks: `pattern`

        Args:
            colName: Regex pattern wrapped in backticks, e.g. "`.*id`"

        Returns:
            Column expression that can be used in select()

        Example:
            >>> df = spark.createDataFrame([{"user_id": 1, "post_id": 2, "name": "Alice"}])
            >>> df.select(df.colRegex("`.*id`")).show()  # Selects user_id and post_id
        """
        import re
        from mock_spark.functions.base import MockColumn

        # Extract pattern from backticks
        pattern = colName.strip()
        if pattern.startswith("`") and pattern.endswith("`"):
            pattern = pattern[1:-1]

        # Find matching columns (preserve order from DataFrame)
        matching_cols = [col for col in self.columns if re.match(pattern, col)]

        if not matching_cols:
            # Return empty column if no matches (PySpark behavior)
            return MockColumn("")

        # For simplicity, we return a special marker that select() will handle
        # In real implementation, this would return a Column that expands to multiple columns
        result = MockColumn(matching_cols[0])
        # Store the full list of matching columns as metadata
        result._regex_matches = matching_cols  # type: ignore
        return result

    def replace(
        self,
        to_replace: Union[int, float, str, List[Any], Dict[Any, Any]],
        value: Optional[Union[int, float, str, List[Any]]] = None,
        subset: Optional[List[str]] = None,
    ) -> "MockDataFrame":
        """Replace values in DataFrame (all PySpark versions).

        Args:
            to_replace: Value(s) to replace - can be scalar, list, or dict
            value: Replacement value(s) - required if to_replace is not a dict
            subset: Optional list of columns to limit replacement to

        Returns:
            New DataFrame with replaced values

        Examples:
            >>> # Replace with dict mapping
            >>> df.replace({'A': 'X', 'B': 'Y'})

            >>> # Replace list of values with single value
            >>> df.replace([1, 2], 99, subset=['col1'])

            >>> # Replace single value
            >>> df.replace(1, 99)
        """
        from copy import deepcopy

        # Determine columns to apply replacement to
        target_columns = subset if subset else self.columns

        # Build replacement map
        replace_map: Dict[Any, Any] = {}
        if isinstance(to_replace, dict):
            replace_map = to_replace
        elif isinstance(to_replace, list):
            if value is None:
                raise ValueError("value cannot be None when to_replace is a list")
            # If value is also a list, create mapping
            if isinstance(value, list):
                if len(to_replace) != len(value):
                    raise ValueError("to_replace and value lists must have same length")
                replace_map = dict(zip(to_replace, value))
            else:
                # All values in list map to single value
                replace_map = {v: value for v in to_replace}
        else:
            # Scalar to_replace
            if value is None:
                raise ValueError("value cannot be None when to_replace is a scalar")
            replace_map = {to_replace: value}

        # Apply replacements
        new_data = []
        for row in self.data:
            new_row = deepcopy(row)
            for col in target_columns:
                if col in new_row and new_row[col] in replace_map:
                    new_row[col] = replace_map[new_row[col]]
            new_data.append(new_row)

        return MockDataFrame(new_data, self.schema, self.storage)

    def selectExpr(self, *exprs: str) -> "MockDataFrame":
        """Select columns or expressions using SQL-like syntax.

        Note: Simplified - treats exprs as column names for mock compatibility.
        """
        # For mock purposes, support bare column names, '*' and simple aliases "col AS alias" or "col alias"
        from typing import Union, List

        columns: List[Union[str, MockColumn]] = []
        for expr in exprs:
            text = expr.strip()
            if text == "*":
                columns.extend([f.name for f in self.schema.fields])
                continue
            lower = text.lower()
            alias = None
            colname = text
            if " as " in lower:
                # split on AS preserving original case of alias
                parts = text.split()
                # find 'as' index case-insensitively
                try:
                    idx = next(i for i, p in enumerate(parts) if p.lower() == "as")
                    colname = " ".join(parts[:idx])
                    alias = " ".join(parts[idx + 1 :])
                except StopIteration:
                    colname = text
            else:
                # support "col alias" form
                parts = text.split()
                if len(parts) == 2:
                    colname, alias = parts[0], parts[1]

            if alias:
                columns.append(MockColumn(colname).alias(alias))
            else:
                columns.append(colname)
        return self.select(*columns)

    def head(self, n: int = 1) -> Union[MockRow, List[MockRow], None]:
        """Return first n rows."""
        if self._operations_queue:
            materialized = self._materialize_if_lazy()
            return self._get_collection_handler().head(
                materialized.data, materialized.schema, n
            )
        return self._get_collection_handler().head(self.data, self.schema, n)

    def tail(self, n: int = 1) -> Union[MockRow, List[MockRow], None]:
        """Return last n rows."""
        if self._operations_queue:
            materialized = self._materialize_if_lazy()
            return self._get_collection_handler().tail(
                materialized.data, materialized.schema, n
            )
        return self._get_collection_handler().tail(self.data, self.schema, n)

    def toJSON(self) -> "MockDataFrame":
        """Return a single-column DataFrame of JSON strings."""
        import json

        json_rows = [{"value": json.dumps(row)} for row in self.data]
        from ..spark_types import MockStructType, MockStructField

        schema = MockStructType([MockStructField("value", StringType())])
        return MockDataFrame(json_rows, schema, self.storage)

    @property
    def isStreaming(self) -> bool:
        """Whether this DataFrame is streaming (always False in mock)."""
        return False

    def repartition(self, numPartitions: int, *cols: Any) -> "MockDataFrame":
        """Repartition DataFrame (no-op in mock; returns self)."""
        return self

    def coalesce(self, numPartitions: int) -> "MockDataFrame":
        """Coalesce partitions (no-op in mock; returns self)."""
        return self

    def checkpoint(self, eager: bool = False) -> "MockDataFrame":
        """Checkpoint the DataFrame (no-op in mock; returns self)."""
        return self

    def sample(
        self, fraction: float, seed: Optional[int] = None, withReplacement: bool = False
    ) -> "MockDataFrame":
        """Sample rows from DataFrame.

        Args:
            fraction: Fraction of rows to sample (0.0 to 1.0).
            seed: Random seed for reproducible sampling.
            withReplacement: Whether to sample with replacement.

        Returns:
            New MockDataFrame with sampled rows.
        """
        import random

        if not withReplacement and not (0.0 <= fraction <= 1.0):
            raise IllegalArgumentException(
                f"Fraction must be between 0.0 and 1.0 when without replacement, got {fraction}"
            )
        if withReplacement and fraction < 0.0:
            raise IllegalArgumentException(
                f"Fraction must be non-negative when with replacement, got {fraction}"
            )

        if seed is not None:
            random.seed(seed)

        if fraction == 0.0:
            return MockDataFrame([], self.schema, self.storage)
        elif fraction == 1.0:
            return MockDataFrame(self.data.copy(), self.schema, self.storage)

        # Calculate number of rows to sample
        total_rows = len(self.data)
        num_rows = int(total_rows * fraction)

        if withReplacement:
            # Sample with replacement
            sampled_indices = [
                random.randint(0, total_rows - 1) for _ in range(num_rows)
            ]
            sampled_data = [self.data[i] for i in sampled_indices]
        else:
            # Sample without replacement
            if num_rows > total_rows:
                num_rows = total_rows
            sampled_indices = random.sample(range(total_rows), num_rows)
            sampled_data = [self.data[i] for i in sampled_indices]

        return MockDataFrame(sampled_data, self.schema, self.storage)

    def randomSplit(
        self, weights: List[float], seed: Optional[int] = None
    ) -> List["MockDataFrame"]:
        """Randomly split DataFrame into multiple DataFrames.

        Args:
            weights: List of weights for each split (must sum to 1.0).
            seed: Random seed for reproducible splitting.

        Returns:
            List of MockDataFrames split according to weights.
        """
        import random

        if not weights or len(weights) < 2:
            raise IllegalArgumentException("Weights must have at least 2 elements")

        if abs(sum(weights) - 1.0) > 1e-6:
            raise IllegalArgumentException(
                f"Weights must sum to 1.0, got {sum(weights)}"
            )

        if any(w < 0 for w in weights):
            raise IllegalArgumentException("All weights must be non-negative")

        if seed is not None:
            random.seed(seed)

        # Create a list of (index, random_value) pairs
        indexed_data = [(i, random.random()) for i in range(len(self.data))]

        # Sort by random value to ensure random distribution
        indexed_data.sort(key=lambda x: x[1])

        # Calculate split points
        cumulative_weight = 0.0
        split_points: List[int] = []
        for weight in weights:
            cumulative_weight += weight
            split_points.append(int(len(self.data) * cumulative_weight))

        # Create splits
        splits = []
        start_idx = 0

        for end_idx in split_points:
            split_indices = [idx for idx, _ in indexed_data[start_idx:end_idx]]
            split_data = [self.data[idx] for idx in split_indices]
            splits.append(MockDataFrame(split_data, self.schema, self.storage))
            start_idx = end_idx

        return splits

    def describe(self, *cols: str) -> "MockDataFrame":
        """Compute basic statistics for numeric columns.

        Args:
            *cols: Column names to describe. If empty, describes all numeric columns.

        Returns:
            MockDataFrame with statistics (count, mean, stddev, min, max).
        """
        import statistics

        # Determine which columns to describe
        if not cols:
            # Describe all numeric columns
            numeric_cols = []
            for field in self.schema.fields:
                field_type = field.dataType.typeName()
                if field_type in [
                    "long",
                    "int",
                    "integer",
                    "bigint",
                    "double",
                    "float",
                ]:
                    numeric_cols.append(field.name)
        else:
            numeric_cols = list(cols)
            # Validate that columns exist
            available_cols = [field.name for field in self.schema.fields]
            for col in numeric_cols:
                if col not in available_cols:
                    raise ColumnNotFoundException(col)

        if not numeric_cols:
            # No numeric columns found
            return MockDataFrame([], self.schema, self.storage)

        # Calculate statistics for each column
        result_data = []

        for col in numeric_cols:
            # Extract values for this column
            values = []
            for row in self.data:
                value = row.get(col)
                if value is not None and isinstance(value, (int, float)):
                    values.append(value)

            if not values:
                # No valid numeric values
                stats_row = {
                    "summary": col,
                    "count": "0",
                    "mean": "NaN",
                    "stddev": "NaN",
                    "min": "NaN",
                    "max": "NaN",
                }
            else:
                stats_row = {
                    "summary": col,
                    "count": str(len(values)),
                    "mean": str(round(statistics.mean(values), 4)),
                    "stddev": str(
                        round(statistics.stdev(values) if len(values) > 1 else 0.0, 4)
                    ),
                    "min": str(min(values)),
                    "max": str(max(values)),
                }

            result_data.append(stats_row)

        # Create result schema
        from ..spark_types import MockStructType, MockStructField

        result_schema = MockStructType(
            [
                MockStructField("summary", StringType()),
                MockStructField("count", StringType()),
                MockStructField("mean", StringType()),
                MockStructField("stddev", StringType()),
                MockStructField("min", StringType()),
                MockStructField("max", StringType()),
            ]
        )

        return MockDataFrame(result_data, result_schema, self.storage)

    def summary(self, *stats: str) -> "MockDataFrame":
        """Compute extended statistics for numeric columns.

        Args:
            *stats: Statistics to compute. Default: ["count", "mean", "stddev", "min", "25%", "50%", "75%", "max"].

        Returns:
            MockDataFrame with extended statistics.
        """
        import statistics

        # Default statistics if none provided
        if not stats:
            stats = ("count", "mean", "stddev", "min", "25%", "50%", "75%", "max")

        # Find numeric columns
        numeric_cols = []
        for field in self.schema.fields:
            field_type = field.dataType.typeName()
            if field_type in ["long", "int", "integer", "bigint", "double", "float"]:
                numeric_cols.append(field.name)

        if not numeric_cols:
            # No numeric columns found
            return MockDataFrame([], self.schema, self.storage)

        # Calculate statistics for each column
        result_data = []

        for col in numeric_cols:
            # Extract values for this column
            values = []
            for row in self.data:
                value = row.get(col)
                if value is not None and isinstance(value, (int, float)):
                    values.append(value)

            if not values:
                # No valid numeric values
                stats_row = {"summary": col}
                for stat in stats:
                    stats_row[stat] = "NaN"
            else:
                stats_row = {"summary": col}
                values_sorted = sorted(values)
                n = len(values)

                for stat in stats:
                    if stat == "count":
                        stats_row[stat] = str(n)
                    elif stat == "mean":
                        stats_row[stat] = str(round(statistics.mean(values), 4))
                    elif stat == "stddev":
                        stats_row[stat] = str(
                            round(statistics.stdev(values) if n > 1 else 0.0, 4)
                        )
                    elif stat == "min":
                        stats_row[stat] = str(values_sorted[0])
                    elif stat == "max":
                        stats_row[stat] = str(values_sorted[-1])
                    elif stat == "25%":
                        q1_idx = int(0.25 * (n - 1))
                        stats_row[stat] = str(values_sorted[q1_idx])
                    elif stat == "50%":
                        q2_idx = int(0.5 * (n - 1))
                        stats_row[stat] = str(values_sorted[q2_idx])
                    elif stat == "75%":
                        q3_idx = int(0.75 * (n - 1))
                        stats_row[stat] = str(values_sorted[q3_idx])
                    else:
                        stats_row[stat] = "NaN"

            result_data.append(stats_row)

        # Create result schema
        from ..spark_types import MockStructType, MockStructField

        result_fields = [MockStructField("summary", StringType())]
        for stat in stats:
            result_fields.append(MockStructField(stat, StringType()))

        result_schema = MockStructType(result_fields)
        return MockDataFrame(result_data, result_schema, self.storage)

    def mapPartitions(
        self, func: Any, preservesPartitioning: bool = False
    ) -> "MockDataFrame":
        """Apply a function to each partition of the DataFrame.

        For mock-spark, we treat the entire DataFrame as a single partition.
        The function receives an iterator of Row objects and should return
        an iterator of Row objects.

        Args:
            func: A function that takes an iterator of Rows and returns an iterator of Rows.
            preservesPartitioning: Whether the function preserves partitioning (unused in mock-spark).

        Returns:
            MockDataFrame: Result of applying the function.

        Example:
            >>> def add_index(iterator):
            ...     for i, row in enumerate(iterator):
            ...         yield MockRow(id=row.id, name=row.name, index=i)
            >>> df.mapPartitions(add_index)
        """
        # Materialize if lazy
        materialized = self._materialize_if_lazy()

        # Convert data to Row objects
        from ..spark_types import MockRow
        from typing import Iterator

        def row_iterator() -> Iterator[MockRow]:
            for row_dict in materialized.data:
                yield MockRow(row_dict)

        # Apply the function
        result_iterator = func(row_iterator())

        # Collect results
        result_data = []
        for result_row in result_iterator:
            if isinstance(result_row, MockRow):
                result_data.append(result_row.asDict())
            elif isinstance(result_row, dict):
                result_data.append(result_row)
            else:
                # Try to convert to dict
                result_data.append(dict(result_row))

        # Infer schema from result data
        from ..core.schema_inference import infer_schema_from_data

        result_schema = (
            infer_schema_from_data(result_data) if result_data else self.schema
        )

        return MockDataFrame(result_data, result_schema, self.storage)

    def mapInPandas(self, func: Any, schema: Any) -> "MockDataFrame":
        """Map an iterator of pandas DataFrames to another iterator of pandas DataFrames.

        For mock-spark, we treat the entire DataFrame as a single partition.
        The function receives an iterator yielding pandas DataFrames and should
        return an iterator yielding pandas DataFrames.

        Args:
            func: A function that takes an iterator of pandas DataFrames and returns
                  an iterator of pandas DataFrames.
            schema: The schema of the output DataFrame (StructType or DDL string).

        Returns:
            MockDataFrame: Result of applying the function.

        Example:
            >>> def multiply_by_two(iterator):
            ...     for pdf in iterator:
            ...         yield pdf * 2
            >>> df.mapInPandas(multiply_by_two, schema="value double")
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "pandas is required for mapInPandas. "
                "Install it with: pip install 'mock-spark[pandas]'"
            )

        # Materialize if lazy
        materialized = self._materialize_if_lazy()

        # Convert to pandas DataFrame
        input_pdf = pd.DataFrame(materialized.data)

        # Create an iterator that yields the pandas DataFrame
        from typing import Iterator

        def input_iterator() -> Iterator[Any]:
            yield input_pdf

        # Apply the function
        result_iterator = func(input_iterator())

        # Collect results from the iterator
        result_pdfs = []
        for result_pdf in result_iterator:
            if not isinstance(result_pdf, pd.DataFrame):
                raise TypeError(
                    f"Function must yield pandas DataFrames, got {type(result_pdf).__name__}"
                )
            result_pdfs.append(result_pdf)

        # Concatenate all results
        result_data: List[Dict[str, Any]] = []
        if result_pdfs:
            combined_pdf = pd.concat(result_pdfs, ignore_index=True)
            # Convert to records and ensure string keys
            result_data = [
                {str(k): v for k, v in row.items()}
                for row in combined_pdf.to_dict("records")
            ]

        # Parse schema
        from ..spark_types import MockStructType
        from ..core.schema_inference import infer_schema_from_data

        result_schema: MockStructType
        if isinstance(schema, str):
            # For DDL string, use schema inference from result data
            # (DDL parsing is complex, so we rely on inference for now)
            result_schema = (
                infer_schema_from_data(result_data) if result_data else self.schema
            )
        elif isinstance(schema, MockStructType):
            result_schema = schema
        else:
            # Try to infer schema from result data
            result_schema = (
                infer_schema_from_data(result_data) if result_data else self.schema
            )

        return MockDataFrame(result_data, result_schema, self.storage)

    def transform(self, func: Any) -> "MockDataFrame":
        """Apply a function to transform a DataFrame.

        This enables functional programming style transformations on DataFrames.

        Args:
            func: Function that takes a MockDataFrame and returns a MockDataFrame.

        Returns:
            MockDataFrame: The result of applying the function to this DataFrame.

        Example:
            >>> def add_id(df):
            ...     return df.withColumn("id", F.monotonically_increasing_id())
            >>> df.transform(add_id)
        """
        result = func(self)
        if not isinstance(result, MockDataFrame):
            raise TypeError(
                f"Function must return a MockDataFrame, got {type(result).__name__}"
            )
        return result

    def unpivot(
        self,
        ids: Union[str, List[str]],
        values: Union[str, List[str]],
        variableColumnName: str = "variable",
        valueColumnName: str = "value",
    ) -> "MockDataFrame":
        """Unpivot columns into rows (opposite of pivot).

        Args:
            ids: Column(s) to keep as identifiers (not unpivoted).
            values: Column(s) to unpivot into rows.
            variableColumnName: Name for the column containing variable names.
            valueColumnName: Name for the column containing values.

        Returns:
            MockDataFrame: Unpivoted DataFrame.

        Example:
            >>> df.unpivot(
            ...     ids=["id", "name"],
            ...     values=["Q1", "Q2", "Q3", "Q4"],
            ...     variableColumnName="quarter",
            ...     valueColumnName="sales"
            ... )
        """
        # Materialize if lazy
        materialized = self._materialize_if_lazy()

        # Normalize inputs
        id_cols = [ids] if isinstance(ids, str) else ids
        value_cols = [values] if isinstance(values, str) else values

        # Validate columns exist
        all_cols = set(materialized.columns)
        for col in id_cols:
            if col not in all_cols:
                raise AnalysisException(
                    f"Cannot resolve column name '{col}' among ({', '.join(materialized.columns)})"
                )
        for col in value_cols:
            if col not in all_cols:
                raise AnalysisException(
                    f"Cannot resolve column name '{col}' among ({', '.join(materialized.columns)})"
                )

        # Create unpivoted data
        unpivoted_data = []
        for row in materialized.data:
            # For each row, create multiple rows (one per value column)
            for value_col in value_cols:
                new_row = {}
                # Add id columns
                for id_col in id_cols:
                    new_row[id_col] = row.get(id_col)
                # Add variable and value
                new_row[variableColumnName] = value_col
                new_row[valueColumnName] = row.get(value_col)
                unpivoted_data.append(new_row)

        # Infer schema for unpivoted DataFrame
        # ID columns keep their types, variable is string, value type is inferred
        from ..spark_types import MockStructType, MockStructField, MockDataType

        fields = []
        # Add id column fields
        for id_col in id_cols:
            for field in materialized.schema.fields:
                if field.name == id_col:
                    fields.append(
                        MockStructField(id_col, field.dataType, field.nullable)
                    )
                    break

        # Add variable column (always string)
        fields.append(MockStructField(variableColumnName, StringType(), False))

        # Add value column (infer from first value column's type)
        value_type: MockDataType = StringType()  # Default to string
        for field in materialized.schema.fields:
            if field.name == value_cols[0]:
                value_type = field.dataType
                break
        fields.append(MockStructField(valueColumnName, value_type, True))

        unpivoted_schema = MockStructType(fields)
        return MockDataFrame(unpivoted_data, unpivoted_schema, self.storage)

    def inputFiles(self) -> List[str]:
        """Return list of input files for this DataFrame (PySpark 3.1+).

        Returns:
            Empty list (mock DataFrames don't have file inputs)
        """
        # Mock DataFrames are in-memory, so no input files
        return []

    def sameSemantics(self, other: "MockDataFrame") -> bool:
        """Check if this DataFrame has the same semantics as another (PySpark 3.1+).

        Simplified implementation that checks schema and data equality.

        Args:
            other: Another DataFrame to compare

        Returns:
            True if semantically equivalent, False otherwise
        """
        # Simplified: check if schemas match
        if len(self.schema.fields) != len(other.schema.fields):
            return False

        for f1, f2 in zip(self.schema.fields, other.schema.fields):
            if f1.name != f2.name or f1.dataType != f2.dataType:
                return False

        return True

    def semanticHash(self) -> int:
        """Return semantic hash of this DataFrame (PySpark 3.1+).

        Simplified implementation based on schema.

        Returns:
            Hash value representing DataFrame semantics
        """
        # Create hash from schema
        schema_str = ",".join([f"{f.name}:{f.dataType}" for f in self.schema.fields])
        return hash(schema_str)

    # Priority 1: Critical DataFrame Method Aliases
    def where(
        self, condition: Union[MockColumnOperation, MockColumn]
    ) -> "MockDataFrame":
        """Alias for filter() - Filter rows based on condition (all PySpark versions).

        Args:
            condition: Boolean condition to filter rows

        Returns:
            Filtered DataFrame
        """
        return self.filter(condition)

    def sort(self, *columns: Union[str, MockColumn], **kwargs: Any) -> "MockDataFrame":
        """Alias for orderBy() - Sort DataFrame by columns (all PySpark versions).

        Args:
            *columns: Column names or Column objects to sort by
            **kwargs: Additional sort options (e.g., ascending)

        Returns:
            Sorted DataFrame
        """
        return self.orderBy(*columns)

    def toDF(self, *cols: str) -> "MockDataFrame":
        """Rename columns of DataFrame (all PySpark versions).

        Args:
            *cols: New column names

        Returns:
            DataFrame with renamed columns

        Raises:
            ValueError: If number of columns doesn't match
        """
        if len(cols) != len(self.schema.fields):
            raise ValueError(
                f"Number of column names ({len(cols)}) must match "
                f"number of columns in DataFrame ({len(self.schema.fields)})"
            )

        # Create new schema with renamed columns
        new_fields = [
            MockStructField(new_name, field.dataType, field.nullable)
            for new_name, field in zip(cols, self.schema.fields)
        ]
        new_schema = MockStructType(new_fields)

        # Rename columns in data
        old_names = [field.name for field in self.schema.fields]
        new_data = []
        for row in self.data:
            new_row = {
                new_name: row[old_name] for new_name, old_name in zip(cols, old_names)
            }
            new_data.append(new_row)

        return MockDataFrame(new_data, new_schema, self.storage)

    def groupby(self, *cols: Union[str, MockColumn], **kwargs: Any) -> MockGroupedData:
        """Lowercase alias for groupBy() (all PySpark versions).

        Args:
            *cols: Column names or Column objects to group by
            **kwargs: Additional grouping options

        Returns:
            MockGroupedData object
        """
        return self.groupBy(*cols, **kwargs)

    def drop_duplicates(self, subset: Optional[List[str]] = None) -> "MockDataFrame":
        """Alias for dropDuplicates() (all PySpark versions).

        Args:
            subset: Optional list of column names to consider for deduplication

        Returns:
            DataFrame with duplicates removed
        """
        return self.dropDuplicates(subset)

    def unionAll(self, other: "MockDataFrame") -> "MockDataFrame":
        """Deprecated alias for union() - Use union() instead (all PySpark versions).

        Args:
            other: DataFrame to union with

        Returns:
            Union of both DataFrames

        Note:
            Deprecated in PySpark 2.0+, use union() instead
        """
        import warnings

        warnings.warn(
            "unionAll is deprecated. Use union instead.", FutureWarning, stacklevel=2
        )
        return self.union(other)

    def subtract(self, other: "MockDataFrame") -> "MockDataFrame":
        """Return rows in this DataFrame but not in another (all PySpark versions).

        Args:
            other: DataFrame to subtract

        Returns:
            DataFrame with rows from this DataFrame that are not in other
        """

        # Convert rows to tuples for comparison
        def row_to_tuple(row: Dict[str, Any]) -> Tuple[Any, ...]:
            return tuple(row.get(field.name) for field in self.schema.fields)

        self_rows = {row_to_tuple(row) for row in self.data}
        other_rows = {row_to_tuple(row) for row in other.data}

        # Find rows in self but not in other
        result_tuples = self_rows - other_rows

        # Convert back to dicts
        result_data = []
        for row_tuple in result_tuples:
            row_dict = {
                field.name: value for field, value in zip(self.schema.fields, row_tuple)
            }
            result_data.append(row_dict)

        return MockDataFrame(result_data, self.schema, self.storage)

    def alias(self, alias: str) -> "MockDataFrame":
        """Give DataFrame an alias for join operations (all PySpark versions).

        Args:
            alias: Alias name

        Returns:
            DataFrame with alias set
        """
        # Store alias in a special attribute
        result = MockDataFrame(self.data, self.schema, self.storage)
        result._alias = alias  # type: ignore
        return result

    def withColumns(
        self,
        colsMap: Dict[str, Union[MockColumn, MockColumnOperation, MockLiteral, Any]],
    ) -> "MockDataFrame":
        """Add or replace multiple columns at once (PySpark 3.3+).

        Args:
            colsMap: Dictionary mapping column names to column expressions

        Returns:
            DataFrame with new/replaced columns
        """
        result = self
        for col_name, col_expr in colsMap.items():
            result = result.withColumn(col_name, col_expr)
        return result

    # Priority 3: Common DataFrame Methods
    def approxQuantile(
        self,
        col: Union[str, List[str]],
        probabilities: List[float],
        relativeError: float,
    ) -> Union[List[float], List[List[float]]]:
        """Calculate approximate quantiles (all PySpark versions).

        Args:
            col: Column name or list of column names
            probabilities: List of quantile probabilities (0.0 to 1.0)
            relativeError: Relative error for approximation (0.0 for exact)

        Returns:
            List of quantile values, or list of lists if multiple columns
        """
        import numpy as np

        def calc_quantiles(column_name: str) -> List[float]:
            values_list: List[float] = []
            for row in self.data:
                val = row.get(column_name)
                if val is not None:
                    values_list.append(float(val))
            if not values_list:
                return [float("nan")] * len(probabilities)
            return [float(np.percentile(values_list, p * 100)) for p in probabilities]

        if isinstance(col, str):
            return calc_quantiles(col)
        else:
            return [calc_quantiles(c) for c in col]

    def cov(self, col1: str, col2: str) -> float:
        """Calculate covariance between two columns (all PySpark versions).

        Args:
            col1: First column name
            col2: Second column name

        Returns:
            Covariance value
        """
        import numpy as np

        # Filter rows where both values are not None and extract numeric values
        pairs = [
            (row.get(col1), row.get(col2))
            for row in self.data
            if row.get(col1) is not None and row.get(col2) is not None
        ]

        if not pairs:
            return 0.0

        values1 = [float(p[0]) for p in pairs]  # type: ignore
        values2 = [float(p[1]) for p in pairs]  # type: ignore

        return float(np.cov(values1, values2)[0][1])

    def crosstab(self, col1: str, col2: str) -> "MockDataFrame":
        """Calculate cross-tabulation (all PySpark versions).

        Args:
            col1: First column name (rows)
            col2: Second column name (columns)

        Returns:
            DataFrame with cross-tabulation
        """
        from collections import defaultdict

        # Build cross-tab structure
        crosstab_data: Dict[Any, Dict[Any, int]] = defaultdict(lambda: defaultdict(int))
        col2_values = set()

        for row in self.data:
            val1 = row.get(col1)
            val2 = row.get(col2)
            crosstab_data[val1][val2] += 1
            col2_values.add(val2)

        # Convert to list of dicts
        # Filter out None values before sorting to avoid comparison issues
        col2_sorted = sorted([v for v in col2_values if v is not None])
        result_data = []
        for val1 in sorted([k for k in crosstab_data.keys() if k is not None]):
            result_row = {f"{col1}_{col2}": val1}
            for val2 in col2_sorted:
                result_row[str(val2)] = crosstab_data[val1].get(val2, 0)
            result_data.append(result_row)

        # Build schema
        fields = [MockStructField(f"{col1}_{col2}", StringType())]
        for val2 in col2_sorted:
            fields.append(MockStructField(str(val2), LongType()))
        result_schema = MockStructType(fields)

        return MockDataFrame(result_data, result_schema, self.storage)

    def freqItems(
        self, cols: List[str], support: Optional[float] = None
    ) -> "MockDataFrame":
        """Find frequent items (all PySpark versions).

        Args:
            cols: List of column names
            support: Minimum support threshold (default 0.01)

        Returns:
            DataFrame with frequent items for each column
        """
        from collections import Counter

        if support is None:
            support = 0.01

        min_count = int(len(self.data) * support)
        result_row = {}

        for col in cols:
            values = [row.get(col) for row in self.data if row.get(col) is not None]
            counter = Counter(values)
            freq_items = [item for item, count in counter.items() if count >= min_count]
            result_row[f"{col}_freqItems"] = freq_items

        # Build schema

        fields = [
            MockStructField(f"{col}_freqItems", ArrayType(StringType())) for col in cols
        ]
        result_schema = MockStructType(fields)

        return MockDataFrame([result_row], result_schema, self.storage)

    def hint(self, name: str, *parameters: Any) -> "MockDataFrame":
        """Provide query optimization hints (all PySpark versions).

        This is a no-op in mock-spark as there's no query optimizer.

        Args:
            name: Hint name
            *parameters: Hint parameters

        Returns:
            Same DataFrame (no-op)
        """
        # No-op for mock implementation
        return self

    def intersectAll(self, other: "MockDataFrame") -> "MockDataFrame":
        """Return intersection with duplicates (PySpark 3.0+).

        Args:
            other: DataFrame to intersect with

        Returns:
            DataFrame with common rows (preserving duplicates)
        """
        from collections import Counter

        def row_to_tuple(row: Dict[str, Any]) -> Tuple[Any, ...]:
            return tuple(row.get(field.name) for field in self.schema.fields)

        # Count occurrences in each DataFrame
        self_counter = Counter(row_to_tuple(row) for row in self.data)
        other_counter = Counter(row_to_tuple(row) for row in other.data)

        # Intersection preserves minimum count
        result_data = []
        for row_tuple, count in self_counter.items():
            min_count = min(count, other_counter.get(row_tuple, 0))
            for _ in range(min_count):
                row_dict = {
                    field.name: value
                    for field, value in zip(self.schema.fields, row_tuple)
                }
                result_data.append(row_dict)

        return MockDataFrame(result_data, self.schema, self.storage)

    def isEmpty(self) -> bool:
        """Check if DataFrame is empty (PySpark 3.3+).

        Returns:
            True if DataFrame has no rows
        """
        return len(self.data) == 0

    def sampleBy(
        self, col: str, fractions: Dict[Any, float], seed: Optional[int] = None
    ) -> "MockDataFrame":
        """Stratified sampling (all PySpark versions).

        Args:
            col: Column to stratify by
            fractions: Dict mapping stratum values to sampling fractions
            seed: Random seed

        Returns:
            Sampled DataFrame
        """
        import random

        if seed is not None:
            random.seed(seed)

        result_data = []
        for row in self.data:
            stratum_value = row.get(col)
            fraction = fractions.get(stratum_value, 0.0)
            if random.random() < fraction:
                result_data.append(row)

        return MockDataFrame(result_data, self.schema, self.storage)

    def withColumnsRenamed(self, colsMap: Dict[str, str]) -> "MockDataFrame":
        """Rename multiple columns (PySpark 3.4+).

        Args:
            colsMap: Dictionary mapping old column names to new names

        Returns:
            DataFrame with renamed columns
        """
        result = self
        for old_name, new_name in colsMap.items():
            result = result.withColumnRenamed(old_name, new_name)
        return result

    def foreach(self, f: Any) -> None:
        """Apply function to each row (action, all PySpark versions).

        Args:
            f: Function to apply to each Row
        """
        for row in self.collect():
            f(row)

    def foreachPartition(self, f: Any) -> None:
        """Apply function to each partition (action, all PySpark versions).

        Args:
            f: Function to apply to each partition Iterator[Row]
        """
        # Mock implementation: treat entire dataset as single partition
        f(iter(self.collect()))

    def repartitionByRange(
        self,
        numPartitions: Union[int, str, "MockColumn"],
        *cols: Union[str, "MockColumn"],
    ) -> "MockDataFrame":
        """Repartition by range of column values (all PySpark versions).

        Args:
            numPartitions: Number of partitions or first column if string/Column
            *cols: Columns to partition by

        Returns:
            New DataFrame repartitioned by range (mock: sorted)
        """
        # For mock purposes, sort by columns to simulate range partitioning
        if isinstance(numPartitions, int):
            return self.orderBy(*cols)
        else:
            # numPartitions is actually the first column
            return self.orderBy(numPartitions, *cols)

    def sortWithinPartitions(
        self, *cols: Union[str, "MockColumn"], **kwargs: Any
    ) -> "MockDataFrame":
        """Sort within partitions (all PySpark versions).

        Args:
            *cols: Columns to sort by
            **kwargs: Additional arguments (ascending, etc.)

        Returns:
            New DataFrame sorted within partitions (mock: equivalent to orderBy)
        """
        # For mock purposes, treat as regular sort since we have single partition
        return self.orderBy(*cols, **kwargs)

    def toLocalIterator(self, prefetchPartitions: bool = False) -> Any:
        """Return iterator over rows (all PySpark versions).

        Args:
            prefetchPartitions: Whether to prefetch partitions (ignored in mock)

        Returns:
            Iterator over Row objects
        """
        if self._operations_queue:
            materialized = self._materialize_if_lazy()
            return self._get_collection_handler().to_local_iterator(
                materialized.data, materialized.schema, prefetchPartitions
            )
        return self._get_collection_handler().to_local_iterator(
            self.data, self.schema, prefetchPartitions
        )

    def localCheckpoint(self, eager: bool = True) -> "MockDataFrame":
        """Local checkpoint to truncate lineage (all PySpark versions).

        Args:
            eager: Whether to checkpoint eagerly

        Returns:
            Same DataFrame with truncated lineage
        """
        if eager:
            # Force materialization
            _ = len(self.data)
        return self

    def isLocal(self) -> bool:
        """Check if running in local mode (all PySpark versions).

        Returns:
            True if running in local mode (mock: always True)
        """
        return True

    def withWatermark(self, eventTime: str, delayThreshold: str) -> "MockDataFrame":
        """Define watermark for streaming (all PySpark versions).

        Args:
            eventTime: Column name for event time
            delayThreshold: Delay threshold (e.g., "1 hour")

        Returns:
            DataFrame with watermark defined (mock: returns self unchanged)
        """
        # In mock implementation, watermarks don't affect behavior
        # Store for potential future use
        self._watermark_col = eventTime
        self._watermark_delay = delayThreshold
        return self

    def melt(
        self,
        ids: Optional[List[str]] = None,
        values: Optional[List[str]] = None,
        variableColumnName: str = "variable",
        valueColumnName: str = "value",
    ) -> "MockDataFrame":
        """Unpivot DataFrame from wide to long format (PySpark 3.4+).

        Args:
            ids: List of column names to use as identifier columns
            values: List of column names to unpivot (None = all non-id columns)
            variableColumnName: Name for the variable column
            valueColumnName: Name for the value column

        Returns:
            New DataFrame in long format

        Example:
            >>> df = spark.createDataFrame([{"id": 1, "A": 10, "B": 20}])
            >>> df.melt(ids=["id"], values=["A", "B"]).show()
        """
        id_cols = ids or []
        value_cols = values or [c for c in self.columns if c not in id_cols]

        result_data = []
        for row in self.data:
            for val_col in value_cols:
                new_row = {col: row[col] for col in id_cols}
                new_row[variableColumnName] = val_col
                new_row[valueColumnName] = row.get(val_col)
                result_data.append(new_row)

        # Build new schema - find fields by name
        fields = []
        for col in id_cols:
            field = [f for f in self.schema.fields if f.name == col][0]
            fields.append(MockStructField(col, field.dataType))

        fields.append(MockStructField(variableColumnName, StringType()))

        # Use first value column's type for value column (or StringType as fallback)
        if value_cols:
            first_value_field = [
                f for f in self.schema.fields if f.name == value_cols[0]
            ][0]
            value_type = first_value_field.dataType
        else:
            value_type = StringType()
        fields.append(MockStructField(valueColumnName, value_type))

        return MockDataFrame(result_data, MockStructType(fields), self.storage)

    def to(self, schema: Union[str, MockStructType]) -> "MockDataFrame":
        """Apply schema with casting (PySpark 3.4+).

        Args:
            schema: Target schema (DDL string or MockStructType)

        Returns:
            New DataFrame with schema applied

        Example:
            >>> df.to("id: long, name: string")
        """
        if isinstance(schema, str):
            from mock_spark.core.ddl_adapter import parse_ddl_schema

            target_schema = parse_ddl_schema(schema)
        else:
            target_schema = schema

        # Cast columns to match target schema
        result_data = []
        for row in self.data:
            new_row = {}
            for field in target_schema.fields:
                if field.name in row:
                    # Type casting would happen here in real implementation
                    new_row[field.name] = row[field.name]
            result_data.append(new_row)

        return MockDataFrame(result_data, target_schema, self.storage)

    def withMetadata(
        self, columnName: str, metadata: Dict[str, Any]
    ) -> "MockDataFrame":
        """Attach metadata to a column (PySpark 3.3+).

        Args:
            columnName: Name of the column to attach metadata to
            metadata: Dictionary of metadata key-value pairs

        Returns:
            New DataFrame with metadata attached

        Example:
            >>> df.withMetadata("id", {"comment": "User identifier"})
        """
        # Find the field and update its metadata
        new_fields = []
        for field in self.schema.fields:
            if field.name == columnName:
                # Create new field with metadata
                new_field = MockStructField(
                    field.name, field.dataType, field.nullable, metadata
                )
                new_fields.append(new_field)
            else:
                new_fields.append(field)

        new_schema = MockStructType(new_fields)
        return MockDataFrame(self.data, new_schema, self.storage)

    def observe(self, name: str, *exprs: "MockColumn") -> "MockDataFrame":
        """Define observation metrics (PySpark 3.3+).

        Args:
            name: Name of the observation
            *exprs: Column expressions to observe

        Returns:
            Same DataFrame with observation registered

        Example:
            >>> df.observe("metrics", F.count(F.lit(1)).alias("count"))
        """
        # In mock implementation, observations don't affect behavior
        # Preserve existing observations and add new ones
        new_df = MockDataFrame(self.data, self.schema, self.storage)

        # Copy existing observations if any
        if hasattr(self, "_observations"):
            new_df._observations = dict(self._observations)  # type: ignore
        else:
            new_df._observations = {}  # type: ignore[attr-defined]

        new_df._observations[name] = exprs
        return new_df

    @property
    def write(self) -> "MockDataFrameWriter":
        """Get DataFrame writer (PySpark-compatible property)."""
        return MockDataFrameWriter(self, self.storage)

    def _parse_cast_type_string(self, type_str: str) -> MockDataType:
        """Parse a cast type string to MockDataType."""
        from ..spark_types import (
            LongType,
            StringType,
            BooleanType,
            DateType,
            TimestampType,
            DecimalType,
        )

        type_str = type_str.strip().lower()

        # Primitive types
        if type_str in ["int", "integer"]:
            return IntegerType()
        elif type_str in ["long", "bigint"]:
            return LongType()
        elif type_str in ["double", "float"]:
            return DoubleType()
        elif type_str in ["string", "varchar"]:
            return StringType()
        elif type_str in ["boolean", "bool"]:
            return BooleanType()
        elif type_str == "date":
            return DateType()
        elif type_str == "timestamp":
            return TimestampType()
        elif type_str.startswith("decimal"):
            import re

            match = re.match(r"decimal\((\d+),(\d+)\)", type_str)
            if match:
                precision, scale = int(match.group(1)), int(match.group(2))
                return DecimalType(precision, scale)
            return DecimalType(10, 2)
        elif type_str.startswith("array<"):
            element_type_str = type_str[6:-1]
            return ArrayType(self._parse_cast_type_string(element_type_str))
        elif type_str.startswith("map<"):
            types = type_str[4:-1].split(",", 1)
            key_type = self._parse_cast_type_string(types[0].strip())
            value_type = self._parse_cast_type_string(types[1].strip())
            return MapType(key_type, value_type)
        else:
            return StringType()  # Default fallback
