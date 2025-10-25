"""
Refactored Mock DataFrame implementation for Mock Spark.

This module provides a complete mock implementation of PySpark DataFrame
that uses the Single Responsibility Principle with specialized components.
"""

from typing import Any, Dict, List, Optional, Union, TYPE_CHECKING, Iterator

if TYPE_CHECKING:
    from .lazy import LazyEvaluationEngine

from ..spark_types import (
    MockStructType,
    MockStructField,
    MockRow,
    StringType,
    DoubleType,
    MockDataType,
    IntegerType,
    BooleanType,
)
from ..functions import MockColumn, MockColumnOperation, MockLiteral
from ..storage import MemoryStorageManager
from .grouped import (
    MockGroupedData,
    MockRollupGroupedData,
    MockCubeGroupedData,
)
from .rdd import MockRDD
from .writer import MockDataFrameWriter
from .evaluation.expression_evaluator import ExpressionEvaluator

# Import specialized components
from .transformer import DataFrameTransformer
from .statistics import DataFrameStatistics
from .display_operations import DataFrameDisplay


class MockDataFrame:
    """Mock DataFrame implementation with complete PySpark API compatibility.

    This refactored version uses the Single Responsibility Principle with
    specialized components for different types of operations.

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

        # Initialize specialized components using dependency injection
        self._transformer = DataFrameTransformer(self)
        self._statistics = DataFrameStatistics(self)
        self._display = DataFrameDisplay(self)

    def _get_lazy_engine(self) -> "LazyEvaluationEngine":
        """Get or create the lazy evaluation engine."""
        if self._lazy_engine is None:
            from .lazy import LazyEvaluationEngine

            self._lazy_engine = LazyEvaluationEngine()
        return self._lazy_engine

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

    def __getattr__(self, name: str) -> Any:
        """Enable df.column_name syntax for column access and delegate to specialized components."""
        # Handle specialized components
        if name == "transformer":
            return self._transformer
        elif name == "statistics":
            return self._statistics
        elif name == "display":
            return self._display

        # Handle column access for df.column_name syntax
        try:
            columns = object.__getattribute__(self, "columns")
            if name in columns:
                # Use F.col to create MockColumn
                from mock_spark.functions import F

                return F.col(name)
        except AttributeError:
            pass

        # If not a column or component, raise AttributeError
        raise AttributeError(f"'MockDataFrame' object has no attribute '{name}'")

    # =============================================================================
    # TRANSFORMATION OPERATIONS (delegated to DataFrameTransformer)
    # =============================================================================

    def select(
        self, *columns: Union[str, MockColumn, MockLiteral, Any]
    ) -> "MockDataFrame":
        """Select columns from the DataFrame."""
        return self._transformer.select(*columns)

    def filter(
        self, condition: Union[MockColumnOperation, MockColumn, MockLiteral]
    ) -> "MockDataFrame":
        """Filter rows based on condition."""
        return self._transformer.filter(condition)

    def withColumn(
        self,
        col_name: str,
        col: Union[MockColumn, MockColumnOperation, MockLiteral, Any],
    ) -> "MockDataFrame":
        """Add or replace column."""
        return self._transformer.withColumn(col_name, col)

    def withColumns(
        self,
        colsMap: Dict[str, Union[MockColumn, MockColumnOperation, MockLiteral, Any]],
    ) -> "MockDataFrame":
        """Add or replace multiple columns."""
        return self._transformer.withColumns(colsMap)

    def withColumnRenamed(self, existing: str, new: str) -> "MockDataFrame":
        """Rename a column."""
        return self._transformer.withColumnRenamed(existing, new)

    def withColumnsRenamed(self, colsMap: Dict[str, str]) -> "MockDataFrame":
        """Rename multiple columns."""
        return self._transformer.withColumnsRenamed(colsMap)

    def orderBy(self, *columns: Union[str, MockColumn]) -> "MockDataFrame":
        """Order by columns."""
        return self._transformer.orderBy(*columns)

    def selectExpr(self, *exprs: str) -> "MockDataFrame":
        """Select columns or expressions using SQL-like syntax."""
        return self._transformer.selectExpr(*exprs)

    # =============================================================================
    # STATISTICAL OPERATIONS (delegated to DataFrameStatistics)
    # =============================================================================

    def count(self) -> int:
        """Count number of rows."""
        return self._statistics.count()

    def describe(self, *cols: str) -> "MockDataFrame":
        """Compute basic statistics for numeric columns."""
        return self._statistics.describe(*cols)

    def summary(self, *stats: str) -> "MockDataFrame":
        """Compute extended statistics for numeric columns."""
        return self._statistics.summary(*stats)

    def collect(self) -> List[MockRow]:
        """Collect all data as list of Row objects."""
        return self._statistics.collect()

    def toPandas(self) -> Any:
        """Convert to pandas DataFrame (requires pandas as optional dependency)."""
        return self._statistics.toPandas()

    def toLocalIterator(self) -> "Iterator[MockRow]":
        """Convert to local iterator."""
        return self._statistics.toLocalIterator()

    def isEmpty(self) -> bool:
        """Check if DataFrame is empty."""
        return self._statistics.isEmpty()

    def distinct(self) -> "MockDataFrame":
        """Return distinct rows."""
        return self._statistics.distinct()

    # =============================================================================
    # DISPLAY OPERATIONS (delegated to DataFrameDisplay)
    # =============================================================================

    def show(self, n: int = 20, truncate: bool = True) -> None:
        """Display DataFrame content in a clean table format."""
        return self._display.show(n, truncate)

    def to_markdown(
        self, n: int = 20, truncate: bool = True, underline_headers: bool = True
    ) -> str:
        """Return DataFrame as a markdown table string."""
        return self._display.to_markdown(n, truncate, underline_headers)

    def printSchema(self) -> None:
        """Print DataFrame schema."""
        return self._display.printSchema()

    def toDuckDB(self, connection: Any = None, table_name: Optional[str] = None) -> str:
        """Convert to DuckDB table for analytical operations."""
        return self._display.toDuckDB(connection, table_name)

    def _get_duckdb_type(self, data_type: Any) -> str:
        """Map MockSpark data type to DuckDB type (backwards compatibility)."""
        return self._display._get_duckdb_type(data_type)

    def take(self, n: int) -> List[MockRow]:
        """Take the first n rows."""
        return self._display.take(n)

    def first(self) -> Optional[MockRow]:
        """Get the first row."""
        return self._display.first()

    def head(self, n: int = 5) -> List[MockRow]:
        """Get the first n rows."""
        return self._display.head(n)

    # =============================================================================
    # PROPERTIES AND CORE FUNCTIONALITY
    # =============================================================================

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

    # =============================================================================
    # SCHEMA PROJECTION AND VALIDATION
    # =============================================================================

    def _project_schema_with_operations(self) -> MockStructType:
        """Compute schema after applying queued lazy operations."""
        from ..spark_types import (
            MockStructType,
            StringType,
        )

        fields_map = {f.name: f for f in self._schema.fields}
        for op_name, op_val in self._operations_queue:
            if op_name == "select":
                # Project schema based on select columns
                new_fields = []
                for col in op_val:
                    if isinstance(col, str):
                        if col == "*":
                            # Select all existing fields
                            new_fields.extend(fields_map.values())
                        else:
                            # Select specific field
                            if col in fields_map:
                                new_fields.append(fields_map[col])
                    elif isinstance(col, MockColumn):
                        if hasattr(col, "operation"):
                            # Complex expression - infer type
                            new_fields.append(
                                MockStructField(
                                    col.name, self._infer_expression_type(col)
                                )
                            )
                        else:
                            # Simple column reference
                            if col.name in fields_map:
                                new_fields.append(fields_map[col.name])
                fields_map = {f.name: f for f in new_fields}
            elif op_name == "withColumn":
                col_name, col_expr = op_val
                if isinstance(col_expr, MockColumn) and hasattr(col_expr, "operation"):
                    # Complex expression - infer type
                    fields_map[col_name] = MockStructField(
                        col_name, self._infer_expression_type(col_expr)
                    )
                else:
                    # Simple column reference or literal
                    fields_map[col_name] = MockStructField(
                        col_name,
                        StringType(),  # Default to StringType
                    )

        return MockStructType(list(fields_map.values()))

    def _infer_expression_type(self, expr: Any) -> MockDataType:
        """Infer the data type of an expression."""
        # Simplified type inference - in a full implementation,
        # this would analyze the expression tree
        if isinstance(expr, MockLiteral):
            if isinstance(expr.value, bool):
                return BooleanType()
            elif isinstance(expr.value, int):
                return IntegerType()
            elif isinstance(expr.value, float):
                return DoubleType()
            else:
                return StringType()
        else:
            # Default to StringType for complex expressions
            return StringType()

    # =============================================================================
    # ADDITIONAL METHODS (simplified for refactor)
    # =============================================================================

    def join(
        self,
        other: "MockDataFrame",
        on: Union[str, List[str], MockColumn],
        how: str = "inner",
    ) -> "MockDataFrame":
        """Join with another DataFrame."""
        return self._queue_op("join", (other, on, how))

    def union(self, other: "MockDataFrame") -> "MockDataFrame":
        """Union with another DataFrame."""
        return self._queue_op("union", other)

    def limit(self, n: int) -> "MockDataFrame":
        """Limit number of rows."""
        return self._queue_op("limit", n)

    def groupBy(self, *cols: Union[str, MockColumn]) -> MockGroupedData:
        """Group by columns."""
        return MockGroupedData(self, cols)

    def rollup(self, *cols: Union[str, MockColumn]) -> MockRollupGroupedData:
        """Create rollup grouping."""
        return MockRollupGroupedData(self, cols)

    def cube(self, *cols: Union[str, MockColumn]) -> MockCubeGroupedData:
        """Create cube grouping."""
        return MockCubeGroupedData(self, cols)

    def rdd(self) -> MockRDD:
        """Convert to RDD."""
        return MockRDD(self.data, self.schema)

    def write(self) -> MockDataFrameWriter:
        """Get DataFrameWriter."""
        return MockDataFrameWriter(self)

    # =============================================================================
    # TEST HELPERS (Phase 4)
    # =============================================================================

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
