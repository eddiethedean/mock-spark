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
    >>> from mock_spark.sql import SparkSession, functions as F
    >>> spark = SparkSession("test")
    >>> data = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
    >>> df = spark.createDataFrame(data)
    >>> df.select("name", "age").filter(F.col("age") > 25).show()
    +----+---+
    |name|age|
    +----+---+
    | Bob| 30|
    +----+---+
"""

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union, cast

from .protocols import SupportsDataFrameOps

if TYPE_CHECKING:
    from .collection_handler import CollectionHandler
    from .condition_handler import ConditionHandler
    from .validation_handler import ValidationHandler
    from .window_handler import WindowFunctionHandler

    def _ensure_dataframe_protocol(df: "DataFrame") -> "SupportsDataFrameOps":
        return df

if TYPE_CHECKING:
    from .lazy import LazyEvaluationEngine
    from ..backend.protocols import StorageBackend
else:
    StorageBackend = Any

from ..spark_types import (
    StructType,
    StructField,
    Row,
    StringType,
    LongType,
    DoubleType,
    DataType,
    IntegerType,
    ArrayType,
    MapType,
)
from ..functions import Column, ColumnOperation, Literal
from ..storage import MemoryStorageManager
from .rdd import MockRDD
from ..core.exceptions import (
    IllegalArgumentException,
)
from ..core.exceptions.analysis import ColumnNotFoundException, AnalysisException
from .writer import DataFrameWriter
from .evaluation.expression_evaluator import ExpressionEvaluator
from .transformations import TransformationOperations
from .joins import JoinOperations
from .aggregations import AggregationOperations
from .display import DisplayOperations
from .schema import SchemaOperations
from .assertions import AssertionOperations
from .operations import MiscellaneousOperations
from .attribute_handler import DataFrameAttributeHandler


class DataFrame(
    TransformationOperations["DataFrame"],
    JoinOperations["DataFrame"],
    AggregationOperations["DataFrame"],
    DisplayOperations,
    SchemaOperations["DataFrame"],
    AssertionOperations,
    MiscellaneousOperations,
):
    """Mock DataFrame implementation with complete PySpark API compatibility.

    Provides a comprehensive mock implementation of PySpark DataFrame that supports
    all major operations including selection, filtering, grouping, joining, and
    window functions. Designed for testing and development without requiring JVM.

    Attributes:
        data: List of dictionaries representing DataFrame rows.
        schema: StructType defining the DataFrame schema.
        storage: Optional storage manager for persistence operations.

    Example:
        >>> from mock_spark.sql import SparkSession, functions as F
        >>> spark = SparkSession("test")
        >>> data = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
        >>> df = spark.createDataFrame(data)
        >>> df.select("name").filter(F.col("age") > 25).show()
        +----+
        |name|
        +----+
        | Bob|
        +----+
    """

    data: List[Dict[str, Any]]
    schema: StructType
    storage: StorageBackend
    _operations_queue: List[Tuple[str, Any]]
    _cached_count: Optional[int]
    _watermark_col: Optional[str]
    _watermark_delay: Optional[str]

    def __init__(
        self,
        data: List[Dict[str, Any]],
        schema: StructType,
        storage: Optional["StorageBackend"] = None,
        operations: Optional[List[Tuple[str, Any]]] = None,
    ):
        """Initialize DataFrame.

        Args:
            data: List of dictionaries representing DataFrame rows.
            schema: StructType defining the DataFrame schema.
            storage: Optional storage manager for persistence operations.
                    Defaults to a new MemoryStorageManager instance.
            operations: Optional list of queued operations as (operation_name, payload) tuples.
        """
        self.data = data
        self.schema = schema
        self.storage: StorageBackend = storage or MemoryStorageManager()
        self._cached_count: Optional[int] = None
        self._operations_queue: List[Tuple[str, Any]] = operations or []
        # Services are lazy-initialized via _get_* methods
        self._lazy_engine: Optional[LazyEvaluationEngine] = None
        self._expression_evaluator: Optional[ExpressionEvaluator] = None
        self._window_handler: Optional[WindowFunctionHandler] = None
        self._collection_handler: Optional[CollectionHandler] = None
        self._validation_handler: Optional[ValidationHandler] = None
        self._condition_handler: Optional[ConditionHandler] = None
        # Observations for metrics tracking (PySpark 3.3+)
        self._observations: Dict[str, Tuple[Column, ...]] = {}

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

    def _get_condition_handler(self) -> "ConditionHandler":
        """Get or create the condition handler."""
        if self._condition_handler is None:
            from .condition_handler import ConditionHandler

            self._condition_handler = ConditionHandler()
        return self._condition_handler

    def _get_expression_evaluator(self) -> ExpressionEvaluator:
        """Get or create the expression evaluator."""
        if self._expression_evaluator is None:
            self._expression_evaluator = ExpressionEvaluator()
        return self._expression_evaluator

    def _queue_op(self, op_name: str, payload: Any) -> SupportsDataFrameOps:
        """Queue an operation for lazy evaluation.

        Args:
            op_name: Name of the operation (e.g., "select", "filter", "join").
            payload: Operation-specific payload (columns, condition, etc.).

        Returns:
            New DataFrame with the operation queued.
        """
        new_ops: List[Tuple[str, Any]] = self._operations_queue + [(op_name, payload)]
        return cast(
            "SupportsDataFrameOps",
            DataFrame(
                data=self.data,
                schema=self.schema,
                storage=self.storage,
                operations=new_ops,
            ),
        )

    def _materialize_if_lazy(self) -> SupportsDataFrameOps:
        """Materialize lazy operations if any are queued."""
        if self._operations_queue:
            lazy_engine = self._get_lazy_engine()
            return lazy_engine.materialize(self)
        return self

    def __repr__(self) -> str:
        return f"DataFrame[{len(self.data)} rows, {len(self.schema.fields)} columns]"

    def __getattribute__(self, name: str) -> Any:
        """Custom attribute access for DataFrame."""
        return DataFrameAttributeHandler.handle_getattribute(
            self, name, super().__getattribute__
        )

    def __getattr__(self, name: str) -> Column:
        """Enable df.column_name syntax for column access (PySpark compatibility)."""
        return DataFrameAttributeHandler.handle_getattr(self, name)

    def _validate_column_exists(
        self,
        column_name: str,
        operation: str,
        allow_ambiguous: bool = False,
    ) -> None:
        """Validate that a column exists in the DataFrame."""
        self._get_validation_handler().validate_column_exists(
            self.schema, column_name, operation
        )

    def _validate_columns_exist(self, column_names: List[str], operation: str) -> None:
        """Validate that multiple columns exist in the DataFrame."""
        self._get_validation_handler().validate_columns_exist(
            self.schema, column_names, operation
        )

    def _validate_filter_expression(
        self,
        condition: Union[Column, ColumnOperation, Literal],
        operation: str,
        has_pending_joins: bool = False,
    ) -> None:
        """Validate filter expression before execution."""
        if not has_pending_joins:
            # Check if there are pending joins (columns might come from other DF)
            has_pending_joins = any(op[0] == "join" for op in self._operations_queue)
        self._get_validation_handler().validate_filter_expression(
            self.schema, condition, operation, has_pending_joins
        )

    def _validate_expression_columns(
        self,
        expression: Union[Column, ColumnOperation, Literal],
        operation: str,
        in_lazy_materialization: bool = False,
    ) -> None:
        """Validate column references in complex expressions."""
        if not in_lazy_materialization:
            # Check if we're in lazy materialization mode by looking at the call stack
            import inspect

            frame = inspect.currentframe()
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

    def _project_schema_with_operations(self) -> StructType:
        """Compute schema after applying queued lazy operations.

        Delegates to SchemaManager for schema projection logic.
        Preserves base schema fields even when data is empty.
        """
        from .schema.schema_manager import SchemaManager

        return SchemaManager.project_schema_with_operations(
            self._schema, self._operations_queue
        )

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
        self, data: List[Dict[str, Any]], condition: ColumnOperation
    ) -> List[Dict[str, Any]]:
        """Apply condition to filter data."""
        return self._get_condition_handler().apply_condition(data, condition)

    def _evaluate_condition(
        self, row: Dict[str, Any], condition: Union[ColumnOperation, Column]
    ) -> bool:
        """Evaluate condition for a single row.

        Delegates to ConditionHandler for consistency.
        """
        return self._get_condition_handler().evaluate_condition(row, condition)

    def _evaluate_column_expression(
        self,
        row: Dict[str, Any],
        column_expression: Union[Column, ColumnOperation, Literal, Any],
    ) -> Any:
        """Evaluate a column expression for a single row.

        Args:
            row: Dictionary representing a single row.
            column_expression: Column expression to evaluate (Column, ColumnOperation, or literal).

        Returns:
            Evaluated value of the expression.
        """
        return self._get_condition_handler().evaluate_column_expression(
            row, column_expression
        )

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
        self,
        data: List[Dict[str, Any]],
        indices: List[int],
        order_by_cols: List[Union[Column, ColumnOperation]],
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
        return self._get_condition_handler().evaluate_case_when(row, case_when_obj)

    def _evaluate_case_when_condition(
        self, row: Dict[str, Any], condition: Any
    ) -> bool:
        """Evaluate a CASE WHEN condition for a row."""
        return self._get_condition_handler()._evaluate_case_when_condition(
            row, condition
        )

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
        self.storage.insert_data("global_temp", name, list(data))

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
        self.storage.insert_data("global_temp", name, list(data))

    # colRegex and replace are now provided by TransformationOperations mixin

    # head, tail, and toJSON are now provided by DisplayOperations mixin

    @property
    def isStreaming(self) -> bool:
        """Whether this DataFrame is streaming (always False in mock)."""
        return False

    # repartition and coalesce are now provided by TransformationOperations mixin

    def checkpoint(self, eager: bool = False) -> "DataFrame":
        """Checkpoint the DataFrame (no-op in mock; returns self)."""
        return self

    # sample is now provided by MiscellaneousOperations mixin

    def randomSplit(
        self, weights: List[float], seed: Optional[int] = None
    ) -> List[SupportsDataFrameOps]:
        """Randomly split DataFrame into multiple DataFrames.

        Args:
            weights: List of weights for each split (must sum to 1.0).
            seed: Random seed for reproducible splitting.

        Returns:
            List of DataFrames split according to weights.
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
        splits: List[SupportsDataFrameOps] = []
        start_idx = 0

        for end_idx in split_points:
            split_indices = [idx for idx, _ in indexed_data[start_idx:end_idx]]
            split_data = [self.data[idx] for idx in split_indices]
            split_df = DataFrame(split_data, self.schema, self.storage)
            splits.append(cast("SupportsDataFrameOps", split_df))
            start_idx = end_idx

        return splits

    def describe(self, *cols: str) -> "DataFrame":
        """Compute basic statistics for numeric columns.

        Args:
            *cols: Column names to describe. If empty, describes all numeric columns.

        Returns:
            DataFrame with statistics (count, mean, stddev, min, max).
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
            return DataFrame([], self.schema, self.storage)

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
        from ..spark_types import StructType, StructField

        result_schema = StructType(
            [
                StructField("summary", StringType()),
                StructField("count", StringType()),
                StructField("mean", StringType()),
                StructField("stddev", StringType()),
                StructField("min", StringType()),
                StructField("max", StringType()),
            ]
        )

        return DataFrame(result_data, result_schema, self.storage)

    def summary(self, *stats: str) -> "DataFrame":
        """Compute extended statistics for numeric columns.

        Args:
            *stats: Statistics to compute. Default: ["count", "mean", "stddev", "min", "25%", "50%", "75%", "max"].

        Returns:
            DataFrame with extended statistics.
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
            return DataFrame([], self.schema, self.storage)

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
        from ..spark_types import StructType, StructField

        result_fields = [StructField("summary", StringType())]
        for stat in stats:
            result_fields.append(StructField(stat, StringType()))

        result_schema = StructType(result_fields)
        return DataFrame(result_data, result_schema, self.storage)

    def mapPartitions(
        self, func: Any, preservesPartitioning: bool = False
    ) -> "DataFrame":
        """Apply a function to each partition of the DataFrame.

        For mock-spark, we treat the entire DataFrame as a single partition.
        The function receives an iterator of Row objects and should return
        an iterator of Row objects.

        Args:
            func: A function that takes an iterator of Rows and returns an iterator of Rows.
            preservesPartitioning: Whether the function preserves partitioning (unused in mock-spark).

        Returns:
            DataFrame: Result of applying the function.

        Example:
            >>> def add_index(iterator):
            ...     for i, row in enumerate(iterator):
            ...         yield Row(id=row.id, name=row.name, index=i)
            >>> df.mapPartitions(add_index)
        """
        # Materialize if lazy
        materialized = self._materialize_if_lazy()

        # Convert data to Row objects
        from typing import Iterator

        def row_iterator() -> Iterator[Row]:
            for row_dict in materialized.data:
                yield Row(row_dict)

        # Apply the function
        result_iterator = func(row_iterator())

        # Collect results
        result_data = []
        for result_row in result_iterator:
            if isinstance(result_row, Row):
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

        return DataFrame(result_data, result_schema, self.storage)

    def mapInPandas(self, func: Any, schema: Any) -> "DataFrame":
        """Map an iterator of pandas DataFrames to another iterator of pandas DataFrames.

        For mock-spark, we treat the entire DataFrame as a single partition.
        The function receives an iterator yielding pandas DataFrames and should
        return an iterator yielding pandas DataFrames.

        Args:
            func: A function that takes an iterator of pandas DataFrames and returns
                  an iterator of pandas DataFrames.
            schema: The schema of the output DataFrame (StructType or DDL string).

        Returns:
            DataFrame: Result of applying the function.

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
                from ..core.exceptions import PySparkTypeError

                raise PySparkTypeError(
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
        from ..spark_types import StructType
        from ..core.schema_inference import infer_schema_from_data

        result_schema: StructType
        if isinstance(schema, str):
            # For DDL string, use schema inference from result data
            # (DDL parsing is complex, so we rely on inference for now)
            result_schema = (
                infer_schema_from_data(result_data) if result_data else self.schema
            )
        elif isinstance(schema, StructType):
            result_schema = schema
        else:
            # Try to infer schema from result data
            result_schema = (
                infer_schema_from_data(result_data) if result_data else self.schema
            )

        return DataFrame(result_data, result_schema, self.storage)

    def transform(self, func: Any) -> "DataFrame":
        """Apply a function to transform a DataFrame.

        This enables functional programming style transformations on DataFrames.

        Args:
            func: Function that takes a DataFrame and returns a DataFrame.

        Returns:
            DataFrame: The result of applying the function to this DataFrame.

        Example:
            >>> def add_id(df):
            ...     return df.withColumn("id", F.monotonically_increasing_id())
            >>> df.transform(add_id)
        """
        result = func(self)
        if not isinstance(result, DataFrame):
            from ..core.exceptions import PySparkTypeError

            raise PySparkTypeError(
                f"Function must return a DataFrame, got {type(result).__name__}"
            )
        return result

    def unpivot(
        self,
        ids: Union[str, List[str]],
        values: Union[str, List[str]],
        variableColumnName: str = "variable",
        valueColumnName: str = "value",
    ) -> "DataFrame":
        """Unpivot columns into rows (opposite of pivot).

        Args:
            ids: Column(s) to keep as identifiers (not unpivoted).
            values: Column(s) to unpivot into rows.
            variableColumnName: Name for the column containing variable names.
            valueColumnName: Name for the column containing values.

        Returns:
            DataFrame: Unpivoted DataFrame.

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
        from ..spark_types import StructType, StructField, DataType

        fields = []
        # Add id column fields
        for id_col in id_cols:
            for field in materialized.schema.fields:
                if field.name == id_col:
                    fields.append(StructField(id_col, field.dataType, field.nullable))
                    break

        # Add variable column (always string)
        fields.append(StructField(variableColumnName, StringType(), False))

        # Add value column (infer from first value column's type)
        value_type: DataType = StringType()  # Default to string
        for field in materialized.schema.fields:
            if field.name == value_cols[0]:
                value_type = field.dataType
                break
        fields.append(StructField(valueColumnName, value_type, True))

        unpivoted_schema = StructType(fields)
        return DataFrame(unpivoted_data, unpivoted_schema, self.storage)

    def inputFiles(self) -> List[str]:
        """Return list of input files for this DataFrame (PySpark 3.1+).

        Returns:
            Empty list (mock DataFrames don't have file inputs)
        """
        # Mock DataFrames are in-memory, so no input files
        return []

    def sameSemantics(self, other: "DataFrame") -> bool:
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
    # where and sort are now provided by TransformationOperations mixin

    def toDF(self, *cols: str) -> "DataFrame":
        """Rename columns of DataFrame (all PySpark versions).

        Args:
            *cols: New column names

        Returns:
            DataFrame with renamed columns

        Raises:
            ValueError: If number of columns doesn't match
        """
        if len(cols) != len(self.schema.fields):
            from ..core.exceptions import PySparkValueError

            raise PySparkValueError(
                f"Number of column names ({len(cols)}) must match "
                f"number of columns in DataFrame ({len(self.schema.fields)})"
            )

        # Create new schema with renamed columns
        new_fields = [
            StructField(new_name, field.dataType, field.nullable)
            for new_name, field in zip(cols, self.schema.fields)
        ]
        new_schema = StructType(new_fields)

        # Rename columns in data
        old_names = [field.name for field in self.schema.fields]
        new_data = []
        for row in self.data:
            new_row = {
                new_name: row[old_name] for new_name, old_name in zip(cols, old_names)
            }
            new_data.append(new_row)

        return DataFrame(new_data, new_schema, self.storage)

    # groupby is now provided by AggregationOperations mixin

    # drop_duplicates is now provided by TransformationOperations mixin

    # unionAll and subtract are now provided by JoinOperations mixin

    def alias(self, alias: str) -> "DataFrame":
        """Give DataFrame an alias for join operations (all PySpark versions).

        Args:
            alias: Alias name

        Returns:
            DataFrame with alias set
        """
        # Store alias in a special attribute
        result = DataFrame(self.data, self.schema, self.storage)
        result._alias = alias  # type: ignore
        return result

    # withColumns is now provided by TransformationOperations mixin

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

    def crosstab(self, col1: str, col2: str) -> "DataFrame":
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
        for val1 in sorted([k for k in crosstab_data if k is not None]):
            result_row = {f"{col1}_{col2}": val1}
            for val2 in col2_sorted:
                result_row[str(val2)] = crosstab_data[val1].get(val2, 0)
            result_data.append(result_row)

        # Build schema
        fields = [StructField(f"{col1}_{col2}", StringType())]
        for val2 in col2_sorted:
            fields.append(StructField(str(val2), LongType()))
        result_schema = StructType(fields)

        return DataFrame(result_data, result_schema, self.storage)

    def freqItems(
        self, cols: List[str], support: Optional[float] = None
    ) -> "DataFrame":
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
            StructField(f"{col}_freqItems", ArrayType(StringType())) for col in cols
        ]
        result_schema = StructType(fields)

        return DataFrame([result_row], result_schema, self.storage)

    def hint(self, name: str, *parameters: Any) -> "DataFrame":
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

    # intersectAll is now provided by JoinOperations mixin

    # isEmpty is now provided by DisplayOperations mixin

    def sampleBy(
        self, col: str, fractions: Dict[Any, float], seed: Optional[int] = None
    ) -> "DataFrame":
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

        return DataFrame(result_data, self.schema, self.storage)

    # withColumnsRenamed is now provided by TransformationOperations mixin

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
        numPartitions: Union[int, str, "Column"],
        *cols: Union[str, "Column"],
    ) -> "DataFrame":
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
        self, *cols: Union[str, "Column"], **kwargs: Any
    ) -> "DataFrame":
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

    def localCheckpoint(self, eager: bool = True) -> "DataFrame":
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

    def withWatermark(self, eventTime: str, delayThreshold: str) -> "DataFrame":
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
    ) -> "DataFrame":
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
            fields.append(StructField(col, field.dataType))

        fields.append(StructField(variableColumnName, StringType()))

        # Use first value column's type for value column (or StringType as fallback)
        if value_cols:
            first_value_field = [
                f for f in self.schema.fields if f.name == value_cols[0]
            ][0]
            value_type = first_value_field.dataType
        else:
            value_type = StringType()
        fields.append(StructField(valueColumnName, value_type))

        return DataFrame(result_data, StructType(fields), self.storage)

    def to(self, schema: Union[str, StructType]) -> "DataFrame":
        """Apply schema with casting (PySpark 3.4+).

        Args:
            schema: Target schema (DDL string or StructType)

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

        return DataFrame(result_data, target_schema, self.storage)

    def withMetadata(self, columnName: str, metadata: Dict[str, Any]) -> "DataFrame":
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
                new_field = StructField(
                    field.name, field.dataType, field.nullable, metadata
                )
                new_fields.append(new_field)
            else:
                new_fields.append(field)

        new_schema = StructType(new_fields)
        return DataFrame(self.data, new_schema, self.storage)

    def observe(self, name: str, *exprs: "Column") -> "DataFrame":
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
        new_df = DataFrame(
            data=self.data,
            schema=self.schema,
            storage=self.storage,
            operations=self._operations_queue,
        )
        new_df._observations = dict(self._observations)
        new_df._observations[name] = exprs
        return new_df

    @property
    def write(self) -> "DataFrameWriter":
        """Get DataFrame writer (PySpark-compatible property)."""
        return DataFrameWriter(self, self.storage)

    def _parse_cast_type_string(self, type_str: str) -> DataType:
        """Parse a cast type string to DataType."""
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
