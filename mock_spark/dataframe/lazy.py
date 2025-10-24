"""
Lazy Evaluation Engine for DataFrames

This module handles lazy evaluation, operation queuing, and materialization
for MockDataFrame. Extracted from dataframe.py to improve organization.
"""

from typing import Any, List, TYPE_CHECKING
from ..spark_types import (
    StringType,
    MockStructField,
    DoubleType,
    LongType,
    IntegerType,
    BooleanType,
    MockDataType,
    MockStructType,
    ArrayType,
)
from ..optimizer.query_optimizer import OperationType

if TYPE_CHECKING:
    from mock_spark.dataframe import MockDataFrame
    from mock_spark.spark_types import MockStructType


class LazyEvaluationEngine:
    """Handles lazy evaluation and materialization for DataFrames."""

    def __init__(self, enable_optimization: bool = True):
        """Initialize lazy evaluation engine.

        Args:
            enable_optimization: Whether to enable query optimization
        """
        self.enable_optimization = enable_optimization
        self._optimizer = None
        if enable_optimization:
            try:
                from ..optimizer import QueryOptimizer

                self._optimizer = QueryOptimizer()
            except ImportError:
                # Fallback if optimizer is not available
                self._optimizer = None

    @staticmethod
    def queue_operation(
        df: "MockDataFrame", op_name: str, payload: Any
    ) -> "MockDataFrame":
        """Queue an operation for lazy evaluation.

        Args:
            df: Source DataFrame
            op_name: Operation name (select, filter, join, etc.)
            payload: Operation parameters

        Returns:
            New DataFrame with queued operation
        """
        from ..dataframe import MockDataFrame

        # Infer new schema for operations that change schema
        new_schema = df.schema
        if op_name == "select":
            new_schema = LazyEvaluationEngine._infer_select_schema(df, payload)
        elif op_name == "join":
            new_schema = LazyEvaluationEngine._infer_join_schema(df, payload)
        elif op_name == "withColumn":
            new_schema = LazyEvaluationEngine._infer_withcolumn_schema(df, payload)

        return MockDataFrame(
            df.data,
            new_schema,
            df.storage,  # type: ignore[has-type]
            is_lazy=True,
            operations=df._operations_queue + [(op_name, payload)],
        )

    def optimize_operations(self, operations: List[tuple]) -> List[tuple]:
        """Optimize operations using the query optimizer.

        Args:
            operations: List of (operation_name, payload) tuples

        Returns:
            Optimized list of operations
        """
        if not self.enable_optimization or self._optimizer is None:
            return operations

        try:
            # Convert operations to optimizer format
            optimizer_ops = self._convert_to_optimizer_operations(operations)

            # Apply optimization
            optimized_ops = self._optimizer.optimize(optimizer_ops)

            # Convert back to original format
            return self._convert_from_optimizer_operations(optimized_ops)
        except Exception:
            # If optimization fails, return original operations
            return operations

    def _convert_to_optimizer_operations(self, operations: List[tuple]) -> List[Any]:
        """Convert operations to optimizer format."""
        from ..optimizer.query_optimizer import Operation, OperationType

        optimizer_ops = []
        for op_name, payload in operations:
            if op_name == "select":
                optimizer_ops.append(
                    Operation(
                        type=OperationType.SELECT,
                        columns=payload if isinstance(payload, list) else [payload],
                        predicates=[],
                        join_conditions=[],
                        group_by_columns=[],
                        order_by_columns=[],
                        limit_count=None,
                        window_specs=[],
                        metadata={},
                    )
                )
            elif op_name == "filter":
                optimizer_ops.append(
                    Operation(
                        type=OperationType.FILTER,
                        columns=[],
                        predicates=[
                            {"column": str(payload), "operator": "=", "value": True}
                        ],
                        join_conditions=[],
                        group_by_columns=[],
                        order_by_columns=[],
                        limit_count=None,
                        window_specs=[],
                        metadata={},
                    )
                )
            # Add more operation types as needed

        return optimizer_ops

    def _convert_from_optimizer_operations(
        self, optimizer_ops: List[Any]
    ) -> List[tuple]:
        """Convert optimizer operations back to original format."""
        operations = []
        for op in optimizer_ops:
            if op.type == OperationType.SELECT:
                operations.append(("select", op.columns))
            elif op.type == OperationType.FILTER:
                for pred in op.predicates:
                    operations.append(("filter", pred["column"]))
            # Add more operation types as needed

        return operations

    @staticmethod
    def materialize(df: "MockDataFrame") -> "MockDataFrame":
        """Materialize queued lazy operations.

        Args:
            df: Lazy DataFrame with queued operations

        Returns:
            Eager DataFrame with operations applied
        """
        if not df._operations_queue:
            from ..dataframe import MockDataFrame

            return MockDataFrame(df.data, df.schema, df.storage, is_lazy=False)  # type: ignore[has-type]

        # Check if operations require manual materialization
        if LazyEvaluationEngine._requires_manual_materialization(df._operations_queue):
            return LazyEvaluationEngine._materialize_manual(df)

        # Use backend factory to get materializer
        try:
            from mock_spark.backend.factory import BackendFactory

            materializer = BackendFactory.create_materializer("duckdb")
            try:
                # Let materializer optimize and execute the operations
                rows = materializer.materialize(
                    df.data, df.schema, df._operations_queue
                )

                # Convert rows back to data format
                materialized_data = LazyEvaluationEngine._convert_materialized_rows(
                    rows, df.schema
                )

                # Check if DuckDB returned default values (0.0 for numeric functions)
                # If so, fall back to manual materialization
                if LazyEvaluationEngine._has_default_values(
                    materialized_data, df.schema
                ):
                    return LazyEvaluationEngine._materialize_manual(df)

                # Create new eager DataFrame with materialized data
                from ..dataframe import MockDataFrame

                return MockDataFrame(
                    materialized_data,
                    df.schema,
                    df.storage,  # type: ignore[has-type]
                    is_lazy=False,
                )
            finally:
                materializer.close()

        except ImportError:
            # Fallback to manual materialization if DuckDB is not available
            return LazyEvaluationEngine._materialize_manual(df)

    @staticmethod
    def _has_default_values(data: List[dict], schema: "MockStructType") -> bool:
        """Check if data contains default values that indicate DuckDB couldn't handle the operations."""
        if not data:
            return False

        # Check if any numeric fields have default values (0.0, 0, None)
        for row in data:
            for field in schema.fields:
                if field.name in row:
                    value = row[field.name]
                    # Check for default values that indicate DuckDB couldn't evaluate the function
                    if value == 0.0 and field.dataType.__class__.__name__ in [
                        "DoubleType",
                        "FloatType",
                    ]:
                        return True
                    if value == 0 and field.dataType.__class__.__name__ in [
                        "IntegerType",
                        "LongType",
                    ]:
                        return True
                    if value is None and field.dataType.__class__.__name__ in [
                        "StringType"
                    ]:
                        return True
                    # Check for None values in any field (indicates DuckDB couldn't evaluate)
                    if value is None:
                        return True

        return False

    @staticmethod
    def _requires_manual_materialization(operations_queue: List[tuple]) -> bool:
        """Check if operations require manual materialization (DuckDB can't handle them)."""
        for op_name, op_val in operations_queue:
            if op_name == "select":
                # Check if select contains operations that DuckDB can't handle
                for col in op_val:
                    # Check for MockCaseWhen (when/otherwise expressions)
                    if hasattr(col, "conditions"):
                        return True
                    # Check for MockColumnOperation
                    if hasattr(col, "operation"):
                        # Check for operations that require MockDataFrame evaluation
                        if col.operation in [
                            "months_between",
                            "datediff",
                            "cast",
                            "when",
                            "otherwise",
                        ]:
                            return True
                        # Check for comparison operations
                        if col.operation in ["==", "!=", "<", ">", "<=", ">="]:
                            return True
                        # Check for arithmetic operations with cast
                        if col.operation in ["+", "-", "*", "/", "%"]:
                            # Check if operands contain cast operations
                            if (
                                hasattr(col, "column")
                                and hasattr(col.column, "operation")
                                and col.column.operation == "cast"
                            ):
                                return True
                            if (
                                hasattr(col, "value")
                                and hasattr(col.value, "operation")
                                and col.value.operation == "cast"
                            ):
                                return True
        return False

    @staticmethod
    def _convert_materialized_rows(
        rows: List[Any], schema: "MockStructType"
    ) -> List[dict]:
        """Convert materialized rows to proper data format with type conversion.

        Args:
            rows: Rows from SQLAlchemy materializer
            schema: Expected schema

        Returns:
            List of dictionaries with proper types
        """
        from ..spark_types import ArrayType

        materialized_data = []
        for row in rows:
            row_dict = row.asDict()

            # Convert values to match their declared schema types
            for field in schema.fields:
                if field.name not in row_dict:
                    continue

                value = row_dict[field.name]

                # Handle ArrayType
                if isinstance(field.dataType, ArrayType):
                    # DuckDB may return arrays as strings like "['a', 'b']" or as lists
                    if isinstance(value, str):
                        # Try different array formats
                        if value.startswith("[") and value.endswith("]"):
                            # Parse string representation of list: "['a', 'b']"
                            import ast

                            try:
                                row_dict[field.name] = ast.literal_eval(value)
                            except:  # noqa: E722
                                # If parsing fails, split manually
                                row_dict[field.name] = value[1:-1].split(",")
                        elif value.startswith("{") and value.endswith("}"):
                            # PostgreSQL/DuckDB array format: "{a,b}"
                            row_dict[field.name] = value[1:-1].split(",")

                # Handle numeric types that come back as strings
                elif isinstance(field.dataType, (IntegerType, LongType)):
                    if isinstance(value, str):
                        try:
                            row_dict[field.name] = int(value)
                        except (ValueError, TypeError):
                            pass  # Keep as string if conversion fails

                elif isinstance(field.dataType, DoubleType):
                    if isinstance(value, str):
                        try:
                            row_dict[field.name] = float(value)
                        except (ValueError, TypeError):
                            pass  # Keep as string if conversion fails
                    else:
                        # Convert Decimal or other numeric types to float
                        try:
                            row_dict[field.name] = float(value)
                        except (ValueError, TypeError):
                            pass  # Keep original value if conversion fails

            materialized_data.append(row_dict)

        return materialized_data

    @staticmethod
    def _materialize_manual(df: "MockDataFrame") -> "MockDataFrame":
        """Fallback manual materialization when DuckDB is not available.

        Args:
            df: Lazy DataFrame

        Returns:
            Eager DataFrame with operations applied
        """
        from ..dataframe import MockDataFrame

        current = MockDataFrame(df.data, df.schema, df.storage, is_lazy=False)  # type: ignore[has-type]
        for op_name, op_val in df._operations_queue:
            try:
                if op_name == "filter":
                    current = current.filter(op_val)  # eager path
                elif op_name == "withColumn":
                    col_name, col = op_val
                    current = current.withColumn(col_name, col)  # eager path
                elif op_name == "select":
                    current = current.select(*op_val)  # eager path
                elif op_name == "groupBy":
                    current = current.groupBy(*op_val)  # type: ignore[assignment] # Returns MockGroupedData
                elif op_name == "join":
                    other_df, on, how = op_val
                    current = current.join(other_df, on, how)  # eager path
                elif op_name == "union":
                    other_df = op_val
                    current = current.union(other_df)  # eager path
                elif op_name == "orderBy":
                    current = current.orderBy(*op_val)  # eager path
                else:
                    # Unknown ops ignored for now
                    continue
            except Exception as e:
                # If an operation fails due to column not found,
                # it might be because the operation was queued but the column
                # was removed by a previous operation. Skip this operation.
                if "Column" in str(e) and "does not exist" in str(e):
                    # Skip this operation - it's likely a dependency issue
                    continue
                else:
                    # Re-raise other exceptions
                    raise e
        return current

    @staticmethod
    def _infer_select_schema(df: "MockDataFrame", columns: Any) -> "MockStructType":
        """Infer schema for select operation.

        Args:
            df: Source DataFrame
            columns: Columns to select

        Returns:
            Inferred schema for selected columns
        """
        from ..functions import MockAggregateFunction

        new_fields = []
        for col in columns:
            if isinstance(col, str):
                if col == "*":
                    # Add all existing fields
                    new_fields.extend(df.schema.fields)
                else:
                    # Use existing field, or add as StringType if not found
                    found = False
                    for field in df.schema.fields:
                        if field.name == col:
                            new_fields.append(field)
                            found = True
                            break
                    if not found:
                        # Column not in current schema, might come from join
                        new_fields.append(MockStructField(col, StringType()))
            elif hasattr(col, "operation") and hasattr(col, "column"):
                # Handle MockColumnOperation
                col_name = col.name

                # Check operation type
                if col.operation == "cast":
                    # Cast operation - infer type from cast parameter
                    cast_type = getattr(col, "value", "string")
                    if isinstance(cast_type, str):
                        # String type name, convert to actual type
                        if cast_type.lower() in ["double", "float"]:
                            new_fields.append(MockStructField(col_name, DoubleType()))
                        elif cast_type.lower() in ["int", "integer"]:
                            new_fields.append(MockStructField(col_name, IntegerType()))
                        elif cast_type.lower() in ["long", "bigint"]:
                            new_fields.append(MockStructField(col_name, LongType()))
                        elif cast_type.lower() in ["string", "varchar"]:
                            new_fields.append(MockStructField(col_name, StringType()))
                        elif cast_type.lower() in ["boolean", "bool"]:
                            new_fields.append(MockStructField(col_name, BooleanType()))
                        else:
                            new_fields.append(MockStructField(col_name, StringType()))
                    else:
                        # Type object, use directly
                        new_fields.append(MockStructField(col_name, cast_type))
                elif col.operation in ["upper", "lower"]:
                    new_fields.append(MockStructField(col_name, StringType()))
                elif col.operation == "length":
                    new_fields.append(MockStructField(col_name, IntegerType()))
                elif col.operation == "split":
                    # Split returns ArrayType of strings
                    new_fields.append(
                        MockStructField(col_name, ArrayType(StringType()))
                    )
                elif col.operation in ["isnull", "isnotnull", "isnan"]:
                    # Boolean operations - always return BooleanType and are non-nullable

                    new_fields.append(
                        MockStructField(col_name, BooleanType(), nullable=False)
                    )
                elif col.operation == "coalesce":
                    # Coalesce with a non-null literal fallback is non-nullable
                    # Determine the result type from the first argument
                    source_type: Any = StringType()
                    if hasattr(col, "column") and hasattr(col.column, "name"):
                        # Find source column type
                        for field in df.schema.fields:
                            if field.name == col.column.name:
                                source_type = field.dataType
                                break
                    # Mark as non-nullable if there's a literal fallback
                    new_fields.append(
                        MockStructField(col_name, source_type, nullable=False)
                    )
                elif col.operation == "datediff":
                    new_fields.append(MockStructField(col_name, IntegerType()))
                elif col.operation == "months_between":
                    new_fields.append(MockStructField(col_name, DoubleType()))
                elif col.operation in [
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
                    new_fields.append(MockStructField(col_name, IntegerType()))
                else:
                    # Default to StringType for unknown operations
                    new_fields.append(MockStructField(col_name, StringType()))
            elif isinstance(col, MockAggregateFunction):
                # Handle aggregate functions - set proper nullability
                col_name = col.name

                # Determine nullable based on function type
                non_nullable_functions = {
                    "count",
                    "countDistinct",
                    "count_if",
                    "row_number",
                    "rank",
                    "dense_rank",
                }

                nullable = col.function_name not in non_nullable_functions

                # Use provided data type or default to LongType for counts, DoubleType otherwise
                if col.data_type:
                    data_type = col.data_type
                    if hasattr(data_type, "nullable"):
                        data_type.nullable = nullable
                elif col.function_name in {"count", "countDistinct", "count_if"}:
                    data_type = LongType(nullable=nullable)
                else:
                    data_type = DoubleType(nullable=nullable)

                new_fields.append(
                    MockStructField(col_name, data_type, nullable=nullable)
                )

            elif hasattr(col, "function_name") and hasattr(col, "window_spec"):
                # Handle MockWindowFunction (e.g., rank().over(window))
                col_name = col.name

                # Window functions that are non-nullable
                non_nullable_window_functions = {
                    "row_number",
                    "rank",
                    "dense_rank",
                }

                # Determine type and nullability based on function
                if col.function_name in non_nullable_window_functions:
                    # Ranking functions return IntegerType and are non-nullable
                    new_fields.append(
                        MockStructField(col_name, IntegerType(), nullable=False)
                    )
                elif col.function_name in ["lag", "lead"]:
                    # Lag/lead can return null (out of bounds)
                    if col.column_name:
                        # Find source column type
                        source_type2: Any = StringType()
                        for field in df.schema.fields:
                            if field.name == col.column_name:
                                source_type2 = field.dataType
                                break
                        new_fields.append(
                            MockStructField(col_name, source_type2, nullable=True)
                        )
                    else:
                        new_fields.append(
                            MockStructField(col_name, StringType(), nullable=True)
                        )
                elif col.function_name in ["sum", "avg", "min", "max"]:
                    # Aggregate window functions - nullable
                    new_fields.append(
                        MockStructField(col_name, DoubleType(), nullable=True)
                    )
                elif col.function_name in ["count", "countDistinct"]:
                    # Count functions are non-nullable
                    new_fields.append(
                        MockStructField(col_name, LongType(), nullable=False)
                    )
                else:
                    # Default for other window functions
                    new_fields.append(
                        MockStructField(col_name, DoubleType(), nullable=True)
                    )

            elif hasattr(col, "value") and hasattr(col, "data_type"):
                # Handle MockLiteral objects - literals are never nullable
                col_name = col.name
                # Use the literal's data_type and explicitly set nullable=False

                data_type = col.data_type
                # Create a new instance of the data type with nullable=False
                data_type_non_null: MockDataType
                if isinstance(data_type, BooleanType):
                    data_type_non_null = BooleanType(nullable=False)
                elif isinstance(data_type, IntegerType):
                    data_type_non_null = IntegerType(nullable=False)
                elif isinstance(data_type, LongType):
                    data_type_non_null = LongType(nullable=False)
                elif isinstance(data_type, DoubleType):
                    data_type_non_null = DoubleType(nullable=False)
                elif isinstance(data_type, StringType):
                    data_type_non_null = StringType(nullable=False)
                else:
                    # For other types, create a new instance with nullable=False
                    data_type_non_null = data_type.__class__(nullable=False)

                new_fields.append(
                    MockStructField(col_name, data_type_non_null, nullable=False)
                )
            elif hasattr(col, "conditions") and hasattr(col, "default_value"):
                # Handle MockCaseWhen objects - use get_result_type() method
                col_name = col.name
                from ..functions.conditional import MockCaseWhen

                if isinstance(col, MockCaseWhen):
                    # Use the proper type inference method
                    inferred_type = col.get_result_type()
                    new_fields.append(
                        MockStructField(col_name, inferred_type, nullable=False)
                    )
                else:
                    # Fallback for other conditional objects
                    new_fields.append(MockStructField(col_name, IntegerType()))
            elif hasattr(col, "conditions"):
                # Handle MockCaseWhen objects that didn't match the first condition
                col_name = col.name
                from ..functions.conditional import MockCaseWhen

                if isinstance(col, MockCaseWhen):
                    # Use the proper type inference method
                    inferred_type = col.get_result_type()
                    new_fields.append(
                        MockStructField(col_name, inferred_type, nullable=False)
                    )
                else:
                    # Default to StringType for unknown operations
                    new_fields.append(MockStructField(col_name, StringType()))
            elif hasattr(col, "name"):
                # Handle MockColumn
                col_name = col.name
                if col_name == "*":
                    # Add all existing fields
                    new_fields.extend(df.schema.fields)
                else:
                    # Use existing field or add as string
                    found = False
                    for field in df.schema.fields:
                        if field.name == col_name:
                            new_fields.append(field)
                            found = True
                            break
                    if not found:
                        # Column not in current schema, might come from join
                        new_fields.append(MockStructField(col_name, StringType()))

        return MockStructType(new_fields)

    @staticmethod
    def _infer_join_schema(df: "MockDataFrame", join_params: Any) -> "MockStructType":
        """Infer schema for join operation.

        Args:
            df: Source DataFrame
            join_params: Join parameters (other_df, on, how)

        Returns:
            Inferred schema after join
        """
        from ..spark_types import MockStructType

        other_df, on, how = join_params

        # Start with all fields from left DataFrame
        new_fields = df.schema.fields.copy()

        # Add fields from right DataFrame that aren't already present
        for field in other_df.schema.fields:
            if not any(f.name == field.name for f in new_fields):
                new_fields.append(field)

        return MockStructType(new_fields)

    @staticmethod
    def _infer_withcolumn_schema(
        df: "MockDataFrame", withcolumn_params: Any
    ) -> "MockStructType":
        """Infer schema for withColumn operation.

        Args:
            df: Source DataFrame
            withcolumn_params: withColumn parameters (col_name, col)

        Returns:
            Inferred schema after withColumn
        """
        from ..spark_types import (
            MockStructType,
            MockStructField,
            BooleanType,
            IntegerType,
            LongType,
            DoubleType,
            StringType,
            DateType,
            TimestampType,
            DecimalType,
        )
        from ..functions.core.column import MockColumnOperation

        col_name, col = withcolumn_params

        # Start with all existing fields except the one being added/replaced
        new_fields = [field for field in df.schema.fields if field.name != col_name]

        # Infer the type of the new column
        if (
            isinstance(col, MockColumnOperation)
            and hasattr(col, "operation")
            and col.operation == "cast"
        ):
            # Cast operation - use the target data type from col.value
            cast_type = col.value
            if isinstance(cast_type, str):
                # String type name, convert to actual type
                if cast_type.lower() in ["double", "float"]:
                    new_fields.append(MockStructField(col_name, DoubleType()))
                elif cast_type.lower() in ["int", "integer"]:
                    new_fields.append(MockStructField(col_name, IntegerType()))
                elif cast_type.lower() in ["long", "bigint"]:
                    new_fields.append(MockStructField(col_name, LongType()))
                elif cast_type.lower() in ["string", "varchar"]:
                    new_fields.append(MockStructField(col_name, StringType()))
                elif cast_type.lower() in ["boolean", "bool"]:
                    new_fields.append(MockStructField(col_name, BooleanType()))
                elif cast_type.lower() in ["date"]:
                    new_fields.append(MockStructField(col_name, DateType()))
                elif cast_type.lower() in ["timestamp"]:
                    new_fields.append(MockStructField(col_name, TimestampType()))
                elif cast_type.lower().startswith("decimal"):
                    # Parse decimal(10,2) format
                    import re

                    match = re.match(r"decimal\((\d+),(\d+)\)", cast_type.lower())
                    if match:
                        precision, scale = int(match.group(1)), int(match.group(2))
                        new_fields.append(
                            MockStructField(col_name, DecimalType(precision, scale))
                        )
                    else:
                        new_fields.append(MockStructField(col_name, DecimalType(10, 2)))
                else:
                    # Default to StringType for unknown types
                    new_fields.append(MockStructField(col_name, StringType()))
            else:
                # Already a MockDataType object
                if hasattr(cast_type, "__class__"):
                    new_fields.append(
                        MockStructField(col_name, cast_type.__class__(nullable=True))
                    )
                else:
                    new_fields.append(MockStructField(col_name, cast_type))
        elif (
            isinstance(col, MockColumnOperation)
            and hasattr(col, "operation")
            and col.operation in ["+", "-", "*", "/", "%"]
        ):
            # Arithmetic operations - infer type from operands
            left_type = None
            right_type = None

            # Get left operand type
            if hasattr(col.column, "name"):
                for field in df.schema.fields:
                    if field.name == col.column.name:
                        left_type = field.dataType
                        break

            # Get right operand type
            if (
                hasattr(col, "value")
                and col.value is not None
                and hasattr(col.value, "name")
            ):
                for field in df.schema.fields:
                    if field.name == col.value.name:
                        right_type = field.dataType
                        break

            # If either operand is DoubleType, result is DoubleType
            if (left_type and isinstance(left_type, DoubleType)) or (
                right_type and isinstance(right_type, DoubleType)
            ):
                new_fields.append(MockStructField(col_name, DoubleType()))
            else:
                new_fields.append(MockStructField(col_name, LongType()))
        elif (
            isinstance(col, MockColumnOperation)
            and hasattr(col, "operation")
            and col.operation == "datediff"
        ):
            new_fields.append(MockStructField(col_name, IntegerType()))
        elif (
            isinstance(col, MockColumnOperation)
            and hasattr(col, "operation")
            and col.operation == "months_between"
        ):
            new_fields.append(MockStructField(col_name, DoubleType()))
        elif (
            isinstance(col, MockColumnOperation)
            and hasattr(col, "operation")
            and col.operation
            in [
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
            ]
        ):
            new_fields.append(MockStructField(col_name, IntegerType()))
        else:
            # For other column types, default to StringType
            # TODO: Add more sophisticated type inference for other operations
            new_fields.append(MockStructField(col_name, StringType()))

        return MockStructType(new_fields)

    @staticmethod
    def _filter_depends_on_original_columns(
        filter_condition: Any, original_schema: "MockStructType"
    ) -> bool:
        """Check if a filter condition depends on original columns.

        Args:
            filter_condition: Filter condition to check
            original_schema: Original schema before operations

        Returns:
            True if filter depends on original columns
        """
        # Get the original column names from the provided schema
        original_columns = {field.name for field in original_schema.fields}

        # Check if the filter references any of the original columns
        if hasattr(filter_condition, "column") and hasattr(
            filter_condition.column, "name"
        ):
            column_name = filter_condition.column.name
            return column_name in original_columns
        elif hasattr(filter_condition, "name"):
            column_name = filter_condition.name
            return column_name in original_columns

        return True  # Default to early filter if we can't determine
