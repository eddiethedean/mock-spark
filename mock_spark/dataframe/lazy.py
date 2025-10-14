"""
Lazy Evaluation Engine for DataFrames

This module handles lazy evaluation, operation queuing, and materialization
for MockDataFrame. Extracted from dataframe.py to improve organization.
"""

from typing import Any, List, TYPE_CHECKING

if TYPE_CHECKING:
    from mock_spark.dataframe import MockDataFrame
    from mock_spark.spark_types import MockStructType


class LazyEvaluationEngine:
    """Handles lazy evaluation and materialization for DataFrames."""

    @staticmethod
    def queue_operation(df: "MockDataFrame", op_name: str, payload: Any) -> "MockDataFrame":
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

        return MockDataFrame(
            df.data,
            new_schema,
            df.storage,  # type: ignore[has-type]
            is_lazy=True,
            operations=df._operations_queue + [(op_name, payload)],
        )

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

        # Use backend factory to get materializer
        try:
            from mock_spark.backend.factory import BackendFactory

            materializer = BackendFactory.create_materializer("duckdb")
            try:
                # Let materializer optimize and execute the operations
                rows = materializer.materialize(df.data, df.schema, df._operations_queue)

                # Convert rows back to data format
                materialized_data = LazyEvaluationEngine._convert_materialized_rows(rows, df.schema)

                # Create new eager DataFrame with materialized data
                from ..dataframe import MockDataFrame

                return MockDataFrame(materialized_data, df.schema, df.storage, is_lazy=False)  # type: ignore[has-type]
            finally:
                materializer.close()

        except ImportError:
            # Fallback to manual materialization if DuckDB is not available
            return LazyEvaluationEngine._materialize_manual(df)

    @staticmethod
    def _convert_materialized_rows(rows: List[Any], schema: "MockStructType") -> List[dict]:
        """Convert materialized rows to proper data format with type conversion.

        Args:
            rows: Rows from SQLAlchemy materializer
            schema: Expected schema

        Returns:
            List of dictionaries with proper types
        """
        from ..spark_types import ArrayType, IntegerType, LongType, DoubleType

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
        from ..spark_types import (
            StringType,
            LongType,
            DoubleType,
            IntegerType,
            MockStructField,
            MockStructType,
            ArrayType,
        )

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
                        elif cast_type.lower() in ["int", "integer", "long", "bigint"]:
                            new_fields.append(MockStructField(col_name, LongType()))
                        elif cast_type.lower() in ["string", "varchar"]:
                            new_fields.append(MockStructField(col_name, StringType()))
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
                    new_fields.append(MockStructField(col_name, ArrayType(StringType())))
                else:
                    # Default to StringType for unknown operations
                    new_fields.append(MockStructField(col_name, StringType()))
            elif hasattr(col, "conditions") and hasattr(col, "default_value"):
                # Handle MockCaseWhen objects
                col_name = col.name
                # CASE WHEN can return various types - infer from default value
                if hasattr(col.default_value, "name"):
                    # Default is a column reference - try to find its type
                    found = False
                    for field in df.schema.fields:
                        if field.name == col.default_value.name:
                            new_fields.append(MockStructField(col_name, field.dataType))
                            found = True
                            break
                    if not found:
                        new_fields.append(MockStructField(col_name, IntegerType()))
                elif isinstance(col.default_value, int):
                    new_fields.append(MockStructField(col_name, IntegerType()))
                elif isinstance(col.default_value, float):
                    new_fields.append(MockStructField(col_name, DoubleType()))
                else:
                    # Default to IntegerType for CASE WHEN
                    new_fields.append(MockStructField(col_name, IntegerType()))
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
        if hasattr(filter_condition, "column") and hasattr(filter_condition.column, "name"):
            column_name = filter_condition.column.name
            return column_name in original_columns
        elif hasattr(filter_condition, "name"):
            column_name = filter_condition.name
            return column_name in original_columns

        return True  # Default to early filter if we can't determine
