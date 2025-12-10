"""
Polars materializer for lazy DataFrame operations.

This module provides materialization of lazy DataFrame operations using Polars,
replacing SQL-based materialization with Polars DataFrame operations.
"""

from typing import Any
import polars as pl
from mock_spark.spark_types import StructType, Row
from .expression_translator import PolarsExpressionTranslator
from .operation_executor import PolarsOperationExecutor


class PolarsMaterializer:
    """Materializes lazy operations using Polars."""

    def __init__(self) -> None:
        """Initialize Polars materializer."""
        self.translator = PolarsExpressionTranslator()
        self.operation_executor = PolarsOperationExecutor(self.translator)

    def materialize(
        self,
        data: list[dict[str, Any]],
        schema: StructType,
        operations: list[tuple[str, Any]],
    ) -> list[Row]:
        """Materialize lazy operations into actual data.

        Args:
            data: Initial data
            schema: DataFrame schema
            operations: List of queued operations (operation_name, payload)

        Returns:
            List of result rows
        """
        # Check if we have operations that require processing even with empty data
        # (e.g., union with non-empty DataFrame)
        has_union_operation = any(op_name == "union" for op_name, _ in operations)

        if not data and not has_union_operation:
            # Empty DataFrame with no operations that need processing
            return []

        # Convert data to Polars DataFrame
        # For empty DataFrames, create from schema if available
        if not data and schema.fields:
            from .type_mapper import mock_type_to_polars_dtype

            schema_dict = {}
            for field in schema.fields:
                polars_dtype = mock_type_to_polars_dtype(field.dataType)
                schema_dict[field.name] = pl.Series(field.name, [], dtype=polars_dtype)
            df = pl.DataFrame(schema_dict)
        elif not data:
            # Empty DataFrame with no schema
            df = pl.DataFrame()
        else:
            # Create DataFrame from data
            # Only enforce schema types if we have a union operation (to prevent type mismatches)
            # For other operations, let Polars infer types naturally
            df = pl.DataFrame(data)
            if has_union_operation and schema.fields:
                from .type_mapper import mock_type_to_polars_dtype

                cast_exprs = []
                for field in schema.fields:
                    polars_dtype = mock_type_to_polars_dtype(field.dataType)
                    # Only cast if column exists and type doesn't match
                    # Only cast numeric types to prevent Int32/Int64 mismatches
                    if (
                        field.name in df.columns
                        and df[field.name].dtype != polars_dtype
                        and polars_dtype in (pl.Int32, pl.Int64, pl.Float32, pl.Float64)
                    ):
                        # Only cast numeric types (Int32/Int64) to prevent union issues
                        # Don't cast string/datetime types as they can cause schema errors
                        cast_exprs.append(pl.col(field.name).cast(polars_dtype))

                if cast_exprs:
                    df = df.with_columns(cast_exprs)

        # Use lazy evaluation for better performance
        lazy_df = df.lazy()

        # Track current schema as operations are applied
        current_schema = schema

        # Apply operations in sequence
        for op_name, payload in operations:
            if op_name == "filter":
                # Filter operation - no schema change
                lazy_df = lazy_df.filter(self.translator.translate(payload))
            elif op_name == "select":
                # Select operation - need to collect first for window functions
                df_collected = lazy_df.collect()
                lazy_df = self.operation_executor.apply_select(
                    df_collected, payload
                ).lazy()
                # Update schema after select
                from ...dataframe.schema.schema_manager import SchemaManager

                current_schema = SchemaManager.project_schema_with_operations(
                    current_schema, [(op_name, payload)]
                )
            elif op_name == "withColumn":
                # WithColumn operation - need to collect first for window functions
                df_collected = lazy_df.collect()
                column_name, expression = payload
                result_df = self.operation_executor.apply_with_column(
                    df_collected, column_name, expression
                )

                # Convert result back to lazy
                # Window functions are already fully materialized in apply_with_column
                lazy_df = result_df.lazy()
                # Update schema after withColumn
                from ...dataframe.schema.schema_manager import SchemaManager

                current_schema = SchemaManager.project_schema_with_operations(
                    current_schema, [(op_name, payload)]
                )
            elif op_name == "join":
                # Join operation - need to handle separately
                other_df, on, how = payload
                # Convert other_df to Polars DataFrame if needed
                if not isinstance(other_df, pl.DataFrame):
                    other_data = getattr(other_df, "data", [])
                    if not other_data:
                        # Empty DataFrame - create from schema if available
                        if hasattr(other_df, "schema"):
                            from .type_mapper import mock_type_to_polars_dtype

                            schema_dict = {}
                            for field in other_df.schema.fields:
                                polars_dtype = mock_type_to_polars_dtype(field.dataType)
                                schema_dict[field.name] = pl.Series(
                                    field.name, [], dtype=polars_dtype
                                )
                            other_df = pl.DataFrame(schema_dict)
                        else:
                            other_df = pl.DataFrame()
                    else:
                        other_df = pl.DataFrame(other_data)
                # Collect lazy_df before joining
                df_collected = lazy_df.collect()
                result_df = self.operation_executor.apply_join(
                    df_collected, other_df, on=on, how=how
                )
                lazy_df = result_df.lazy()
            elif op_name == "union":
                # Union operation - need to collect first
                df_collected = lazy_df.collect()
                other_df_payload = payload

                # Validate schema compatibility before union (PySpark compatibility)
                # current_schema is the schema after all previous operations
                if hasattr(other_df_payload, "schema"):
                    other_schema = other_df_payload.schema
                else:
                    # If other_df doesn't have schema, we can't validate - skip validation
                    # This shouldn't happen in normal usage
                    other_schema = None

                if other_schema is not None:
                    from ...dataframe.operations.set_operations import SetOperations
                    from ...core.exceptions.analysis import AnalysisException

                    # Check column count
                    if len(current_schema.fields) != len(other_schema.fields):
                        raise AnalysisException(
                            f"Union can only be performed on tables with the same number of columns, "
                            f"but the first table has {len(current_schema.fields)} columns and "
                            f"the second table has {len(other_schema.fields)} columns"
                        )

                    # Check column names and types
                    for i, (field1, field2) in enumerate(
                        zip(current_schema.fields, other_schema.fields)
                    ):
                        if field1.name != field2.name:
                            raise AnalysisException(
                                f"Union can only be performed on tables with compatible column names. "
                                f"Column {i} name mismatch: '{field1.name}' vs '{field2.name}'"
                            )

                        # Type compatibility check
                        if not SetOperations._are_types_compatible(
                            field1.dataType, field2.dataType
                        ):
                            raise AnalysisException(
                                f"Union can only be performed on tables with compatible column types. "
                                f"Column '{field1.name}' type mismatch: "
                                f"{field1.dataType} vs {field2.dataType}"
                            )

                # Convert other_df to Polars DataFrame if needed
                if not isinstance(other_df_payload, pl.DataFrame):
                    other_data = getattr(other_df_payload, "data", [])
                    if not other_data:
                        # Empty DataFrame - create from schema if available
                        if hasattr(other_df_payload, "schema"):
                            from .type_mapper import mock_type_to_polars_dtype

                            schema_dict = {}
                            for field in other_df_payload.schema.fields:
                                polars_dtype = mock_type_to_polars_dtype(field.dataType)
                                schema_dict[field.name] = pl.Series(
                                    field.name, [], dtype=polars_dtype
                                )
                            other_df = pl.DataFrame(schema_dict)
                        else:
                            other_df = pl.DataFrame()
                    else:
                        # Create DataFrame from data
                        # Only enforce schema types for union operations to prevent type mismatches
                        other_df = pl.DataFrame(other_data)
                        if (
                            hasattr(other_df_payload, "schema")
                            and other_df_payload.schema.fields
                        ):
                            from .type_mapper import mock_type_to_polars_dtype

                            cast_exprs = []
                            for field in other_df_payload.schema.fields:
                                polars_dtype = mock_type_to_polars_dtype(field.dataType)
                                # Only cast if column exists and type doesn't match
                                # Only cast numeric types to prevent Int32/Int64 mismatches
                                # Only cast numeric types (Int32/Int64) to prevent union issues
                                # Don't cast string/datetime types as they can cause schema errors
                                if (
                                    field.name in other_df.columns
                                    and other_df[field.name].dtype != polars_dtype
                                    and polars_dtype
                                    in (pl.Int32, pl.Int64, pl.Float32, pl.Float64)
                                ):
                                    cast_exprs.append(
                                        pl.col(field.name).cast(polars_dtype)
                                    )

                            if cast_exprs:
                                other_df = other_df.with_columns(cast_exprs)
                else:
                    other_df = other_df_payload

                result_df = self.operation_executor.apply_union(df_collected, other_df)
                lazy_df = result_df.lazy()
                # Schema doesn't change after union (uses first DataFrame's schema)
                # current_schema remains the same
            elif op_name == "orderBy":
                # OrderBy operation - can be done lazily
                # Payload can be just columns (tuple) or (columns, ascending)
                if (
                    isinstance(payload, tuple)
                    and len(payload) == 2
                    and isinstance(payload[1], bool)
                ):
                    columns, ascending = payload
                else:
                    # Payload is just columns, default to ascending=True
                    columns = (
                        payload if isinstance(payload, (tuple, list)) else (payload,)
                    )
                    ascending = True

                # Build sort expressions with descending flags
                # Polars doesn't have .desc() on Expr, use sort() with descending parameter
                sort_by = []
                descending_flags = []
                for col in columns:
                    is_desc = False
                    col_expr = None
                    if isinstance(col, str):
                        col_expr = pl.col(col)
                        is_desc = not ascending
                    elif hasattr(col, "operation") and col.operation == "desc":
                        col_name = (
                            col.column.name if hasattr(col, "column") else col.name
                        )
                        col_expr = pl.col(col_name)
                        is_desc = True
                    else:
                        # For ColumnOperation with asc/desc, get the actual column name
                        if hasattr(col, "column") and hasattr(col.column, "name"):
                            col_name = col.column.name
                        elif hasattr(col, "name"):
                            col_name = col.name
                        else:
                            col_name = str(col)
                        # Remove any " ASC" or " DESC" suffix that might be in the name
                        col_name = (
                            col_name.replace(" ASC", "").replace(" DESC", "").strip()
                        )
                        col_expr = pl.col(col_name)
                        is_desc = not ascending

                    if col_expr is not None:
                        sort_by.append(col_expr)
                        descending_flags.append(is_desc)

                if sort_by:
                    # Polars sort() accepts by (list of expressions) and descending (list of bools)
                    lazy_df = lazy_df.sort(sort_by, descending=descending_flags)
            elif op_name == "limit":
                # Limit operation
                n = payload
                lazy_df = lazy_df.head(n)
            elif op_name == "offset":
                # Offset operation (skip first n rows)
                n = payload
                lazy_df = lazy_df.slice(n)
            elif op_name == "groupBy":
                # GroupBy operation - need to collect first
                df_collected = lazy_df.collect()
                group_by, aggs = payload
                result_df = self.operation_executor.apply_group_by_agg(
                    df_collected, group_by, aggs
                )
                lazy_df = result_df.lazy()
            elif op_name == "distinct":
                # Distinct operation
                lazy_df = lazy_df.unique()
            elif op_name == "drop":
                # Drop operation
                columns = payload
                lazy_df = lazy_df.drop(columns)
            elif op_name == "withColumnRenamed":
                # WithColumnRenamed operation
                old_name, new_name = payload
                lazy_df = lazy_df.rename({old_name: new_name})
            else:
                raise ValueError(f"Unsupported operation: {op_name}")

        # Materialize (collect) the lazy DataFrame
        result_df = lazy_df.collect()

        # Convert to list[Row]
        # For joins with duplicate columns, Polars uses _right suffix
        # We need to convert these to match PySpark's duplicate column handling
        rows = []
        for row_dict in result_df.to_dicts():
            # Create Row from dict - Row will handle the conversion
            # The schema will be applied later in _convert_materialized_rows
            rows.append(Row(row_dict, schema=None))
        return rows

    def close(self) -> None:
        """Close the materializer and clean up resources."""
        # Polars doesn't require explicit cleanup
        pass
