"""
Polars materializer for lazy DataFrame operations.

This module provides materialization of lazy DataFrame operations using Polars,
replacing SQL-based materialization with Polars DataFrame operations.
"""

from typing import Any, Optional
import polars as pl
from sparkless.spark_types import StructType, Row
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
            # Handle tuple/list format by converting to dicts using schema field names
            # Note: Type signature says list[dict], but we defensively handle tuples at runtime
            if data:
                first_row: Any = data[0]  # Allow runtime check for tuples/lists
                if isinstance(first_row, (list, tuple)):
                    # Convert tuples to dicts using schema field names
                    converted_data = []
                    field_names = [f.name for f in schema.fields]
                    for row in data:
                        row_any: Any = row  # Allow runtime check for tuples/lists
                        if isinstance(row_any, (list, tuple)):
                            converted_data.append(
                                {
                                    field_names[i]: row_any[i]
                                    for i in range(len(row_any))
                                }
                            )
                        else:
                            converted_data.append(row_any)
                    df = pl.DataFrame(converted_data)
                else:
                    df = pl.DataFrame(data)
            else:
                df = pl.DataFrame(data)
            # Only enforce schema types if we have a union operation (to prevent type mismatches)
            # For other operations, let Polars infer types naturally
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

        # Track original schema BEFORE any operations
        # The schema parameter should be the original schema before any operations
        # If we have data, we can verify by checking the data keys match schema fields
        original_schema = schema
        # Verify schema matches data (if data exists)
        if data and len(data) > 0:
            first_row = data[0]
            # Handle both dict and tuple formats
            is_dict_format = isinstance(first_row, dict)
            is_tuple_format = isinstance(first_row, (list, tuple))

            if is_dict_format:
                data_keys = set(first_row.keys())
            elif is_tuple_format:
                # Tuple/list format - use schema field names
                data_keys = {f.name for f in schema.fields}
            else:
                # Fallback: try to get keys if possible
                data_keys = set(getattr(first_row, "keys", lambda: [])())

            schema_keys = {field.name for field in schema.fields}
            # If schema doesn't match data, infer from data (only for dict format)
            if is_dict_format and data_keys != schema_keys:
                from ...spark_types import StructType, StructField
                from .type_mapper import polars_dtype_to_mock_type

                # Create Polars DataFrame to infer types from dict data
                temp_df = pl.DataFrame(data)
                original_fields = []
                for col_name in temp_df.columns:
                    polars_dtype = temp_df[col_name].dtype
                    mock_type = polars_dtype_to_mock_type(polars_dtype)
                    original_fields.append(StructField(col_name, mock_type, True))
                original_schema = StructType(original_fields)

        original_columns = {field.name for field in original_schema.fields}

        # Track current schema as operations are applied
        current_schema = original_schema

        # Build mapping of computed expressions to column names for optimization
        from ...dataframe.lazy import LazyEvaluationEngine

        computed_expressions = LazyEvaluationEngine._build_computed_expressions_map(
            operations
        )

        # Build column dependency graph for drop operation optimization
        # This tracks which columns depend on which other columns
        # IMPORTANT: Use the projected schema (which includes computed columns) to determine
        # available columns, not just original_columns. This ensures that intermediate
        # columns created by earlier operations are available for dependency tracking.
        # The schema parameter already reflects the projected schema after all operations.
        projected_columns = {field.name for field in schema.fields}
        column_dependencies = LazyEvaluationEngine._build_column_dependency_graph(
            operations, projected_columns
        )

        # Track current columns as we process operations (for dependency tracking)
        # Start with projected columns (which includes computed columns from earlier operations)
        current_available_columns = projected_columns.copy()

        # Track if we have a materialized DataFrame available (e.g., after drop operation)
        # This avoids schema issues when converting back to lazy and then collecting again
        df_materialized: Optional[pl.DataFrame] = None

        # Group operations to handle filter-before-select optimization
        # When filter references original columns not in current schema, push it before select
        optimized_operations = []
        i = 0
        while i < len(operations):
            op_name, payload = operations[i]
            if op_name == "select" and i + 1 < len(operations):
                # Check if next operation is a filter that references original columns
                next_op_name, next_payload = operations[i + 1]
                if next_op_name == "filter":
                    # Check if filter references columns not in select result
                    # Extract column names that will be in the result of select
                    # (using schema inference, not just looking at the expressions)
                    from ...dataframe.schema.schema_manager import SchemaManager

                    select_schema = SchemaManager.project_schema_with_operations(
                        current_schema, [(op_name, payload)]
                    )
                    current_columns = {field.name for field in select_schema.fields}
                    # Check if filter references original columns not in current columns
                    missing_cols = LazyEvaluationEngine._extract_column_names(
                        next_payload, current_columns
                    )
                    # If filter references original columns, push it before select
                    if missing_cols and missing_cols.issubset(original_columns):
                        # Filter references original columns - apply it before select
                        # Add filter first, then select
                        optimized_operations.append((next_op_name, next_payload))
                        optimized_operations.append(
                            (op_name, payload)
                        )  # Add select after filter
                        i += 2  # Skip both select and filter
                        continue
            optimized_operations.append((op_name, payload))
            i += 1

        # Apply optimized operations in sequence
        # If no optimization happened, optimized_operations will be the same as operations
        for op_name, payload in optimized_operations:
            if op_name == "filter":
                # Filter operation - optimize to use computed columns if available
                # Note: When filter is pushed before select, computed_expressions will be empty
                # because select hasn't been applied yet. This is fine - the filter references
                # original columns, not computed ones.
                # If we have a materialized DataFrame, convert it back to lazy first
                if df_materialized is not None:
                    lazy_df = df_materialized.lazy()
                    df_materialized = None
                optimized_condition = (
                    LazyEvaluationEngine._replace_with_computed_column(
                        payload, computed_expressions
                    )
                )
                filter_expr = self.translator.translate(optimized_condition)
                # Apply filter to lazy DataFrame
                lazy_df = lazy_df.filter(filter_expr)
                # Verify filter worked by checking row count (for debugging)
                # Note: We don't update current_schema for filter as it doesn't change columns
            elif op_name == "select":
                # Select operation - need to collect first for window functions
                # Use materialized DataFrame if available, otherwise collect from lazy
                if df_materialized is not None:
                    df_collected = df_materialized
                    df_materialized = None
                else:
                    df_collected = lazy_df.collect()
                # Apply select - this should work even if filter was applied first
                # because select expressions reference column names, not DataFrame objects
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
                # Use materialized DataFrame if available, otherwise collect from lazy
                # This avoids schema mismatch issues when converting back to lazy after drop
                if df_materialized is not None:
                    df_collected = df_materialized
                    df_materialized = None
                else:
                    df_collected = lazy_df.collect()

                column_name, expression = payload
                result_df = self.operation_executor.apply_with_column(
                    df_collected, column_name, expression
                )

                # Convert result back to lazy
                # Window functions are already fully materialized in apply_with_column
                lazy_df = result_df.lazy()
                # Update schema and available columns after withColumn
                from ...dataframe.schema.schema_manager import SchemaManager

                current_schema = SchemaManager.project_schema_with_operations(
                    current_schema, [(op_name, payload)]
                )
                current_available_columns.add(column_name)
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

                # Optimize orderBy columns to use computed columns if available
                optimized_columns = []
                for col in columns:
                    optimized_col = LazyEvaluationEngine._replace_with_computed_column(
                        col, computed_expressions
                    )
                    optimized_columns.append(optimized_col)

                # Build sort expressions with descending flags
                # Polars doesn't have .desc() on Expr, use sort() with descending parameter
                sort_by = []
                descending_flags = []
                for col in optimized_columns:
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
                # Drop operation - need to handle Polars lazy evaluation limitation
                # Polars drops columns that depend on dropped columns during lazy evaluation
                # Solution: materialize ALL columns before dropping, then re-select what we need
                # This breaks the lazy dependency chain
                columns_to_drop = (
                    payload if isinstance(payload, (list, tuple)) else [payload]
                )

                # Filter out non-existent columns (PySpark allows dropping non-existent columns silently)
                # We need to check against current available columns
                existing_columns_to_drop = [
                    col for col in columns_to_drop if col in current_available_columns
                ]

                # If no columns to actually drop, skip this operation
                if not existing_columns_to_drop:
                    continue

                columns_to_drop = existing_columns_to_drop

                # Find all columns that depend on the columns being dropped
                columns_to_preserve: set[str] = set()
                for col_name, deps in column_dependencies.items():
                    # If this column depends on any column being dropped, we need to preserve it
                    if deps.intersection(columns_to_drop):
                        columns_to_preserve.add(col_name)

                # Also check subsequent operations for columns they need
                current_op_index = optimized_operations.index((op_name, payload))
                for future_op_name, future_payload in optimized_operations[
                    current_op_index + 1 :
                ]:
                    if future_op_name == "withColumn":
                        col_name, expr = future_payload
                        # Extract all columns this expression depends on
                        expr_deps = (
                            LazyEvaluationEngine._extract_all_column_dependencies(expr)
                        )
                        # If expr depends on a column that depends on dropped columns, preserve it
                        for dep_col in expr_deps:
                            if dep_col in column_dependencies:
                                dep_deps = column_dependencies[dep_col]
                                if dep_deps.intersection(columns_to_drop):
                                    columns_to_preserve.add(dep_col)
                            # Also check if dep_col is in current available columns
                            elif (
                                dep_col in current_available_columns
                                and dep_col in column_dependencies
                                and column_dependencies[dep_col].intersection(
                                    columns_to_drop
                                )
                            ):
                                # This column exists and is needed - preserve it if it depends on dropped columns
                                columns_to_preserve.add(dep_col)

                # Always materialize before dropping if there are subsequent operations
                # This ensures dependent columns are preserved even if dependency graph is incomplete
                if current_op_index + 1 < len(optimized_operations):
                    # Collect current state to materialize all columns
                    df_collected = lazy_df.collect()

                    # Drop columns using select to preserve schema correctly
                    # Using select instead of drop ensures schema is properly maintained
                    cols_to_keep = [
                        col
                        for col in df_collected.columns
                        if col not in columns_to_drop
                    ]

                    # Handle edge case: dropping all columns
                    # PySpark preserves row count even when all columns are dropped
                    if not cols_to_keep:
                        # Create empty DataFrame with same number of rows
                        # Use select with empty column list to preserve row structure
                        row_count = df_collected.height
                        # Create a dummy column, select it, then drop it to preserve row count
                        # Actually, Polars can't represent a DataFrame with rows but no columns
                        # So we create a single dummy column with null values
                        df_collected = pl.DataFrame({"_dummy": [None] * row_count})
                        # But wait, PySpark behavior is to return empty schema but preserve rows
                        # For now, we'll use the dummy column approach and handle it in schema projection
                        # The schema will show no columns, but the row count will be preserved
                    else:
                        df_collected = df_collected.select(cols_to_keep)

                    # Store the materialized DataFrame instead of converting back to lazy
                    # This avoids schema mismatch issues when subsequent operations (like withColumn)
                    # need to collect again. They can use the materialized DataFrame directly.
                    # Only convert to lazy if the next operation requires it (like filter)
                    next_op_index = current_op_index + 1
                    if next_op_index < len(optimized_operations):
                        next_op_name, _ = optimized_operations[next_op_index]
                        # Operations that can work with lazy frames (don't collect)
                        lazy_ops = {
                            "filter",
                            "distinct",
                            "limit",
                            "offset",
                            "orderBy",
                            "sort",
                        }
                        if next_op_name in lazy_ops:
                            # Next operation works with lazy, so convert back to lazy
                            # Create a fresh lazy frame by converting to dicts and back to avoid schema issues
                            lazy_df = df_collected.lazy()
                            df_materialized = None
                        else:
                            # Next operation will collect anyway (withColumn, select, etc.)
                            # Keep it materialized to avoid schema issues when converting back to lazy
                            # Store the materialized DataFrame directly - no conversion to dicts needed
                            # as we'll use it directly in the next operation
                            df_materialized = df_collected
                            # Don't create lazy_df here - we'll use df_materialized directly
                            # Set lazy_df to None or keep it as is, but operations will check df_materialized first
                            lazy_df = (
                                df_collected.lazy()
                            )  # Still need for final collection if no more ops
                    else:
                        # No more operations, convert back to lazy for final collection
                        lazy_df = df_collected.lazy()
                        df_materialized = None
                elif columns_to_preserve:
                    # No future operations but we have columns to preserve
                    df_collected = lazy_df.collect()
                    current_cols = set(df_collected.columns)
                    cols_to_keep = list(current_cols - set(columns_to_drop))
                    if not cols_to_keep:
                        # All columns dropped - preserve row count
                        row_count = df_collected.height
                        df_collected = pl.DataFrame({"_dummy": [None] * row_count})
                    else:
                        df_collected = df_collected.select(cols_to_keep)
                    lazy_df = df_collected.lazy()
                else:
                    # No dependencies and no future operations - can drop directly
                    # But we still need to handle non-existent columns and all-columns case
                    df_collected = lazy_df.collect()
                    cols_to_keep = [
                        col
                        for col in df_collected.columns
                        if col not in columns_to_drop
                    ]
                    if not cols_to_keep:
                        # All columns dropped - preserve row count
                        row_count = df_collected.height
                        df_collected = pl.DataFrame({"_dummy": [None] * row_count})
                    else:
                        df_collected = df_collected.select(cols_to_keep)
                    lazy_df = df_collected.lazy()

                # Update available columns after drop
                for col in columns_to_drop:
                    current_available_columns.discard(col)
            elif op_name == "withColumnRenamed":
                # WithColumnRenamed operation
                old_name, new_name = payload
                lazy_df = lazy_df.rename({old_name: new_name})
            else:
                raise ValueError(f"Unsupported operation: {op_name}")

        # Materialize (collect) the lazy DataFrame
        # Use materialized DataFrame if available, otherwise collect from lazy
        if df_materialized is not None:
            result_df = df_materialized
        elif lazy_df is not None:
            result_df = lazy_df.collect()
        else:
            # Should not happen, but handle gracefully
            raise ValueError("No DataFrame to materialize")

        # Convert to list[Row]
        # Handle special case: if all columns were dropped, result_df may have a _dummy column
        # We need to convert this to empty dicts to match PySpark's behavior
        if "_dummy" in result_df.columns and len(result_df.columns) == 1:
            # All columns were dropped - create empty rows preserving row count
            rows = []
            row_count = result_df.height
            for _ in range(row_count):
                rows.append(Row({}, schema=None))
            return rows

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
