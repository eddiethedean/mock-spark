"""
DataFrame operation executor for Polars.

This module provides execution of DataFrame operations (filter, select, join, etc.)
using Polars DataFrame API.
"""

from typing import Any, List, Tuple, Optional, Union
import polars as pl
from .expression_translator import PolarsExpressionTranslator
from .window_handler import PolarsWindowHandler
from mock_spark.functions.window_execution import WindowFunction


class PolarsOperationExecutor:
    """Executes DataFrame operations using Polars DataFrame API."""

    def __init__(self, expression_translator: PolarsExpressionTranslator):
        """Initialize operation executor.

        Args:
            expression_translator: Polars expression translator instance
        """
        self.translator = expression_translator
        self.window_handler = PolarsWindowHandler()

    def apply_filter(self, df: pl.DataFrame, condition: Any) -> pl.DataFrame:
        """Apply a filter operation.

        Args:
            df: Source Polars DataFrame
            condition: Filter condition (ColumnOperation or expression)

        Returns:
            Filtered Polars DataFrame
        """
        filter_expr = self.translator.translate(condition)
        return df.filter(filter_expr)

    def apply_select(self, df: pl.DataFrame, columns: Tuple[Any, ...]) -> pl.DataFrame:
        """Apply a select operation.

        Args:
            df: Source Polars DataFrame
            columns: Columns to select

        Returns:
            Selected Polars DataFrame
        """
        select_exprs = []
        select_names = []
        map_op_indices = set()  # Track which columns are map operations

        # First pass: handle map_keys, map_values, map_entries using struct operations
        for i, col in enumerate(columns):
            # Check if this is a map_keys, map_values, or map_entries operation
            is_map_op = False
            map_op_name = None
            map_col_name = None
            if hasattr(col, "operation"):
                if col.operation in [
                    "map_keys",
                    "map_values",
                    "map_entries",
                    "map_concat",
                ]:
                    is_map_op = True
                    map_op_name = col.operation
                    if hasattr(col, "column") and hasattr(col.column, "name"):
                        map_col_name = col.column.name
            elif hasattr(col, "function_name") and col.function_name in [
                "map_keys",
                "map_values",
                "map_entries",
                "map_concat",
            ]:
                is_map_op = True
                map_op_name = col.function_name
                if hasattr(col, "column") and hasattr(col.column, "name"):
                    map_col_name = col.column.name

            if is_map_op and map_col_name and map_col_name in df.columns:
                # Get the struct dtype for this column
                struct_dtype = df[map_col_name].dtype
                if hasattr(struct_dtype, "fields") and struct_dtype.fields:
                    # Build expression using struct.field() checks
                    field_names = [f.name for f in struct_dtype.fields]
                    alias_name = (
                        getattr(col, "name", None) or f"{map_op_name}({map_col_name})"
                    )
                    if map_op_name == "map_keys":
                        # Get only non-null field names
                        keys_expr = pl.concat_list(
                            [
                                pl.when(
                                    pl.col(map_col_name)
                                    .struct.field(fname)
                                    .is_not_null()
                                )
                                .then(pl.lit(fname))
                                .otherwise(None)
                                for fname in field_names
                            ]
                        ).list.drop_nulls()
                        select_exprs.append(keys_expr.alias(alias_name))
                        select_names.append(alias_name)
                        map_op_indices.add(i)
                    elif map_op_name == "map_values":
                        # Get only non-null field values
                        values_expr = pl.concat_list(
                            [
                                pl.when(
                                    pl.col(map_col_name)
                                    .struct.field(fname)
                                    .is_not_null()
                                )
                                .then(pl.col(map_col_name).struct.field(fname))
                                .otherwise(None)
                                for fname in field_names
                            ]
                        ).list.drop_nulls()
                        select_exprs.append(values_expr.alias(alias_name))
                        select_names.append(alias_name)
                        map_op_indices.add(i)
                    elif map_op_name == "map_entries":
                        # Create array of structs with key and value
                        entries_list = pl.concat_list(
                            [
                                pl.struct(
                                    [
                                        pl.lit(fname).cast(pl.Utf8).alias("key"),
                                        pl.col(map_col_name)
                                        .struct.field(fname)
                                        .alias("value"),
                                    ]
                                )
                                for fname in field_names
                            ]
                        ).list.filter(pl.element().struct.field("value").is_not_null())
                        select_exprs.append(entries_list.alias(alias_name))
                        select_names.append(alias_name)
                        map_op_indices.add(i)
                    elif map_op_name == "map_concat":
                        # map_concat(*cols) - merge multiple maps
                        # col.value contains additional columns (first column is in col.column)
                        if (
                            hasattr(col, "value")
                            and col.value
                            and isinstance(col.value, (list, tuple))
                        ):
                            # Get all map column names
                            map_cols = [map_col_name]  # Start with first column
                            for other_col in col.value:
                                if isinstance(other_col, str):
                                    map_cols.append(other_col)
                                elif hasattr(other_col, "name"):
                                    map_cols.append(other_col.name)
                                elif hasattr(other_col, "column") and hasattr(
                                    other_col.column, "name"
                                ):
                                    # Handle nested column references
                                    map_cols.append(other_col.column.name)
                                else:
                                    # Try to get name from string representation or other attributes
                                    col_str = str(other_col)
                                    if col_str in df.columns:
                                        map_cols.append(col_str)

                            # Verify all map columns exist in DataFrame
                            available_map_cols = [
                                mc for mc in map_cols if mc in df.columns
                            ]
                            if len(available_map_cols) < len(map_cols):
                                # Some columns missing - this shouldn't happen but handle gracefully
                                map_cols = available_map_cols

                            # Merge all struct columns - combine all fields from all maps
                            # Get all field names from all struct columns
                            all_field_names = set()
                            for map_col in map_cols:
                                if map_col in df.columns:
                                    struct_dtype = df[map_col].dtype
                                    if hasattr(struct_dtype, "fields"):
                                        field_names = [
                                            f.name for f in struct_dtype.fields
                                        ]
                                        all_field_names.update(field_names)
                            all_field_names = sorted(all_field_names)

                            # Build merged struct: for each field, take value from later maps first (they override)
                            # Later maps override earlier ones (PySpark behavior)
                            # Then filter out null fields per row (PySpark only includes non-null keys)
                            struct_field_exprs = []
                            for fname in all_field_names:
                                # Check each map column in reverse order (later maps override earlier)
                                value_exprs = []
                                for map_col in reversed(map_cols):
                                    if map_col in df.columns:
                                        struct_dtype = df[map_col].dtype
                                        if hasattr(struct_dtype, "fields") and any(
                                            f.name == fname for f in struct_dtype.fields
                                        ):
                                            value_exprs.append(
                                                pl.col(map_col).struct.field(fname)
                                            )

                                if value_exprs:
                                    # Use coalesce to take first non-null value (later maps first)
                                    if len(value_exprs) == 1:
                                        struct_field_exprs.append(
                                            value_exprs[0].alias(fname)
                                        )
                                    else:
                                        struct_field_exprs.append(
                                            pl.coalesce(value_exprs).alias(fname)
                                        )

                            # Create merged struct with all fields
                            merged_struct = pl.struct(struct_field_exprs)

                            # Filter out null fields per row using map_elements
                            # PySpark only includes keys that have non-null values
                            filtered_merged = merged_struct.map_elements(
                                lambda x: {k: v for k, v in x.items() if v is not None}
                                if isinstance(x, dict)
                                else (
                                    {
                                        k: getattr(x, k)
                                        for k in dir(x)
                                        if not k.startswith("_")
                                        and getattr(x, k, None) is not None
                                    }
                                    if hasattr(x, "__dict__")
                                    else None
                                )
                                if x is not None
                                else None,
                                return_dtype=pl.Object,
                            )

                            select_exprs.append(filtered_merged.alias(alias_name))
                            select_names.append(alias_name)
                            map_op_indices.add(i)

        # Second pass: handle all other columns (skip map operations already handled)
        for i, col in enumerate(columns):
            if i in map_op_indices:
                continue  # Skip map operations already handled
            if isinstance(col, str):
                if col == "*":
                    # Select all columns - return original DataFrame
                    return df
                else:
                    # Select specific column
                    select_exprs.append(pl.col(col))
                    select_names.append(col)
            elif isinstance(col, WindowFunction):
                # Handle window functions
                window_expr = self.window_handler.translate_window_function(col, df)
                alias_name = (
                    getattr(col, "name", None) or f"{col.function_name.lower()}_window"
                )
                select_exprs.append(window_expr.alias(alias_name))
                select_names.append(alias_name)
            else:
                # Translate expression
                expr = self.translator.translate(col)
                # Get alias if available
                alias_name = getattr(col, "name", None) or getattr(
                    col, "_alias_name", None
                )
                if alias_name:
                    expr = expr.alias(alias_name)
                    select_exprs.append(expr)
                    select_names.append(alias_name)
                else:
                    select_exprs.append(expr)
                    # Try to infer name from expression
                    if hasattr(col, "name"):
                        select_names.append(col.name)
                    elif isinstance(col, str):
                        select_names.append(col)
                    else:
                        select_names.append(f"col_{len(select_exprs)}")

        if not select_exprs:
            return df

        # Check if any column uses explode or explode_outer operation
        has_explode = False
        has_explode_outer = False
        explode_index = None
        explode_outer_index = None
        for i, col in enumerate(columns):
            col_operation = getattr(col, "operation", None) or getattr(
                col, "function_name", None
            )
            if col_operation == "explode":
                has_explode = True
                explode_index = i
            elif col_operation == "explode_outer":
                has_explode_outer = True
                explode_outer_index = i

        if has_explode or has_explode_outer:
            # For explode/explode_outer, we need to use Polars' explode() method which expands rows
            # First, translate the explode expression to get the array column
            # Then select all columns and explode the DataFrame
            result = df.select(select_exprs)
            # Use the alias name for the exploded column
            exploded_col_name = None
            if (
                has_explode
                and explode_index is not None
                and explode_index < len(select_names)
            ):
                exploded_col_name = select_names[explode_index]
            elif (
                has_explode_outer
                and explode_outer_index is not None
                and explode_outer_index < len(select_names)
            ):
                exploded_col_name = select_names[explode_outer_index]
            if exploded_col_name:
                # For explode_outer, Polars' explode() already handles null/empty arrays correctly
                # by producing null values for those cases, which matches PySpark behavior
                result = result.explode(exploded_col_name)
        else:
            result = df.select(select_exprs)

        # Special handling: if we're selecting only literals (no column references),
        # Polars returns 1 row by default. We need to ensure the literal broadcasts
        # to all rows in the source DataFrame.
        # Check if result has fewer rows than source and we're selecting expressions
        # (not string column names)
        if len(result) == 1 and len(df) > 1:
            # Check if all selected items are expressions (not string column names)
            # If all are expressions and none reference columns from df, they're literals
            has_column_reference = False
            for col in columns:
                if isinstance(col, str):
                    # String column name - definitely references a column
                    has_column_reference = True
                    break
                # Check if expression references columns from the DataFrame
                # We can't easily inspect Polars expressions, so we use a heuristic:
                # If the result has 1 row and source has >1 rows, and we're not selecting
                # string column names, it's likely all literals

            # If no column references and result is shorter, replicate
            if not has_column_reference and len(result) < len(df):
                # Replicate the single row to match DataFrame length
                result = pl.concat([result] * len(df))

        return result

    def apply_with_column(
        self, df: pl.DataFrame, column_name: str, expression: Any
    ) -> pl.DataFrame:
        """Apply a withColumn operation.

        Args:
            df: Source Polars DataFrame
            column_name: Name of new/updated column
            expression: Expression for the column

        Returns:
            DataFrame with new column
        """
        if isinstance(expression, WindowFunction):
            # Window functions need special handling
            # For window functions with order_by, we need to sort the DataFrame first
            # to ensure correct window function evaluation
            window_spec = expression.window_spec
            function_name = getattr(expression, "function_name", "").upper()

            # Build sort columns from partition_by and order_by
            sort_cols = []
            has_order_by = hasattr(window_spec, "_order_by") and window_spec._order_by
            has_partition_by = (
                hasattr(window_spec, "_partition_by") and window_spec._partition_by
            )

            if has_order_by:
                # Add partition_by columns first
                if has_partition_by:
                    for col in window_spec._partition_by:
                        if isinstance(col, str):
                            sort_cols.append(col)
                        elif hasattr(col, "name"):
                            sort_cols.append(col.name)
                # Add order_by columns
                for col in window_spec._order_by:
                    col_name = None
                    is_desc = False
                    if isinstance(col, str):
                        col_name = col
                    elif hasattr(col, "name"):
                        col_name = col.name
                    elif hasattr(col, "operation") and col.operation == "desc":
                        is_desc = True
                        if hasattr(col, "column") and hasattr(col.column, "name"):
                            col_name = col.column.name
                        elif hasattr(col, "name"):
                            col_name = col.name

                    if col_name:
                        if is_desc:
                            # For descending, convert to Polars expression
                            if all(isinstance(c, str) for c in sort_cols):
                                # Convert all to expressions
                                expr_cols = [pl.col(c) for c in sort_cols]
                                expr_cols.append(pl.col(col_name).desc())
                                df = df.sort(expr_cols)
                                sort_cols = []  # Mark as sorted
                            else:
                                sort_cols.append(pl.col(col_name).desc())
                        else:
                            sort_cols.append(col_name)

            # Sort if we have string column names (and haven't already sorted with expressions)
            # CRITICAL: For lag/lead functions, we MUST sort before applying the window function
            if function_name in ("LAG", "LEAD") and has_order_by:
                # Rebuild sort_cols if needed to ensure we sort
                if not sort_cols or not all(isinstance(c, str) for c in sort_cols):
                    sort_cols = []
                    if has_partition_by:
                        for col in window_spec._partition_by:
                            if isinstance(col, str):
                                sort_cols.append(col)
                            elif hasattr(col, "name"):
                                sort_cols.append(col.name)
                    for col in window_spec._order_by:
                        if isinstance(col, str):
                            sort_cols.append(col)
                        elif hasattr(col, "name"):
                            sort_cols.append(col.name)
                if sort_cols and all(isinstance(c, str) for c in sort_cols):
                    df = df.sort(sort_cols)
            elif sort_cols and all(isinstance(c, str) for c in sort_cols):
                df = df.sort(sort_cols)

            window_expr = self.window_handler.translate_window_function(expression, df)
            result = df.with_columns(window_expr.alias(column_name))

            # For lag/lead, ensure result maintains sort order
            # CRITICAL: Must sort result to preserve correct window function values
            if function_name in ("LAG", "LEAD"):
                # Rebuild sort_cols if needed
                if not sort_cols or not all(isinstance(c, str) for c in sort_cols):
                    sort_cols = []
                    if has_partition_by:
                        for col in window_spec._partition_by:
                            if isinstance(col, str):
                                sort_cols.append(col)
                            elif hasattr(col, "name"):
                                sort_cols.append(col.name)
                    if has_order_by:
                        for col in window_spec._order_by:
                            if isinstance(col, str):
                                sort_cols.append(col)
                            elif hasattr(col, "name"):
                                sort_cols.append(col.name)
                if sort_cols and all(isinstance(c, str) for c in sort_cols):
                    result = result.sort(sort_cols)

            return result
        else:
            expr = self.translator.translate(expression)
            return df.with_columns(expr.alias(column_name))

    def apply_join(
        self,
        df1: pl.DataFrame,
        df2: pl.DataFrame,
        on: Optional[Union[str, List[str]]] = None,
        how: str = "inner",
    ) -> pl.DataFrame:
        """Apply a join operation.

        Args:
            df1: Left DataFrame
            df2: Right DataFrame
            on: Join key(s) - column name(s), list of column names, or ColumnOperation with ==
            how: Join type ("inner", "left", "right", "outer", "cross", "semi", "anti")

        Returns:
            Joined DataFrame
        """
        # Extract column names from join condition if it's a ColumnOperation
        if on is not None and hasattr(on, "operation") and on.operation == "==":
            # Extract column names from == comparison
            # Type narrowing: ensure on has column and value attributes
            if not hasattr(on, "column") or not hasattr(on, "value"):
                raise ValueError("Join condition must have column and value attributes")
            left_col = on.column.name if hasattr(on.column, "name") else str(on.column)
            right_col = on.value.name if hasattr(on.value, "name") else str(on.value)
            # Ensure both are strings
            left_col_str = str(left_col)
            right_col_str = str(right_col)
            # Both columns should have the same name for == comparison
            # Use left column name (both DataFrames should have this column)
            if left_col_str in df1.columns and left_col_str in df2.columns:
                on = [left_col_str]
            elif right_col_str in df1.columns and right_col_str in df2.columns:
                on = [right_col_str]
            else:
                # Fallback: use left column name anyway
                on = [left_col_str]
        elif on is None:
            # Natural join - use common columns
            common_cols = set(df1.columns) & set(df2.columns)
            if not common_cols:
                raise ValueError("No common columns found for join")
            on = list(common_cols)

        # Map join types
        join_type_map = {
            "inner": "inner",
            "left": "left",
            "right": "right",
            "outer": "outer",
            "full": "outer",
            "full_outer": "outer",
            "cross": "cross",
        }

        polars_how = join_type_map.get(how.lower(), "inner")

        if isinstance(on, str):
            on = [on]

        # Handle semi and anti joins (Polars doesn't support natively)
        if how.lower() in ("semi", "left_semi"):
            # Semi join: return rows from left where match exists in right
            # Do inner join, then select only left columns and distinct
            joined = df1.join(df2, on=on, how="inner")
            # Select only columns from df1 (preserve original column order)
            left_cols = [col for col in df1.columns if col in joined.columns]
            return joined.select(left_cols).unique()
        elif how.lower() in ("anti", "left_anti"):
            # Anti join: return rows from left where no match exists in right
            # Do left join, then filter where right columns are null
            joined = df1.join(df2, on=on, how="left")
            # Find right-side columns (columns in df2 but not in df1)
            right_cols = [col for col in df2.columns if col not in df1.columns]
            if right_cols:
                # Filter where any right column is null
                filter_expr = pl.col(right_cols[0]).is_null()
                for col in right_cols[1:]:
                    filter_expr = filter_expr | pl.col(col).is_null()
                joined = joined.filter(filter_expr)
            else:
                # If no right columns (all match left), check if join key exists
                # This case shouldn't happen, but handle it
                joined = joined.filter(pl.col(on[0]).is_null())
            # Select only columns from df1
            left_cols = [col for col in joined.columns if col in df1.columns]
            return joined.select(left_cols)
        elif polars_how == "cross":
            return df1.join(df2, how="cross")
        else:
            # Ensure on is a list of strings
            if not isinstance(on, list):
                on = [on] if isinstance(on, str) else list(on)
            # Verify columns exist in both DataFrames
            for col in on:
                if col not in df1.columns:
                    raise ValueError(
                        f"Join column '{col}' not found in left DataFrame. Available columns: {df1.columns}"
                    )
                if col not in df2.columns:
                    raise ValueError(
                        f"Join column '{col}' not found in right DataFrame. Available columns: {df2.columns}"
                    )
            return df1.join(df2, on=on, how=polars_how)

    def apply_union(self, df1: pl.DataFrame, df2: pl.DataFrame) -> pl.DataFrame:
        """Apply a union operation.

        Args:
            df1: First DataFrame
            df2: Second DataFrame

        Returns:
            Unioned DataFrame
        """
        # Ensure schemas match
        df1_cols = set(df1.columns)
        df2_cols = set(df2.columns)

        # Add missing columns
        for col in df1_cols - df2_cols:
            df2 = df2.with_columns(pl.lit(None).alias(col))

        for col in df2_cols - df1_cols:
            df1 = df1.with_columns(pl.lit(None).alias(col))

        # Ensure column order matches
        column_order = df1.columns
        df2 = df2.select(column_order)

        return pl.concat([df1, df2])

    def apply_order_by(
        self, df: pl.DataFrame, columns: List[Any], ascending: bool = True
    ) -> pl.DataFrame:
        """Apply an orderBy operation.

        Args:
            df: Source Polars DataFrame
            columns: Columns to sort by
            ascending: Sort direction

        Returns:
            Sorted DataFrame
        """
        sort_by = []
        for col in columns:
            if isinstance(col, str):
                if ascending:
                    sort_by.append(pl.col(col))
                else:
                    sort_by.append(pl.col(col).desc())
            elif hasattr(col, "operation") and col.operation == "desc":
                col_name = col.column.name if hasattr(col, "column") else col.name
                sort_by.append(pl.col(col_name).desc())
            else:
                col_name = col.name if hasattr(col, "name") else str(col)
                if ascending:
                    sort_by.append(pl.col(col_name))
                else:
                    sort_by.append(pl.col(col_name).desc())

        if not sort_by:
            return df

        return df.sort(sort_by)

    def apply_limit(self, df: pl.DataFrame, n: int) -> pl.DataFrame:
        """Apply a limit operation.

        Args:
            df: Source Polars DataFrame
            n: Number of rows to return

        Returns:
            Limited DataFrame
        """
        return df.head(n)

    def apply_offset(self, df: pl.DataFrame, n: int) -> pl.DataFrame:
        """Apply an offset operation (skip first n rows).

        Args:
            df: Source Polars DataFrame
            n: Number of rows to skip

        Returns:
            DataFrame with first n rows skipped
        """
        return df.slice(n)

    def apply_group_by_agg(
        self, df: pl.DataFrame, group_by: List[Any], aggs: List[Any]
    ) -> pl.DataFrame:
        """Apply a groupBy().agg() operation.

        Args:
            df: Source Polars DataFrame
            group_by: Columns to group by
            aggs: Aggregation expressions

        Returns:
            Aggregated DataFrame
        """
        # Translate group by columns
        group_by_cols = []
        for col in group_by:
            if isinstance(col, str):
                group_by_cols.append(col)
            elif hasattr(col, "name"):
                group_by_cols.append(col.name)
            else:
                raise ValueError(f"Cannot determine column name for group by: {col}")

        # Translate aggregation expressions
        agg_exprs = []
        for agg in aggs:
            expr = self.translator.translate(agg)
            # Get alias if available
            alias_name = getattr(agg, "name", None) or getattr(agg, "_alias_name", None)
            if alias_name:
                expr = expr.alias(alias_name)
            agg_exprs.append(expr)

        if not group_by_cols:
            # Global aggregation
            return df.select(agg_exprs)
        else:
            return df.group_by(group_by_cols).agg(agg_exprs)

    def apply_distinct(self, df: pl.DataFrame) -> pl.DataFrame:
        """Apply a distinct operation.

        Args:
            df: Source Polars DataFrame

        Returns:
            DataFrame with distinct rows
        """
        return df.unique()

    def apply_drop(self, df: pl.DataFrame, columns: List[str]) -> pl.DataFrame:
        """Apply a drop operation.

        Args:
            df: Source Polars DataFrame
            columns: Columns to drop

        Returns:
            DataFrame with columns dropped
        """
        return df.drop(columns)

    def apply_with_column_renamed(
        self, df: pl.DataFrame, old_name: str, new_name: str
    ) -> pl.DataFrame:
        """Apply a withColumnRenamed operation.

        Args:
            df: Source Polars DataFrame
            old_name: Old column name
            new_name: New column name

        Returns:
            DataFrame with renamed column
        """
        return df.rename({old_name: new_name})
