"""
DataFrame operation executor for Polars.

This module provides execution of DataFrame operations (filter, select, join, etc.)
using Polars DataFrame API.
"""

from typing import Any, List, Tuple, Optional, Union
import polars as pl
from .expression_translator import PolarsExpressionTranslator
from .window_handler import PolarsWindowHandler
from mock_spark.functions.window_execution import MockWindowFunction


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
            condition: Filter condition (MockColumnOperation or expression)

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

        for col in columns:
            if isinstance(col, str):
                if col == "*":
                    # Select all columns - return original DataFrame
                    return df
                else:
                    # Select specific column
                    select_exprs.append(pl.col(col))
                    select_names.append(col)
            elif isinstance(col, MockWindowFunction):
                # Handle window functions
                window_expr = self.window_handler.translate_window_function(col, df)
                alias_name = getattr(col, "name", None) or f"{col.function_name.lower()}_window"
                select_exprs.append(window_expr.alias(alias_name))
                select_names.append(alias_name)
            else:
                # Translate expression
                expr = self.translator.translate(col)
                # Get alias if available
                alias_name = getattr(col, "name", None) or getattr(col, "_alias_name", None)
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

        # Check if any column uses explode operation
        has_explode = False
        explode_index = None
        explode_col_name = None
        for i, col in enumerate(columns):
            if hasattr(col, "operation") and col.operation == "explode":
                has_explode = True
                explode_index = i
                # Get the column name from the expression
                if hasattr(col, "column") and hasattr(col.column, "name"):
                    explode_col_name = col.column.name
                break
            elif hasattr(col, "function_name") and col.function_name == "explode":
                has_explode = True
                explode_index = i
                # Get the column name from the expression
                if hasattr(col, "column") and hasattr(col.column, "name"):
                    explode_col_name = col.column.name
                break
        
        if has_explode:
            # For explode, we need to use Polars' explode() method which expands rows
            # First, translate the explode expression to get the array column
            # Then select all columns and explode the DataFrame
            result = df.select(select_exprs)
            # Use the alias name for the exploded column
            exploded_col_name = select_names[explode_index] if explode_index < len(select_names) else explode_col_name
            if exploded_col_name:
                # Use Polars explode on the DataFrame to expand rows
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
            on: Join key(s) - column name(s) or list of column names
            how: Join type ("inner", "left", "right", "outer", "cross")

        Returns:
            Joined DataFrame
        """
        if on is None:
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

        if polars_how == "cross":
            return df1.join(df2, how="cross")
        else:
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

