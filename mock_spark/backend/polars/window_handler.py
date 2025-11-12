"""
Window function handler for Polars.

This module handles window functions using Polars `.over()` expressions.
"""

from typing import Optional
import polars as pl
from mock_spark.functions.window_execution import WindowFunction


class PolarsWindowHandler:
    """Handles window functions using Polars expressions."""

    def translate_window_function(
        self, window_func: WindowFunction, df: pl.DataFrame
    ) -> pl.Expr:
        """Translate window function to Polars expression.

        Args:
            window_func: WindowFunction instance
            df: Polars DataFrame (for context)

        Returns:
            Polars expression with window function
        """
        function_name = window_func.function_name.upper()
        window_spec = window_func.window_spec

        # Build partition_by
        partition_by: list[pl.Expr] = []
        if hasattr(window_spec, "_partition_by") and window_spec._partition_by:
            for col in window_spec._partition_by:
                if isinstance(col, str):
                    partition_by.append(pl.col(col))
                elif hasattr(col, "name"):
                    partition_by.append(pl.col(col.name))

        # Build order_by
        order_by: list[pl.Expr] = []
        if hasattr(window_spec, "_order_by") and window_spec._order_by:
            for col in window_spec._order_by:
                if isinstance(col, str):
                    order_by.append(pl.col(col))
                elif hasattr(col, "name"):
                    order_by.append(pl.col(col.name))
                elif hasattr(col, "operation") and col.operation == "desc":
                    col_name = col.column.name if hasattr(col, "column") else col.name
                    order_by.append(pl.col(col_name).desc())
                else:
                    order_by.append(pl.col(col.name))

        # Get column expression if available
        column_expr: Optional[pl.Expr] = None
        # Check for dummy columns (functions without real columns like rank(), row_number())
        dummy_columns = {
            "__rank__",
            "__dense_rank__",
            "__row_number__",
            "__cume_dist__",
            "__percent_rank__",
            "__ntile__",
        }

        # Try to get column from window_func.column_name first (set in WindowFunction.__init__)
        if hasattr(window_func, "column_name") and window_func.column_name:
            # Skip dummy columns - these functions don't take a real column
            if window_func.column_name not in dummy_columns:
                column_expr = pl.col(window_func.column_name)
        elif hasattr(window_func, "function") and hasattr(
            window_func.function, "column"
        ):
            col = window_func.function.column
            if isinstance(col, str):
                if col not in dummy_columns:
                    column_expr = pl.col(col)
            elif hasattr(col, "name") and col.name not in dummy_columns:
                column_expr = pl.col(col.name)

        # Build window expression based on function name
        if function_name == "ROW_NUMBER":
            # Polars doesn't have row_number, use int_range + 1 for 1-based indexing
            if partition_by:
                if order_by:
                    return (pl.int_range(pl.len()) + 1).over(
                        partition_by, order_by=order_by[0]
                    )
                else:
                    return (pl.int_range(pl.len()) + 1).over(partition_by)
            else:
                return pl.int_range(pl.len()) + 1
        elif function_name == "RANK":
            if column_expr is not None:
                if partition_by:
                    if order_by:
                        return column_expr.rank().over(
                            partition_by, order_by=order_by[0]
                        )
                    else:
                        return column_expr.rank().over(partition_by)
                else:
                    if order_by:
                        return column_expr.rank().over(order_by=order_by[0])
                    else:
                        return column_expr.rank()
            else:
                # Fallback: use first order_by column (rank() doesn't take a column)
                if order_by:
                    order_col = order_by[0]
                    if partition_by:
                        # When ranking on order_by column with partition_by, just use partition_by
                        # The rank is already computed on the ordered column
                        return order_col.rank().over(partition_by)
                    else:
                        # No partition_by, just rank on the ordered column
                        return order_col.rank()
                else:
                    # No order_by and no column - use row number as fallback
                    if partition_by:
                        return (pl.int_range(pl.len()) + 1).over(partition_by)
                    else:
                        return pl.int_range(pl.len()) + 1
        elif function_name == "DENSE_RANK":
            if column_expr is not None:
                if partition_by:
                    if order_by:
                        return column_expr.rank(method="dense").over(
                            partition_by, order_by=order_by[0]
                        )
                    else:
                        return column_expr.rank(method="dense").over(partition_by)
                else:
                    if order_by:
                        return column_expr.rank(method="dense").over(
                            order_by=order_by[0]
                        )
                    else:
                        return column_expr.rank(method="dense")
            else:
                # Fallback: use first order_by column (dense_rank() doesn't take a column)
                if order_by:
                    order_col = order_by[0]
                    if partition_by:
                        # When ranking on order_by column with partition_by, use rank with method='dense'
                        # Polars doesn't have dense_rank(), use rank(method='dense')
                        return order_col.rank(method="dense").over(partition_by)
                    else:
                        # No partition_by, just rank on the ordered column
                        return order_col.rank(method="dense")
                else:
                    # No order_by and no column - use row number as fallback
                    if partition_by:
                        return (pl.int_range(pl.len()) + 1).over(partition_by)
                    else:
                        return pl.int_range(pl.len()) + 1
        elif function_name == "SUM":
            if column_expr is not None:
                if partition_by:
                    return column_expr.sum().over(partition_by)
                else:
                    return column_expr.sum()
            else:
                raise ValueError("SUM window function requires a column")
        elif function_name == "AVG" or function_name == "MEAN":
            if column_expr is not None:
                if partition_by:
                    return column_expr.mean().over(partition_by)
                else:
                    return column_expr.mean()
            else:
                raise ValueError("AVG window function requires a column")
        elif function_name == "COUNT":
            if column_expr is not None:
                if partition_by:
                    return column_expr.count().over(partition_by)
                else:
                    return column_expr.count()
            else:
                # COUNT(*)
                if partition_by:
                    return pl.len().over(partition_by)
                else:
                    return pl.len()
        elif function_name == "MAX":
            if column_expr is not None:
                if partition_by:
                    return column_expr.max().over(partition_by)
                else:
                    return column_expr.max()
            else:
                raise ValueError("MAX window function requires a column")
        elif function_name == "MIN":
            if column_expr is not None:
                if partition_by:
                    return column_expr.min().over(partition_by)
                else:
                    return column_expr.min()
            else:
                raise ValueError("MIN window function requires a column")
        elif function_name == "LAG":
            if column_expr is not None:
                offset = getattr(window_func, "offset", 1)
                default = getattr(window_func, "default", None)
                # Polars shift() takes periods as first arg, fill_value as keyword
                shift_expr = (
                    column_expr.shift(offset, fill_value=default)
                    if default is not None
                    else column_expr.shift(offset)
                )
                if partition_by:
                    if order_by:
                        return shift_expr.over(partition_by, order_by=order_by[0])
                    else:
                        return shift_expr.over(partition_by)
                else:
                    return shift_expr
            else:
                raise ValueError("LAG window function requires a column")
        elif function_name == "LEAD":
            if column_expr is not None:
                offset = getattr(window_func, "offset", 1)
                default = getattr(window_func, "default", None)
                # Polars shift() takes periods as first arg, fill_value as keyword
                # LEAD uses negative offset
                shift_expr = (
                    column_expr.shift(-offset, fill_value=default)
                    if default is not None
                    else column_expr.shift(-offset)
                )
                if partition_by:
                    if order_by:
                        return shift_expr.over(partition_by, order_by=order_by[0])
                    else:
                        return shift_expr.over(partition_by)
                else:
                    return shift_expr
            else:
                raise ValueError("LEAD window function requires a column")
        elif function_name == "FIRST_VALUE":
            if column_expr is not None:
                if partition_by:
                    if order_by:
                        return column_expr.first().over(
                            partition_by, order_by=order_by[0]
                        )
                    else:
                        return column_expr.first().over(partition_by)
                else:
                    return column_expr.first()
            else:
                raise ValueError("FIRST_VALUE window function requires a column")
        elif function_name == "LAST_VALUE":
            if column_expr is not None:
                if partition_by:
                    if order_by:
                        return column_expr.last().over(
                            partition_by, order_by=order_by[0]
                        )
                    else:
                        return column_expr.last().over(partition_by)
                else:
                    return column_expr.last()
            else:
                raise ValueError("LAST_VALUE window function requires a column")
        elif function_name == "CUME_DIST":
            # CUME_DIST is not directly available in Polars, use approximation
            if partition_by:
                if order_by:
                    return pl.int_range(pl.len()).over(
                        partition_by, order_by=order_by[0]
                    ) / pl.len().over(partition_by)
                else:
                    return pl.int_range(pl.len()).over(partition_by) / pl.len().over(
                        partition_by
                    )
            else:
                return pl.int_range(pl.len()) / pl.len()
        elif function_name == "PERCENT_RANK":
            # PERCENT_RANK is not directly available in Polars, use approximation
            if partition_by:
                if order_by:
                    rank_expr = (
                        pl.int_range(pl.len()).over(partition_by, order_by=order_by[0])
                        - 1
                    )
                    count_expr = pl.len().over(partition_by) - 1
                    return rank_expr / count_expr
                else:
                    rank_expr = pl.int_range(pl.len()).over(partition_by) - 1
                    count_expr = pl.len().over(partition_by) - 1
                    return rank_expr / count_expr
            else:
                rank_expr = pl.int_range(pl.len()) - 1
                count_expr = pl.len() - 1
                return rank_expr / count_expr
        else:
            raise ValueError(f"Unsupported window function: {function_name}")
