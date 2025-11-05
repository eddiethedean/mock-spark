"""
Window function handler for Polars.

This module handles window functions using Polars `.over()` expressions.
"""

from typing import List, Optional, Any
import polars as pl
from mock_spark.functions.window_execution import MockWindowFunction
from mock_spark.window import MockWindowSpec


class PolarsWindowHandler:
    """Handles window functions using Polars expressions."""

    def translate_window_function(
        self, window_func: MockWindowFunction, df: pl.DataFrame
    ) -> pl.Expr:
        """Translate window function to Polars expression.

        Args:
            window_func: MockWindowFunction instance
            df: Polars DataFrame (for context)

        Returns:
            Polars expression with window function
        """
        function_name = window_func.function_name.upper()
        window_spec = window_func.window_spec

        # Build partition_by
        partition_by: List[pl.Expr] = []
        if hasattr(window_spec, "_partition_by") and window_spec._partition_by:
            for col in window_spec._partition_by:
                if isinstance(col, str):
                    partition_by.append(pl.col(col))
                elif hasattr(col, "name"):
                    partition_by.append(pl.col(col.name))

        # Build order_by
        order_by: List[pl.Expr] = []
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
        if hasattr(window_func, "function") and hasattr(
            window_func.function, "column"
        ):
            col = window_func.function.column
            if isinstance(col, str):
                column_expr = pl.col(col)
            elif hasattr(col, "name"):
                column_expr = pl.col(col.name)

        # Build window expression based on function name
        if function_name == "ROW_NUMBER":
            # Polars doesn't have row_number, use int_range
            if partition_by:
                if order_by:
                    return pl.int_range(pl.len()).over(partition_by, order_by=order_by[0])
                else:
                    return pl.int_range(pl.len()).over(partition_by)
            else:
                return pl.int_range(pl.len())
        elif function_name == "RANK":
            if column_expr:
                if partition_by:
                    if order_by:
                        return column_expr.rank().over(partition_by, order_by=order_by[0])
                    else:
                        return column_expr.rank().over(partition_by)
                else:
                    return column_expr.rank()
            else:
                # Fallback: use first order_by column
                if order_by:
                    order_col = order_by[0]
                    if partition_by:
                        return order_col.rank().over(partition_by)
                    else:
                        return order_col.rank()
        elif function_name == "DENSE_RANK":
            if column_expr:
                if partition_by:
                    if order_by:
                        return column_expr.dense_rank().over(partition_by, order_by=order_by[0])
                    else:
                        return column_expr.dense_rank().over(partition_by)
                else:
                    return column_expr.dense_rank()
            else:
                if order_by:
                    order_col = order_by[0]
                    if partition_by:
                        return order_col.dense_rank().over(partition_by)
                    else:
                        return order_col.dense_rank()
        elif function_name == "SUM":
            if column_expr:
                if partition_by:
                    return column_expr.sum().over(partition_by)
                else:
                    return column_expr.sum()
            else:
                raise ValueError("SUM window function requires a column")
        elif function_name == "AVG" or function_name == "MEAN":
            if column_expr:
                if partition_by:
                    return column_expr.mean().over(partition_by)
                else:
                    return column_expr.mean()
            else:
                raise ValueError("AVG window function requires a column")
        elif function_name == "COUNT":
            if column_expr:
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
            if column_expr:
                if partition_by:
                    return column_expr.max().over(partition_by)
                else:
                    return column_expr.max()
            else:
                raise ValueError("MAX window function requires a column")
        elif function_name == "MIN":
            if column_expr:
                if partition_by:
                    return column_expr.min().over(partition_by)
                else:
                    return column_expr.min()
            else:
                raise ValueError("MIN window function requires a column")
        elif function_name == "LAG":
            if column_expr:
                offset = getattr(window_func, "offset", 1)
                default = getattr(window_func, "default", None)
                if partition_by:
                    if order_by:
                        return column_expr.shift(offset, default).over(
                            partition_by, order_by=order_by[0]
                        )
                    else:
                        return column_expr.shift(offset, default).over(partition_by)
                else:
                    return column_expr.shift(offset, default)
            else:
                raise ValueError("LAG window function requires a column")
        elif function_name == "LEAD":
            if column_expr:
                offset = getattr(window_func, "offset", 1)
                default = getattr(window_func, "default", None)
                if partition_by:
                    if order_by:
                        return column_expr.shift(-offset, default).over(
                            partition_by, order_by=order_by[0]
                        )
                    else:
                        return column_expr.shift(-offset, default).over(partition_by)
                else:
                    return column_expr.shift(-offset, default)
            else:
                raise ValueError("LEAD window function requires a column")
        elif function_name == "FIRST_VALUE":
            if column_expr:
                if partition_by:
                    if order_by:
                        return column_expr.first().over(partition_by, order_by=order_by[0])
                    else:
                        return column_expr.first().over(partition_by)
                else:
                    return column_expr.first()
            else:
                raise ValueError("FIRST_VALUE window function requires a column")
        elif function_name == "LAST_VALUE":
            if column_expr:
                if partition_by:
                    if order_by:
                        return column_expr.last().over(partition_by, order_by=order_by[0])
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
                    return (
                        pl.int_range(pl.len())
                        .over(partition_by, order_by=order_by[0])
                        / pl.len().over(partition_by)
                    )
                else:
                    return (
                        pl.int_range(pl.len()).over(partition_by)
                        / pl.len().over(partition_by)
                    )
            else:
                return pl.int_range(pl.len()) / pl.len()
        elif function_name == "PERCENT_RANK":
            # PERCENT_RANK is not directly available in Polars, use approximation
            if partition_by:
                if order_by:
                    rank_expr = (
                        pl.int_range(pl.len())
                        .over(partition_by, order_by=order_by[0])
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

