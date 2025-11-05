"""
Polars materializer for lazy DataFrame operations.

This module provides materialization of lazy DataFrame operations using Polars,
replacing SQL-based materialization with Polars DataFrame operations.
"""

from typing import List, Dict, Any, Tuple
import polars as pl
from mock_spark.spark_types import MockStructType, MockRow
from .expression_translator import PolarsExpressionTranslator
from .operation_executor import PolarsOperationExecutor


class PolarsMaterializer:
    """Materializes lazy operations using Polars."""

    def __init__(self):
        """Initialize Polars materializer."""
        self.translator = PolarsExpressionTranslator()
        self.operation_executor = PolarsOperationExecutor(self.translator)

    def materialize(
        self,
        data: List[Dict[str, Any]],
        schema: MockStructType,
        operations: List[Tuple[str, Any]],
    ) -> List[MockRow]:
        """Materialize lazy operations into actual data.

        Args:
            data: Initial data
            schema: DataFrame schema
            operations: List of queued operations (operation_name, payload)

        Returns:
            List of result rows
        """
        if not data:
            # Empty DataFrame
            return []

        # Convert data to Polars DataFrame
        df = pl.DataFrame(data)

        # Use lazy evaluation for better performance
        lazy_df = df.lazy()

        # Apply operations in sequence
        for op_name, payload in operations:
            if op_name == "filter":
                # Filter operation
                lazy_df = lazy_df.filter(self.translator.translate(payload))
            elif op_name == "select":
                # Select operation - need to collect first for window functions
                df_collected = lazy_df.collect()
                lazy_df = self.operation_executor.apply_select(df_collected, payload).lazy()
            elif op_name == "withColumn":
                # WithColumn operation
                column_name, expression = payload
                expr = self.translator.translate(expression)
                lazy_df = lazy_df.with_columns(expr.alias(column_name))
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
                                schema_dict[field.name] = pl.Series(field.name, [], dtype=polars_dtype)
                            other_df = pl.DataFrame(schema_dict)
                        else:
                            other_df = pl.DataFrame()
                    else:
                        other_df = pl.DataFrame(other_data)
                # Collect lazy_df before joining
                df_collected = lazy_df.collect()
                result_df = self.operation_executor.apply_join(df_collected, other_df, on=on, how=how)
                lazy_df = result_df.lazy()
            elif op_name == "union":
                # Union operation - need to collect first
                df_collected = lazy_df.collect()
                other_df = payload
                if not isinstance(other_df, pl.DataFrame):
                    other_data = getattr(other_df, "data", [])
                    if not other_data:
                        # Empty DataFrame - create from schema if available
                        if hasattr(other_df, "schema"):
                            from .type_mapper import mock_type_to_polars_dtype
                            schema_dict = {}
                            for field in other_df.schema.fields:
                                polars_dtype = mock_type_to_polars_dtype(field.dataType)
                                schema_dict[field.name] = pl.Series(field.name, [], dtype=polars_dtype)
                            other_df = pl.DataFrame(schema_dict)
                        else:
                            other_df = pl.DataFrame()
                    else:
                        other_df = pl.DataFrame(other_data)
                result_df = self.operation_executor.apply_union(df_collected, other_df)
                lazy_df = result_df.lazy()
            elif op_name == "orderBy":
                # OrderBy operation - can be done lazily
                # Payload can be just columns (tuple) or (columns, ascending)
                if isinstance(payload, tuple) and len(payload) == 2 and isinstance(payload[1], bool):
                    columns, ascending = payload
                else:
                    # Payload is just columns, default to ascending=True
                    columns = payload if isinstance(payload, (tuple, list)) else (payload,)
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
                        col_name = col.column.name if hasattr(col, "column") else col.name
                        col_expr = pl.col(col_name)
                        is_desc = True
                    else:
                        col_name = col.name if hasattr(col, "name") else str(col)
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
            elif op_name == "groupBy":
                # GroupBy operation - need to collect first
                df_collected = lazy_df.collect()
                group_by, aggs = payload
                result_df = self.operation_executor.apply_group_by_agg(df_collected, group_by, aggs)
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

        # Convert to List[MockRow]
        # Note: Don't pass schema here - MockRow will handle dict correctly
        # The schema will be applied later in _convert_materialized_rows if needed
        return [MockRow(row, schema=None) for row in result_df.to_dicts()]

    def close(self) -> None:
        """Close the materializer and clean up resources."""
        # Polars doesn't require explicit cleanup
        pass

