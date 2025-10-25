"""
CTE (Common Table Expression) query builder for Mock Spark.

This module provides CTE query construction for complex DataFrame operations.
"""

from typing import Any, List, Tuple
from .sql_expression_translator import SQLExpressionTranslator


class CTEQueryBuilder:
    """Builds CTE (Common Table Expression) queries for DataFrame operations."""

    def __init__(self, expression_translator: SQLExpressionTranslator):
        """Initialize CTE query builder.

        Args:
            expression_translator: SQL expression translator for converting expressions
        """
        self.expression_translator = expression_translator

    def build_cte_query(
        self, source_table_name: str, operations: List[Tuple[str, Any]]
    ) -> str:
        """Build a single SQL query with CTEs for all operations.

        Args:
            source_table_name: Name of the initial table with data
            operations: List of (operation_name, operation_payload) tuples

        Returns:
            Complete SQL query with CTEs
        """
        # Get source table from table manager
        table_manager = self.expression_translator.table_manager
        source_table_obj = table_manager.get_table(source_table_name)
        if source_table_obj is None:
            raise ValueError(f"Table {source_table_name} not found")

        cte_definitions = []
        current_cte_name = source_table_name
        # Track columns as we build CTEs for operations that modify schema
        current_columns = [c.name for c in source_table_obj.columns]

        for i, (op_name, op_val) in enumerate(operations):
            cte_name = f"cte_{i}"

            if op_name == "filter":
                cte_sql = self.build_filter_cte(
                    current_cte_name, op_val, source_table_obj
                )
            elif op_name == "select":
                cte_sql = self.build_select_cte(
                    current_cte_name, op_val, source_table_obj
                )
                # Update current columns for select operations
                # Note: This is a simplification - full implementation would parse the select
                # For now, we'll just use source columns as we don't modify in place
            elif op_name == "withColumn":
                col_name, col = op_val
                cte_sql = self.build_with_column_cte(
                    current_cte_name, col_name, col, current_columns
                )
                # Track the new column
                if col_name not in current_columns:
                    current_columns.append(col_name)
            elif op_name == "orderBy":
                cte_sql = self.build_order_by_cte(
                    current_cte_name, op_val, source_table_obj
                )
            elif op_name == "limit":
                cte_sql = self.build_limit_cte(current_cte_name, op_val)
            elif op_name == "join":
                cte_sql = self.build_join_cte(
                    current_cte_name, op_val, source_table_obj
                )
            elif op_name == "union":
                cte_sql = self.build_union_cte(
                    current_cte_name, op_val, source_table_obj
                )
            else:
                # Unknown operation, skip
                continue

            cte_definitions.append(f"{cte_name} AS ({cte_sql})")
            current_cte_name = cte_name

        # Build final query
        if cte_definitions:
            cte_clause = "WITH " + ",\n     ".join(cte_definitions)
            final_query = f"{cte_clause}\nSELECT * FROM {current_cte_name}"
        else:
            final_query = f"SELECT * FROM {source_table_name}"

        return final_query

    def build_filter_cte(
        self, source_name: str, condition: Any, source_table_obj: Any
    ) -> str:
        """Build CTE SQL for filter operation.

        Args:
            source_name: Name of the source CTE/table
            condition: Filter condition
            source_table_obj: Source table object

        Returns:
            SQL string for filter CTE
        """
        # Convert condition to SQL
        filter_sql = self.expression_translator.condition_to_sql(
            condition, source_table_obj
        )
        return f"SELECT * FROM {source_name} WHERE {filter_sql}"

    def build_select_cte(
        self, source_name: str, columns: Tuple[Any, ...], source_table_obj: Any
    ) -> str:
        """Build CTE SQL for select operation.

        Args:
            source_name: Name of the source CTE/table
            columns: Columns to select
            source_table_obj: Source table object

        Returns:
            SQL string for select CTE
        """
        # Check for window functions
        has_window_functions = any(
            (hasattr(col, "function_name") and hasattr(col, "window_spec"))
            or (
                hasattr(col, "function_name")
                and hasattr(col, "column")
                and col.__class__.__name__ == "MockAggregateFunction"
            )
            for col in columns
        )

        if has_window_functions:
            return self.build_select_with_window_cte(
                source_name, columns, source_table_obj
            )

        # Build column list
        select_parts = []
        for col in columns:
            if isinstance(col, str):
                if col == "*":
                    select_parts.append("*")
                else:
                    select_parts.append(f'"{col}"')
            elif hasattr(col, "value") and hasattr(col, "data_type"):
                # MockLiteral
                if isinstance(col.value, str):
                    select_parts.append(f"'{col.value}' AS \"{col.name}\"")
                else:
                    select_parts.append(f'{col.value} AS "{col.name}"')
            elif hasattr(col, "operation"):
                # Column operation
                expr_sql = self.expression_translator.expression_to_sql(col)
                col_name = getattr(col, "name", "result")
                select_parts.append(f'{expr_sql} AS "{col_name}"')
            elif hasattr(col, "name"):
                # Check for alias
                original_col = getattr(col, "_original_column", None) or getattr(
                    col, "original_column", None
                )
                if original_col is not None:
                    select_parts.append(f'"{original_col.name}" AS "{col.name}"')
                elif col.name == "*":
                    select_parts.append("*")
                else:
                    select_parts.append(f'"{col.name}"')

        # Remove duplicate "*" entries and keep only one
        if select_parts.count("*") > 1:
            select_parts = ["*"]

        columns_clause = ", ".join(select_parts) if select_parts else "*"
        return f"SELECT {columns_clause} FROM {source_name}"

    def build_select_with_window_cte(
        self, source_name: str, columns: Tuple[Any, ...], source_table_obj: Any
    ) -> str:
        """Build CTE SQL for select with window functions.

        Args:
            source_name: Name of the source CTE/table
            columns: Columns to select
            source_table_obj: Source table object

        Returns:
            SQL string for select with window functions CTE
        """
        select_parts = []
        
        # Track aliases before adding window functions
        existing_cols = {}
        for col in columns:
            if hasattr(col, "name") and hasattr(col, "_original_column") and col._original_column is not None:
                existing_cols[col.name] = col._original_column.name
        
        # Handle wildcard columns first - if we have "*", include all source table columns
        has_wildcard = any(
            (isinstance(col, str) and col == "*") or 
            (hasattr(col, "name") and col.name == "*")
            for col in columns
        )
        
        if has_wildcard and source_table_obj is not None and hasattr(source_table_obj, 'columns'):
            # Add all source table columns when we have "*"
            for col in source_table_obj.columns:
                select_parts.append(f'"{col.name}"')

        for col in columns:
            if isinstance(col, str):
                if col == "*":
                    # Already handled above
                    continue
                else:
                    # Only add if not already included
                    if f'"{col}"' not in select_parts:
                        select_parts.append(f'"{col}"')
            elif hasattr(col, "name") and col.name == "*":
                # Already handled above
                continue
            elif hasattr(col, "function_name") and hasattr(col, "window_spec"):
                # Window function
                func_name = col.function_name.upper()
                if col.column is None or col.column == "*" or (hasattr(col.column, "name") and col.column.name == "*"):
                    # For window functions, we don't need a column expression for functions like ROW_NUMBER
                    if func_name in ["ROW_NUMBER", "RANK", "DENSE_RANK"]:
                        col_expr = ""
                    elif func_name in ["LAG", "LEAD"]:
                        # LAG and LEAD need a specific column, use the first available column
                        if source_table_obj is not None and hasattr(source_table_obj, 'columns'):
                            try:
                                columns_list = list(source_table_obj.columns)
                                if columns_list:
                                    col_expr = f'"{columns_list[0].name}"'
                                else:
                                    col_expr = "*"
                            except:
                                col_expr = "*"
                        else:
                            col_expr = "*"
                    else:
                        col_expr = "*"
                else:
                    if hasattr(col.column, 'name'):
                        col_expr = f'"{col.column.name}"'
                    else:
                        col_expr = f'"{col.column}"'

                window_sql = self.expression_translator.window_spec_to_sql(
                    col.window_spec, source_table_obj, existing_cols
                )
                result_name = getattr(col, "name", f"{func_name.lower()}_result")
                if col_expr:
                    select_parts.append(
                        f'{func_name}({col_expr}) OVER ({window_sql}) AS "{result_name}"'
                    )
                else:
                    select_parts.append(
                        f'{func_name}() OVER ({window_sql}) AS "{result_name}"'
                    )
            elif (
                hasattr(col, "function_name")
                and col.__class__.__name__ == "MockAggregateFunction"
            ):
                # Aggregate function
                func_name = col.function_name.upper()
                if col.column is None or col.column == "*":
                    col_expr = "*"
                elif isinstance(col.column, str):
                    col_expr = f'"{col.column}"'
                else:
                    col_expr = f'"{col.column.name}"'
                result_name = getattr(col, "name", f"{func_name.lower()}_result")
                select_parts.append(f'{func_name}({col_expr}) AS "{result_name}"')
            elif hasattr(col, "name"):
                # Check if this is an aliased column
                if hasattr(col, "_original_column") and col._original_column is not None:
                    # This is an aliased column - use original column name with alias
                    original_name = col._original_column.name
                    alias_name = col.name
                    select_parts.append(f'"{original_name}" AS "{alias_name}"')
                else:
                    # Regular column
                    select_parts.append(f'"{col.name}"')

        columns_clause = ", ".join(select_parts) if select_parts else "*"
        return f"SELECT {columns_clause} FROM {source_name}"

    def build_with_column_cte(
        self, source_name: str, col_name: str, col: Any, existing_columns: List[str]
    ) -> str:
        """Build CTE SQL for withColumn operation.

        Args:
            source_name: Name of the source CTE/table
            col_name: Name of the column to add/replace
            col: Column expression
            existing_columns: List of column names in the source

        Returns:
            SQL string for withColumn CTE
        """
        # Check if we're replacing an existing column or adding a new one
        select_parts = []
        column_added = False

        for existing_col in existing_columns:
            if existing_col == col_name:
                # Replace this column with new expression
                if hasattr(col, "value") and hasattr(col, "data_type"):
                    # Literal value
                    if isinstance(col.value, str):
                        select_parts.append(f"'{col.value}' AS \"{col_name}\"")
                    else:
                        select_parts.append(f'{col.value} AS "{col_name}"')
                elif hasattr(col, "operation"):
                    # Column operation
                    expr_sql = self.expression_translator.expression_to_sql(col)
                    select_parts.append(f'{expr_sql} AS "{col_name}"')
                else:
                    # Simple column reference
                    select_parts.append(f'"{col_name}"')
                column_added = True
            else:
                # Keep existing column
                select_parts.append(f'"{existing_col}"')

        # If column wasn't replaced, add it as new
        if not column_added:
            if hasattr(col, "value") and hasattr(col, "data_type"):
                # Literal value
                if isinstance(col.value, str):
                    select_parts.append(f"'{col.value}' AS \"{col_name}\"")
                else:
                    select_parts.append(f'{col.value} AS "{col_name}"')
            elif hasattr(col, "operation"):
                # Column operation
                expr_sql = self.expression_translator.expression_to_sql(col)
                select_parts.append(f'{expr_sql} AS "{col_name}"')
            else:
                # Simple column reference
                select_parts.append(f'"{col_name}"')

        columns_clause = ", ".join(select_parts)
        return f"SELECT {columns_clause} FROM {source_name}"

    def build_order_by_cte(
        self, source_name: str, columns: Tuple[Any, ...], source_table_obj: Any
    ) -> str:
        """Build CTE SQL for orderBy operation.

        Args:
            source_name: Name of the source CTE/table
            columns: Order by columns
            source_table_obj: Source table object

        Returns:
            SQL string for orderBy CTE
        """
        order_parts = []

        for col in columns:
            if isinstance(col, str):
                order_parts.append(f'"{col}"')
            elif hasattr(col, "operation") and col.operation == "desc":
                order_parts.append(f'"{col.column.name}" DESC')
            elif hasattr(col, "operation") and col.operation == "asc":
                order_parts.append(f'"{col.column.name}" ASC')
            elif hasattr(col, "name"):
                order_parts.append(f'"{col.name}"')

        order_clause = ", ".join(order_parts)
        return f"SELECT * FROM {source_name} ORDER BY {order_clause}"

    def build_limit_cte(self, source_name: str, limit_count: int) -> str:
        """Build CTE SQL for limit operation.

        Args:
            source_name: Name of the source CTE/table
            limit_count: Number of rows to limit

        Returns:
            SQL string for limit CTE
        """
        return f"SELECT * FROM {source_name} LIMIT {limit_count}"

    def build_join_cte(
        self, source_name: str, join_params: Tuple[Any, ...], source_table_obj: Any
    ) -> str:
        """Build CTE SQL for join operation.

        Args:
            source_name: Name of the source CTE/table
            join_params: Join parameters (other_df, on, how)
            source_table_obj: Source table object

        Returns:
            SQL string for join CTE
        """
        other_df, on, how = join_params

        # Materialize the other DataFrame if it's lazy
        if hasattr(other_df, "_operations_queue") and other_df._operations_queue:
            other_df = other_df._materialize_if_lazy()

        # Create a temporary table for the other DataFrame
        table_manager = self.expression_translator.table_manager
        other_table_name = f"temp_join_{id(other_df)}"
        table_manager.create_table_with_data(other_table_name, other_df.data)

        # Build join condition
        if isinstance(on, str):
            join_condition = f'{source_name}."{on}" = {other_table_name}."{on}"'
        elif isinstance(on, list):
            conditions = [
                f'{source_name}."{col}" = {other_table_name}."{col}"' for col in on
            ]
            join_condition = " AND ".join(conditions)
        elif hasattr(on, "operation"):
            # Column operation as join condition
            join_condition = self.expression_translator.condition_to_sql(
                on, source_table_obj
            )
        else:
            join_condition = "1=1"  # Fallback

        # Map join type
        join_type_map = {
            "inner": "INNER JOIN",
            "left": "LEFT JOIN",
            "right": "RIGHT JOIN",
            "outer": "FULL OUTER JOIN",
            "full": "FULL OUTER JOIN",
            "left_outer": "LEFT JOIN",
            "right_outer": "RIGHT JOIN",
            "full_outer": "FULL OUTER JOIN",
        }
        join_type = join_type_map.get(how, "INNER JOIN")

        return f"SELECT * FROM {source_name} {join_type} {other_table_name} ON {join_condition}"

    def build_union_cte(
        self, source_name: str, other_df: Any, source_table_obj: Any
    ) -> str:
        """Build CTE SQL for union operation.

        Args:
            source_name: Name of the source CTE/table
            other_df: Other DataFrame to union with
            source_table_obj: Source table object

        Returns:
            SQL string for union CTE
        """
        # Materialize the other DataFrame if it's lazy
        if hasattr(other_df, "_operations_queue") and other_df._operations_queue:
            other_df = other_df._materialize_if_lazy()

        # Create a temporary table for the other DataFrame
        table_manager = self.expression_translator.table_manager
        other_table_name = f"temp_union_{id(other_df)}"
        table_manager.create_table_with_data(other_table_name, other_df.data)

        return f"SELECT * FROM {source_name} UNION ALL SELECT * FROM {other_table_name}"
