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
                columns_to_select = op_val
                cte_sql = self.build_select_cte(
                    current_cte_name, columns_to_select, source_table_obj
                )
                # Update current columns for select operations
                # Extract column names from the select operation, including aliases
                current_columns = self._extract_column_names_from_select(
                    columns_to_select, current_columns
                )
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

    def _extract_column_names_from_select(
        self, columns: Tuple[Any, ...], existing_columns: List[str]
    ) -> List[str]:
        """Extract column names from a select operation.

        Args:
            columns: Columns tuple from select operation
            existing_columns: Current list of available columns

        Returns:
            Updated list of column names after select
        """
        selected_columns = []

        for col in columns:
            if isinstance(col, str):
                if col == "*":
                    # Wildcard - include all existing columns
                    return existing_columns.copy()
                else:
                    selected_columns.append(col)
            elif hasattr(col, "name"):
                # Column with name (alias or original)
                col_name = col.name
                if col_name == "*":
                    return existing_columns.copy()
                selected_columns.append(col_name)
            elif hasattr(col, "operation"):
                # Column operation - use its name attribute if available
                col_name = getattr(col, "name", None)
                if col_name:
                    selected_columns.append(col_name)
                # If no name, it's an expression without alias - skip for column tracking
            elif hasattr(col, "value") and hasattr(col, "data_type"):
                # MockLiteral - use its name
                selected_columns.append(col.name)

        # If no columns selected (shouldn't happen), return existing
        return selected_columns if selected_columns else existing_columns.copy()

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

        # Convert condition to SQL using the CURRENT CTE alias for qualification
        class _Alias:
            def __init__(self, name: str) -> None:
                self.name = name

        filter_sql = self.expression_translator.condition_to_sql(
            condition, _Alias(source_name)
        )

        # Ensure all column references use the CTE alias, not original table names
        # Replace any table-qualified column references with the CTE alias
        # This handles cases where column references were generated with original table names
        import re

        if filter_sql:
            # Pattern to match table-qualified columns in various formats:
            # - "temp_table_0"."column" -> source_name."column"
            # - temp_table_0."column" -> source_name."column"
            # - temp_table_0.column -> source_name."column" (if column is quoted elsewhere)
            # Replace with source_name (CTE alias)
            # Match quoted table names
            filter_sql = re.sub(
                r'"[a-z0-9_]+"\.(".*?")', rf"{source_name}.\1", filter_sql
            )
            # Match unquoted table names followed by quoted column names
            filter_sql = re.sub(
                r'[a-z0-9_]+\.(".*?")', rf"{source_name}.\1", filter_sql
            )
            # Also handle unquoted column names that might have table qualifiers
            # This is a more aggressive pattern that catches any table.column pattern
            filter_sql = re.sub(
                r'\b[a-z0-9_]+\s*\.\s*"([^"]+)"',
                rf'{source_name}."\1"',
                filter_sql,
                flags=re.IGNORECASE,
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

        # Check for explode operations - need special handling
        explode_cols = []
        non_explode_parts = []

        for col in columns:
            is_explode = hasattr(col, "operation") and col.operation in [
                "explode",
                "explode_outer",
            ]

            if is_explode:
                explode_cols.append(col)
            elif isinstance(col, str):
                if col == "*":
                    non_explode_parts.append("*")
                else:
                    non_explode_parts.append(f'"{col}"')
            elif hasattr(col, "value") and hasattr(col, "data_type"):
                # MockLiteral
                if isinstance(col.value, str):
                    non_explode_parts.append(f"'{col.value}' AS \"{col.name}\"")
                else:
                    non_explode_parts.append(f'{col.value} AS "{col.name}"')
            elif hasattr(col, "operation"):
                # Column operation (but not explode)
                expr_sql = self.expression_translator.expression_to_sql(
                    col, source_name
                )
                col_name = getattr(col, "name", "result")
                non_explode_parts.append(f'{expr_sql} AS "{col_name}"')
            elif hasattr(col, "name"):
                # Check for alias or regular column
                original_col = getattr(col, "_original_column", None) or getattr(
                    col, "original_column", None
                )
                if original_col is not None:
                    non_explode_parts.append(f'"{original_col.name}" AS "{col.name}"')
                elif col.name == "*":
                    non_explode_parts.append("*")
                else:
                    # Regular column reference (e.g., MockColumn)
                    non_explode_parts.append(f'"{col.name}"')

        # Handle explode operations - use simpler UNNEST pattern
        if explode_cols:
            # For explode, we need to select other columns plus the UNNEST result
            columns_list = ", ".join(non_explode_parts) if non_explode_parts else "*"

            # For each explode column, create UNNEST in SELECT
            explode_parts = []
            for explode_col in explode_cols:
                array_col = explode_col.column.name
                alias = getattr(explode_col, "name", "exploded")

                if explode_col.operation == "explode_outer":
                    unnest_expr = f'UNNEST(COALESCE("{array_col}", [NULL]))'
                else:
                    unnest_expr = f'UNNEST("{array_col}")'

                # Use UNNEST as scalar subquery: SELECT ..., UNNEST(...) AS col FROM table
                # Note: This syntax doesn't expand rows, it just returns the array as-is
                # We need to detect this and handle it differently - for now, return
                # a placeholder that will be handled in _apply_select for table-based execution
                explode_parts.append(f'{unnest_expr} AS "{alias}"')

            all_parts = [columns_list] if columns_list else []
            all_parts.extend(explode_parts)
            return f"SELECT {', '.join(all_parts)} FROM {source_name}"
        else:
            # No explode operations - build regular SELECT
            if non_explode_parts.count("*") > 1:
                non_explode_parts = ["*"]

            columns_clause = ", ".join(non_explode_parts) if non_explode_parts else "*"
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
            if (
                hasattr(col, "name")
                and hasattr(col, "_original_column")
                and col._original_column is not None
            ):
                existing_cols[col.name] = col._original_column.name

        # Handle wildcard columns first - if we have "*", include all source table columns
        has_wildcard = any(
            (isinstance(col, str) and col == "*")
            or (hasattr(col, "name") and col.name == "*")
            for col in columns
        )

        if (
            has_wildcard
            and source_table_obj is not None
            and hasattr(source_table_obj, "columns")
        ):
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
                if (
                    col.column is None
                    or col.column == "*"
                    or (hasattr(col.column, "name") and col.column.name == "*")
                ):
                    # For window functions, we don't need a column expression for functions like ROW_NUMBER
                    if func_name in ["ROW_NUMBER", "RANK", "DENSE_RANK"]:
                        col_expr = ""
                    elif func_name in ["LAG", "LEAD"]:
                        # LAG and LEAD need a specific column, use the first available column
                        if source_table_obj is not None and hasattr(
                            source_table_obj, "columns"
                        ):
                            try:
                                columns_list = list(source_table_obj.columns)
                                if columns_list:
                                    col_expr = f'"{columns_list[0].name}"'
                                else:
                                    col_expr = "*"
                            except Exception:
                                col_expr = "*"
                        else:
                            col_expr = "*"
                    else:
                        col_expr = "*"
                else:
                    if hasattr(col.column, "name"):
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
                if (
                    hasattr(col, "_original_column")
                    and col._original_column is not None
                ):
                    # This is an aliased column - use original column name with alias
                    original_name = col._original_column.name
                    alias_name = col.name
                    select_parts.append(f'"{original_name}" AS "{alias_name}"')
                elif hasattr(col, "operation") or (
                    hasattr(col, "column") and hasattr(col, "value")
                ):
                    # This is a MockColumnOperation expression (e.g., col1 + col2, col.cast(...))
                    # Convert to SQL using expression translator
                    col_expr_sql = self.expression_translator.expression_to_sql(
                        col, source_name
                    )
                    col_alias = col.name if hasattr(col, "name") else "col_expr"
                    select_parts.append(f'{col_expr_sql} AS "{col_alias}"')
                else:
                    # Regular column name
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
                    # Pass source_name to expression_to_sql so it can check column types
                    expr_sql = self.expression_translator.expression_to_sql(
                        col, source_table=source_name
                    )
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
                # Pass source_name to expression_to_sql so it can check column types
                expr_sql = self.expression_translator.expression_to_sql(
                    col, source_table=source_name
                )
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
        elif hasattr(on, "operation") and on.operation == "==":
            # Handle MockColumnOperation (e.g., emp_df.dept_id == dept_df.dept_id)
            # Extract column names from equality condition
            left_col = on.column.name if hasattr(on.column, "name") else str(on.column)
            # For join conditions with two different DataFrames, use the column name
            # from both sides (they should be the same in most cases)
            join_condition = (
                f'{source_name}."{left_col}" = {other_table_name}."{left_col}"'
            )
        elif hasattr(on, "operation"):
            # Other column operation as join condition - fallback
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

        # Explicitly select columns to avoid ambiguous column names
        # Get columns from source table
        source_columns = [
            f'{source_name}."{col.name}"' for col in source_table_obj.columns
        ]
        # Get columns from other table (excluding join keys to avoid duplication)
        other_schema = other_df.schema if hasattr(other_df, "schema") else other_df
        other_column_names = [field.name for field in other_schema.fields]

        # Determine join keys for exclusion
        if isinstance(on, str):
            join_keys = [on]
        elif isinstance(on, list):
            join_keys = on
        else:
            join_keys = []

        other_columns = [
            f'{other_table_name}."{col}"'
            for col in other_column_names
            if col not in join_keys
        ]

        # Combine columns: source columns first, then other columns
        all_columns = source_columns + other_columns
        select_clause = ", ".join(all_columns)

        return f"SELECT {select_clause} FROM {source_name} {join_type} {other_table_name} ON {join_condition}"

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
