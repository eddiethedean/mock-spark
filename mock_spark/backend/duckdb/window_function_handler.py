"""
Window Function Handler for Mock Spark.

This module provides handling of window functions and window specifications
for DataFrame operations using SQLAlchemy with DuckDB.
"""

from typing import Any, Tuple, Dict
from sqlalchemy import Session, text, func, Column, String, Table
from sqlalchemy.sql import ColumnElement

from .table_manager import DuckDBTableManager
from .sql_expression_converter import SQLExpressionConverter


class WindowFunctionHandler:
    """Handles window functions and window specifications."""

    def __init__(self, engine, metadata, created_tables: Dict[str, Any]):
        """Initialize the window function handler.
        
        Args:
            engine: SQLAlchemy engine
            metadata: SQLAlchemy metadata
            created_tables: Dictionary of created tables
        """
        self.engine = engine
        self.metadata = metadata
        self._created_tables = created_tables
        self.table_manager = DuckDBTableManager(engine, metadata, created_tables)
        self.sql_converter = SQLExpressionConverter()

    def apply_select_with_window_functions(
        self, source_table: str, target_table: str, columns: Tuple[Any, ...]
    ) -> None:
        """Apply select operation with window functions using raw SQL."""
        source_table_obj = self._created_tables[source_table]
        
        # Build the SQL query using window function SQL
        window_sql = self._build_window_sql(source_table, columns, source_table_obj)
        
        # Execute the query and create the target table
        with Session(self.engine) as session:
            # Execute the window function query
            results = session.execute(text(window_sql)).all()
            
            # Get column information from the results
            if results:
                # Create target table with appropriate columns
                new_columns = []
                for i, col in enumerate(columns):
                    if isinstance(col, str):
                        if col == "*":
                            # Add all columns from source table
                            for source_col in source_table_obj.columns:
                                new_columns.append(Column(source_col.name, source_col.type, primary_key=False))
                        else:
                            new_columns.append(Column(col, String, primary_key=False))
                    elif hasattr(col, "name"):
                        new_columns.append(Column(col.name, String, primary_key=False))
                    else:
                        new_columns.append(Column(f"col_{i}", String, primary_key=False))
                
                # Create target table
                target_table_obj = Table(target_table, self.metadata, *new_columns)
                target_table_obj.create(self.engine, checkfirst=True)
                self._created_tables[target_table] = target_table_obj
                
                # Insert results
                for result in results:
                    result_dict = {}
                    for i, column in enumerate(new_columns):
                        if i < len(result):
                            result_dict[column.name] = result[i]
                    insert_stmt = target_table_obj.insert().values(result_dict)
                    session.execute(insert_stmt)
                session.commit()

    def apply_window_function(
        self, source_table: str, target_table: str, window_func: Any
    ) -> None:
        """Apply a window function operation."""
        source_table_obj = self._created_tables[source_table]

        # Create target table with same structure plus window function column
        new_columns = []
        for existing_col in source_table_obj.columns:
            new_columns.append(Column(existing_col.name, existing_col.type, primary_key=False))
        
        # Add window function column
        new_columns.append(Column("window_result", String, primary_key=False))

        # Create target table
        target_table_obj = Table(target_table, self.metadata, *new_columns)
        target_table_obj.create(self.engine, checkfirst=True)
        self._created_tables[target_table] = target_table_obj

        # Build window function SQL
        window_sql = self._build_window_function_sql(source_table, window_func, source_table_obj)

        # Execute the query and insert results
        with Session(self.engine) as session:
            results = session.execute(text(window_sql)).all()

            # Insert into target table
            for result in results:
                result_dict = {}
                for i, column in enumerate(new_columns):
                    if i < len(result):
                        result_dict[column.name] = result[i]
                insert_stmt = target_table_obj.insert().values(result_dict)
                session.execute(insert_stmt)
            session.commit()

    def _build_window_sql(
        self, source_table: str, columns: Tuple[Any, ...], source_table_obj: Any
    ) -> str:
        """Build SQL query with window functions."""
        # Build SELECT clause
        select_parts = []
        
        for col in columns:
            if isinstance(col, str):
                if col == "*":
                    # Add all columns from source table
                    for source_col in source_table_obj.columns:
                        select_parts.append(f'"{source_col.name}"')
                else:
                    select_parts.append(f'"{col}"')
            elif hasattr(col, "function_name") and hasattr(col, "window_spec"):
                # Window function
                func_sql = self._build_window_function_sql_part(col)
                select_parts.append(func_sql)
            elif hasattr(col, "name"):
                select_parts.append(f'"{col.name}"')
            else:
                select_parts.append(f'"{str(col)}"')
        
        # Build FROM clause
        from_clause = f'FROM "{source_table}"'
        
        # Build ORDER BY clause if needed
        order_by_clause = ""
        for col in columns:
            if hasattr(col, "window_spec") and hasattr(col.window_spec, "order_by"):
                order_by_parts = []
                for order_col in col.window_spec.order_by:
                    if hasattr(order_col, "column") and hasattr(order_col.column, "name"):
                        col_name = order_col.column.name
                        if hasattr(order_col, "operation") and order_col.operation == "desc":
                            order_by_parts.append(f'"{col_name}" DESC')
                        else:
                            order_by_parts.append(f'"{col_name}" ASC')
                if order_by_parts:
                    order_by_clause = f"ORDER BY {', '.join(order_by_parts)}"
                break
        
        # Combine all parts
        sql = f"SELECT {', '.join(select_parts)} {from_clause} {order_by_clause}".strip()
        return sql

    def _build_window_function_sql(
        self, source_table: str, window_func: Any, source_table_obj: Any
    ) -> str:
        """Build SQL query for a single window function."""
        # Get all columns from source table
        column_names = [col.name for col in source_table_obj.columns]
        
        # Build window function SQL
        func_sql = self._build_window_function_sql_part(window_func)
        
        # Build the complete query
        quoted_names = [f'"{name}"' for name in column_names]
        sql = f"SELECT {', '.join(quoted_names)}, {func_sql} FROM \"{source_table}\""
        
        return sql

    def _build_window_function_sql_part(self, window_func: Any) -> str:
        """Build SQL part for a window function."""
        if not hasattr(window_func, "function_name"):
            return "NULL"
        
        func_name = window_func.function_name.upper()
        
        # Build OVER clause
        over_clause = self._build_over_clause(window_func)
        
        # Build function call
        if func_name == "ROW_NUMBER":
            return f"ROW_NUMBER() {over_clause}"
        elif func_name == "RANK":
            return f"RANK() {over_clause}"
        elif func_name == "DENSE_RANK":
            return f"DENSE_RANK() {over_clause}"
        elif func_name == "LAG":
            offset = getattr(window_func, "offset", 1)
            default = getattr(window_func, "default", None)
            if hasattr(window_func, "column"):
                col_name = window_func.column.name
                default_sql = f", {default}" if default is not None else ""
                return f"LAG(\"{col_name}\", {offset}{default_sql}) {over_clause}"
            else:
                return f"LAG(NULL, {offset}) {over_clause}"
        elif func_name == "LEAD":
            offset = getattr(window_func, "offset", 1)
            default = getattr(window_func, "default", None)
            if hasattr(window_func, "column"):
                col_name = window_func.column.name
                default_sql = f", {default}" if default is not None else ""
                return f"LEAD(\"{col_name}\", {offset}{default_sql}) {over_clause}"
            else:
                return f"LEAD(NULL, {offset}) {over_clause}"
        elif func_name == "FIRST_VALUE":
            if hasattr(window_func, "column"):
                col_name = window_func.column.name
                return f"FIRST_VALUE(\"{col_name}\") {over_clause}"
            else:
                return f"FIRST_VALUE(NULL) {over_clause}"
        elif func_name == "LAST_VALUE":
            if hasattr(window_func, "column"):
                col_name = window_func.column.name
                return f"LAST_VALUE(\"{col_name}\") {over_clause}"
            else:
                return f"LAST_VALUE(NULL) {over_clause}"
        elif func_name == "SUM":
            if hasattr(window_func, "column"):
                col_name = window_func.column.name
                return f"SUM(\"{col_name}\") {over_clause}"
            else:
                return f"SUM(NULL) {over_clause}"
        elif func_name == "AVG":
            if hasattr(window_func, "column"):
                col_name = window_func.column.name
                return f"AVG(\"{col_name}\") {over_clause}"
            else:
                return f"AVG(NULL) {over_clause}"
        elif func_name == "COUNT":
            if hasattr(window_func, "column"):
                col_name = window_func.column.name
                return f"COUNT(\"{col_name}\") {over_clause}"
            else:
                return f"COUNT(*) {over_clause}"
        elif func_name == "MIN":
            if hasattr(window_func, "column"):
                col_name = window_func.column.name
                return f"MIN(\"{col_name}\") {over_clause}"
            else:
                return f"MIN(NULL) {over_clause}"
        elif func_name == "MAX":
            if hasattr(window_func, "column"):
                col_name = window_func.column.name
                return f"MAX(\"{col_name}\") {over_clause}"
            else:
                return f"MAX(NULL) {over_clause}"
        else:
            return f"NULL {over_clause}"

    def _build_over_clause(self, window_func: Any) -> str:
        """Build OVER clause for window function."""
        if not hasattr(window_func, "window_spec"):
            return "OVER ()"
        
        window_spec = window_func.window_spec
        over_parts = []
        
        # Add PARTITION BY clause
        if hasattr(window_spec, "partition_by") and window_spec.partition_by:
            partition_cols = []
            for col in window_spec.partition_by:
                if hasattr(col, "name"):
                    partition_cols.append(f'"{col.name}"')
                else:
                    partition_cols.append(f'"{str(col)}"')
            if partition_cols:
                over_parts.append(f"PARTITION BY {', '.join(partition_cols)}")
        
        # Add ORDER BY clause
        if hasattr(window_spec, "order_by") and window_spec.order_by:
            order_cols = []
            for col in window_spec.order_by:
                if hasattr(col, "column") and hasattr(col.column, "name"):
                    col_name = col.column.name
                    if hasattr(col, "operation") and col.operation == "desc":
                        order_cols.append(f'"{col_name}" DESC')
                    else:
                        order_cols.append(f'"{col_name}" ASC')
                else:
                    order_cols.append(f'"{str(col)}" ASC')
            if order_cols:
                over_parts.append(f"ORDER BY {', '.join(order_cols)}")
        
        # Add ROWS/RANGE clause
        if hasattr(window_spec, "rows_between"):
            rows_between = window_spec.rows_between
            if rows_between:
                over_parts.append(f"ROWS BETWEEN {rows_between}")
        elif hasattr(window_spec, "range_between"):
            range_between = window_spec.range_between
            if range_between:
                over_parts.append(f"RANGE BETWEEN {range_between}")
        
        # Combine all parts
        if over_parts:
            return f"OVER ({' '.join(over_parts)})"
        else:
            return "OVER ()"

    def window_spec_to_sql(self, window_spec: Any, table_obj: Any = None) -> str:
        """Convert window specification to SQL."""
        if not window_spec:
            return "OVER ()"
        
        over_parts = []
        
        # Add PARTITION BY clause
        if hasattr(window_spec, "partition_by") and window_spec.partition_by:
            partition_cols = []
            for col in window_spec.partition_by:
                if hasattr(col, "name"):
                    partition_cols.append(f'"{col.name}"')
                else:
                    partition_cols.append(f'"{str(col)}"')
            if partition_cols:
                over_parts.append(f"PARTITION BY {', '.join(partition_cols)}")
        
        # Add ORDER BY clause
        if hasattr(window_spec, "order_by") and window_spec.order_by:
            order_cols = []
            for col in window_spec.order_by:
                if hasattr(col, "column") and hasattr(col.column, "name"):
                    col_name = col.column.name
                    if hasattr(col, "operation") and col.operation == "desc":
                        order_cols.append(f'"{col_name}" DESC')
                    else:
                        order_cols.append(f'"{col_name}" ASC')
                else:
                    order_cols.append(f'"{str(col)}" ASC')
            if order_cols:
                over_parts.append(f"ORDER BY {', '.join(order_cols)}")
        
        # Add ROWS/RANGE clause
        if hasattr(window_spec, "rows_between"):
            rows_between = window_spec.rows_between
            if rows_between:
                over_parts.append(f"ROWS BETWEEN {rows_between}")
        elif hasattr(window_spec, "range_between"):
            range_between = window_spec.range_between
            if range_between:
                over_parts.append(f"RANGE BETWEEN {range_between}")
        
        # Combine all parts
        if over_parts:
            return f"OVER ({' '.join(over_parts)})"
        else:
            return "OVER ()"
