"""
DuckDB-based materialization for lazy evaluation.

This module uses DuckDB's query optimizer to handle operation ordering and execution,
eliminating the need for manual operation dependency analysis.
"""

import duckdb
from typing import Any, Dict, List, Optional, Union, Tuple
from ..spark_types import MockStructType, MockStructField, MockRow
from .sql_builder import SQLQueryBuilder


class DuckDBMaterializer:
    """Materializes lazy DataFrames using DuckDB's query optimizer."""

    def __init__(self, max_memory: str = "1GB", allow_disk_spillover: bool = False):
        """Initialize DuckDB materializer.

        Args:
            max_memory: Maximum memory for DuckDB to use (e.g., '1GB', '4GB', '8GB').
            allow_disk_spillover: If True, allows DuckDB to spill to disk when memory is full.
        """
        self.connection = duckdb.connect(":memory:")
        self._temp_table_counter = 0
        self._temp_dir = None

        # Configure DuckDB memory and spillover settings
        try:
            self.connection.execute(f"SET max_memory='{max_memory}'")

            if allow_disk_spillover:
                # Create unique temp directory for this materializer
                import tempfile
                import uuid

                self._temp_dir = tempfile.mkdtemp(prefix=f"duckdb_mat_{uuid.uuid4().hex[:8]}_")
                self.connection.execute(f"SET temp_directory='{self._temp_dir}'")
            else:
                # Disable disk spillover for test isolation
                self.connection.execute("SET temp_directory=''")
        except:
            pass  # Ignore if settings not supported

    def materialize(
        self, data: List[Dict[str, Any]], schema: MockStructType, operations: List[Tuple[str, Any]]
    ) -> List[MockRow]:
        """Materialize a lazy DataFrame using DuckDB optimization."""
        if not operations:
            # No operations to apply, return original data as rows
            return [MockRow(row) for row in data]

        # Create a temporary table name
        temp_table = f"temp_table_{self._temp_table_counter}"
        self._temp_table_counter += 1

        # Build SQL query from operations - use original schema for table creation
        builder = SQLQueryBuilder(temp_table, schema)

        # Create initial temporary table with data
        create_sql = builder.create_temp_table_sql(data)
        if create_sql:
            self.connection.execute(create_sql)

        current_table = builder.table_name
        temp_counter = 1

        # Apply operations step by step, creating intermediate tables
        for op_name, op_val in operations:
            # Create a new builder for the next step
            next_table = f"temp_table_{self._temp_table_counter}_{temp_counter}"
            temp_counter += 1
            step_builder = SQLQueryBuilder(next_table, builder.schema)

            if op_name == "filter":
                step_builder.add_filter(op_val)
            elif op_name == "select":
                step_builder.add_select(op_val)
            elif op_name == "withColumn":
                col_name, col = op_val
                step_builder.add_with_column(col_name, col)
            elif op_name == "groupBy":
                step_builder.add_group_by(op_val)
            elif op_name == "join":
                other_df, on, how = op_val
                # For now, assume other_df is already materialized
                step_builder.add_join(f"other_table", on, how)
            elif op_name == "union":
                # Similar to join, would need to handle nested DataFrames
                step_builder.add_union("other_table")
            elif op_name == "orderBy":
                step_builder.add_order_by(op_val)

            # Update the FROM clause to use the current table
            # We need to set the table_name to the current table for the FROM clause
            step_builder.table_name = current_table

            # Execute this step
            step_sql = step_builder.build_sql()
            if step_sql:
                # Create the result table for this step
                create_step_sql = f'CREATE TEMPORARY TABLE "{next_table}" AS {step_sql}'
                if "PARTITION BY" in step_sql or "ORDER BY" in step_sql:
                    print(f"DEBUG: Window SQL - {create_step_sql}")
                self.connection.execute(create_step_sql)
                current_table = next_table

        # Final query to get results from the last table
        final_query = f'SELECT * FROM "{current_table}"'
        result = self.connection.execute(final_query).fetchall()

        # Get column names
        columns = [desc[0] for desc in self.connection.execute(final_query).description]

        # Convert to MockRow objects
        rows = []
        for row_data in result:
            row_dict = {}
            for i, value in enumerate(row_data):
                row_dict[columns[i]] = value
            rows.append(MockRow(row_dict))

        # Clean up temporary table
        self.connection.execute(f'DROP TABLE IF EXISTS "{temp_table}"')

        return rows

    def close(self):
        """Close the DuckDB connection."""
        if self.connection:
            try:
                self.connection.close()
                self.connection = None
            except Exception:
                pass  # Ignore errors during cleanup

        # Clean up unique temp directory if it exists
        if self._temp_dir:
            try:
                import os
                import shutil

                if os.path.exists(self._temp_dir):
                    shutil.rmtree(self._temp_dir, ignore_errors=True)
                self._temp_dir = None
            except:
                pass  # Ignore cleanup errors

    def __del__(self):
        """Cleanup on deletion to prevent resource leaks."""
        try:
            self.close()
        except:
            pass
