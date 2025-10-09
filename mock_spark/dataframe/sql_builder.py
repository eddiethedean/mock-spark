"""
SQL query builder for lazy evaluation using DuckDB optimization.

DEPRECATED: This module is deprecated in favor of sqlalchemy_query_builder.SQLAlchemyQueryBuilder
which provides database-agnostic query building. This file remains for backwards compatibility only.
"""

import warnings
from typing import Any, Dict, List, Optional, Union, Tuple
from ..functions import MockColumn, MockColumnOperation, F, MockLiteral
from ..spark_types import MockStructType

# Emit deprecation warning when this module is imported
warnings.warn(
    "sql_builder.SQLQueryBuilder is deprecated and will be removed in a future version. "
    "Use sqlalchemy_query_builder.SQLAlchemyQueryBuilder for database-agnostic query building.",
    DeprecationWarning,
    stacklevel=2
)


class SQLQueryBuilder:
    """Builds SQL queries from DataFrame operations for DuckDB optimization."""

    def __init__(self, table_name: str = "data", schema: MockStructType = None):
        self.table_name = table_name
        self.schema = schema
        self.select_columns = ["*"]  # Default to all columns
        self.where_conditions = []
        self.with_columns = {}  # col_name -> expression
        self.group_by_columns = []
        self.order_by_columns = []
        self.join_tables = []  # (table, condition, type)
        self.union_tables = []

    def add_filter(self, condition: MockColumnOperation) -> None:
        """Add a WHERE condition."""
        sql_condition = self._column_to_sql(condition)
        self.where_conditions.append(sql_condition)

    def add_select(self, columns: Tuple[Any, ...]) -> None:
        """Add SELECT columns."""
        sql_columns = []
        for col in columns:
            if isinstance(col, str):
                if col == "*":
                    sql_columns.append("*")
                else:
                    sql_columns.append(f'"{col}"')
            elif isinstance(col, MockColumn):
                if col.name == "*":
                    sql_columns.append("*")
                else:
                    sql_columns.append(f'"{col.name}"')
            else:
                # Handle expressions
                sql_columns.append(self._column_to_sql(col))
        self.select_columns = sql_columns

    def add_with_column(
        self, col_name: str, col: Union[MockColumn, MockColumnOperation, MockLiteral]
    ) -> None:
        """Add a computed column."""
        sql_expression = self._column_to_sql(col)
        self.with_columns[col_name] = sql_expression

    def add_group_by(self, columns: Tuple[Any, ...]) -> None:
        """Add GROUP BY columns."""
        for col in columns:
            if isinstance(col, str):
                self.group_by_columns.append(f'"{col}"')
            elif isinstance(col, MockColumn):
                self.group_by_columns.append(f'"{col.name}"')

    def add_order_by(self, columns: Tuple[Any, ...]) -> None:
        """Add ORDER BY columns."""
        for col in columns:
            if isinstance(col, str):
                self.order_by_columns.append(f'"{col}"')
            elif isinstance(col, MockColumn):
                self.order_by_columns.append(f'"{col.name}"')
            elif isinstance(col, MockColumnOperation):
                # Handle desc() operations
                if hasattr(col, "operation") and col.operation == "desc":
                    self.order_by_columns.append(f'"{col.column.name}" DESC')
                else:
                    self.order_by_columns.append(f'"{col.column.name}"')

    def add_join(self, other_table: str, on: Union[str, List[str]], how: str = "INNER") -> None:
        """Add a JOIN operation."""
        if isinstance(on, str):
            condition = f'"{on}"'
        else:
            condition = " AND ".join(f'"{col}"' for col in on)

        self.join_tables.append((other_table, condition, how))

    def add_union(self, other_table: str) -> None:
        """Add a UNION operation."""
        self.union_tables.append(other_table)

    def _column_to_sql(self, col: Any) -> str:
        """Convert a column/expression to SQL."""
        if isinstance(col, MockColumn):
            return f'"{col.name}"'
        elif isinstance(col, MockColumnOperation):
            return self._operation_to_sql(col)
        elif isinstance(col, MockLiteral):
            if isinstance(col.value, str):
                return f"'{col.value}'"
            else:
                return str(col.value)
        elif hasattr(col, "function_name") and hasattr(col, "window_spec"):
            # Handle window functions
            return self._window_function_to_sql(col)
        else:
            return str(col)

    def _operation_to_sql(self, op: MockColumnOperation) -> str:
        """Convert a column operation to SQL."""
        if hasattr(op, "operation") and hasattr(op, "column"):
            left = self._column_to_sql(op.column)
            right = self._value_to_sql(op.value) if hasattr(op, "value") else str(op.value)

            if op.operation == ">":
                return f"({left} > {right})"
            elif op.operation == "<":
                return f"({left} < {right})"
            elif op.operation == ">=":
                return f"({left} >= {right})"
            elif op.operation == "<=":
                return f"({left} <= {right})"
            elif op.operation == "==":
                return f"({left} = {right})"
            elif op.operation == "!=":
                return f"({left} != {right})"
            elif op.operation == "+":
                return f"({left} + {right})"
            elif op.operation == "-":
                return f"({left} - {right})"
            elif op.operation == "*":
                return f"({left} * {right})"
            elif op.operation == "/":
                return f"({left} / {right})"
            # Add more operations as needed

        return str(op)

    def _value_to_sql(self, value: Any) -> str:
        """Convert a value to SQL literal."""
        if isinstance(value, str):
            return f"'{value}'"
        elif value is None:
            return "NULL"
        else:
            return str(value)

    def _window_function_to_sql(self, window_func: Any) -> str:
        """Convert a window function to SQL."""
        # Get the function name (e.g., "rank", "row_number")
        function_name = getattr(window_func, "function_name", "window_function")

        # Build the OVER clause
        over_clause = self._window_spec_to_sql(window_func.window_spec)

        return f"{function_name.upper()}() OVER {over_clause}"

    def _window_spec_to_sql(self, window_spec: Any) -> str:
        """Convert a window specification to SQL."""
        parts = []

        # Handle PARTITION BY
        if hasattr(window_spec, "_partition_by") and window_spec._partition_by:
            partition_cols = []
            for col in window_spec._partition_by:
                if isinstance(col, str):
                    partition_cols.append(f'"{col}"')
                elif hasattr(col, "name"):
                    partition_cols.append(f'"{col.name}"')
                else:
                    partition_cols.append(str(col))
            parts.append(f"PARTITION BY {', '.join(partition_cols)}")

        # Handle ORDER BY
        if hasattr(window_spec, "_order_by") and window_spec._order_by:
            order_cols = []
            for col in window_spec._order_by:
                if isinstance(col, str):
                    order_cols.append(f'"{col}"')
                elif isinstance(col, MockColumnOperation):
                    # Handle desc() operations in window specs
                    if hasattr(col, "operation") and col.operation == "desc":
                        order_cols.append(f'"{col.column.name}" DESC')
                    else:
                        order_cols.append(f'"{col.column.name}"')
                elif hasattr(col, "name"):
                    order_cols.append(f'"{col.name}"')
                else:
                    order_cols.append(str(col))
            parts.append(f"ORDER BY {', '.join(order_cols)}")

        # Handle window frames (ROWS BETWEEN, RANGE BETWEEN)
        if hasattr(window_spec, "_rows_between") and window_spec._rows_between:
            start, end = window_spec._rows_between
            parts.append(
                f"ROWS BETWEEN {self._frame_bound_to_sql(start)} AND {self._frame_bound_to_sql(end)}"
            )
        elif hasattr(window_spec, "_range_between") and window_spec._range_between:
            start, end = window_spec._range_between
            parts.append(
                f"RANGE BETWEEN {self._frame_bound_to_sql(start)} AND {self._frame_bound_to_sql(end)}"
            )

        return f"({', '.join(parts)})"

    def _frame_bound_to_sql(self, bound: Any) -> str:
        """Convert a window frame bound to SQL."""
        if hasattr(bound, "__name__"):
            # Handle constants like UNBOUNDED_PRECEDING
            bound_name = bound.__name__.upper()
            if "UNBOUNDED" in bound_name:
                if "PRECEDING" in bound_name:
                    return "UNBOUNDED PRECEDING"
                elif "FOLLOWING" in bound_name:
                    return "UNBOUNDED FOLLOWING"
            elif "CURRENT" in bound_name:
                return "CURRENT ROW"
            elif "PRECEDING" in bound_name:
                return f"{bound.value} PRECEDING"
            elif "FOLLOWING" in bound_name:
                return f"{bound.value} FOLLOWING"

        # Default fallback
        return str(bound)

    def build_sql(self) -> str:
        """Build the final SQL query."""
        sql_parts = []

        # Build SELECT clause
        select_clause = "SELECT "
        if self.with_columns:
            # Include computed columns
            all_columns = []

            # Add original columns (excluding any that are being computed)
            computed_col_names = set(self.with_columns.keys())
            for col in self.select_columns:
                if col != "*":
                    # Only include if it's not being computed
                    if col not in computed_col_names:
                        all_columns.append(col)
                else:
                    # Add all original columns (excluding computed ones)
                    if self.schema:
                        for field in self.schema.fields:
                            if field.name not in computed_col_names:
                                all_columns.append(f'"{field.name}"')

            # Add computed columns
            for col_name, expression in self.with_columns.items():
                all_columns.append(f'{expression} AS "{col_name}"')

            select_clause += ", ".join(all_columns)
        else:
            select_clause += ", ".join(self.select_columns)

        sql_parts.append(select_clause)

        # Build FROM clause
        from_clause = f'FROM "{self.table_name}"'
        sql_parts.append(from_clause)

        # Build JOIN clauses
        for table, condition, join_type in self.join_tables:
            join_clause = f'{join_type} JOIN "{table}" ON {condition}'
            sql_parts.append(join_clause)

        # Build WHERE clause
        if self.where_conditions:
            where_clause = "WHERE " + " AND ".join(self.where_conditions)
            sql_parts.append(where_clause)

        # Build GROUP BY clause
        if self.group_by_columns:
            group_clause = "GROUP BY " + ", ".join(self.group_by_columns)
            sql_parts.append(group_clause)

        # Build ORDER BY clause
        if self.order_by_columns:
            order_clause = "ORDER BY " + ", ".join(self.order_by_columns)
            sql_parts.append(order_clause)

        # Build UNION clauses
        for table in self.union_tables:
            union_clause = f'UNION SELECT * FROM "{table}"'
            sql_parts.append(union_clause)

        return " ".join(sql_parts)

    def create_temp_table_sql(self, data: List[Dict[str, Any]]) -> str:
        """Create SQL to insert data into a temporary table."""
        if not data:
            return ""

        # Create table based on actual data structure, not schema
        # This handles cases where schema projection has reduced columns
        columns = []
        if data:
            for key in data[0].keys():
                # Infer type from sample data
                sample_value = data[0][key]
                if isinstance(sample_value, int):
                    columns.append(f'"{key}" INTEGER')
                elif isinstance(sample_value, float):
                    columns.append(f'"{key}" DOUBLE')
                elif isinstance(sample_value, bool):
                    columns.append(f'"{key}" BOOLEAN')
                else:
                    columns.append(f'"{key}" VARCHAR')

        create_sql = f"CREATE TABLE \"{self.table_name}\" ({', '.join(columns)})"

        # Insert data
        insert_sql = f'INSERT INTO "{self.table_name}" VALUES '
        values = []
        for row in data:
            row_values = []
            for key in row.keys():
                value = row[key]
                if isinstance(value, str):
                    row_values.append(f"'{value}'")
                elif value is None:
                    row_values.append("NULL")
                else:
                    row_values.append(str(value))
            values.append(f"({', '.join(row_values)})")

        insert_sql += ", ".join(values)

        return f"{create_sql}; {insert_sql};"

    def _type_to_sql(self, data_type) -> str:
        """Convert MockSpark data type to SQL type."""
        type_name = data_type.__class__.__name__
        if "String" in type_name:
            return "VARCHAR"
        elif "Integer" in type_name or "Long" in type_name:
            return "INTEGER"
        elif "Double" in type_name or "Float" in type_name:
            return "DOUBLE"
        elif "Boolean" in type_name:
            return "BOOLEAN"
        else:
            return "VARCHAR"  # Default
