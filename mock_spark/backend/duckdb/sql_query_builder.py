"""
SQL Query Builder for Mock Spark.

This module provides building of SQL queries including CTE queries,
CASE WHEN expressions, and other SQL construction utilities.
"""

from typing import Any, List, Tuple
from .cte_query_builder import CTEQueryBuilder
from .sql_expression_translator import SQLExpressionTranslator
from ...functions.core.column import MockColumnOperation


class SQLQueryBuilder:
    """Builds SQL queries for Mock Spark operations."""

    def __init__(self, cte_builder: CTEQueryBuilder):
        """Initialize the SQL query builder.

        Args:
            cte_builder: CTE query builder instance
        """
        self.cte_builder = cte_builder
        # Create a dummy table manager for the translator
        from .table_manager import DuckDBTableManager
        from sqlalchemy import create_engine, MetaData

        dummy_engine = create_engine("duckdb:///:memory:")
        dummy_metadata = MetaData()
        table_manager = DuckDBTableManager(dummy_engine, dummy_metadata)
        self.sql_converter = SQLExpressionTranslator(table_manager)

    def build_cte_query(
        self, source_table_name: str, operations: List[Tuple[str, Any]]
    ) -> str:
        """Build a single SQL query with CTEs for all operations.

        Args:
            source_table_name: Name of the source table
            operations: List of operations to apply

        Returns:
            SQL query string
        """
        return self.cte_builder.build_cte_query(source_table_name, operations)

    def build_case_when_sql(self, case_when_obj: Any, source_table_obj: Any) -> str:
        """Build SQL CASE WHEN expression from MockCaseWhen object.

        Args:
            case_when_obj: MockCaseWhen object
            source_table_obj: Source table object

        Returns:
            SQL CASE WHEN expression string
        """
        sql_parts = ["CASE"]

        # Add WHEN conditions
        for condition, value in case_when_obj.conditions:
            # Convert condition to SQL - check if it's a complex expression
            if isinstance(condition, MockColumnOperation):
                # Generate raw SQL without quoting for complex expressions
                condition_sql = self.sql_converter.expression_to_sql(condition)
            else:
                condition_sql = self.sql_converter.condition_to_sql(
                    condition, source_table_obj
                )

            # Convert value to SQL - handle MockLiteral with boolean values specially
            if hasattr(value, "value") and isinstance(value.value, bool):
                value_sql = "TRUE" if value.value else "FALSE"
            else:
                value_sql = self.sql_converter.value_to_sql(value)
            sql_parts.append(f"WHEN {condition_sql} THEN {value_sql}")

        # Add ELSE clause if default_value is set
        if case_when_obj.default_value is not None:
            # Update the ELSE clause to handle boolean MockLiterals
            if hasattr(case_when_obj.default_value, "value") and isinstance(
                case_when_obj.default_value.value, bool
            ):
                else_sql = "TRUE" if case_when_obj.default_value.value else "FALSE"
            else:
                else_sql = self.sql_converter.value_to_sql(case_when_obj.default_value)
            sql_parts.append(f"ELSE {else_sql}")

        sql_parts.append("END")
        return " ".join(sql_parts)

    def build_select_with_window_cte(
        self, source_name: str, columns: Tuple[Any, ...], source_table_obj: Any
    ) -> str:
        """Build CTE SQL for select with window functions.

        Args:
            source_name: Name of the source table
            columns: Columns to select
            source_table_obj: Source table object

        Returns:
            SQL query string
        """
        return self.cte_builder.build_select_with_window_cte(
            source_name, columns, source_table_obj
        )

    def build_filter_cte(
        self, source_name: str, condition: Any, source_table_obj: Any
    ) -> str:
        """Build CTE SQL for filter operation.

        Args:
            source_name: Name of the source table
            condition: Filter condition
            source_table_obj: Source table object

        Returns:
            SQL query string
        """
        return self.cte_builder.build_filter_cte(
            source_name, condition, source_table_obj
        )

    def build_select_cte(
        self, source_name: str, columns: Tuple[Any, ...], source_table_obj: Any
    ) -> str:
        """Build CTE SQL for select operation.

        Args:
            source_name: Name of the source table
            columns: Columns to select
            source_table_obj: Source table object

        Returns:
            SQL query string
        """
        return self.cte_builder.build_select_cte(source_name, columns, source_table_obj)

    def build_with_column_cte(
        self, source_name: str, col_name: str, col: Any, source_table_obj: Any
    ) -> str:
        """Build CTE SQL for withColumn operation.

        Args:
            source_name: Name of the source table
            col_name: Name of the new column
            col: Column expression
            source_table_obj: Source table object

        Returns:
            SQL query string
        """
        return self.cte_builder.build_with_column_cte(
            source_name, col_name, col, source_table_obj
        )

    def build_order_by_cte(
        self, source_name: str, columns: Tuple[Any, ...], source_table_obj: Any
    ) -> str:
        """Build CTE SQL for orderBy operation.

        Args:
            source_name: Name of the source table
            columns: Columns to order by
            source_table_obj: Source table object

        Returns:
            SQL query string
        """
        return self.cte_builder.build_order_by_cte(
            source_name, columns, source_table_obj
        )

    def build_limit_cte(
        self, source_name: str, limit_count: int, source_table_obj: Any
    ) -> str:
        """Build CTE SQL for limit operation.

        Args:
            source_name: Name of the source table
            limit_count: Number of rows to limit
            source_table_obj: Source table object

        Returns:
            SQL query string
        """
        return self.cte_builder.build_limit_cte(source_name, limit_count)

    def build_join_cte(
        self, source_name: str, join_params: Tuple[Any, ...], source_table_obj: Any
    ) -> str:
        """Build CTE SQL for join operation.

        Args:
            source_name: Name of the source table
            join_params: Join parameters
            source_table_obj: Source table object

        Returns:
            SQL query string
        """
        return self.cte_builder.build_join_cte(
            source_name, join_params, source_table_obj
        )

    def build_union_cte(
        self, source_name: str, other_df: Any, source_table_obj: Any
    ) -> str:
        """Build CTE SQL for union operation.

        Args:
            source_name: Name of the source table
            other_df: Other DataFrame to union with
            source_table_obj: Source table object

        Returns:
            SQL query string
        """
        return self.cte_builder.build_union_cte(source_name, other_df, source_table_obj)
