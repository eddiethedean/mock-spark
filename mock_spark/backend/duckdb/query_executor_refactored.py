"""
Refactored SQLAlchemy-based materializer for Mock Spark lazy evaluation.

This module uses the new SRP-compliant components to provide SQL generation
and execution capabilities for complex DataFrame operations.
"""

from typing import Any, Dict, List, Tuple
from sqlalchemy import create_engine, MetaData

from ...spark_types import MockStructType, MockRow
from .table_manager import DuckDBTableManager
from .sql_expression_translator import SQLExpressionTranslator
from .cte_query_builder import CTEQueryBuilder
from .operation_executor import DataFrameOperationExecutor
from .window_processor import WindowFunctionProcessor
from .error_translator import DuckDBErrorTranslator
from .date_format_converter import DateFormatConverter


class SQLAlchemyMaterializer:
    """Materializes lazy DataFrames using SQLAlchemy with DuckDB.

    This refactored version uses dependency injection to compose specialized components,
    following the Single Responsibility Principle.
    """

    def __init__(self, engine_url: str = "duckdb:///:memory:"):
        """Initialize SQLAlchemy materializer.

        Args:
            engine_url: Database URL for DuckDB engine
        """
        # Create DuckDB engine with SQLAlchemy
        self.engine = create_engine(engine_url, echo=False)
        self.metadata = MetaData()

        # Compose specialized components using dependency injection
        self.table_manager = DuckDBTableManager(self.engine, self.metadata)
        self.expression_translator = SQLExpressionTranslator(self.table_manager)
        self.cte_builder = CTEQueryBuilder(self.expression_translator)
        self.operation_executor = DataFrameOperationExecutor(
            self.engine, self.table_manager, self.expression_translator
        )
        self.window_processor = WindowFunctionProcessor(self.expression_translator)
        self.error_translator = DuckDBErrorTranslator()
        self.format_converter = DateFormatConverter()

    def materialize(
        self,
        data: List[Dict[str, Any]],
        schema: MockStructType,
        operations: List[Tuple[str, Any]],
    ) -> List[MockRow]:
        """
        Materializes the DataFrame by building and executing operations using CTEs.
        Uses a single SQL query with Common Table Expressions for better performance.

        Args:
            data: Raw data to materialize
            schema: Schema definition
            operations: List of operations to apply

        Returns:
            List of MockRow objects representing the materialized data
        """
        if not operations:
            # No operations to apply, return original data as rows
            return [MockRow(row) for row in data]

        # Create initial table with data
        source_table_name = f"temp_table_{id(data)}"
        self.table_manager.create_table_with_data(source_table_name, data)

        # Try CTE-based approach first for better performance
        try:
            return self._materialize_with_cte(source_table_name, operations)
        except Exception as e:
            # Fallback to old table-per-operation approach if CTE fails
            # This ensures backward compatibility for complex operations
            import warnings

            warnings.warn(
                f"CTE optimization failed, falling back to table-per-operation: {e}"
            )
            return self._materialize_with_tables(source_table_name, operations)

    def _materialize_with_cte(
        self, source_table_name: str, operations: List[Tuple[str, Any]]
    ) -> List[MockRow]:
        """Materialize using CTE optimization.

        Args:
            source_table_name: Name of the source table
            operations: List of operations to apply

        Returns:
            List of MockRow objects
        """
        # Build CTE query using the CTE builder
        cte_query = self.cte_builder.build_cte_query(source_table_name, operations)

        # Execute the CTE query
        with self.engine.connect() as conn:
            from sqlalchemy import text

            result = conn.execute(text(cte_query))
            rows = result.fetchall()

            # Convert to MockRow objects
            mock_rows = []
            for row in rows:
                row_dict = dict(row._mapping)
                mock_rows.append(MockRow(row_dict))

            return mock_rows

    def _materialize_with_tables(
        self, source_table_name: str, operations: List[Tuple[str, Any]]
    ) -> List[MockRow]:
        """Materialize using table-per-operation approach (fallback).

        Args:
            source_table_name: Name of the source table
            operations: List of operations to apply

        Returns:
            List of MockRow objects
        """
        current_table = source_table_name

        for i, (op_name, op_val) in enumerate(operations):
            target_table = f"temp_table_{i}_{id(op_val)}"

            try:
                if op_name == "filter":
                    self.operation_executor.apply_filter(
                        current_table, target_table, op_val
                    )
                elif op_name == "select":
                    self.operation_executor.apply_select(
                        current_table, target_table, op_val
                    )
                elif op_name == "withColumn":
                    col_name, col_expr = op_val
                    self.operation_executor.apply_with_column(
                        current_table, target_table, col_name, col_expr
                    )
                elif op_name == "orderBy":
                    self.operation_executor.apply_order_by(
                        current_table, target_table, op_val
                    )
                elif op_name == "limit":
                    self.operation_executor.apply_limit(
                        current_table, target_table, op_val
                    )
                elif op_name == "join":
                    self.operation_executor.apply_join(
                        current_table, target_table, op_val
                    )
                elif op_name == "union":
                    self.operation_executor.apply_union(
                        current_table, target_table, op_val
                    )
                else:
                    # Unknown operation, skip
                    continue

                current_table = target_table

            except Exception as e:
                # Translate DuckDB errors to MockSpark exceptions
                context = {
                    "operation": op_name,
                    "sql": f"Operation: {op_name}",
                    "available_columns": list(
                        self.table_manager.get_table(current_table).c.keys()  # type: ignore[union-attr]
                    )
                    if self.table_manager.get_table(current_table)
                    else [],
                }
                raise self.error_translator.translate_error(e, context)

        # Get final results
        return self.table_manager.get_table_results(current_table)

    def close(self) -> None:
        """Close the SQLAlchemy engine and cleanup resources."""
        try:
            if hasattr(self, "engine") and self.engine:
                self.engine.dispose()
                self.engine = None  # type: ignore[assignment]
        except Exception:
            pass  # Ignore errors during cleanup

    def __del__(self) -> None:
        """Cleanup on deletion to prevent resource leaks."""
        try:
            self.close()
        except:  # noqa: E722
            pass
