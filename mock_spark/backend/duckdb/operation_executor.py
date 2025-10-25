"""
DataFrame operation executor for Mock Spark.

This module provides execution of DataFrame operations (filter, select, join, etc.).
"""

from typing import Any, List, Tuple
from sqlalchemy import (
    select,
    func,
    text,
    Column,
    Integer,
    Float,
    Double,
    Boolean,
    String,
    BigInteger,
    Table,
    MetaData,
)
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from .table_manager import DuckDBTableManager
from .sql_expression_translator import SQLExpressionTranslator
from .error_translator import DuckDBErrorTranslator


class DataFrameOperationExecutor:
    """Executes DataFrame operations using SQLAlchemy and DuckDB."""

    def __init__(
        self,
        engine: Engine,
        table_manager: DuckDBTableManager,
        expression_translator: SQLExpressionTranslator,
    ):
        """Initialize operation executor.

        Args:
            engine: SQLAlchemy engine instance
            table_manager: Table manager instance
            expression_translator: SQL expression translator instance
        """
        self.engine = engine
        self.table_manager = table_manager
        self.expression_translator = expression_translator
        self.error_translator = DuckDBErrorTranslator()
        self._strict_column_validation = False

    def apply_filter(
        self, source_table: str, target_table: str, condition: Any
    ) -> None:
        """Apply a filter operation using SQLAlchemy expressions.

        Args:
            source_table: Name of source table
            target_table: Name of target table
            condition: Filter condition
        """
        source_table_obj = self.table_manager.get_table(source_table)
        if not source_table_obj:
            raise ValueError(f"Source table {source_table} not found")

        # Check if source table has any rows
        with Session(self.engine) as session:
            row_count = session.execute(
                select(func.count()).select_from(source_table_obj)
            ).scalar()

        # Set flag to enable strict column validation for filters
        # Only validate if table has rows (errors should only occur when processing actual data)
        self._strict_column_validation = bool(row_count and row_count > 0)

        # Convert condition to SQLAlchemy expression
        try:
            filter_expr = self.expression_translator.condition_to_sqlalchemy(
                source_table_obj, condition
            )
        finally:
            self._strict_column_validation = False

        # Create target table with same structure
        self.table_manager.copy_table_structure(source_table, target_table)
        target_table_obj = self.table_manager.get_table(target_table)

        # Execute filter and insert results
        with Session(self.engine) as session:
            # Build raw SQL query
            column_names = [col.name for col in source_table_obj.columns]
            sql = f"SELECT {', '.join(column_names)} FROM {source_table}"

            if filter_expr is not None:
                # Convert SQLAlchemy expression to SQL string
                filter_sql = str(
                    filter_expr.compile(compile_kwargs={"literal_binds": True})
                )
                sql += f" WHERE {filter_sql}"

            results = session.execute(text(sql)).all()

            # Insert into target table
            for result in results:
                # Convert result to dict using column names
                result_dict = {}
                for i, column in enumerate(source_table_obj.columns):
                    result_dict[column.name] = result[i]
                insert_stmt = target_table_obj.insert().values(result_dict)  # type: ignore[union-attr]
                session.execute(insert_stmt)
            session.commit()

    def apply_select(
        self, source_table: str, target_table: str, columns: Tuple[Any, ...]
    ) -> None:
        """Apply a select operation.

        Args:
            source_table: Name of source table
            target_table: Name of target table
            columns: Columns to select
        """
        source_table_obj = self.table_manager.get_table(source_table)
        if not source_table_obj:
            raise ValueError(f"Source table {source_table} not found")

        # Check if we have window functions, aggregate functions, or complex operations - if so, use raw SQL
        has_window_functions = any(
            (
                hasattr(col, "function_name") and hasattr(col, "window_spec")
            )  # MockWindowFunction
            or (
                hasattr(col, "function_name")
                and hasattr(col, "column")
                and col.__class__.__name__ == "MockAggregateFunction"
            )
            for col in columns
        )

        # Check if we have arithmetic operations with complex expressions (like casts)
        has_complex_arithmetic = any(
            (
                hasattr(col, "function_name")
                and col.function_name in ["+", "-", "*", "/", "%"]
                and (
                    # Either the column is a MockColumnOperation (cast operation)
                    (hasattr(col, "column") and hasattr(col.column, "operation"))
                    or
                    # Or the operation itself is complex (contains casts)
                    (
                        hasattr(col, "operation")
                        and col.operation in ["+", "-", "*", "/", "%"]
                    )
                )
            )
            for col in columns
        )

        # Combine the checks
        has_window_functions = has_window_functions or has_complex_arithmetic

        if has_window_functions:
            # Use raw SQL for window functions
            self.apply_select_with_window(source_table, target_table, columns)
            return

        # Build select columns and new table structure
        select_columns = []
        new_columns: List[Any] = []

        for col in columns:
            if isinstance(col, str):
                if col == "*":
                    # Select all columns
                    for column in source_table_obj.columns:
                        select_columns.append(column)
                        new_columns.append(
                            Column(column.name, column.type, primary_key=False)
                        )
                else:
                    # Select specific column
                    try:
                        source_column = source_table_obj.c[col]
                        select_columns.append(source_column)
                        new_columns.append(
                            Column(col, source_column.type, primary_key=False)
                        )
                    except KeyError:
                        # Column not found - raise AnalysisException
                        from ...core.exceptions import AnalysisException

                        raise AnalysisException(
                            f"Column '{col}' not found. Available columns: {list(source_table_obj.c.keys())}"
                        )
            elif hasattr(col, "value") and hasattr(col, "data_type"):
                # Handle MockLiteral objects (literal values) - check this before MockColumn
                if isinstance(col.value, str):
                    select_columns.append(text(f"'{col.value}'"))  # type: ignore[arg-type]
                else:
                    select_columns.append(text(str(col.value)))  # type: ignore[arg-type]
                # Use appropriate column type based on the literal value
                if isinstance(col.value, int):
                    # Check for overflow values that exceed INT32 range
                    if col.value > 2147483647 or col.value < -2147483648:
                        new_columns.append(Column(col.name, String, primary_key=False))
                    else:
                        new_columns.append(Column(col.name, Integer, primary_key=False))
                elif isinstance(col.value, float):
                    new_columns.append(Column(col.name, Double, primary_key=False))
                elif isinstance(col.value, str):
                    # Check if string represents a large number that would overflow INT32
                    try:
                        num_val = float(col.value)
                        if num_val > 2147483647 or num_val < -2147483648:
                            new_columns.append(
                                Column(col.name, String, primary_key=False)
                            )
                        else:
                            new_columns.append(
                                Column(col.name, String, primary_key=False)
                            )
                    except (ValueError, OverflowError):
                        new_columns.append(Column(col.name, String, primary_key=False))
                else:
                    new_columns.append(Column(col.name, String, primary_key=False))
            elif hasattr(col, "name") and hasattr(col, "column_type"):
                # Handle MockColumn objects
                col_name = col.name

                # Check if this is an aliased column (check both _original_column and original_column)
                original_col = getattr(col, "_original_column", None) or getattr(
                    col, "original_column", None
                )
                if original_col is not None:
                    # Use original column name for lookup, alias name for output
                    original_name = original_col.name
                    alias_name = col.name
                    try:
                        source_column = source_table_obj.c[original_name]
                        select_columns.append(source_column.label(alias_name))
                        new_columns.append(
                            Column(alias_name, source_column.type, primary_key=False)
                        )
                    except KeyError:
                        print(
                            f"Warning: Column '{original_name}' not found in table {source_table}"
                        )
                        continue
                # Check if this is a wildcard selector
                elif col_name == "*":
                    # Select all columns
                    for column in source_table_obj.columns:
                        select_columns.append(column)
                        new_columns.append(
                            Column(column.name, column.type, primary_key=False)
                        )
                else:
                    # Check if column exists in source table (might come from join)
                    if col_name in source_table_obj.c:
                        source_column = source_table_obj.c[col_name]
                        select_columns.append(source_column)
                        new_columns.append(
                            Column(col_name, source_column.type, primary_key=False)
                        )
                    else:
                        # Column doesn't exist in source, might come from join
                        # Add as text column reference with default String type
                        select_columns.append(text(f'"{col_name}"'))  # type: ignore[arg-type]
                        new_columns.append(Column(col_name, String, primary_key=False))
            elif hasattr(col, "conditions") and hasattr(col, "default_value"):
                # Handle MockCaseWhen objects
                try:
                    # Build CASE WHEN SQL expression
                    case_expr = self.expression_translator.build_case_when_sql(
                        col, source_table_obj
                    )
                    select_columns.append(text(case_expr))  # type: ignore[arg-type]

                    # Infer the correct column type from MockCaseWhen
                    from ...spark_types import (
                        BooleanType,
                        IntegerType,
                        LongType,
                        DoubleType,
                        StringType,
                    )

                    inferred_type = col.get_result_type()
                    if isinstance(inferred_type, BooleanType):
                        new_columns.append(Column(col.name, Boolean, primary_key=False))
                    elif isinstance(inferred_type, IntegerType):
                        # Use VARCHAR for IntegerType to handle overflow gracefully
                        # TRY_CAST will return NULL for overflow values
                        new_columns.append(Column(col.name, String, primary_key=False))
                    elif isinstance(inferred_type, LongType):
                        new_columns.append(
                            Column(col.name, BigInteger, primary_key=False)
                        )
                    elif isinstance(inferred_type, DoubleType):
                        new_columns.append(Column(col.name, Float, primary_key=False))
                    elif isinstance(inferred_type, StringType):
                        new_columns.append(Column(col.name, String, primary_key=False))
                    else:
                        # Default to String for unknown types
                        new_columns.append(Column(col.name, String, primary_key=False))
                except Exception as e:
                    print(f"Warning: Error handling MockCaseWhen: {e}")
                    continue
            elif (
                hasattr(col, "operation")
                and hasattr(col, "column")
                and hasattr(col, "function_name")
            ):
                # Handle arithmetic operations and other column operations
                try:
                    left_col = source_table_obj.c[col.column.name]
                    # Extract value from MockLiteral or MockColumn
                    if hasattr(col.value, "value") and hasattr(col.value, "data_type"):
                        # This is a MockLiteral
                        right_val = col.value.value
                    elif hasattr(col.value, "name"):
                        # This is a MockColumn - convert to SQL column reference
                        right_val = source_table_obj.c[col.value.name]
                    else:
                        right_val = col.value

                    # Apply arithmetic operation
                    if col.function_name == "+":
                        expr = left_col + right_val
                    elif col.function_name == "-":
                        expr = left_col - right_val
                    elif col.function_name == "*":
                        expr = left_col * right_val
                    elif col.function_name == "/":
                        expr = left_col / right_val
                    elif col.function_name == "%":
                        expr = left_col % right_val

                    select_columns.append(expr.label(col.name))
                    # For arithmetic operations, determine result type based on operand types
                    if col.function_name == "/":
                        # Division always returns float
                        new_columns.append(Column(col.name, Float, primary_key=False))
                    else:
                        # For other operations, if either operand is float, result is float
                        left_is_float = (
                            "FLOAT" in str(left_col.type).upper()
                            or "DOUBLE" in str(left_col.type).upper()
                            or "REAL" in str(left_col.type).upper()
                        )
                        right_is_float = False
                        if hasattr(right_val, "type"):
                            right_is_float = (
                                "FLOAT" in str(right_val.type).upper()
                                or "DOUBLE" in str(right_val.type).upper()
                                or "REAL" in str(right_val.type).upper()
                            )

                        if left_is_float or right_is_float:
                            new_columns.append(
                                Column(col.name, Float, primary_key=False)
                            )
                        else:
                            new_columns.append(
                                Column(col.name, left_col.type, primary_key=False)
                            )
                except KeyError:
                    print(
                        f"Warning: Column '{col.column.name}' not found in table {source_table}"
                    )
                    continue

        # Create target table with new structure

        metadata = MetaData()
        target_table_obj = Table(target_table, metadata, *new_columns)
        target_table_obj.create(self.engine, checkfirst=True)
        self.table_manager._created_tables[target_table] = target_table_obj

        # Execute select and insert results
        with Session(self.engine) as session:
            results = session.execute(
                select(*select_columns).select_from(source_table_obj)
            ).all()

            # Insert into target table
            for result in results:
                # Convert result to dict using column names
                result_dict = {}
                for i, column in enumerate(target_table_obj.columns):
                    if i < len(result):
                        result_dict[column.name] = result[i]
                insert_stmt = target_table_obj.insert().values(result_dict)
                session.execute(insert_stmt)
            session.commit()

    def apply_select_with_window(
        self, source_table: str, target_table: str, columns: Tuple[Any, ...]
    ) -> None:
        """Apply select operation with window functions.

        Args:
            source_table: Name of source table
            target_table: Name of target table
            columns: Columns to select
        """
        # This is a simplified version - full implementation would handle window functions
        # For now, delegate to regular select
        self.apply_select(source_table, target_table, columns)

    def apply_with_column(
        self, source_table: str, target_table: str, col_name: str, col_expr: Any
    ) -> None:
        """Apply withColumn operation.

        Args:
            source_table: Name of source table
            target_table: Name of target table
            col_name: Name of the column to add/replace
            col_expr: Column expression
        """
        # Simplified implementation - would need full withColumn logic
        # For now, just copy the table structure
        self.table_manager.copy_table_structure(source_table, target_table)

    def apply_order_by(
        self, source_table: str, target_table: str, order_cols: List[Any]
    ) -> None:
        """Apply orderBy operation.

        Args:
            source_table: Name of source table
            target_table: Name of target table
            order_cols: Order by columns
        """
        # Simplified implementation - would need full orderBy logic
        # For now, just copy the table structure
        self.table_manager.copy_table_structure(source_table, target_table)

    def apply_limit(
        self, source_table: str, target_table: str, limit_count: int
    ) -> None:
        """Apply limit operation.

        Args:
            source_table: Name of source table
            target_table: Name of target table
            limit_count: Number of rows to limit
        """
        # Simplified implementation - would need full limit logic
        # For now, just copy the table structure
        self.table_manager.copy_table_structure(source_table, target_table)

    def apply_join(
        self, source_table: str, target_table: str, join_params: Any
    ) -> None:
        """Apply join operation.

        Args:
            source_table: Name of source table
            target_table: Name of target table
            join_params: Join parameters
        """
        # Simplified implementation - would need full join logic
        # For now, just copy the table structure
        self.table_manager.copy_table_structure(source_table, target_table)

    def apply_union(self, source_table: str, target_table: str, other_df: Any) -> None:
        """Apply union operation.

        Args:
            source_table: Name of source table
            target_table: Name of target table
            other_df: Other DataFrame to union with
        """
        # Simplified implementation - would need full union logic
        # For now, just copy the table structure
        self.table_manager.copy_table_structure(source_table, target_table)
