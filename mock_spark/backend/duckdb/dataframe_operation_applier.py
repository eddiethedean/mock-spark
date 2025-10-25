"""
DataFrame Operation Applier for Mock Spark.

This module provides application of DataFrame operations like filter, select, withColumn,
orderBy, limit, join, and union using SQLAlchemy with DuckDB.
"""

from typing import Any, Tuple, Dict
from sqlalchemy import (
    select,
    func,
    text,
    literal_column,
    literal,
    Column,
    String,
    Table,
    MetaData,
)
from sqlalchemy.orm import Session

from .table_manager import DuckDBTableManager
from .sql_expression_translator import SQLExpressionTranslator


class DataFrameOperationApplier:
    """Applies DataFrame operations using SQLAlchemy with DuckDB."""

    def __init__(
        self,
        engine: Any,
        metadata: MetaData,
        created_tables: Dict[str, Table],
        table_manager: DuckDBTableManager,
        expression_converter: Any,
    ) -> None:
        """Initialize the DataFrame operation applier.

        Args:
            engine: SQLAlchemy engine
            metadata: SQLAlchemy metadata
            created_tables: Dictionary of created tables
        """
        self.engine = engine
        self.metadata = metadata
        self._created_tables = created_tables
        self.table_manager = DuckDBTableManager(engine, metadata)
        self.sql_converter = SQLExpressionTranslator(self.table_manager)
        self._strict_column_validation = False

    def apply_filter(
        self, source_table: str, target_table: str, condition: Any
    ) -> None:
        """Apply a filter operation using SQLAlchemy expressions."""
        source_table_obj = self._created_tables[source_table]

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
            filter_expr = self.sql_converter.condition_to_sqlalchemy(
                source_table_obj, condition
            )
        finally:
            self._strict_column_validation = False

        # Create target table with same structure
        self.table_manager.copy_table_structure(source_table, target_table)
        target_table_obj = self._created_tables[target_table]

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
                insert_stmt = target_table_obj.insert().values(result_dict)
                session.execute(insert_stmt)
            session.commit()

    def apply_select(
        self, source_table: str, target_table: str, columns: Tuple[Any, ...]
    ) -> None:
        """Apply a select operation."""
        source_table_obj = self._created_tables[source_table]

        # Create target table with selected columns
        new_columns = []
        select_columns = []

        for col in columns:
            if hasattr(col, "name"):
                col_name = col.name
            elif isinstance(col, str):
                col_name = col
            else:
                col_name = str(col)

            # Handle different column types
            if hasattr(col, "function_name"):
                # Function column (e.g., col("name").upper())
                if col.function_name in ["upper", "lower", "length", "trim"]:
                    # String functions
                    from sqlalchemy import String

                    new_columns.append(Column("id", String, primary_key=False))
                    select_columns.append(
                        func.upper(source_table_obj.c[col.column.name]).label(col_name)
                        if col.function_name == "upper"
                        else func.lower(source_table_obj.c[col.column.name]).label(
                            col_name
                        )
                        if col.function_name == "lower"
                        else func.length(source_table_obj.c[col.column.name]).label(
                            col_name
                        )
                        if col.function_name == "length"
                        else func.trim(source_table_obj.c[col.column.name]).label(
                            col_name
                        )
                    )
                elif col.function_name in ["abs", "round"]:
                    # Math functions
                    from sqlalchemy import Float

                    new_columns.append(Column(col_name, Float(), primary_key=False))  # type: ignore[arg-type]
                    select_columns.append(
                        func.abs(source_table_obj.c[col.column.name]).label(col_name)
                        if col.function_name == "abs"
                        else func.round(source_table_obj.c[col.column.name]).label(
                            col_name
                        )
                    )
                else:
                    # Other functions - use raw SQL
                    func_sql = self.sql_converter.expression_to_sql(
                        col, source_table=source_table
                    )
                    select_columns.append(literal_column(func_sql).label(col_name))
                    new_columns.append(Column(col_name, String(), primary_key=False))
            else:
                # Regular column
                if hasattr(col, "column") and hasattr(col.column, "name"):
                    col_name = col.column.name
                else:
                    col_name = str(col)

                # Get column type from source table
                source_col = source_table_obj.c[col_name]
                new_columns.append(Column(col_name, source_col.type, primary_key=False))
                select_columns.append(source_col.label(col_name))

        # Create target table
        from sqlalchemy import Table

        target_table_obj = Table(target_table, self.metadata, *new_columns)
        target_table_obj.create(self.engine, checkfirst=True)
        self._created_tables[target_table] = target_table_obj

        # Execute select and insert results
        with Session(self.engine) as session:
            select_stmt = select(*select_columns).select_from(source_table_obj)
            results = session.execute(select_stmt).all()

            # Insert into target table
            for result in results:
                result_dict = {}
                for i, column in enumerate(new_columns):
                    result_dict[column.name] = result[i]
                insert_stmt = target_table_obj.insert().values(result_dict)
                session.execute(insert_stmt)
            session.commit()

    def apply_with_column(
        self, source_table: str, target_table: str, col_name: str, col: Any
    ) -> None:
        """Apply a withColumn operation."""
        source_table_obj = self._created_tables[source_table]

        # Copy all existing columns
        new_columns = []
        select_columns = []

        for existing_col in source_table_obj.columns:
            new_columns.append(
                Column(existing_col.name, existing_col.type, primary_key=False)
            )
            select_columns.append(source_table_obj.c[existing_col.name])

        # Add the new column
        if hasattr(col, "function_name"):
            # Function column
            if col.function_name in ["upper", "lower", "length", "trim"]:
                new_columns.append(Column(col_name, String(), primary_key=False))
                select_columns.append(
                    func.upper(source_table_obj.c[col.column.name]).label(col_name)  # type: ignore
                    if col.function_name == "upper"
                    else func.lower(source_table_obj.c[col.column.name]).label(col_name)
                    if col.function_name == "lower"
                    else func.length(source_table_obj.c[col.column.name]).label(
                        col_name
                    )
                    if col.function_name == "length"
                    else func.trim(source_table_obj.c[col.column.name]).label(col_name)
                )
            else:
                # Use raw SQL for other functions
                col_sql = self.sql_converter.expression_to_sql(
                    col, source_table=source_table
                )
                select_columns.append(literal_column(col_sql).label(col_name))  # type: ignore[arg-type]
                new_columns.append(Column(col_name, String(), primary_key=False))
        else:
            # Regular column or expression
            if hasattr(col, "column"):
                # Column reference
                source_col = source_table_obj.c[col.column.name]
                new_columns.append(Column(col_name, source_col.type, primary_key=False))
                select_columns.append(source_col.label(col_name))
            else:
                # Literal value
                new_columns.append(Column(col_name, String(), primary_key=False))
                select_columns.append(literal(str(col)).label(col_name))  # type: ignore[arg-type]

        # Create target table
        from sqlalchemy import Table

        target_table_obj = Table(target_table, self.metadata, *new_columns)
        target_table_obj.create(self.engine, checkfirst=True)
        self._created_tables[target_table] = target_table_obj

        # Execute select and insert results
        with Session(self.engine) as session:
            select_stmt = select(*select_columns).select_from(source_table_obj)
            results = session.execute(select_stmt).all()

            # Insert into target table
            for result in results:
                result_dict = {}
                for i, column in enumerate(new_columns):
                    result_dict[column.name] = result[i]
                insert_stmt = target_table_obj.insert().values(result_dict)
                session.execute(insert_stmt)
            session.commit()

    def apply_order_by(
        self, source_table: str, target_table: str, columns: Tuple[Any, ...]
    ) -> None:
        """Apply an orderBy operation."""
        source_table_obj = self._created_tables[source_table]

        # Copy table structure
        self.table_manager.copy_table_structure(source_table, target_table)
        target_table_obj = self._created_tables[target_table]

        # Build order by clause
        order_by_clauses = []
        for col in columns:
            if hasattr(col, "column") and hasattr(col.column, "name"):
                col_name = col.column.name
                if hasattr(col, "operation") and col.operation == "desc":
                    order_by_clauses.append(source_table_obj.c[col_name].desc())
                else:
                    order_by_clauses.append(source_table_obj.c[col_name].asc())
            else:
                # Handle direct column references
                col_name = str(col)
                order_by_clauses.append(source_table_obj.c[col_name].asc())

        # Execute select with order by and insert results
        with Session(self.engine) as session:
            select_stmt = select(source_table_obj).order_by(*order_by_clauses)
            results = session.execute(select_stmt).all()

            # Insert into target table
            for result in results:
                result_dict = {}
                for i, column in enumerate(source_table_obj.columns):
                    result_dict[column.name] = result[i]
                insert_stmt = target_table_obj.insert().values(result_dict)
                session.execute(insert_stmt)
            session.commit()

    def apply_limit(
        self, source_table: str, target_table: str, limit_count: int
    ) -> None:
        """Apply a limit operation."""
        source_table_obj = self._created_tables[source_table]

        # Copy table structure
        self.table_manager.copy_table_structure(source_table, target_table)
        target_table_obj = self._created_tables[target_table]

        # Execute select with limit and insert results
        with Session(self.engine) as session:
            select_stmt = select(source_table_obj).limit(limit_count)
            results = session.execute(select_stmt).all()

            # Insert into target table
            for result in results:
                result_dict = {}
                for i, column in enumerate(source_table_obj.columns):
                    result_dict[column.name] = result[i]
                insert_stmt = target_table_obj.insert().values(result_dict)
                session.execute(insert_stmt)
            session.commit()

    def apply_join(
        self, source_table: str, target_table: str, join_params: Tuple[Any, ...]
    ) -> None:
        """Apply a join operation."""
        # This is a simplified implementation
        # In a full implementation, this would handle different join types
        # For now, just copy the source table structure
        self.table_manager.copy_table_structure(source_table, target_table)

    def apply_union(self, source_table: str, target_table: str, other_df: Any) -> None:
        """Apply a union operation."""
        # This is a simplified implementation
        # In a full implementation, this would handle union operations
        # For now, just copy the source table structure
        self.table_manager.copy_table_structure(source_table, target_table)
