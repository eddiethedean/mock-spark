"""
SQLAlchemy-based materialization for lazy evaluation.

This module uses SQLAlchemy to materialize lazy DataFrames in a database-agnostic way,
supporting DuckDB, PostgreSQL, MySQL, SQLite, and any other SQLAlchemy-supported backend.
"""

from typing import Any, Dict, List, Optional, Tuple
from sqlalchemy import create_engine, MetaData, Table, Column, insert, Integer, Float, String, Boolean
from sqlalchemy.engine import Engine
from sqlalchemy.schema import CreateTable

from ..spark_types import MockStructType, MockStructField, MockRow, IntegerType, LongType, FloatType, DoubleType, StringType, BooleanType
from ..storage.sqlalchemy_helpers import mock_type_to_sqlalchemy
from .sqlalchemy_query_builder import SQLAlchemyQueryBuilder


class SQLAlchemyMaterializer:
    """Materializes lazy DataFrames using SQLAlchemy with any backend."""

    def __init__(self, engine_url: str = "duckdb:///:memory:", **engine_kwargs):
        """Initialize SQLAlchemy materializer.

        Args:
            engine_url: SQLAlchemy database URL (e.g., 'duckdb:///:memory:', 'sqlite:///:memory:')
            **engine_kwargs: Additional arguments to pass to create_engine
        """
        self.engine: Engine = create_engine(engine_url, **engine_kwargs)
        self.metadata = MetaData()
        self._temp_table_counter = 0

    def materialize(
        self, data: List[Dict[str, Any]], schema: MockStructType, operations: List[Tuple[str, Any]]
    ) -> List[MockRow]:
        """Materialize a lazy DataFrame using SQLAlchemy.
        
        Args:
            data: List of dictionaries representing rows
            schema: MockStructType defining the schema
            operations: List of (operation_name, operation_value) tuples
            
        Returns:
            List of MockRow objects with materialized data
        """
        if not operations:
            # No operations to apply, return original data as rows
            return [MockRow(row) for row in data]

        # Create temporary table and insert data
        temp_table = self._create_temp_table(data, schema)

        # Build SQLAlchemy query from operations
        builder = SQLAlchemyQueryBuilder(temp_table, schema)

        # Apply operations
        for op_name, op_val in operations:
            if op_name == "filter":
                builder.add_filter(op_val)
            elif op_name == "select":
                builder.add_select(op_val)
            elif op_name == "withColumn":
                col_name, col = op_val
                builder.add_with_column(col_name, col)
            elif op_name == "groupBy":
                builder.add_group_by(op_val)
            elif op_name == "orderBy":
                builder.add_order_by(op_val)
            elif op_name == "limit":
                builder.add_limit(op_val)
            # Note: join and union need special handling with other DataFrames

        # Build and execute the final query
        stmt = builder.build_select()
        
        with self.engine.connect() as conn:
            result = conn.execute(stmt)
            columns = list(result.keys())
            
            # Convert to MockRow objects
            rows = []
            for row_data in result:
                row_dict = {}
                for i, value in enumerate(row_data):
                    row_dict[columns[i]] = value
                rows.append(MockRow(row_dict))

        # Clean up temporary table
        temp_table.drop(self.engine, checkfirst=True)

        return rows

    def _create_temp_table(self, data: List[Dict[str, Any]], schema: MockStructType) -> Table:
        """Create a temporary table and insert data.
        
        Args:
            data: List of dictionaries to insert
            schema: MockStructType defining table schema
            
        Returns:
            SQLAlchemy Table object
        """
        table_name = f"temp_{self._temp_table_counter}"
        self._temp_table_counter += 1

        # Convert schema to SQLAlchemy columns
        columns = self._schema_to_columns(data, schema)

        # Create table with TEMPORARY prefix (works on most databases)
        table = Table(
            table_name,
            self.metadata,
            *columns,
            prefixes=['TEMPORARY']
        )

        # Create the table
        table.create(self.engine, checkfirst=True)

        # Insert data
        if data:
            with self.engine.connect() as conn:
                conn.execute(insert(table), data)
                conn.commit()

        return table

    def _schema_to_columns(self, data: List[Dict[str, Any]], schema: Optional[MockStructType]) -> List[Column]:
        """Convert MockSpark schema to SQLAlchemy columns.
        
        Args:
            data: Sample data to infer types if schema is None
            schema: Optional MockStructType schema
            
        Returns:
            List of SQLAlchemy Column objects
        """
        columns = []

        if schema:
            # Use provided schema
            for field in schema.fields:
                sql_type = mock_type_to_sqlalchemy(field.dataType)
                columns.append(Column(field.name, sql_type))
        elif data:
            # Infer from data
            for key in data[0].keys():
                sample_value = data[0][key]
                if isinstance(sample_value, bool):
                    sql_type = Boolean()
                elif isinstance(sample_value, int):
                    sql_type = Integer()
                elif isinstance(sample_value, float):
                    sql_type = Float()
                else:
                    sql_type = String()
                columns.append(Column(key, sql_type))

        return columns

    def close(self):
        """Close the SQLAlchemy engine and clean up resources."""
        if self.engine:
            try:
                self.engine.dispose()
            except Exception:
                pass  # Ignore errors during cleanup

    def __del__(self):
        """Cleanup on deletion to prevent resource leaks."""
        try:
            self.close()
        except:
            pass

