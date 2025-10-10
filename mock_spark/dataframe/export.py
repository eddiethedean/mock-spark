"""
DataFrame Export Utilities

This module handles exporting MockDataFrame to different formats like Pandas and DuckDB.
Extracted from dataframe.py to improve organization and maintainability.
"""

from typing import Any, Optional, TYPE_CHECKING
from sqlalchemy import create_engine, MetaData, insert

if TYPE_CHECKING:
    from mock_spark.dataframe import MockDataFrame


class DataFrameExporter:
    """Handles exporting DataFrame to various formats."""

    @staticmethod
    def to_pandas(df: "MockDataFrame") -> Any:
        """Convert DataFrame to pandas DataFrame.

        Args:
            df: MockDataFrame to convert

        Returns:
            pandas.DataFrame

        Raises:
            ImportError: If pandas is not installed
        """
        # Handle lazy evaluation
        if df.is_lazy and df._operations_queue:
            materialized = df._materialize_if_lazy()
            return DataFrameExporter.to_pandas(materialized)

        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "pandas is required for toPandas() method. "
                "Install with: pip install mock-spark[pandas] or pip install pandas"
            )

        if not df.data:
            # Create empty DataFrame with correct column structure
            return pd.DataFrame(columns=[field.name for field in df.schema.fields])

        return pd.DataFrame(df.data)

    @staticmethod
    def to_duckdb(
        df: "MockDataFrame", connection: Any = None, table_name: Optional[str] = None
    ) -> str:
        """Convert DataFrame to DuckDB table for analytical operations.

        Args:
            df: MockDataFrame to convert
            connection: DuckDB connection or SQLAlchemy Engine (creates temporary if None)
            table_name: Name for the table (auto-generated if None)

        Returns:
            Table name in DuckDB

        Raises:
            ImportError: If duckdb is not installed
        """
        try:
            import duckdb
        except ImportError:
            raise ImportError(
                "duckdb is required for toDuckDB() method. " "Install with: pip install duckdb"
            )

        # Handle SQLAlchemy Engine objects
        if hasattr(connection, "raw_connection"):
            # It's a SQLAlchemy Engine, get the raw DuckDB connection
            try:
                raw_conn = connection.raw_connection()
                # The raw_conn is already the DuckDB connection, not a wrapper
                connection = raw_conn
            except Exception:
                # If we can't get the raw connection, create a new one
                connection = duckdb.connect(":memory:")
        elif connection is None:
            connection = duckdb.connect(":memory:")

        if table_name is None:
            table_name = f"temp_df_{id(df)}"

        # Create table from schema using SQLAlchemy
        table = DataFrameExporter._create_duckdb_table(df, connection, table_name)

        # Insert data
        if df.data:
            import duckdb

            if isinstance(connection, duckdb.DuckDBPyConnection):
                # Use DuckDB connection directly for backward compatibility
                values_list = [
                    tuple(row.get(field.name) for field in df.schema.fields) for row in df.data
                ]
                placeholders = ", ".join(["?" for _ in df.schema.fields])
                connection.executemany(
                    f"INSERT INTO {table_name} VALUES ({placeholders})", values_list
                )
            else:
                # Use SQLAlchemy for engine-based connections
                rows = [
                    {field.name: row.get(field.name) for field in df.schema.fields}
                    for row in df.data
                ]
                with connection.begin() as conn:
                    conn.execute(insert(table), rows)

        return table_name

    @staticmethod
    def _create_duckdb_table(df: "MockDataFrame", connection: Any, table_name: str) -> Any:
        """Create DuckDB table from MockSpark schema using SQLAlchemy.

        Args:
            df: MockDataFrame with schema
            connection: DuckDB connection or SQLAlchemy Engine
            table_name: Name for the table

        Returns:
            SQLAlchemy Table object
        """
        try:
            import duckdb
        except ImportError:
            raise ImportError("duckdb is required")

        from mock_spark.storage.sqlalchemy_helpers import create_table_from_mock_schema

        # Create SQLAlchemy engine from DuckDB connection if needed
        if isinstance(connection, duckdb.DuckDBPyConnection):
            # For DuckDB connections, use the connection directly with executemany for compatibility
            # Build column definitions
            columns = []
            for field in df.schema.fields:
                # Get DuckDB type
                type_name = type(field.dataType).__name__
                if "String" in type_name:
                    duckdb_type = "VARCHAR"
                elif "Integer" in type_name or "Long" in type_name:
                    duckdb_type = "INTEGER"
                elif "Double" in type_name or "Float" in type_name:
                    duckdb_type = "DOUBLE"
                elif "Boolean" in type_name:
                    duckdb_type = "BOOLEAN"
                elif "Date" in type_name:
                    duckdb_type = "DATE"
                elif "Timestamp" in type_name:
                    duckdb_type = "TIMESTAMP"
                else:
                    duckdb_type = "VARCHAR"
                columns.append(f"{field.name} {duckdb_type}")

            # Create table using DuckDB connection (keep for backward compatibility)
            create_sql = f"CREATE TABLE {table_name} ({', '.join(columns)})"
            connection.execute(create_sql)

            # Also create metadata representation for return
            metadata = MetaData()
            table = create_table_from_mock_schema(table_name, df.schema, metadata)
        else:
            # Assume it's a SQLAlchemy engine
            engine = connection
            metadata = MetaData()
            table = create_table_from_mock_schema(table_name, df.schema, metadata)
            table.create(engine, checkfirst=True)

        return table

    @staticmethod
    def _get_duckdb_type(data_type: Any) -> str:
        """Map MockSpark data type to DuckDB type.

        Args:
            data_type: MockSpark data type

        Returns:
            DuckDB type string
        """
        type_mapping = {
            "StringType": "VARCHAR",
            "IntegerType": "INTEGER",
            "LongType": "BIGINT",
            "DoubleType": "DOUBLE",
            "FloatType": "DOUBLE",
            "BooleanType": "BOOLEAN",
            "DateType": "DATE",
            "TimestampType": "TIMESTAMP",
            "ArrayType": "BLOB",
            "MapType": "BLOB",
            "StructType": "BLOB",
            "BinaryType": "BLOB",
            "DecimalType": "DECIMAL",
            "ShortType": "SMALLINT",
            "ByteType": "TINYINT",
            "NullType": "VARCHAR",
        }
        return type_mapping.get(data_type.__class__.__name__, "VARCHAR")
