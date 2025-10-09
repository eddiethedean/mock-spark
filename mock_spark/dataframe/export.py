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
    def to_duckdb(df: "MockDataFrame", connection=None, table_name: Optional[str] = None) -> str:
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

        # Insert data using SQLAlchemy insert() for type safety
        if df.data:
            # Convert data to use actual column names
            rows = [
                {field.name: row.get(field.name) for field in df.schema.fields}
                for row in df.data
            ]
            
            # Get engine for insert operation
            if hasattr(connection, "raw_connection"):
                engine = connection
            else:
                # Create engine from DuckDB connection
                import duckdb
                if isinstance(connection, duckdb.DuckDBPyConnection):
                    engine = create_engine("duckdb:///:memory:")
                else:
                    engine = connection
            
            # Use SQLAlchemy bulk insert
            with engine.begin() as conn:
                conn.execute(insert(table), rows)

        return table_name

    @staticmethod
    def _create_duckdb_table(df: "MockDataFrame", connection, table_name: str) -> Any:
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
            # Create engine from existing DuckDB connection
            from duckdb_engine import Connection
            engine = create_engine("duckdb:///:memory:")
            # Note: We'll use a new engine for now; ideally would reuse connection
        else:
            # Assume it's already an engine
            engine = connection
        
        metadata = MetaData()
        table = create_table_from_mock_schema(table_name, df.schema, metadata)
        table.create(engine, checkfirst=True)
        
        return table

    @staticmethod
    def _get_duckdb_type(data_type) -> str:
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
