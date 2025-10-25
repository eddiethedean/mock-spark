"""
DataFrame Export Utilities

This module handles exporting MockDataFrame to different formats like Pandas and DuckDB.
Extracted from dataframe.py to improve organization and maintainability.

Note: DuckDB export logic has been moved to backend.duckdb.export module for better
separation of concerns. This module now delegates to the backend implementation.
"""

from typing import Any, Optional, TYPE_CHECKING

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
        if df._operations_queue:
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
        # Use backend export functionality
        from mock_spark.backend.factory import BackendFactory

        # Detect backend type from DataFrame's storage
        backend_type = BackendFactory.get_backend_type(df.storage)
        exporter = BackendFactory.create_export_backend(backend_type)
        return exporter.to_duckdb(df, connection, table_name)

    @staticmethod
    def _create_duckdb_table(
        df: "MockDataFrame", connection: Any, table_name: str
    ) -> Any:
        """Create DuckDB table from MockSpark schema.

        Deprecated: Use backend.duckdb.export.DuckDBExporter instead.

        Args:
            df: MockDataFrame with schema
            connection: DuckDB connection or SQLAlchemy Engine
            table_name: Name for the table

        Returns:
            SQLAlchemy Table object
        """
        # Delegate to backend implementation
        from mock_spark.backend.factory import BackendFactory

        # Detect backend type from DataFrame's storage
        backend_type = BackendFactory.get_backend_type(df.storage)
        exporter = BackendFactory.create_export_backend(backend_type)
        return exporter.create_duckdb_table(df, connection, table_name)

    @staticmethod
    def _get_duckdb_type(data_type: Any) -> str:
        """Map MockSpark data type to DuckDB type.

        Deprecated: Use backend.duckdb.export.DuckDBExporter instead.

        Args:
            data_type: MockSpark data type

        Returns:
            DuckDB type string
        """
        # Directly use DuckDBExporter to avoid protocol issues with private methods
        from mock_spark.backend.duckdb.export import DuckDBExporter

        exporter = DuckDBExporter()
        return exporter._get_duckdb_type(data_type)
