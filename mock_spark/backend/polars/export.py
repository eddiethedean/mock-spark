"""
Polars export utilities for DataFrames.

This module handles exporting MockDataFrame to various formats using Polars.
"""

from typing import Any, Optional, TYPE_CHECKING
import polars as pl

if TYPE_CHECKING:
    from mock_spark.dataframe import MockDataFrame


class PolarsExporter:
    """Handles exporting DataFrame to various formats using Polars."""

    def to_polars(self, df: "MockDataFrame") -> pl.DataFrame:
        """Convert DataFrame to Polars DataFrame.

        Args:
            df: MockDataFrame to convert

        Returns:
            Polars DataFrame
        """
        # Get data from DataFrame
        data = df.collect()
        if not data:
            # Empty DataFrame - create from schema
            from .type_mapper import mock_type_to_polars_dtype
            from mock_spark.spark_types import MockStructType

            schema_dict = {}
            if hasattr(df, "schema") and isinstance(df.schema, MockStructType):
                for field in df.schema.fields:
                    polars_dtype = mock_type_to_polars_dtype(field.dataType)
                    schema_dict[field.name] = pl.Series(field.name, [], dtype=polars_dtype)
            return pl.DataFrame(schema_dict)

        # Convert to list of dicts if needed
        if isinstance(data[0], dict):
            return pl.DataFrame(data)
        else:
            # Convert MockRow objects to dicts
            dict_data = [dict(row) for row in data]
            return pl.DataFrame(dict_data)

    def to_pandas(self, df: "MockDataFrame") -> Any:
        """Convert DataFrame to Pandas DataFrame.

        Args:
            df: MockDataFrame to convert

        Returns:
            Pandas DataFrame

        Raises:
            ImportError: If pandas is not installed
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "pandas is required for toPandas() method. "
                "Install with: pip install pandas"
            )

        polars_df = self.to_polars(df)
        return polars_df.to_pandas()

    def to_parquet(
        self, df: "MockDataFrame", path: str, compression: str = "snappy"
    ) -> None:
        """Export DataFrame to Parquet file.

        Args:
            df: MockDataFrame to export
            path: Path to output Parquet file
            compression: Compression codec (default: "snappy")
        """
        polars_df = self.to_polars(df)
        polars_df.write_parquet(path, compression=compression)

    def to_csv(
        self, df: "MockDataFrame", path: str, header: bool = True, separator: str = ","
    ) -> None:
        """Export DataFrame to CSV file.

        Args:
            df: MockDataFrame to export
            path: Path to output CSV file
            header: Whether to include header row
            separator: CSV separator character
        """
        polars_df = self.to_polars(df)
        polars_df.write_csv(path, include_header=header, separator=separator)

    def to_json(
        self, df: "MockDataFrame", path: str, pretty: bool = False
    ) -> None:
        """Export DataFrame to JSON file.

        Args:
            df: MockDataFrame to export
            path: Path to output JSON file
            pretty: Whether to use pretty formatting
        """
        polars_df = self.to_polars(df)
        polars_df.write_json(path, pretty=pretty)

    def to_duckdb(
        self, df: "MockDataFrame", connection: Any = None, table_name: Optional[str] = None
    ) -> str:
        """Export DataFrame to DuckDB (legacy method for compatibility).

        Note: This method is kept for backward compatibility but uses Polars internally.
        For new code, consider using to_polars() instead.

        Args:
            df: MockDataFrame to convert
            connection: DuckDB connection (optional, creates temporary if None)
            table_name: Table name (auto-generated if None)

        Returns:
            Table name in DuckDB
        """
        try:
            import duckdb
        except ImportError:
            raise ImportError(
                "duckdb is required for toDuckDB() method. "
                "Install with: pip install duckdb"
            )

        if connection is None:
            connection = duckdb.connect(":memory:")

        if table_name is None:
            table_name = f"temp_df_{id(df)}"

        # Convert to Polars, then to DuckDB
        polars_df = self.to_polars(df)
        connection.register(table_name, polars_df)

        return table_name

    def create_duckdb_table(
        self, df: "MockDataFrame", connection: Any, table_name: str
    ) -> Any:
        """Create a DuckDB table from DataFrame schema (legacy method).

        Args:
            df: Source DataFrame
            connection: DuckDB connection
            table_name: Table name

        Returns:
            Table object
        """
        # Convert to Polars and register with DuckDB
        polars_df = self.to_polars(df)
        connection.register(table_name, polars_df)
        return table_name

