"""
Polars export utilities for DataFrames.

This module handles exporting DataFrame to various formats using Polars.
"""

from typing import Any, TYPE_CHECKING
import polars as pl

if TYPE_CHECKING:
    from sparkless.dataframe import DataFrame


class PolarsExporter:
    """Handles exporting DataFrame to various formats using Polars."""

    def to_polars(self, df: "DataFrame") -> pl.DataFrame:
        """Convert DataFrame to Polars DataFrame.

        Args:
            df: DataFrame to convert

        Returns:
            Polars DataFrame
        """
        # Get data from DataFrame
        data = df.collect()
        if not data:
            # Empty DataFrame - create from schema
            from .type_mapper import mock_type_to_polars_dtype
            from sparkless.spark_types import StructType

            schema_dict = {}
            if hasattr(df, "schema") and isinstance(df.schema, StructType):
                for field in df.schema.fields:
                    polars_dtype = mock_type_to_polars_dtype(field.dataType)
                    schema_dict[field.name] = pl.Series(
                        field.name, [], dtype=polars_dtype
                    )
            return pl.DataFrame(schema_dict)

        # Convert to list of dicts if needed
        # Check first element to determine if conversion is needed
        from ...core.type_utils import is_row

        first_item = data[0]
        if isinstance(first_item, dict):
            return pl.DataFrame(data)
        # Convert Row objects to dicts
        # Row objects can be converted to dict using dict() constructor
        # Note: Row and dict can coexist at runtime even though mypy flags this
        dict_data = [dict(row) if is_row(row) else row for row in data]
        return pl.DataFrame(dict_data)

    def to_pandas(self, df: "DataFrame") -> Any:
        """Convert DataFrame to Pandas DataFrame.

        Args:
            df: DataFrame to convert

        Returns:
            Pandas DataFrame

        Raises:
            ImportError: If pandas is not installed
        """
        try:
            import pandas as pd  # noqa: F401
        except ImportError:
            raise ImportError(
                "pandas is required for toPandas() method. "
                "Install with: pip install pandas"
            )

        polars_df = self.to_polars(df)
        return polars_df.to_pandas()

    def to_parquet(
        self, df: "DataFrame", path: str, compression: str = "snappy"
    ) -> None:
        """Export DataFrame to Parquet file.

        Args:
            df: DataFrame to export
            path: Path to output Parquet file
            compression: Compression codec (default: "snappy")
        """
        polars_df = self.to_polars(df)
        polars_df.write_parquet(path, compression=compression)

    def to_csv(
        self, df: "DataFrame", path: str, header: bool = True, separator: str = ","
    ) -> None:
        """Export DataFrame to CSV file.

        Args:
            df: DataFrame to export
            path: Path to output CSV file
            header: Whether to include header row
            separator: CSV separator character
        """
        polars_df = self.to_polars(df)
        polars_df.write_csv(path, include_header=header, separator=separator)

    def to_json(self, df: "DataFrame", path: str, pretty: bool = False) -> None:
        """Export DataFrame to JSON file.

        Args:
            df: DataFrame to export
            path: Path to output JSON file
            pretty: Whether to use pretty formatting
        """
        polars_df = self.to_polars(df)
        polars_df.write_json(path, pretty=pretty)
