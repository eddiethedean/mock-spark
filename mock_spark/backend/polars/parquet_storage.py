"""
Parquet storage handler for Polars DataFrames.

This module handles reading and writing Parquet files using Polars,
including support for append mode and schema evolution.
"""

import os
from typing import Optional
import polars as pl


class ParquetStorage:
    """Handles Parquet file persistence with Polars."""

    @staticmethod
    def write_parquet(
        df: pl.DataFrame, path: str, mode: str = "overwrite", compression: str = "snappy"
    ) -> None:
        """Write DataFrame to Parquet file.

        Args:
            df: Polars DataFrame to write
            path: Path to Parquet file
            mode: Write mode ("overwrite" or "append")
            compression: Compression codec (default: "snappy")

        Raises:
            ValueError: If mode is not supported
        """
        os.makedirs(os.path.dirname(path), exist_ok=True)

        if mode == "overwrite":
            df.write_parquet(path, compression=compression)
        elif mode == "append":
            if os.path.exists(path):
                # Read existing data and append
                existing_df = pl.read_parquet(path)
                combined_df = pl.concat([existing_df, df])
                combined_df.write_parquet(path, compression=compression)
            else:
                # File doesn't exist, just write
                df.write_parquet(path, compression=compression)
        else:
            raise ValueError(f"Unsupported write mode: {mode}")

    @staticmethod
    def read_parquet(path: str) -> pl.DataFrame:
        """Read DataFrame from Parquet file.

        Args:
            path: Path to Parquet file

        Returns:
            Polars DataFrame

        Raises:
            FileNotFoundError: If file doesn't exist
        """
        if not os.path.exists(path):
            raise FileNotFoundError(f"Parquet file not found: {path}")
        return pl.read_parquet(path)

    @staticmethod
    def schema_evolution(old_path: str, new_df: pl.DataFrame) -> pl.DataFrame:
        """Handle schema evolution when appending data with new columns.

        Args:
            old_path: Path to existing Parquet file
            new_df: New DataFrame to append with potentially new columns

        Returns:
            Combined DataFrame with schema evolution applied
        """
        if not os.path.exists(old_path):
            return new_df

        old_df = pl.read_parquet(old_path)

        # Get all columns from both DataFrames
        old_columns = set(old_df.columns)
        new_columns = set(new_df.columns)

        # Add missing columns to old DataFrame with null values
        missing_in_old = new_columns - old_columns
        for col in missing_in_old:
            old_df = old_df.with_columns(pl.lit(None).alias(col))

        # Add missing columns to new DataFrame with null values
        missing_in_new = old_columns - new_columns
        for col in missing_in_new:
            new_df = new_df.with_columns(pl.lit(None).alias(col))

        # Ensure column order matches old DataFrame
        column_order = old_df.columns
        new_df = new_df.select(column_order)

        # Combine DataFrames
        return pl.concat([old_df, new_df])

    @staticmethod
    def parquet_exists(path: str) -> bool:
        """Check if Parquet file exists.

        Args:
            path: Path to Parquet file

        Returns:
            True if file exists, False otherwise
        """
        return os.path.exists(path)

