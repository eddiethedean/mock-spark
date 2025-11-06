"""
DataFrame Export Utilities

This module handles exporting DataFrame to different formats like Pandas.
Extracted from dataframe.py to improve organization and maintainability.
"""

from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from mock_spark.dataframe import DataFrame


class DataFrameExporter:
    """Handles exporting DataFrame to various formats."""

    @staticmethod
    def to_pandas(df: "DataFrame") -> Any:
        """Convert DataFrame to pandas DataFrame.

        Args:
            df: DataFrame to convert

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
