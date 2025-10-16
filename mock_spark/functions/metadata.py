"""
Metadata and utility functions for Mock Spark.

This module provides metadata functions like input_file_name, partition IDs,
and special utilities like broadcast hints.
"""

from typing import Union, Any
from mock_spark.functions.base import MockColumn, MockColumnOperation
from mock_spark.functions.core.literals import MockLiteral


class MetadataFunctions:
    """Collection of metadata and utility functions."""

    @staticmethod
    def input_file_name() -> MockColumnOperation:
        """Returns the name of the file being read (returns empty string in mock).

        Returns:
            MockColumnOperation representing input_file_name
        """
        return MockColumnOperation(
            MockLiteral(""),
            "input_file_name",
            name="input_file_name()"
        )

    @staticmethod
    def monotonically_increasing_id() -> MockColumnOperation:
        """Generate monotonically increasing 64-bit integers.

        Returns:
            MockColumnOperation representing monotonically_increasing_id
        """
        return MockColumnOperation(
            MockLiteral(0),
            "monotonically_increasing_id",
            name="monotonically_increasing_id()"
        )

    @staticmethod
    def spark_partition_id() -> MockColumnOperation:
        """Returns the partition ID (returns 0 in mock).

        Returns:
            MockColumnOperation representing spark_partition_id
        """
        return MockColumnOperation(
            MockLiteral(0),
            "spark_partition_id",
            name="spark_partition_id()"
        )

    @staticmethod
    def broadcast(df: Any) -> Any:
        """Mark DataFrame for broadcast join (pass-through in mock).

        Args:
            df: DataFrame to broadcast

        Returns:
            The same DataFrame (broadcast is a hint, no-op in mock)
        """
        return df

    @staticmethod
    def column(col_name: str) -> MockColumn:
        """Create a column reference (alias for col).

        Args:
            col_name: Column name

        Returns:
            MockColumn reference
        """
        return MockColumn(col_name)


class GroupingFunctions:
    """Grouping indicator functions."""

    @staticmethod
    def grouping(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Indicates whether a column is aggregated (for CUBE/ROLLUP).

        Args:
            column: Column name

        Returns:
            MockColumnOperation representing grouping
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(
            column,
            "grouping",
            name=f"grouping({column.name})"
        )

    @staticmethod
    def grouping_id(*cols: Union[MockColumn, str]) -> MockColumnOperation:
        """Computes grouping ID for CUBE/ROLLUP.

        Args:
            *cols: Columns to compute grouping ID

        Returns:
            MockColumnOperation representing grouping_id
        """
        columns = []
        for col in cols:
            if isinstance(col, str):
                columns.append(MockColumn(col))
            else:
                columns.append(col)

        return MockColumnOperation(
            columns[0] if columns else MockColumn(""),
            "grouping_id",
            value=columns[1:] if len(columns) > 1 else [],
            name="grouping_id(...)"
        )

