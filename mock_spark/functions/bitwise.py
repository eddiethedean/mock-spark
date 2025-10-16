"""
Bitwise functions for Mock Spark (PySpark 3.2+).

This module provides bitwise operations on integer columns.
"""

from typing import Union
from mock_spark.functions.base import MockColumn, MockColumnOperation


class BitwiseFunctions:
    """Collection of bitwise manipulation functions."""

    @staticmethod
    def bit_count(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Count the number of set bits (population count).

        Args:
            column: Integer column.

        Returns:
            MockColumnOperation representing the bit_count function.

        Example:
            >>> df.select(F.bit_count(F.col("value")))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(column, "bit_count", name=f"bit_count({column.name})")

    @staticmethod
    def bit_get(column: Union[MockColumn, str], pos: int) -> MockColumnOperation:
        """Get bit value at position.

        Args:
            column: Integer column.
            pos: Bit position (0-based, from right).

        Returns:
            MockColumnOperation representing the bit_get function.

        Example:
            >>> df.select(F.bit_get(F.col("value"), 0))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(
            column, "bit_get", pos, name=f"bit_get({column.name}, {pos})"
        )

    @staticmethod
    def bitwise_not(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Perform bitwise NOT operation.

        Args:
            column: Integer column.

        Returns:
            MockColumnOperation representing the bitwise_not function.

        Example:
            >>> df.select(F.bitwise_not(F.col("value")))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(column, "bitwise_not", name=f"bitwise_not({column.name})")

