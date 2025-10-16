"""
Bitwise functions for Mock Spark (PySpark 3.2+).

This module provides bitwise operations on integer columns.
"""

from typing import Union, TYPE_CHECKING

if TYPE_CHECKING:
    from mock_spark.functions.base import MockAggregateFunction

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

    # Priority 2: Bitwise Aggregate Functions
    @staticmethod
    def bit_and(column: Union[MockColumn, str]) -> "MockAggregateFunction":
        """Aggregate function - bitwise AND of all values (PySpark 3.5+).

        Args:
            column: Integer column.

        Returns:
            MockAggregateFunction representing the bit_and aggregate function.

        Example:
            >>> df.groupBy("dept").agg(F.bit_and("flags"))
        """
        from mock_spark.functions.base import MockAggregateFunction
        from mock_spark.spark_types import LongType

        return MockAggregateFunction(column, "bit_and", LongType())

    @staticmethod
    def bit_or(column: Union[MockColumn, str]) -> "MockAggregateFunction":
        """Aggregate function - bitwise OR of all values (PySpark 3.5+).

        Args:
            column: Integer column.

        Returns:
            MockAggregateFunction representing the bit_or aggregate function.

        Example:
            >>> df.groupBy("dept").agg(F.bit_or("flags"))
        """
        from mock_spark.functions.base import MockAggregateFunction
        from mock_spark.spark_types import LongType

        return MockAggregateFunction(column, "bit_or", LongType())

    @staticmethod
    def bit_xor(column: Union[MockColumn, str]) -> "MockAggregateFunction":
        """Aggregate function - bitwise XOR of all values (PySpark 3.5+).

        Args:
            column: Integer column.

        Returns:
            MockAggregateFunction representing the bit_xor aggregate function.

        Example:
            >>> df.groupBy("dept").agg(F.bit_xor("flags"))
        """
        from mock_spark.functions.base import MockAggregateFunction
        from mock_spark.spark_types import LongType

        return MockAggregateFunction(column, "bit_xor", LongType())

    # Deprecated Aliases
    @staticmethod
    def bitwiseNOT(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Deprecated alias for bitwise_not (all PySpark versions).
        
        Use bitwise_not instead.
        
        Args:
            column: Integer column.
            
        Returns:
            MockColumnOperation representing bitwise NOT.
        """
        import warnings
        warnings.warn(
            "bitwiseNOT is deprecated. Use bitwise_not instead.",
            FutureWarning,
            stacklevel=2
        )
        return BitwiseFunctions.bitwise_not(column)

