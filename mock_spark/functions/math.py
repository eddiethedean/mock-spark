"""
Mathematical functions for Mock Spark.

This module contains mathematical functions including abs, round, ceil, floor, etc.
"""

from typing import Any, Union, Optional
from mock_spark.functions.base import MockColumn, MockColumnOperation


class MathFunctions:
    """Collection of mathematical functions."""

    @staticmethod
    def abs(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Get absolute value.

        Args:
            column: The column to get absolute value of.

        Returns:
            MockColumnOperation representing the abs function.
        """
        if isinstance(column, str):
            column = MockColumn(column)
        
        operation = MockColumnOperation(column, "abs")
        operation.name = f"abs({column.name})"
        return operation

    @staticmethod
    def round(column: Union[MockColumn, str], scale: int = 0) -> MockColumnOperation:
        """Round to specified number of decimal places.

        Args:
            column: The column to round.
            scale: Number of decimal places (default: 0).

        Returns:
            MockColumnOperation representing the round function.
        """
        if isinstance(column, str):
            column = MockColumn(column)
        
        operation = MockColumnOperation(column, "round", scale)
        operation.name = f"round({column.name}, {scale})"
        return operation

    @staticmethod
    def ceil(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Round up to nearest integer.

        Args:
            column: The column to round up.

        Returns:
            MockColumnOperation representing the ceil function.
        """
        if isinstance(column, str):
            column = MockColumn(column)
        
        operation = MockColumnOperation(column, "ceil")
        operation.name = f"ceil({column.name})"
        return operation

    @staticmethod
    def floor(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Round down to nearest integer.

        Args:
            column: The column to round down.

        Returns:
            MockColumnOperation representing the floor function.
        """
        if isinstance(column, str):
            column = MockColumn(column)
        
        operation = MockColumnOperation(column, "floor")
        operation.name = f"floor({column.name})"
        return operation

    @staticmethod
    def sqrt(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Get square root.

        Args:
            column: The column to get square root of.

        Returns:
            MockColumnOperation representing the sqrt function.
        """
        if isinstance(column, str):
            column = MockColumn(column)
        
        operation = MockColumnOperation(column, "sqrt")
        operation.name = f"sqrt({column.name})"
        return operation

    @staticmethod
    def exp(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Get exponential (e^x).

        Args:
            column: The column to get exponential of.

        Returns:
            MockColumnOperation representing the exp function.
        """
        if isinstance(column, str):
            column = MockColumn(column)
        
        operation = MockColumnOperation(column, "exp")
        operation.name = f"exp({column.name})"
        return operation

    @staticmethod
    def log(column: Union[MockColumn, str], base: Optional[float] = None) -> MockColumnOperation:
        """Get logarithm.

        Args:
            column: The column to get logarithm of.
            base: Optional base for logarithm (default: natural log).

        Returns:
            MockColumnOperation representing the log function.
        """
        if isinstance(column, str):
            column = MockColumn(column)
        
        operation = MockColumnOperation(column, "log", base)
        if base is not None:
            operation.name = f"log({base}, {column.name})"
        else:
            operation.name = f"log({column.name})"
        return operation

    @staticmethod
    def pow(column: Union[MockColumn, str], exponent: Union[MockColumn, float, int]) -> MockColumnOperation:
        """Raise to power.

        Args:
            column: The column to raise to power.
            exponent: The exponent.

        Returns:
            MockColumnOperation representing the pow function.
        """
        if isinstance(column, str):
            column = MockColumn(column)
        
        operation = MockColumnOperation(column, "pow", exponent)
        operation.name = f"pow({column.name}, {exponent})"
        return operation

    @staticmethod
    def sin(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Get sine.

        Args:
            column: The column to get sine of.

        Returns:
            MockColumnOperation representing the sin function.
        """
        if isinstance(column, str):
            column = MockColumn(column)
        
        operation = MockColumnOperation(column, "sin")
        operation.name = f"sin({column.name})"
        return operation

    @staticmethod
    def cos(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Get cosine.

        Args:
            column: The column to get cosine of.

        Returns:
            MockColumnOperation representing the cos function.
        """
        if isinstance(column, str):
            column = MockColumn(column)
        
        operation = MockColumnOperation(column, "cos")
        operation.name = f"cos({column.name})"
        return operation

    @staticmethod
    def tan(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Get tangent.

        Args:
            column: The column to get tangent of.

        Returns:
            MockColumnOperation representing the tan function.
        """
        if isinstance(column, str):
            column = MockColumn(column)
        
        operation = MockColumnOperation(column, "tan")
        operation.name = f"tan({column.name})"
        return operation
