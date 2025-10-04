"""
String functions for Mock Spark.

This module contains string manipulation functions including upper, lower, length, etc.
"""

from typing import Any, Union, Optional
from mock_spark.functions.base import MockColumn, MockColumnOperation


class StringFunctions:
    """Collection of string manipulation functions."""

    @staticmethod
    def upper(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Convert string to uppercase.

        Args:
            column: The column to convert.

        Returns:
            MockColumnOperation representing the upper function.
        """
        if isinstance(column, str):
            column = MockColumn(column)
        
        operation = MockColumnOperation(column, "upper")
        operation.name = f"upper({column.name})"
        return operation

    @staticmethod
    def lower(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Convert string to lowercase.

        Args:
            column: The column to convert.

        Returns:
            MockColumnOperation representing the lower function.
        """
        if isinstance(column, str):
            column = MockColumn(column)
        
        operation = MockColumnOperation(column, "lower")
        operation.name = f"lower({column.name})"
        return operation

    @staticmethod
    def length(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Get the length of a string.

        Args:
            column: The column to get length of.

        Returns:
            MockColumnOperation representing the length function.
        """
        if isinstance(column, str):
            column = MockColumn(column)
        
        operation = MockColumnOperation(column, "length")
        operation.name = f"length({column.name})"
        return operation

    @staticmethod
    def trim(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Trim whitespace from string.

        Args:
            column: The column to trim.

        Returns:
            MockColumnOperation representing the trim function.
        """
        if isinstance(column, str):
            column = MockColumn(column)
        
        operation = MockColumnOperation(column, "trim")
        operation.name = f"trim({column.name})"
        return operation

    @staticmethod
    def ltrim(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Trim whitespace from left side of string.

        Args:
            column: The column to trim.

        Returns:
            MockColumnOperation representing the ltrim function.
        """
        if isinstance(column, str):
            column = MockColumn(column)
        
        operation = MockColumnOperation(column, "ltrim")
        operation.name = f"ltrim({column.name})"
        return operation

    @staticmethod
    def rtrim(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Trim whitespace from right side of string.

        Args:
            column: The column to trim.

        Returns:
            MockColumnOperation representing the rtrim function.
        """
        if isinstance(column, str):
            column = MockColumn(column)
        
        operation = MockColumnOperation(column, "rtrim")
        operation.name = f"rtrim({column.name})"
        return operation

    @staticmethod
    def regexp_replace(column: Union[MockColumn, str], pattern: str, replacement: str) -> MockColumnOperation:
        """Replace regex pattern in string.

        Args:
            column: The column to replace in.
            pattern: The regex pattern to match.
            replacement: The replacement string.

        Returns:
            MockColumnOperation representing the regexp_replace function.
        """
        if isinstance(column, str):
            column = MockColumn(column)
        
        operation = MockColumnOperation(column, "regexp_replace", (pattern, replacement))
        operation.name = f"regexp_replace({column.name}, '{pattern}', '{replacement}')"
        return operation

    @staticmethod
    def split(column: Union[MockColumn, str], delimiter: str) -> MockColumnOperation:
        """Split string by delimiter.

        Args:
            column: The column to split.
            delimiter: The delimiter to split on.

        Returns:
            MockColumnOperation representing the split function.
        """
        if isinstance(column, str):
            column = MockColumn(column)
        
        operation = MockColumnOperation(column, "split", delimiter)
        operation.name = f"split({column.name}, '{delimiter}')"
        return operation

    @staticmethod
    def substring(column: Union[MockColumn, str], start: int, length: Optional[int] = None) -> MockColumnOperation:
        """Extract substring from string.

        Args:
            column: The column to extract from.
            start: Starting position (1-indexed).
            length: Optional length of substring.

        Returns:
            MockColumnOperation representing the substring function.
        """
        if isinstance(column, str):
            column = MockColumn(column)
        
        operation = MockColumnOperation(column, "substring", (start, length))
        if length is not None:
            operation.name = f"substring({column.name}, {start}, {length})"
        else:
            operation.name = f"substring({column.name}, {start})"
        return operation

    @staticmethod
    def concat(*columns: Union[MockColumn, str]) -> MockColumnOperation:
        """Concatenate multiple strings.

        Args:
            *columns: Columns or strings to concatenate.

        Returns:
            MockColumnOperation representing the concat function.
        """
        # Use the first column as the base
        if not columns:
            raise ValueError("At least one column must be provided")
        
        base_column = MockColumn(columns[0]) if isinstance(columns[0], str) else columns[0]
        operation = MockColumnOperation(base_column, "concat", columns[1:])
        column_names = [col.name if hasattr(col, 'name') else str(col) for col in columns]
        operation.name = f"concat({', '.join(column_names)})"
        return operation
