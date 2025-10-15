"""
String functions for Mock Spark.

This module provides comprehensive string manipulation functions that match PySpark's
string function API. Includes case conversion, trimming, pattern matching, and string
transformation operations for text processing in DataFrames.

Key Features:
    - Complete PySpark string function API compatibility
    - Case conversion (upper, lower)
    - Length and trimming operations (length, trim, ltrim, rtrim)
    - Pattern matching and replacement (regexp_replace, split)
    - String manipulation (substring, concat)
    - Type-safe operations with proper return types
    - Support for both column references and string literals

Example:
    >>> from mock_spark import MockSparkSession, F
    >>> spark = MockSparkSession("test")
    >>> data = [{"name": "  Alice  ", "email": "alice@example.com"}]
    >>> df = spark.createDataFrame(data)
    >>> df.select(
    ...     F.upper(F.trim(F.col("name"))),
    ...     F.regexp_replace(F.col("email"), "@.*", "@company.com")
    ... ).show()
    +--- MockDataFrame: 1 rows ---+
    upper(trim(name)) | regexp_replace(email, '@.*', '@company.com')
    ----------------------------------------------------------------
           ALICE | alice@company.com
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

        operation = MockColumnOperation(column, "upper", name=f"upper({column.name})")
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

        operation = MockColumnOperation(column, "lower", name=f"lower({column.name})")
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

        operation = MockColumnOperation(column, "length", name=f"length({column.name})")
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

        operation = MockColumnOperation(column, "trim", name=f"trim({column.name})")
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

        operation = MockColumnOperation(column, "ltrim", name=f"ltrim({column.name})")
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

        operation = MockColumnOperation(column, "rtrim", name=f"rtrim({column.name})")
        return operation

    @staticmethod
    def regexp_replace(
        column: Union[MockColumn, str], pattern: str, replacement: str
    ) -> MockColumnOperation:
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

        operation = MockColumnOperation(
            column,
            "regexp_replace",
            (pattern, replacement),
            name=f"regexp_replace({column.name}, '{pattern}', '{replacement}')",
        )
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

        operation = MockColumnOperation(
            column, "split", delimiter, name=f"split({column.name}, '{delimiter}')"
        )
        return operation

    @staticmethod
    def substring(
        column: Union[MockColumn, str], start: int, length: Optional[int] = None
    ) -> MockColumnOperation:
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

        name = (
            f"substring({column.name}, {start}, {length})"
            if length is not None
            else f"substring({column.name}, {start})"
        )
        operation = MockColumnOperation(column, "substring", (start, length), name=name)
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
        column_names = [col.name if hasattr(col, "name") else str(col) for col in columns]
        operation = MockColumnOperation(
            base_column,
            "concat",
            columns[1:],
            name=f"concat({', '.join(column_names)})",
        )
        return operation

    @staticmethod
    def format_string(format_str: str, *columns: Union[MockColumn, str]) -> MockColumnOperation:
        """Format string using printf-style format string.

        Args:
            format_str: The format string (e.g., "Hello %s, you are %d years old").
            *columns: Columns to use as format arguments.

        Returns:
            MockColumnOperation representing the format_string function.
        """
        if not columns:
            raise ValueError("At least one column must be provided for format_string")

        base_column = MockColumn(columns[0]) if isinstance(columns[0], str) else columns[0]
        column_names = [col.name if hasattr(col, "name") else str(col) for col in columns]
        operation = MockColumnOperation(
            base_column,
            "format_string",
            (format_str, columns[1:]),
            name=f"format_string('{format_str}', {', '.join(column_names)})",
        )
        return operation

    @staticmethod
    def translate(
        column: Union[MockColumn, str], matching_string: str, replace_string: str
    ) -> MockColumnOperation:
        """Translate characters in string using character mapping.

        Args:
            column: The column to translate.
            matching_string: Characters to match.
            replace_string: Characters to replace with (must be same length as matching_string).

        Returns:
            MockColumnOperation representing the translate function.
        """
        if isinstance(column, str):
            column = MockColumn(column)

        operation = MockColumnOperation(
            column,
            "translate",
            (matching_string, replace_string),
            name=f"translate({column.name}, '{matching_string}', '{replace_string}')",
        )
        return operation

    @staticmethod
    def ascii(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Get ASCII value of first character in string.

        Args:
            column: The column to get ASCII value of.

        Returns:
            MockColumnOperation representing the ascii function.
        """
        if isinstance(column, str):
            column = MockColumn(column)

        operation = MockColumnOperation(column, "ascii", name=f"ascii({column.name})")
        return operation

    @staticmethod
    def base64(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Encode string to base64.

        Args:
            column: The column to encode.

        Returns:
            MockColumnOperation representing the base64 function.
        """
        if isinstance(column, str):
            column = MockColumn(column)

        operation = MockColumnOperation(column, "base64", name=f"base64({column.name})")
        return operation

    @staticmethod
    def unbase64(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Decode base64 string.

        Args:
            column: The column to decode.

        Returns:
            MockColumnOperation representing the unbase64 function.
        """
        if isinstance(column, str):
            column = MockColumn(column)

        operation = MockColumnOperation(column, "unbase64", name=f"unbase64({column.name})")
        return operation

    @staticmethod
    def regexp_extract_all(
        column: Union[MockColumn, str], pattern: str, idx: int = 0
    ) -> MockColumnOperation:
        """Extract all matches of a regex pattern.

        Args:
            column: The column to extract from.
            pattern: The regex pattern to match.
            idx: Group index to extract (default: 0 for entire match).

        Returns:
            MockColumnOperation representing the regexp_extract_all function.

        Example:
            >>> df.select(F.regexp_extract_all(F.col("text"), r"\d+", 0))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        operation = MockColumnOperation(
            column,
            "regexp_extract_all",
            (pattern, idx),
            name=f"regexp_extract_all({column.name}, '{pattern}', {idx})",
        )
        return operation

    @staticmethod
    def array_join(
        column: Union[MockColumn, str],
        delimiter: str,
        null_replacement: Optional[str] = None,
    ) -> MockColumnOperation:
        """Join array elements with a delimiter.

        Args:
            column: The array column to join.
            delimiter: The delimiter to use for joining.
            null_replacement: Optional string to replace nulls with.

        Returns:
            MockColumnOperation representing the array_join function.

        Example:
            >>> df.select(F.array_join(F.col("tags"), ", "))
            >>> df.select(F.array_join(F.col("tags"), "|", "N/A"))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        if null_replacement is not None:
            name = f"array_join({column.name}, '{delimiter}', '{null_replacement}')"
            args: Any = (delimiter, null_replacement)
        else:
            name = f"array_join({column.name}, '{delimiter}')"
            args = (delimiter, None)

        operation = MockColumnOperation(column, "array_join", args, name=name)
        return operation

    @staticmethod
    def repeat(column: Union[MockColumn, str], n: int) -> MockColumnOperation:
        """Repeat a string N times.

        Args:
            column: The column to repeat.
            n: Number of times to repeat.

        Returns:
            MockColumnOperation representing the repeat function.

        Example:
            >>> df.select(F.repeat(F.col("text"), 3))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        operation = MockColumnOperation(
            column, "repeat", n, name=f"repeat({column.name}, {n})"
        )
        return operation

    @staticmethod
    def initcap(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Capitalize first letter of each word.

        Args:
            column: The column to capitalize.

        Returns:
            MockColumnOperation representing the initcap function.

        Example:
            >>> df.select(F.initcap(F.col("name")))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        operation = MockColumnOperation(column, "initcap", name=f"initcap({column.name})")
        return operation

    @staticmethod
    def soundex(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Soundex encoding for phonetic matching.

        Args:
            column: The column to encode.

        Returns:
            MockColumnOperation representing the soundex function.

        Example:
            >>> df.select(F.soundex(F.col("name")))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        operation = MockColumnOperation(column, "soundex", name=f"soundex({column.name})")
        return operation
