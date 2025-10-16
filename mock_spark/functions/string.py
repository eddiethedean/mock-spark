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
    def char_length(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Alias for length() - Get the character length of a string (PySpark 3.5+).

        Args:
            column: The column to get length of.

        Returns:
            MockColumnOperation representing the char_length function.
        """
        if isinstance(column, str):
            column = MockColumn(column)

        operation = MockColumnOperation(column, "length", name=f"char_length({column.name})")
        return operation

    @staticmethod
    def character_length(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Alias for length() - Get the character length of a string (PySpark 3.5+).

        Args:
            column: The column to get length of.

        Returns:
            MockColumnOperation representing the character_length function.
        """
        if isinstance(column, str):
            column = MockColumn(column)

        operation = MockColumnOperation(column, "length", name=f"character_length({column.name})")
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
        r"""Extract all matches of a regex pattern.

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

    # URL Functions (PySpark 3.2+)

    @staticmethod
    def parse_url(url: Union[MockColumn, str], part: str) -> MockColumnOperation:
        """Extract a part from a URL.

        Args:
            url: URL column or string.
            part: Part to extract (HOST, PATH, QUERY, REF, PROTOCOL, FILE, AUTHORITY, USERINFO).

        Returns:
            MockColumnOperation representing the parse_url function.

        Example:
            >>> df.select(F.parse_url(F.col("url"), "HOST"))
        """
        if isinstance(url, str):
            url = MockColumn(url)

        return MockColumnOperation(
            url,
            "parse_url",
            part,
            name=f"parse_url({url.name}, '{part}')",
        )

    @staticmethod
    def url_encode(url: Union[MockColumn, str]) -> MockColumnOperation:
        """URL-encode a string.

        Args:
            url: String column to encode.

        Returns:
            MockColumnOperation representing the url_encode function.

        Example:
            >>> df.select(F.url_encode(F.col("text")))
        """
        if isinstance(url, str):
            url = MockColumn(url)

        return MockColumnOperation(url, "url_encode", name=f"url_encode({url.name})")

    @staticmethod
    def url_decode(url: Union[MockColumn, str]) -> MockColumnOperation:
        """URL-decode a string.

        Args:
            url: String column to decode.

        Returns:
            MockColumnOperation representing the url_decode function.

        Example:
            >>> df.select(F.url_decode(F.col("encoded")))
        """
        if isinstance(url, str):
            url = MockColumn(url)

        return MockColumnOperation(url, "url_decode", name=f"url_decode({url.name})")

    @staticmethod
    def concat_ws(sep: str, *cols: Union[MockColumn, str]) -> MockColumnOperation:
        """Concatenate multiple columns with a separator.

        Args:
            sep: Separator string
            *cols: Columns to concatenate

        Returns:
            MockColumnOperation representing concat_ws

        Example:
            >>> df.select(F.concat_ws("-", F.col("first"), F.col("last")))
        """
        columns = []
        for col in cols:
            if isinstance(col, str):
                columns.append(MockColumn(col))
            else:
                columns.append(col)

        return MockColumnOperation(
            columns[0] if columns else MockColumn(""),
            "concat_ws",
            value=(sep, columns[1:] if len(columns) > 1 else []),
            name=f"concat_ws({sep}, ...)"
        )

    @staticmethod
    def regexp_extract(
        column: Union[MockColumn, str],
        pattern: str,
        idx: int = 0
    ) -> MockColumnOperation:
        """Extract a specific group matched by a regex pattern.

        Args:
            column: Input column
            pattern: Regular expression pattern
            idx: Group index to extract (default 0)

        Returns:
            MockColumnOperation representing regexp_extract

        Example:
            >>> df.select(F.regexp_extract(F.col("email"), r"(.+)@(.+)", 1))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(
            column,
            "regexp_extract",
            value=(pattern, idx),
            name=f"regexp_extract({column.name}, {pattern}, {idx})"
        )

    @staticmethod
    def substring_index(
        column: Union[MockColumn, str],
        delim: str,
        count: int
    ) -> MockColumnOperation:
        """Returns substring before/after count occurrences of delimiter.

        Args:
            column: Input string column
            delim: Delimiter string
            count: Number of delimiters (positive for left, negative for right)

        Returns:
            MockColumnOperation representing substring_index

        Example:
            >>> df.select(F.substring_index(F.col("path"), "/", 2))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(
            column,
            "substring_index",
            value=(delim, count),
            name=f"substring_index({column.name}, {delim}, {count})"
        )

    @staticmethod
    def format_number(
        column: Union[MockColumn, str],
        d: int
    ) -> MockColumnOperation:
        """Format number with d decimal places and thousands separator.

        Args:
            column: Numeric column
            d: Number of decimal places

        Returns:
            MockColumnOperation representing format_number

        Example:
            >>> df.select(F.format_number(F.col("amount"), 2))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(
            column,
            "format_number",
            value=d,
            name=f"format_number({column.name}, {d})"
        )

    @staticmethod
    def instr(
        column: Union[MockColumn, str],
        substr: str
    ) -> MockColumnOperation:
        """Locate the position of the first occurrence of substr (1-indexed).

        Args:
            column: Input string column
            substr: Substring to locate

        Returns:
            MockColumnOperation representing instr

        Example:
            >>> df.select(F.instr(F.col("text"), "spark"))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(
            column,
            "instr",
            value=substr,
            name=f"instr({column.name}, {substr})"
        )

    @staticmethod
    def locate(
        substr: str,
        column: Union[MockColumn, str],
        pos: int = 1
    ) -> MockColumnOperation:
        """Locate the position of substr starting from pos (1-indexed).

        Args:
            substr: Substring to locate
            column: Input string column
            pos: Starting position (default 1)

        Returns:
            MockColumnOperation representing locate

        Example:
            >>> df.select(F.locate("spark", F.col("text"), 1))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(
            column,
            "locate",
            value=(substr, pos),
            name=f"locate({substr}, {column.name}, {pos})"
        )

    @staticmethod
    def lpad(
        column: Union[MockColumn, str],
        len: int,
        pad: str
    ) -> MockColumnOperation:
        """Left-pad string column to length len with pad string.

        Args:
            column: Input string column
            len: Target length
            pad: Padding string

        Returns:
            MockColumnOperation representing lpad

        Example:
            >>> df.select(F.lpad(F.col("id"), 5, "0"))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(
            column,
            "lpad",
            value=(len, pad),
            name=f"lpad({column.name}, {len}, {pad})"
        )

    @staticmethod
    def rpad(
        column: Union[MockColumn, str],
        len: int,
        pad: str
    ) -> MockColumnOperation:
        """Right-pad string column to length len with pad string.

        Args:
            column: Input string column
            len: Target length
            pad: Padding string

        Returns:
            MockColumnOperation representing rpad

        Example:
            >>> df.select(F.rpad(F.col("id"), 5, "0"))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(
            column,
            "rpad",
            value=(len, pad),
            name=f"rpad({column.name}, {len}, {pad})"
        )

    @staticmethod
    def levenshtein(
        left: Union[MockColumn, str],
        right: Union[MockColumn, str]
    ) -> MockColumnOperation:
        """Compute Levenshtein distance between two strings.

        Args:
            left: First string column
            right: Second string column

        Returns:
            MockColumnOperation representing levenshtein

        Example:
            >>> df.select(F.levenshtein(F.col("word1"), F.col("word2")))
        """
        if isinstance(left, str):
            left = MockColumn(left)
        if isinstance(right, str):
            right = MockColumn(right)

        return MockColumnOperation(
            left,
            "levenshtein",
            value=right,
            name=f"levenshtein({left.name}, {right.name})"
        )

    @staticmethod
    def overlay(
        src: Union[MockColumn, str],
        replace: Union[MockColumn, str],
        pos: Union[MockColumn, int],
        len: Union[MockColumn, int] = -1
    ) -> MockColumnOperation:
        """Replace part of a string with another string starting at a position (PySpark 3.0+).

        Args:
            src: Source string column
            replace: Replacement string
            pos: Starting position (1-indexed)
            len: Length to replace (default -1 means to end of string)

        Returns:
            MockColumnOperation for overlay operation

        Example:
            >>> df.select(F.overlay(F.col("text"), F.lit("NEW"), F.lit(5), F.lit(3)))
        """
        if isinstance(src, str):
            src = MockColumn(src)

        return MockColumnOperation(
            src,
            "overlay",
            value=(replace, pos, len),
            name=f"overlay({src.name})"
        )

    @staticmethod
    def bin(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Convert to binary string representation.

        Args:
            column: Numeric column

        Returns:
            MockColumnOperation representing bin
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(column, "bin", name=f"bin({column.name})")

    @staticmethod
    def hex(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Convert to hexadecimal string.

        Args:
            column: Column to convert

        Returns:
            MockColumnOperation representing hex
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(column, "hex", name=f"hex({column.name})")

    @staticmethod
    def unhex(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Convert hex string to binary.

        Args:
            column: Hex string column

        Returns:
            MockColumnOperation representing unhex
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(column, "unhex", name=f"unhex({column.name})")

    @staticmethod
    def hash(*cols: Union[MockColumn, str]) -> MockColumnOperation:
        """Compute hash value of given columns.

        Args:
            *cols: Columns to hash

        Returns:
            MockColumnOperation representing hash
        """
        columns = []
        for col in cols:
            if isinstance(col, str):
                columns.append(MockColumn(col))
            else:
                columns.append(col)

        return MockColumnOperation(
            columns[0] if columns else MockColumn(""),
            "hash",
            value=columns[1:] if len(columns) > 1 else [],
            name="hash(...)"
        )

    @staticmethod
    def xxhash64(*cols: Union[MockColumn, str]) -> MockColumnOperation:
        """Compute xxHash64 value of given columns (all PySpark versions).

        Args:
            *cols: Columns to hash

        Returns:
            MockColumnOperation representing xxhash64
        """
        columns = []
        for col in cols:
            if isinstance(col, str):
                columns.append(MockColumn(col))
            else:
                columns.append(col)

        return MockColumnOperation(
            columns[0] if columns else MockColumn(""),
            "xxhash64",
            value=columns[1:] if len(columns) > 1 else [],
            name="xxhash64(...)"
        )

    @staticmethod
    def encode(column: Union[MockColumn, str], charset: str) -> MockColumnOperation:
        """Encode string to binary using charset.

        Args:
            column: String column
            charset: Character set (e.g., 'UTF-8')

        Returns:
            MockColumnOperation representing encode
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(
            column,
            "encode",
            value=charset,
            name=f"encode({column.name}, {charset})"
        )

    @staticmethod
    def decode(column: Union[MockColumn, str], charset: str) -> MockColumnOperation:
        """Decode binary to string using charset.

        Args:
            column: Binary column
            charset: Character set (e.g., 'UTF-8')

        Returns:
            MockColumnOperation representing decode
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(
            column,
            "decode",
            value=charset,
            name=f"decode({column.name}, {charset})"
        )

    @staticmethod
    def conv(
        column: Union[MockColumn, str],
        from_base: int,
        to_base: int
    ) -> MockColumnOperation:
        """Convert number from one base to another.

        Args:
            column: Number column
            from_base: Source base (2-36)
            to_base: Target base (2-36)

        Returns:
            MockColumnOperation representing conv
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(
            column,
            "conv",
            value=(from_base, to_base),
            name=f"conv({column.name}, {from_base}, {to_base})"
        )

    @staticmethod
    def md5(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Calculate MD5 hash of string (PySpark 3.0+).

        Args:
            column: String column to hash

        Returns:
            MockColumnOperation representing md5 function (returns 32-char hex string)

        Example:
            >>> df.select(F.md5(F.col("text")))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(column, "md5", name=f"md5({column.name})")

    @staticmethod
    def sha1(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Calculate SHA-1 hash of string (PySpark 3.0+).

        Args:
            column: String column to hash

        Returns:
            MockColumnOperation representing sha1 function (returns 40-char hex string)

        Example:
            >>> df.select(F.sha1(F.col("text")))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(column, "sha1", name=f"sha1({column.name})")

    @staticmethod
    def sha2(column: Union[MockColumn, str], numBits: int) -> MockColumnOperation:
        """Calculate SHA-2 family hash (PySpark 3.0+).

        Args:
            column: String column to hash
            numBits: Bit length - 224, 256, 384, or 512

        Returns:
            MockColumnOperation representing sha2 function (returns hex string)

        Example:
            >>> df.select(F.sha2(F.col("text"), 256))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        if numBits not in [224, 256, 384, 512]:
            raise ValueError(f"numBits must be 224, 256, 384, or 512, got {numBits}")

        return MockColumnOperation(
            column,
            "sha2",
            value=numBits,
            name=f"sha2({column.name}, {numBits})"
        )

    @staticmethod
    def crc32(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Calculate CRC32 checksum (PySpark 3.0+).

        Args:
            column: String column to checksum

        Returns:
            MockColumnOperation representing crc32 function (returns signed 32-bit int)

        Example:
            >>> df.select(F.crc32(F.col("text")))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(column, "crc32", name=f"crc32({column.name})")

    @staticmethod
    def to_str(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Convert column to string representation (all PySpark versions).
        
        Args:
            column: Column to convert to string
            
        Returns:
            Column operation for string conversion
            
        Example:
            >>> df.select(F.to_str(F.col("value")))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(column, "to_str", name=f"to_str({column.name})")
