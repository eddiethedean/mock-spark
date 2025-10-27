"""
Core functions module for Mock Spark.

This module provides the main F namespace and re-exports all function classes
for backward compatibility with the original functions.py structure. The MockFunctions
class serves as the primary interface for all PySpark-compatible functions.

Key Features:
    - Complete PySpark F namespace compatibility
    - Column functions (col, lit, when, coalesce, isnull)
    - String functions (upper, lower, length, trim, regexp_replace, split)
    - Math functions (abs, round, ceil, floor, sqrt, exp, log, pow, sin, cos, tan)
    - Aggregate functions (count, sum, avg, max, min, stddev, variance)
    - DateTime functions (current_timestamp, current_date, to_date, to_timestamp)
    - Window functions (row_number, rank, dense_rank, lag, lead)

Example:
    >>> from mock_spark import MockSparkSession, F
    >>> spark = MockSparkSession("test")
    >>> data = [{"name": "Alice", "age": 25}]
    >>> df = spark.createDataFrame(data)
    >>> df.select(F.upper(F.col("name")), F.col("age") * 2).show()
    +--- MockDataFrame: 1 rows ---+
     upper(name) |    (age * 2)
    ---------------------------
           ALICE |           50
"""

from typing import Any, Optional, Union, Callable, Tuple, Dict
from .core.column import MockColumn, MockColumnOperation
from .core.literals import MockLiteral
from .base import MockAggregateFunction
from .conditional import MockCaseWhen, ConditionalFunctions
from .window_execution import MockWindowFunction
from .string import StringFunctions
from .math import MathFunctions
from .aggregate import AggregateFunctions
from .datetime import DateTimeFunctions
from .array import ArrayFunctions
from .map import MapFunctions
from .bitwise import BitwiseFunctions
from .xml import XMLFunctions


class MockFunctions:
    """Main functions namespace (F) for Mock Spark.

    This class provides access to all functions in a PySpark-compatible way.
    """

    # Column functions
    @staticmethod
    def col(name: str) -> MockColumn:
        """Create a column reference."""
        return MockColumn(name)

    @staticmethod
    def lit(value: Any) -> MockLiteral:
        """Create a literal value."""
        return MockLiteral(value)

    # String functions
    @staticmethod
    def upper(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Convert string to uppercase."""
        return StringFunctions.upper(column)

    @staticmethod
    def lower(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Convert string to lowercase."""
        return StringFunctions.lower(column)

    @staticmethod
    def length(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Get string length."""
        return StringFunctions.length(column)

    @staticmethod
    def char_length(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Get character length (alias for length) (PySpark 3.5+)."""
        return StringFunctions.char_length(column)

    @staticmethod
    def character_length(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Get character length (alias for length) (PySpark 3.5+)."""
        return StringFunctions.character_length(column)

    @staticmethod
    def trim(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Trim whitespace."""
        return StringFunctions.trim(column)

    @staticmethod
    def ltrim(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Trim left whitespace."""
        return StringFunctions.ltrim(column)

    @staticmethod
    def rtrim(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Trim right whitespace."""
        return StringFunctions.rtrim(column)

    @staticmethod
    def regexp_replace(
        column: Union[MockColumn, str], pattern: str, replacement: str
    ) -> MockColumnOperation:
        """Replace regex pattern."""
        return StringFunctions.regexp_replace(column, pattern, replacement)

    @staticmethod
    def split(column: Union[MockColumn, str], delimiter: str) -> MockColumnOperation:
        """Split string by delimiter."""
        return StringFunctions.split(column, delimiter)

    @staticmethod
    def substring(
        column: Union[MockColumn, str], start: int, length: Optional[int] = None
    ) -> MockColumnOperation:
        """Extract substring."""
        return StringFunctions.substring(column, start, length)

    @staticmethod
    def concat(*columns: Union[MockColumn, str]) -> MockColumnOperation:
        """Concatenate strings."""
        return StringFunctions.concat(*columns)

    @staticmethod
    def format_string(
        format_str: str, *columns: Union[MockColumn, str]
    ) -> MockColumnOperation:
        """Format string using printf-style placeholders."""
        return StringFunctions.format_string(format_str, *columns)

    @staticmethod
    def translate(
        column: Union[MockColumn, str], matching_string: str, replace_string: str
    ) -> MockColumnOperation:
        """Translate characters in a string using a character mapping."""
        return StringFunctions.translate(column, matching_string, replace_string)

    @staticmethod
    def ascii(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Return ASCII value of the first character."""
        return StringFunctions.ascii(column)

    @staticmethod
    def base64(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Encode the string to base64."""
        return StringFunctions.base64(column)

    @staticmethod
    def unbase64(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Decode a base64-encoded string."""
        return StringFunctions.unbase64(column)

    @staticmethod
    def md5(column: Union[MockColumn, str]) -> MockColumnOperation:
        """MD5 hash (PySpark 3.0+)."""
        return StringFunctions.md5(column)

    @staticmethod
    def sha1(column: Union[MockColumn, str]) -> MockColumnOperation:
        """SHA-1 hash (PySpark 3.0+)."""
        return StringFunctions.sha1(column)

    @staticmethod
    def sha2(column: Union[MockColumn, str], numBits: int) -> MockColumnOperation:
        """SHA-2 hash family (PySpark 3.0+)."""
        return StringFunctions.sha2(column, numBits)

    @staticmethod
    def crc32(column: Union[MockColumn, str]) -> MockColumnOperation:
        """CRC32 checksum (PySpark 3.0+)."""
        return StringFunctions.crc32(column)

    @staticmethod
    def to_str(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Convert column to string (all PySpark versions)."""
        return StringFunctions.to_str(column)

    @staticmethod
    def regexp_extract_all(
        column: Union[MockColumn, str], pattern: str, idx: int = 0
    ) -> MockColumnOperation:
        """Extract all matches of a regex pattern."""
        return StringFunctions.regexp_extract_all(column, pattern, idx)

    @staticmethod
    def array_join(
        column: Union[MockColumn, str],
        delimiter: str,
        null_replacement: Optional[str] = None,
    ) -> MockColumnOperation:
        """Join array elements with a delimiter."""
        return StringFunctions.array_join(column, delimiter, null_replacement)

    @staticmethod
    def repeat(column: Union[MockColumn, str], n: int) -> MockColumnOperation:
        """Repeat a string N times."""
        return StringFunctions.repeat(column, n)

    @staticmethod
    def concat_ws(sep: str, *cols: Union[MockColumn, str]) -> MockColumnOperation:
        """Concatenate multiple columns with separator."""
        return StringFunctions.concat_ws(sep, *cols)

    @staticmethod
    def regexp_extract(
        column: Union[MockColumn, str], pattern: str, idx: int = 0
    ) -> MockColumnOperation:
        """Extract specific group matched by regex."""
        return StringFunctions.regexp_extract(column, pattern, idx)

    @staticmethod
    def substring_index(
        column: Union[MockColumn, str], delim: str, count: int
    ) -> MockColumnOperation:
        """Returns substring before/after count occurrences of delimiter."""
        return StringFunctions.substring_index(column, delim, count)

    @staticmethod
    def format_number(column: Union[MockColumn, str], d: int) -> MockColumnOperation:
        """Format number with d decimal places and thousands separator."""
        return StringFunctions.format_number(column, d)

    @staticmethod
    def instr(column: Union[MockColumn, str], substr: str) -> MockColumnOperation:
        """Locate position of first occurrence of substr."""
        return StringFunctions.instr(column, substr)

    @staticmethod
    def locate(
        substr: str, column: Union[MockColumn, str], pos: int = 1
    ) -> MockColumnOperation:
        """Locate position of substr starting from pos."""
        return StringFunctions.locate(substr, column, pos)

    @staticmethod
    def lpad(column: Union[MockColumn, str], len: int, pad: str) -> MockColumnOperation:
        """Left-pad string to length len with pad string."""
        return StringFunctions.lpad(column, len, pad)

    @staticmethod
    def rpad(column: Union[MockColumn, str], len: int, pad: str) -> MockColumnOperation:
        """Right-pad string to length len with pad string."""
        return StringFunctions.rpad(column, len, pad)

    @staticmethod
    def levenshtein(
        left: Union[MockColumn, str], right: Union[MockColumn, str]
    ) -> MockColumnOperation:
        """Compute Levenshtein distance between two strings."""
        return StringFunctions.levenshtein(left, right)

    @staticmethod
    def bin(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Convert to binary string."""
        return StringFunctions.bin(column)

    @staticmethod
    def hex(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Convert to hexadecimal string."""
        return StringFunctions.hex(column)

    @staticmethod
    def unhex(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Convert hex string to binary."""
        return StringFunctions.unhex(column)

    @staticmethod
    def hash(*cols: Union[MockColumn, str]) -> MockColumnOperation:
        """Compute hash value."""
        return StringFunctions.hash(*cols)

    @staticmethod
    def xxhash64(*cols: Union[MockColumn, str]) -> MockColumnOperation:
        """Compute xxHash64 value (all PySpark versions)."""
        return StringFunctions.xxhash64(*cols)

    @staticmethod
    def encode(column: Union[MockColumn, str], charset: str) -> MockColumnOperation:
        """Encode string to binary."""
        return StringFunctions.encode(column, charset)

    @staticmethod
    def decode(column: Union[MockColumn, str], charset: str) -> MockColumnOperation:
        """Decode binary to string."""
        return StringFunctions.decode(column, charset)

    @staticmethod
    def conv(
        column: Union[MockColumn, str], from_base: int, to_base: int
    ) -> MockColumnOperation:
        """Convert number between bases."""
        return StringFunctions.conv(column, from_base, to_base)

    @staticmethod
    def initcap(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Capitalize first letter of each word."""
        return StringFunctions.initcap(column)

    @staticmethod
    def soundex(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Soundex encoding for phonetic matching."""
        return StringFunctions.soundex(column)

    # Math functions
    @staticmethod
    def abs(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Get absolute value."""
        return MathFunctions.abs(column)

    @staticmethod
    def round(column: Union[MockColumn, str], scale: int = 0) -> MockColumnOperation:
        """Round to decimal places."""
        return MathFunctions.round(column, scale)

    @staticmethod
    def ceil(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Round up."""
        return MathFunctions.ceil(column)

    @staticmethod
    def floor(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Round down."""
        return MathFunctions.floor(column)

    @staticmethod
    def sqrt(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Square root."""
        return MathFunctions.sqrt(column)

    @staticmethod
    def exp(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Exponential."""
        return MathFunctions.exp(column)

    @staticmethod
    def log(
        column: Union[MockColumn, str], base: Optional[float] = None
    ) -> MockColumnOperation:
        """Logarithm."""
        return MathFunctions.log(column, base)

    @staticmethod
    def log10(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Base-10 logarithm (PySpark 3.0+)."""
        return MathFunctions.log10(column)

    @staticmethod
    def log2(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Base-2 logarithm (PySpark 3.0+)."""
        return MathFunctions.log2(column)

    @staticmethod
    def log1p(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Natural log of (1 + x) (PySpark 3.0+)."""
        return MathFunctions.log1p(column)

    @staticmethod
    def expm1(column: Union[MockColumn, str]) -> MockColumnOperation:
        """exp(x) - 1 (PySpark 3.0+)."""
        return MathFunctions.expm1(column)

    @staticmethod
    def pow(
        column: Union[MockColumn, str], exponent: Union[MockColumn, float, int]
    ) -> MockColumnOperation:
        """Power."""
        return MathFunctions.pow(column, exponent)

    @staticmethod
    def sin(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Sine."""
        return MathFunctions.sin(column)

    @staticmethod
    def cos(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Cosine."""
        return MathFunctions.cos(column)

    @staticmethod
    def tan(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Tangent."""
        return MathFunctions.tan(column)

    @staticmethod
    def acosh(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Inverse hyperbolic cosine (PySpark 3.0+)."""
        return MathFunctions.acosh(column)

    @staticmethod
    def asinh(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Inverse hyperbolic sine (PySpark 3.0+)."""
        return MathFunctions.asinh(column)

    @staticmethod
    def atanh(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Inverse hyperbolic tangent (PySpark 3.0+)."""
        return MathFunctions.atanh(column)

    @staticmethod
    def acos(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Inverse cosine (arc cosine)."""
        return MathFunctions.acos(column)

    @staticmethod
    def asin(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Inverse sine (arc sine)."""
        return MathFunctions.asin(column)

    @staticmethod
    def atan(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Inverse tangent (arc tangent)."""
        return MathFunctions.atan(column)

    @staticmethod
    def atan2(
        y: Union[MockColumn, str, float, int], x: Union[MockColumn, str, float, int]
    ) -> MockColumnOperation:
        """2-argument arctangent (PySpark 3.0+)."""
        return MathFunctions.atan2(y, x)

    @staticmethod
    def cosh(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Hyperbolic cosine."""
        return MathFunctions.cosh(column)

    @staticmethod
    def sinh(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Hyperbolic sine."""
        return MathFunctions.sinh(column)

    @staticmethod
    def tanh(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Hyperbolic tangent."""
        return MathFunctions.tanh(column)

    @staticmethod
    def degrees(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Convert radians to degrees."""
        return MathFunctions.degrees(column)

    @staticmethod
    def radians(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Convert degrees to radians."""
        return MathFunctions.radians(column)

    @staticmethod
    def cbrt(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Cube root."""
        return MathFunctions.cbrt(column)

    @staticmethod
    def factorial(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Factorial of non-negative integer."""
        return MathFunctions.factorial(column)

    @staticmethod
    def rand(seed: Optional[int] = None) -> MockColumnOperation:
        """Generate random column with uniform distribution [0.0, 1.0]."""
        return MathFunctions.rand(seed)

    @staticmethod
    def randn(seed: Optional[int] = None) -> MockColumnOperation:
        """Generate random column with standard normal distribution."""
        return MathFunctions.randn(seed)

    @staticmethod
    def rint(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Round to nearest integer using banker's rounding."""
        return MathFunctions.rint(column)

    @staticmethod
    def bround(column: Union[MockColumn, str], scale: int = 0) -> MockColumnOperation:
        """Round using HALF_EVEN rounding mode."""
        return MathFunctions.bround(column, scale)

    @staticmethod
    def sign(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Sign of number (matches PySpark signum)."""
        return MathFunctions.sign(column)

    @staticmethod
    def hypot(
        col1: Union[MockColumn, str], col2: Union[MockColumn, str]
    ) -> MockColumnOperation:
        """Compute hypotenuse."""
        return MathFunctions.hypot(col1, col2)

    @staticmethod
    def nanvl(
        col1: Union[MockColumn, str], col2: Union[MockColumn, str]
    ) -> MockColumnOperation:
        """Return col1 if not NaN, else col2."""
        return MathFunctions.nanvl(col1, col2)

    @staticmethod
    def signum(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Compute signum (sign)."""
        return MathFunctions.signum(column)

    @staticmethod
    def cot(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Compute cotangent (PySpark 3.3+)."""
        return MathFunctions.cot(column)

    @staticmethod
    def csc(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Compute cosecant (PySpark 3.3+)."""
        return MathFunctions.csc(column)

    @staticmethod
    def sec(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Compute secant (PySpark 3.3+)."""
        return MathFunctions.sec(column)

    @staticmethod
    def e() -> MockColumnOperation:
        """Euler's number e (PySpark 3.5+)."""
        return MathFunctions.e()

    @staticmethod
    def pi() -> MockColumnOperation:
        """Pi constant (PySpark 3.5+)."""
        return MathFunctions.pi()

    @staticmethod
    def ln(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Natural logarithm (PySpark 3.5+)."""
        return MathFunctions.ln(column)

    @staticmethod
    def greatest(*columns: Union[MockColumn, str]) -> MockColumnOperation:
        """Greatest value among columns."""
        return MathFunctions.greatest(*columns)

    @staticmethod
    def least(*columns: Union[MockColumn, str]) -> MockColumnOperation:
        """Least value among columns."""
        return MathFunctions.least(*columns)

    # Aggregate functions
    @staticmethod
    def count(column: Union[MockColumn, str, None] = None) -> MockAggregateFunction:
        """Count values."""
        return AggregateFunctions.count(column)

    @staticmethod
    def sum(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Sum values."""
        return AggregateFunctions.sum(column)

    @staticmethod
    def avg(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Average values."""
        return AggregateFunctions.avg(column)

    @staticmethod
    def max(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Maximum value."""
        return AggregateFunctions.max(column)

    @staticmethod
    def min(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Minimum value."""
        return AggregateFunctions.min(column)

    @staticmethod
    def first(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """First value."""
        return AggregateFunctions.first(column)

    @staticmethod
    def last(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Last value."""
        return AggregateFunctions.last(column)

    @staticmethod
    def collect_list(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Collect values into list."""
        return AggregateFunctions.collect_list(column)

    @staticmethod
    def collect_set(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Collect unique values into set."""
        return AggregateFunctions.collect_set(column)

    @staticmethod
    def stddev(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Standard deviation."""
        return AggregateFunctions.stddev(column)

    @staticmethod
    def variance(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Variance."""
        return AggregateFunctions.variance(column)

    @staticmethod
    def skewness(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Skewness."""
        return AggregateFunctions.skewness(column)

    @staticmethod
    def kurtosis(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Kurtosis."""
        return AggregateFunctions.kurtosis(column)

    @staticmethod
    def countDistinct(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Count distinct values."""
        return AggregateFunctions.countDistinct(column)

    @staticmethod
    def percentile_approx(
        column: Union[MockColumn, str], percentage: float, accuracy: int = 10000
    ) -> MockAggregateFunction:
        """Approximate percentile."""
        return AggregateFunctions.percentile_approx(column, percentage, accuracy)

    @staticmethod
    def corr(
        column1: Union[MockColumn, str], column2: Union[MockColumn, str]
    ) -> MockAggregateFunction:
        """Correlation between two columns."""
        return AggregateFunctions.corr(column1, column2)

    @staticmethod
    def covar_samp(
        column1: Union[MockColumn, str], column2: Union[MockColumn, str]
    ) -> MockAggregateFunction:
        """Sample covariance between two columns."""
        return AggregateFunctions.covar_samp(column1, column2)

    @staticmethod
    def mean(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Mean of values (alias for avg)."""
        return AggregateFunctions.mean(column)

    @staticmethod
    def approx_count_distinct(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Approximate count of distinct elements."""
        return AggregateFunctions.approx_count_distinct(column)

    @staticmethod
    def stddev_pop(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Population standard deviation."""
        return AggregateFunctions.stddev_pop(column)

    @staticmethod
    def stddev_samp(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Sample standard deviation."""
        return AggregateFunctions.stddev_samp(column)

    @staticmethod
    def var_pop(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Population variance."""
        return AggregateFunctions.var_pop(column)

    @staticmethod
    def var_samp(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Sample variance."""
        return AggregateFunctions.var_samp(column)

    @staticmethod
    def covar_pop(
        column1: Union[MockColumn, str], column2: Union[MockColumn, str]
    ) -> MockAggregateFunction:
        """Population covariance."""
        return AggregateFunctions.covar_pop(column1, column2)

    @staticmethod
    def median(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Median value (PySpark 3.4+)."""
        return AggregateFunctions.median(column)

    @staticmethod
    def mode(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Most frequent value (PySpark 3.4+)."""
        return AggregateFunctions.mode(column)

    @staticmethod
    def percentile(
        column: Union[MockColumn, str], percentage: float
    ) -> MockAggregateFunction:
        """Exact percentile (PySpark 3.5+)."""
        return AggregateFunctions.percentile(column, percentage)

    @staticmethod
    def bool_and(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Aggregate AND (PySpark 3.1+)."""
        return AggregateFunctions.bool_and(column)

    @staticmethod
    def bool_or(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Aggregate OR (PySpark 3.1+)."""
        return AggregateFunctions.bool_or(column)

    @staticmethod
    def every(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Alias for bool_and (PySpark 3.1+)."""
        return AggregateFunctions.every(column)

    @staticmethod
    def some(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Alias for bool_or (PySpark 3.1+)."""
        return AggregateFunctions.some(column)

    @staticmethod
    def max_by(
        column: Union[MockColumn, str], ord: Union[MockColumn, str]
    ) -> MockAggregateFunction:
        """Value with max of ord column (PySpark 3.1+)."""
        return AggregateFunctions.max_by(column, ord)

    @staticmethod
    def min_by(
        column: Union[MockColumn, str], ord: Union[MockColumn, str]
    ) -> MockAggregateFunction:
        """Value with min of ord column (PySpark 3.1+)."""
        return AggregateFunctions.min_by(column, ord)

    @staticmethod
    def count_if(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Count where condition is true (PySpark 3.1+)."""
        return AggregateFunctions.count_if(column)

    @staticmethod
    def any_value(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Return any non-null value (PySpark 3.1+)."""
        return AggregateFunctions.any_value(column)

    # Datetime functions
    @staticmethod
    def current_timestamp() -> MockColumnOperation:
        """Current timestamp."""
        return DateTimeFunctions.current_timestamp()

    @staticmethod
    def current_date() -> MockColumnOperation:
        """Current date."""
        return DateTimeFunctions.current_date()

    @staticmethod
    def version() -> MockLiteral:
        """Return Spark version string (PySpark 3.0+).

        Returns:
            MockLiteral with mock-spark version
        """
        from mock_spark import __version__

        # Return mock-spark version as a constant expression
        return MockLiteral(f"mock-spark-{__version__}")

    @staticmethod
    def to_date(
        column: Union[MockColumn, str], format: Optional[str] = None
    ) -> MockColumnOperation:
        """Convert to date."""
        return DateTimeFunctions.to_date(column, format)

    @staticmethod
    def to_timestamp(
        column: Union[MockColumn, str], format: Optional[str] = None
    ) -> MockColumnOperation:
        """Convert to timestamp."""
        return DateTimeFunctions.to_timestamp(column, format)

    @staticmethod
    def hour(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Extract hour."""
        return DateTimeFunctions.hour(column)

    @staticmethod
    def day(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Extract day."""
        return DateTimeFunctions.day(column)

    @staticmethod
    def dayofmonth(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Extract day of month (alias for day)."""
        return DateTimeFunctions.dayofmonth(column)

    @staticmethod
    def month(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Extract month."""
        return DateTimeFunctions.month(column)

    @staticmethod
    def year(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Extract year."""
        return DateTimeFunctions.year(column)

    # Conditional functions
    @staticmethod
    def coalesce(*columns: Union[MockColumn, str, Any]) -> MockColumnOperation:
        """Return first non-null value."""
        return ConditionalFunctions.coalesce(*columns)

    @staticmethod
    def isnull(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Check if column is null."""
        return ConditionalFunctions.isnull(column)

    @staticmethod
    def isnotnull(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Check if column is not null."""
        return ConditionalFunctions.isnotnull(column)

    @staticmethod
    def isnan(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Check if column is NaN."""
        return ConditionalFunctions.isnan(column)

    @staticmethod
    def when(condition: Any, value: Any = None) -> MockCaseWhen:
        """Start CASE WHEN expression."""
        if value is not None:
            return ConditionalFunctions.when(condition, value)
        return ConditionalFunctions.when(condition)

    @staticmethod
    def case_when(*conditions: Tuple[Any, Any], else_value: Any = None) -> MockCaseWhen:
        """Create CASE WHEN expression with multiple conditions."""
        return ConditionalFunctions.case_when(*conditions, else_value=else_value)

    @staticmethod
    def dayofweek(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Extract day of week."""
        return DateTimeFunctions.dayofweek(column)

    @staticmethod
    def dayofyear(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Extract day of year."""
        return DateTimeFunctions.dayofyear(column)

    @staticmethod
    def weekofyear(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Extract week of year."""
        return DateTimeFunctions.weekofyear(column)

    @staticmethod
    def quarter(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Extract quarter."""
        return DateTimeFunctions.quarter(column)

    # SQL expression function
    @staticmethod
    def expr(expression: str) -> MockColumnOperation:
        """Parse SQL expression into a column (simplified mock)."""
        # Represent as a column operation on a dummy column
        from mock_spark.functions.base import MockColumn

        dummy = MockColumn("__expr__")
        operation = MockColumnOperation(dummy, "expr", expression, name=expression)
        operation.function_name = "expr"
        return operation

    @staticmethod
    def minute(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Extract minute."""
        return DateTimeFunctions.minute(column)

    @staticmethod
    def second(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Extract second."""
        return DateTimeFunctions.second(column)

    @staticmethod
    def add_months(
        column: Union[MockColumn, str], num_months: int
    ) -> MockColumnOperation:
        """Add months to date."""
        return DateTimeFunctions.add_months(column, num_months)

    @staticmethod
    def months_between(
        column1: Union[MockColumn, str], column2: Union[MockColumn, str]
    ) -> MockColumnOperation:
        """Calculate months between two dates."""
        return DateTimeFunctions.months_between(column1, column2)

    @staticmethod
    def date_add(column: Union[MockColumn, str], days: int) -> MockColumnOperation:
        """Add days to date."""
        return DateTimeFunctions.date_add(column, days)

    @staticmethod
    def date_sub(column: Union[MockColumn, str], days: int) -> MockColumnOperation:
        """Subtract days from date."""
        return DateTimeFunctions.date_sub(column, days)

    @staticmethod
    def date_format(column: Union[MockColumn, str], format: str) -> MockColumnOperation:
        """Format date/timestamp as string."""
        return DateTimeFunctions.date_format(column, format)

    @staticmethod
    def make_date(
        year: Union[MockColumn, int],
        month: Union[MockColumn, int],
        day: Union[MockColumn, int],
    ) -> MockColumnOperation:
        """Construct date from year, month, day (PySpark 3.0+)."""
        return DateTimeFunctions.make_date(year, month, day)

    @staticmethod
    def date_trunc(
        format: str, timestamp: Union[MockColumn, str]
    ) -> MockColumnOperation:
        """Truncate timestamp to specified unit."""
        return DateTimeFunctions.date_trunc(format, timestamp)

    @staticmethod
    def datediff(
        end: Union[MockColumn, str], start: Union[MockColumn, str]
    ) -> MockColumnOperation:
        """Number of days between two dates."""
        return DateTimeFunctions.datediff(end, start)

    @staticmethod
    def unix_timestamp(
        timestamp: Optional[Union[MockColumn, str]] = None,
        format: str = "yyyy-MM-dd HH:mm:ss",
    ) -> MockColumnOperation:
        """Convert timestamp to Unix timestamp."""
        return DateTimeFunctions.unix_timestamp(timestamp, format)

    @staticmethod
    def last_day(date: Union[MockColumn, str]) -> MockColumnOperation:
        """Last day of the month for given date."""
        return DateTimeFunctions.last_day(date)

    @staticmethod
    def next_day(date: Union[MockColumn, str], dayOfWeek: str) -> MockColumnOperation:
        """First date later than date on specified day of week."""
        return DateTimeFunctions.next_day(date, dayOfWeek)

    @staticmethod
    def trunc(date: Union[MockColumn, str], format: str) -> MockColumnOperation:
        """Truncate date to specified unit."""
        return DateTimeFunctions.trunc(date, format)

    @staticmethod
    def timestamp_seconds(col: Union[MockColumn, str, int]) -> MockColumnOperation:
        """Convert seconds since epoch to timestamp (PySpark 3.1+)."""
        return DateTimeFunctions.timestamp_seconds(col)

    @staticmethod
    def weekday(col: Union[MockColumn, str]) -> MockColumnOperation:
        """Day of week as integer (0=Monday, 6=Sunday) (PySpark 3.5+)."""
        return DateTimeFunctions.weekday(col)

    @staticmethod
    def extract(field: str, source: Union[MockColumn, str]) -> MockColumnOperation:
        """Extract field from date/timestamp (PySpark 3.5+)."""
        return DateTimeFunctions.extract(field, source)

    @staticmethod
    def raise_error(msg: Union[MockColumn, str]) -> MockColumnOperation:
        """Raise an error with the specified message (PySpark 3.1+).

        Args:
            msg: Error message

        Returns:
            MockColumnOperation representing the raise_error function
        """
        if isinstance(msg, str):
            from mock_spark.functions.core.literals import MockLiteral

            msg = MockLiteral(msg)  # type: ignore[assignment]

        return MockColumnOperation(
            msg,
            "raise_error",
            name=f"raise_error({msg})",
        )

    @staticmethod
    def from_unixtime(
        column: Union[MockColumn, str], format: str = "yyyy-MM-dd HH:mm:ss"
    ) -> MockColumnOperation:
        """Convert unix timestamp to string."""
        return DateTimeFunctions.from_unixtime(column, format)

    @staticmethod
    def timestampadd(
        unit: str, quantity: Union[int, MockColumn], timestamp: Union[str, MockColumn]
    ) -> MockColumnOperation:
        """Add time units to a timestamp."""
        return DateTimeFunctions.timestampadd(unit, quantity, timestamp)

    @staticmethod
    def timestampdiff(
        unit: str, start: Union[str, MockColumn], end: Union[str, MockColumn]
    ) -> MockColumnOperation:
        """Calculate difference between two timestamps."""
        return DateTimeFunctions.timestampdiff(unit, start, end)

    @staticmethod
    def nvl(column: Union[MockColumn, str], default_value: Any) -> MockColumnOperation:
        """Return default if null. PySpark uses coalesce internally."""
        # Use coalesce for SQL generation compatibility
        from .conditional import ConditionalFunctions

        return ConditionalFunctions.coalesce(column, default_value)

    @staticmethod
    def nvl2(
        column: Union[MockColumn, str], value_if_not_null: Any, value_if_null: Any
    ) -> Any:
        """Return value based on null check. PySpark uses when/otherwise internally."""
        # Use when/otherwise for SQL generation compatibility
        from .conditional import ConditionalFunctions
        from mock_spark.functions.base import MockColumn

        # Convert string to MockColumn if needed
        col = MockColumn(column) if isinstance(column, str) else column

        # nvl2 should check if column IS NULL, not if column is truthy
        return ConditionalFunctions.when(col.isNull(), value_if_null).otherwise(
            value_if_not_null
        )

    # Window functions
    @staticmethod
    def row_number() -> MockColumnOperation:
        """Row number window function."""
        # Create a special column for functions without input
        from mock_spark.functions.base import MockColumn
        from mock_spark.spark_types import IntegerType

        dummy_column = MockColumn("__row_number__")
        operation = MockColumnOperation(dummy_column, "row_number")
        operation.name = "row_number()"
        operation.function_name = "row_number"
        operation.return_type = IntegerType(nullable=False)
        return operation

    @staticmethod
    def rank() -> MockColumnOperation:
        """Rank window function."""
        # Create a special column for functions without input
        from mock_spark.functions.base import MockColumn

        dummy_column = MockColumn("__rank__")
        operation = MockColumnOperation(dummy_column, "rank")
        operation.name = "rank()"
        operation.function_name = "rank"
        return operation

    @staticmethod
    def dense_rank() -> MockColumnOperation:
        """Dense rank window function."""
        # Create a special column for functions without input
        from mock_spark.functions.base import MockColumn

        dummy_column = MockColumn("__dense_rank__")
        operation = MockColumnOperation(dummy_column, "dense_rank")
        operation.name = "dense_rank()"
        operation.function_name = "dense_rank"
        return operation

    @staticmethod
    def lag(
        column: Union[MockColumn, str], offset: int = 1, default_value: Any = None
    ) -> MockColumnOperation:
        """Lag window function."""
        if isinstance(column, str):
            column = MockColumn(column)

        operation = MockColumnOperation(column, "lag", (offset, default_value))
        operation.name = f"lag({column.name}, {offset})"
        operation.function_name = "lag"
        return operation

    @staticmethod
    def lead(
        column: Union[MockColumn, str], offset: int = 1, default_value: Any = None
    ) -> MockColumnOperation:
        """Lead window function."""
        if isinstance(column, str):
            column = MockColumn(column)

        operation = MockColumnOperation(column, "lead", (offset, default_value))
        operation.name = f"lead({column.name}, {offset})"
        operation.function_name = "lead"
        return operation

    @staticmethod
    def nth_value(column: Union[MockColumn, str], n: int) -> MockColumnOperation:
        """Nth value window function."""
        if isinstance(column, str):
            column = MockColumn(column)

        operation = MockColumnOperation(column, "nth_value", n)
        operation.name = f"nth_value({column.name}, {n})"
        operation.function_name = "nth_value"
        return operation

    @staticmethod
    def ntile(n: int) -> MockColumnOperation:
        """NTILE window function."""
        from mock_spark.functions.base import MockColumn

        dummy_column = MockColumn("__ntile__")
        operation = MockColumnOperation(dummy_column, "ntile", n)
        operation.name = f"ntile({n})"
        operation.function_name = "ntile"
        return operation

    @staticmethod
    def cume_dist() -> MockColumnOperation:
        """Cumulative distribution window function."""
        from mock_spark.functions.base import MockColumn

        dummy_column = MockColumn("__cume_dist__")
        operation = MockColumnOperation(dummy_column, "cume_dist")
        operation.name = "cume_dist()"
        operation.function_name = "cume_dist"
        return operation

    @staticmethod
    def percent_rank() -> MockColumnOperation:
        """Percent rank window function."""
        from mock_spark.functions.base import MockColumn

        dummy_column = MockColumn("__percent_rank__")
        operation = MockColumnOperation(dummy_column, "percent_rank")
        operation.name = "percent_rank()"
        operation.function_name = "percent_rank"
        return operation

    @staticmethod
    def desc(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Create descending order column."""
        if isinstance(column, str):
            column = MockColumn(column)

        operation = MockColumnOperation(
            column, "desc", None, name=f"{column.name} DESC"
        )
        operation.function_name = "desc"
        return operation

    # Array functions
    @staticmethod
    def array(*cols: Union[MockColumn, str]) -> MockColumnOperation:
        """Create array from columns (PySpark 3.0+)."""
        return ArrayFunctions.array(*cols)

    @staticmethod
    def array_repeat(col: Union[MockColumn, str], count: int) -> MockColumnOperation:
        """Repeat value to create array (PySpark 3.0+)."""
        return ArrayFunctions.array_repeat(col, count)

    @staticmethod
    def sort_array(
        col: Union[MockColumn, str], asc: bool = True
    ) -> MockColumnOperation:
        """Sort array elements (PySpark 3.0+)."""
        return ArrayFunctions.sort_array(col, asc)

    @staticmethod
    def array_agg(col: Union[MockColumn, str]) -> MockAggregateFunction:
        """Aggregate values into array (PySpark 3.5+)."""
        return ArrayFunctions.array_agg(col)

    @staticmethod
    def cardinality(col: Union[MockColumn, str]) -> MockColumnOperation:
        """Return size of array or map (PySpark 3.5+)."""
        return ArrayFunctions.cardinality(col)

    @staticmethod
    def array_distinct(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Remove duplicate elements from array."""
        return ArrayFunctions.array_distinct(column)

    @staticmethod
    def array_intersect(
        column1: Union[MockColumn, str], column2: Union[MockColumn, str]
    ) -> MockColumnOperation:
        """Intersection of two arrays."""
        return ArrayFunctions.array_intersect(column1, column2)

    @staticmethod
    def array_union(
        column1: Union[MockColumn, str], column2: Union[MockColumn, str]
    ) -> MockColumnOperation:
        """Union of two arrays."""
        return ArrayFunctions.array_union(column1, column2)

    @staticmethod
    def array_except(
        column1: Union[MockColumn, str], column2: Union[MockColumn, str]
    ) -> MockColumnOperation:
        """Elements in first array but not second."""
        return ArrayFunctions.array_except(column1, column2)

    @staticmethod
    def array_position(
        column: Union[MockColumn, str], value: Any
    ) -> MockColumnOperation:
        """Position of element in array."""
        return ArrayFunctions.array_position(column, value)

    @staticmethod
    def array_remove(column: Union[MockColumn, str], value: Any) -> MockColumnOperation:
        """Remove all occurrences of element from array."""
        return ArrayFunctions.array_remove(column, value)

    # Higher-order array functions (PySpark 3.2+)
    @staticmethod
    def transform(
        column: Union[MockColumn, str], function: Callable[[Any], Any]
    ) -> MockColumnOperation:
        """Apply function to each array element."""
        return ArrayFunctions.transform(column, function)

    @staticmethod
    def filter(
        column: Union[MockColumn, str], function: Callable[[Any], bool]
    ) -> MockColumnOperation:
        """Filter array elements with predicate."""
        return ArrayFunctions.filter(column, function)

    @staticmethod
    def exists(
        column: Union[MockColumn, str], function: Callable[[Any], bool]
    ) -> MockColumnOperation:
        """Check if any element satisfies predicate."""
        return ArrayFunctions.exists(column, function)

    @staticmethod
    def forall(
        column: Union[MockColumn, str], function: Callable[[Any], bool]
    ) -> MockColumnOperation:
        """Check if all elements satisfy predicate."""
        return ArrayFunctions.forall(column, function)

    @staticmethod
    def aggregate(
        column: Union[MockColumn, str],
        initial_value: Any,
        merge: Callable[[Any, Any], Any],
        finish: Optional[Callable[[Any], Any]] = None,
    ) -> MockColumnOperation:
        """Aggregate array elements to single value."""
        return ArrayFunctions.aggregate(column, initial_value, merge, finish)

    @staticmethod
    def zip_with(
        left: Union[MockColumn, str],
        right: Union[MockColumn, str],
        function: Callable[[Any, Any], Any],
    ) -> MockColumnOperation:
        """Merge two arrays element-wise."""
        return ArrayFunctions.zip_with(left, right, function)

    # Basic array functions (PySpark 3.2+)
    @staticmethod
    def array_compact(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Remove null values from array."""
        return ArrayFunctions.array_compact(column)

    @staticmethod
    def slice(
        column: Union[MockColumn, str], start: int, length: int
    ) -> MockColumnOperation:
        """Extract array slice."""
        return ArrayFunctions.slice(column, start, length)

    @staticmethod
    def element_at(column: Union[MockColumn, str], index: int) -> MockColumnOperation:
        """Get element at index."""
        return ArrayFunctions.element_at(column, index)

    @staticmethod
    def array_append(
        column: Union[MockColumn, str], element: Any
    ) -> MockColumnOperation:
        """Append element to array."""
        return ArrayFunctions.array_append(column, element)

    @staticmethod
    def array_prepend(
        column: Union[MockColumn, str], element: Any
    ) -> MockColumnOperation:
        """Prepend element to array."""
        return ArrayFunctions.array_prepend(column, element)

    @staticmethod
    def array_insert(
        column: Union[MockColumn, str], pos: int, value: Any
    ) -> MockColumnOperation:
        """Insert element at position."""
        return ArrayFunctions.array_insert(column, pos, value)

    @staticmethod
    def array_size(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Get array length."""
        return ArrayFunctions.array_size(column)

    @staticmethod
    def array_sort(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Sort array elements."""
        return ArrayFunctions.array_sort(column)

    @staticmethod
    def arrays_overlap(
        column1: Union[MockColumn, str], column2: Union[MockColumn, str]
    ) -> MockColumnOperation:
        """Check if arrays have common elements."""
        return ArrayFunctions.arrays_overlap(column1, column2)

    @staticmethod
    def array_contains(
        column: Union[MockColumn, str], value: Any
    ) -> MockColumnOperation:
        """Check if array contains value."""
        return ArrayFunctions.array_contains(column, value)

    @staticmethod
    def array_max(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Return maximum value from array."""
        return ArrayFunctions.array_max(column)

    @staticmethod
    def array_min(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Return minimum value from array."""
        return ArrayFunctions.array_min(column)

    @staticmethod
    def explode(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Returns a new row for each element in array or map."""
        return ArrayFunctions.explode(column)

    @staticmethod
    def size(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Return size of array or map."""
        return ArrayFunctions.size(column)

    @staticmethod
    def flatten(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Flatten array of arrays into single array."""
        return ArrayFunctions.flatten(column)

    @staticmethod
    def reverse(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Reverse array elements."""
        return ArrayFunctions.reverse(column)

    @staticmethod
    def explode_outer(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Explode array including null/empty arrays."""
        return ArrayFunctions.explode_outer(column)

    @staticmethod
    def posexplode(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Explode array with position."""
        return ArrayFunctions.posexplode(column)

    @staticmethod
    def posexplode_outer(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Explode array with position including null/empty."""
        return ArrayFunctions.posexplode_outer(column)

    @staticmethod
    def arrays_zip(*columns: Union[MockColumn, str]) -> MockColumnOperation:
        """Merge arrays into array of structs."""
        return ArrayFunctions.arrays_zip(*columns)

    @staticmethod
    def sequence(
        start: Union[MockColumn, str, int],
        stop: Union[MockColumn, str, int],
        step: Union[MockColumn, str, int] = 1,
    ) -> MockColumnOperation:
        """Generate array sequence from start to stop."""
        return ArrayFunctions.sequence(start, stop, step)

    @staticmethod
    def shuffle(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Randomly shuffle array elements."""
        return ArrayFunctions.shuffle(column)

    # Map functions
    @staticmethod
    def map_keys(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Get all keys from map."""
        return MapFunctions.map_keys(column)

    @staticmethod
    def map_values(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Get all values from map."""
        return MapFunctions.map_values(column)

    @staticmethod
    def map_entries(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Get key-value pairs as array of structs."""
        return MapFunctions.map_entries(column)

    @staticmethod
    def map_concat(*columns: Union[MockColumn, str]) -> MockColumnOperation:
        """Concatenate multiple maps."""
        return MapFunctions.map_concat(*columns)

    @staticmethod
    def map_from_arrays(
        keys: Union[MockColumn, str], values: Union[MockColumn, str]
    ) -> MockColumnOperation:
        """Create map from key and value arrays."""
        return MapFunctions.map_from_arrays(keys, values)

    # Advanced map functions (PySpark 3.2+)
    @staticmethod
    def create_map(*cols: Union[MockColumn, str, Any]) -> MockColumnOperation:
        """Create map from key-value pairs."""
        return MapFunctions.create_map(*cols)

    @staticmethod
    def map_contains_key(
        column: Union[MockColumn, str], key: Any
    ) -> MockColumnOperation:
        """Check if map contains key."""
        return MapFunctions.map_contains_key(column, key)

    @staticmethod
    def map_from_entries(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Convert array of structs to map."""
        return MapFunctions.map_from_entries(column)

    @staticmethod
    def map_filter(
        column: Union[MockColumn, str], function: Callable[[Any, Any], bool]
    ) -> MockColumnOperation:
        """Filter map entries with predicate."""
        return MapFunctions.map_filter(column, function)

    @staticmethod
    def transform_keys(
        column: Union[MockColumn, str], function: Callable[[Any, Any], Any]
    ) -> MockColumnOperation:
        """Transform map keys with function."""
        return MapFunctions.transform_keys(column, function)

    @staticmethod
    def transform_values(
        column: Union[MockColumn, str], function: Callable[[Any, Any], Any]
    ) -> MockColumnOperation:
        """Transform map values with function."""
        return MapFunctions.transform_values(column, function)

    @staticmethod
    def map_zip_with(
        col1: Union[MockColumn, str],
        col2: Union[MockColumn, str],
        function: Callable[[Any, Any, Any], Any],
    ) -> MockColumnOperation:
        """Merge two maps using function (PySpark 3.1+)."""
        return MapFunctions.map_zip_with(col1, col2, function)

    # Struct functions (PySpark 3.2+)
    @staticmethod
    def struct(*cols: Union[MockColumn, str]) -> MockColumnOperation:
        """Create a struct column from given columns."""
        if not cols:
            raise ValueError("struct requires at least one column")

        # Use first column as base
        base_col = (
            cols[0] if isinstance(cols[0], MockColumn) else MockColumn(str(cols[0]))
        )

        return MockColumnOperation(
            base_col,
            "struct",
            value=cols[1:] if len(cols) > 1 else None,
            name="struct(...)",
        )

    @staticmethod
    def named_struct(*cols: Any) -> MockColumnOperation:
        """Create a struct column with named fields.

        Args:
            *cols: Alternating field names (strings) and column values.
        """
        if len(cols) < 2 or len(cols) % 2 != 0:
            raise ValueError("named_struct requires alternating field names and values")

        # Use first value column as base (skip first name)
        base_col = (
            cols[1] if isinstance(cols[1], MockColumn) else MockColumn(str(cols[1]))
        )

        return MockColumnOperation(
            base_col,
            "named_struct",
            value=cols,
            name="named_struct(...)",
        )

    # Bitwise functions (PySpark 3.2+)
    @staticmethod
    def bit_count(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Count set bits."""
        return BitwiseFunctions.bit_count(column)

    @staticmethod
    def bit_get(column: Union[MockColumn, str], pos: int) -> MockColumnOperation:
        """Get bit at position."""
        return BitwiseFunctions.bit_get(column, pos)

    @staticmethod
    def bitwise_not(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Bitwise NOT."""
        return BitwiseFunctions.bitwise_not(column)

    @staticmethod
    def bit_and(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Bitwise AND aggregate (PySpark 3.5+)."""
        return BitwiseFunctions.bit_and(column)

    @staticmethod
    def bit_or(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Bitwise OR aggregate (PySpark 3.5+)."""
        return BitwiseFunctions.bit_or(column)

    @staticmethod
    def bit_xor(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Bitwise XOR aggregate (PySpark 3.5+)."""
        return BitwiseFunctions.bit_xor(column)

    # Timezone functions (PySpark 3.2+)
    @staticmethod
    def convert_timezone(
        sourceTz: str, targetTz: str, sourceTs: Union[MockColumn, str]
    ) -> MockColumnOperation:
        """Convert timestamp between timezones."""
        return DateTimeFunctions.convert_timezone(sourceTz, targetTz, sourceTs)

    @staticmethod
    def current_timezone() -> MockColumnOperation:
        """Get current timezone."""
        return DateTimeFunctions.current_timezone()

    @staticmethod
    def from_utc_timestamp(ts: Union[MockColumn, str], tz: str) -> MockColumnOperation:
        """Convert UTC timestamp to timezone."""
        return DateTimeFunctions.from_utc_timestamp(ts, tz)

    @staticmethod
    def to_utc_timestamp(ts: Union[MockColumn, str], tz: str) -> MockColumnOperation:
        """Convert timestamp to UTC."""
        return DateTimeFunctions.to_utc_timestamp(ts, tz)

    # URL functions (PySpark 3.2+)
    @staticmethod
    def parse_url(url: Union[MockColumn, str], part: str) -> MockColumnOperation:
        """Extract part from URL."""
        return StringFunctions.parse_url(url, part)

    @staticmethod
    def url_encode(url: Union[MockColumn, str]) -> MockColumnOperation:
        """URL-encode string."""
        return StringFunctions.url_encode(url)

    @staticmethod
    def url_decode(url: Union[MockColumn, str]) -> MockColumnOperation:
        """URL-decode string."""
        return StringFunctions.url_decode(url)

    @staticmethod
    def overlay(
        src: Union[MockColumn, str],
        replace: Union[MockColumn, str],
        pos: Union[MockColumn, int],
        len: Union[MockColumn, int] = -1,
    ) -> MockColumnOperation:
        """Replace part of string (PySpark 3.0+)."""
        return StringFunctions.overlay(src, replace, pos, len)

    # Miscellaneous functions (PySpark 3.2+)
    @staticmethod
    def date_part(field: str, source: Union[MockColumn, str]) -> MockColumnOperation:
        """Extract date/time part."""
        return DateTimeFunctions.date_part(field, source)

    @staticmethod
    def dayname(date: Union[MockColumn, str]) -> MockColumnOperation:
        """Get day of week name."""
        return DateTimeFunctions.dayname(date)

    @staticmethod
    def assert_true(
        condition: Union[MockColumn, MockColumnOperation],
    ) -> MockColumnOperation:
        """Assert condition is true."""
        return ConditionalFunctions.assert_true(condition)

    @staticmethod
    def ifnull(
        col1: Union[MockColumn, str], col2: Union[MockColumn, str]
    ) -> MockColumnOperation:
        """Return col2 if col1 is null (PySpark 3.5+)."""
        return ConditionalFunctions.ifnull(col1, col2)

    @staticmethod
    def nullif(
        col1: Union[MockColumn, str], col2: Union[MockColumn, str]
    ) -> MockColumnOperation:
        """Return null if col1 equals col2 (PySpark 3.5+)."""
        return ConditionalFunctions.nullif(col1, col2)

    # XML functions (PySpark 3.2+)
    @staticmethod
    def from_xml(col: Union[MockColumn, str], schema: str) -> MockColumnOperation:
        """Parse XML string to struct."""
        return XMLFunctions.from_xml(col, schema)

    @staticmethod
    def to_xml(col: Union[MockColumn, MockColumnOperation]) -> MockColumnOperation:
        """Convert struct to XML string."""
        return XMLFunctions.to_xml(col)

    @staticmethod
    def schema_of_xml(col: Union[MockColumn, str]) -> MockColumnOperation:
        """Infer schema from XML."""
        return XMLFunctions.schema_of_xml(col)

    @staticmethod
    def xpath(xml: Union[MockColumn, str], path: str) -> MockColumnOperation:
        """Extract array from XML using XPath."""
        return XMLFunctions.xpath(xml, path)

    @staticmethod
    def xpath_boolean(xml: Union[MockColumn, str], path: str) -> MockColumnOperation:
        """Extract boolean from XML using XPath."""
        return XMLFunctions.xpath_boolean(xml, path)

    @staticmethod
    def xpath_double(xml: Union[MockColumn, str], path: str) -> MockColumnOperation:
        """Extract double from XML using XPath."""
        return XMLFunctions.xpath_double(xml, path)

    @staticmethod
    def xpath_float(xml: Union[MockColumn, str], path: str) -> MockColumnOperation:
        """Extract float from XML using XPath."""
        return XMLFunctions.xpath_float(xml, path)

    @staticmethod
    def xpath_int(xml: Union[MockColumn, str], path: str) -> MockColumnOperation:
        """Extract integer from XML using XPath."""
        return XMLFunctions.xpath_int(xml, path)

    @staticmethod
    def xpath_long(xml: Union[MockColumn, str], path: str) -> MockColumnOperation:
        """Extract long from XML using XPath."""
        return XMLFunctions.xpath_long(xml, path)

    @staticmethod
    def xpath_short(xml: Union[MockColumn, str], path: str) -> MockColumnOperation:
        """Extract short from XML using XPath."""
        return XMLFunctions.xpath_short(xml, path)

    @staticmethod
    def xpath_string(xml: Union[MockColumn, str], path: str) -> MockColumnOperation:
        """Extract string from XML using XPath."""
        return XMLFunctions.xpath_string(xml, path)

    # JSON/CSV functions
    @staticmethod
    def from_json(
        column: Union[MockColumn, str],
        schema: Any,
        options: Optional[Dict[str, Any]] = None,
    ) -> MockColumnOperation:
        """Parse JSON string into struct/array."""
        from mock_spark.functions.json_csv import JSONCSVFunctions

        return JSONCSVFunctions.from_json(column, schema, options)

    @staticmethod
    def to_json(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Convert struct/array to JSON string."""
        from mock_spark.functions.json_csv import JSONCSVFunctions

        return JSONCSVFunctions.to_json(column)

    @staticmethod
    def get_json_object(
        column: Union[MockColumn, str], path: str
    ) -> MockColumnOperation:
        """Extract JSON object at path."""
        from mock_spark.functions.json_csv import JSONCSVFunctions

        return JSONCSVFunctions.get_json_object(column, path)

    @staticmethod
    def json_tuple(column: Union[MockColumn, str], *fields: str) -> MockColumnOperation:
        """Extract multiple fields from JSON."""
        from mock_spark.functions.json_csv import JSONCSVFunctions

        return JSONCSVFunctions.json_tuple(column, *fields)

    @staticmethod
    def schema_of_json(json_string: str) -> MockColumnOperation:
        """Infer schema from JSON string."""
        from mock_spark.functions.json_csv import JSONCSVFunctions

        return JSONCSVFunctions.schema_of_json(json_string)

    @staticmethod
    def from_csv(
        column: Union[MockColumn, str],
        schema: Any,
        options: Optional[Dict[str, Any]] = None,
    ) -> MockColumnOperation:
        """Parse CSV string into struct."""
        from mock_spark.functions.json_csv import JSONCSVFunctions

        return JSONCSVFunctions.from_csv(column, schema, options)

    @staticmethod
    def to_csv(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Convert struct to CSV string."""
        from mock_spark.functions.json_csv import JSONCSVFunctions

        return JSONCSVFunctions.to_csv(column)

    @staticmethod
    def schema_of_csv(csv_string: str) -> MockColumnOperation:
        """Infer schema from CSV string."""
        from mock_spark.functions.json_csv import JSONCSVFunctions

        return JSONCSVFunctions.schema_of_csv(csv_string)

    # Column ordering functions
    @staticmethod
    def asc(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Sort ascending."""
        from mock_spark.functions.ordering import OrderingFunctions

        return OrderingFunctions.asc(column)

    @staticmethod
    def asc_nulls_first(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Sort ascending, nulls first."""
        from mock_spark.functions.ordering import OrderingFunctions

        return OrderingFunctions.asc_nulls_first(column)

    @staticmethod
    def asc_nulls_last(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Sort ascending, nulls last."""
        from mock_spark.functions.ordering import OrderingFunctions

        return OrderingFunctions.asc_nulls_last(column)

    @staticmethod
    def desc_nulls_first(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Sort descending, nulls first."""
        from mock_spark.functions.ordering import OrderingFunctions

        return OrderingFunctions.desc_nulls_first(column)

    @staticmethod
    def desc_nulls_last(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Sort descending, nulls last."""
        from mock_spark.functions.ordering import OrderingFunctions

        return OrderingFunctions.desc_nulls_last(column)

    # Metadata/utility functions
    @staticmethod
    def input_file_name() -> MockColumnOperation:
        """Return input file name."""
        from mock_spark.functions.metadata import MetadataFunctions

        return MetadataFunctions.input_file_name()

    @staticmethod
    def monotonically_increasing_id() -> MockColumnOperation:
        """Generate monotonically increasing ID."""
        from mock_spark.functions.metadata import MetadataFunctions

        return MetadataFunctions.monotonically_increasing_id()

    @staticmethod
    def spark_partition_id() -> MockColumnOperation:
        """Return partition ID."""
        from mock_spark.functions.metadata import MetadataFunctions

        return MetadataFunctions.spark_partition_id()

    @staticmethod
    def broadcast(df: Any) -> Any:
        """Mark DataFrame for broadcast (hint)."""
        from mock_spark.functions.metadata import MetadataFunctions

        return MetadataFunctions.broadcast(df)

    @staticmethod
    def column(col_name: str) -> MockColumn:
        """Create column reference (alias for col)."""
        from mock_spark.functions.metadata import MetadataFunctions

        return MetadataFunctions.column(col_name)

    @staticmethod
    def grouping(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Grouping indicator for CUBE/ROLLUP."""
        from mock_spark.functions.metadata import GroupingFunctions

        return GroupingFunctions.grouping(column)

    @staticmethod
    def grouping_id(*cols: Union[MockColumn, str]) -> MockColumnOperation:
        """Grouping ID for CUBE/ROLLUP."""
        from mock_spark.functions.metadata import GroupingFunctions

        return GroupingFunctions.grouping_id(*cols)

    @staticmethod
    def udf(
        f: Optional[Callable[..., Any]] = None, returnType: Any = None
    ) -> Callable[..., Any]:
        """Create a user-defined function (all PySpark versions).

        Args:
            f: Python function to wrap
            returnType: Return type of the function (defaults to StringType)

        Returns:
            Wrapped function that can be used in DataFrame operations

        Example:
            >>> from mock_spark import MockSparkSession, F
            >>> from mock_spark.spark_types import IntegerType
            >>> spark = MockSparkSession("test")
            >>> square = F.udf(lambda x: x * x, IntegerType())
            >>> df = spark.createDataFrame([{"value": 5}])
            >>> df.select(square("value").alias("squared")).show()
        """
        from mock_spark.spark_types import StringType

        if returnType is None:
            returnType = StringType()

        def udf_wrapper(func: Callable[..., Any]) -> Callable[..., Any]:
            """Wrap function to create MockColumnOperation."""

            def apply_udf(col: Union[MockColumn, str]) -> MockColumnOperation:
                column = MockColumn(col) if isinstance(col, str) else col
                # Create a UDF operation that stores the function
                op = MockColumnOperation(column, "udf", name=f"udf({column.name})")
                op._udf_func = func  # type: ignore
                op._udf_return_type = returnType  # type: ignore
                return op

            return apply_udf

        # Support decorator pattern: @udf or udf(lambda x: x)
        if f is None:
            return udf_wrapper
        else:
            return udf_wrapper(f)

    @staticmethod
    def pandas_udf(
        f: Optional[Any] = None, returnType: Any = None, functionType: Any = None
    ) -> Any:
        """Create a Pandas UDF (vectorized UDF) (all PySpark versions).

        Pandas UDFs are user-defined functions that execute vectorized operations
        using Pandas Series/DataFrame, providing better performance than row-at-a-time UDFs.

        Args:
            f: Python function to wrap OR return type (if used as decorator)
            returnType: Return type of the function (defaults to StringType)
            functionType: Type of Pandas UDF (optional, for compatibility)

        Returns:
            Wrapped function that can be used in DataFrame operations

        Example:
            >>> from mock_spark import MockSparkSession, F
            >>> from mock_spark.spark_types import IntegerType
            >>> spark = MockSparkSession("test")
            >>> @F.pandas_udf(IntegerType())
            >>> def multiply_by_two(s):
            ...     return s * 2
            >>> df = spark.createDataFrame([{"value": 5}])
            >>> df.select(multiply_by_two("value").alias("doubled")).show()
        """
        from mock_spark.spark_types import StringType
        from mock_spark.functions.udf import UserDefinedFunction

        # Handle different call patterns:
        # 1. @pandas_udf(IntegerType()) - f is the type, returnType is None
        # 2. @pandas_udf(returnType=IntegerType()) - f is None, returnType is the type
        # 3. pandas_udf(lambda x: x, IntegerType()) - f is function, returnType is the type

        # Check if first argument is a data type (not a function)
        if f is not None and not callable(f):
            # f is actually the return type
            actual_returnType = f
            f = None
        else:
            actual_returnType = returnType if returnType is not None else StringType()

        def pandas_udf_wrapper(func: Callable[..., Any]) -> UserDefinedFunction:
            """Wrap function to create UserDefinedFunction with Pandas eval type."""
            udf_obj = UserDefinedFunction(func, actual_returnType, evalType="PANDAS")
            return udf_obj

        # Support decorator pattern: @pandas_udf or pandas_udf(lambda x: x)
        if f is None:
            return pandas_udf_wrapper
        else:
            return pandas_udf_wrapper(f)

    @staticmethod
    def window(
        timeColumn: Union[MockColumn, str],
        windowDuration: str,
        slideDuration: Optional[str] = None,
        startTime: Optional[str] = None,
    ) -> MockColumnOperation:
        """Create time-based window for grouping operations (all PySpark versions).

        Args:
            timeColumn: Timestamp column to window
            windowDuration: Duration string (e.g., "10 seconds", "1 minute", "2 hours")
            slideDuration: Slide duration for sliding windows (defaults to windowDuration)
            startTime: Offset for window alignment (e.g., "0 seconds")

        Returns:
            Column representing window struct with start and end times

        Example:
            >>> df.groupBy(F.window("timestamp", "10 minutes")).count()
            >>> df.groupBy(F.window("timestamp", "10 minutes", "5 minutes")).agg(F.sum("value"))
        """
        column = MockColumn(timeColumn) if isinstance(timeColumn, str) else timeColumn

        # Create a window operation
        op = MockColumnOperation(column, "window", name=f"window({column.name})")
        op._window_duration = windowDuration  # type: ignore
        op._window_slide = slideDuration or windowDuration  # type: ignore
        op._window_start = startTime or "0 seconds"  # type: ignore
        return op

    @staticmethod
    def window_time(windowColumn: Union[MockColumn, str]) -> MockColumnOperation:
        """Extract window start time from window column (PySpark 3.4+).

        Args:
            windowColumn: Window column to extract time from

        Returns:
            Column operation representing window start timestamp

        Example:
            >>> df.groupBy(F.window("timestamp", "1 hour")).agg(
            ...     F.window_time(F.col("window")).alias("window_start")
            ... )
        """
        column = (
            MockColumn(windowColumn) if isinstance(windowColumn, str) else windowColumn
        )
        op = MockColumnOperation(
            column, "window_time", name=f"window_time({column.name})"
        )
        return op

    # Deprecated Aliases
    @staticmethod
    def approxCountDistinct(*cols: Union[MockColumn, str]) -> MockAggregateFunction:
        """Deprecated alias for approx_count_distinct (all PySpark versions)."""
        return AggregateFunctions.approxCountDistinct(*cols)

    @staticmethod
    def sumDistinct(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Deprecated alias for sum_distinct (all PySpark versions)."""
        return AggregateFunctions.sumDistinct(column)

    @staticmethod
    def bitwiseNOT(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Deprecated alias for bitwise_not (all PySpark versions)."""
        return BitwiseFunctions.bitwiseNOT(column)

    @staticmethod
    def toDegrees(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Deprecated alias for degrees (all PySpark versions)."""
        return MathFunctions.toDegrees(column)

    @staticmethod
    def toRadians(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Deprecated alias for radians (all PySpark versions)."""
        return MathFunctions.toRadians(column)


# Create the F namespace instance
F = MockFunctions()

# Re-export all the main classes for backward compatibility
__all__ = [
    "MockColumn",
    "MockColumnOperation",
    "MockLiteral",
    "MockAggregateFunction",
    "MockCaseWhen",
    "MockWindowFunction",
    "MockFunctions",
    "F",
    "StringFunctions",
    "MathFunctions",
    "AggregateFunctions",
    "DateTimeFunctions",
]
