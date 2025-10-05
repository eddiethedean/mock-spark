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

from typing import Any, Optional, Union
from .base import MockColumn, MockColumnOperation, MockLiteral, MockAggregateFunction
from .conditional import MockCaseWhen, ConditionalFunctions
from .window_execution import MockWindowFunction
from .string import StringFunctions
from .math import MathFunctions
from .aggregate import AggregateFunctions
from .datetime import DateTimeFunctions


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
    def format_string(format_str: str, *columns: Union[MockColumn, str]) -> MockColumnOperation:
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
    def log(column: Union[MockColumn, str], base: Optional[float] = None) -> MockColumnOperation:
        """Logarithm."""
        return MathFunctions.log(column, base)

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
    def sign(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Sign of number (matches PySpark signum)."""
        return MathFunctions.sign(column)

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
    def to_date(column, format: Optional[str] = None) -> MockColumnOperation:
        """Convert to date."""
        return DateTimeFunctions.to_date(column, format)

    @staticmethod
    def to_timestamp(column, format: Optional[str] = None) -> MockColumnOperation:
        """Convert to timestamp."""
        return DateTimeFunctions.to_timestamp(column, format)

    @staticmethod
    def hour(column) -> MockColumnOperation:
        """Extract hour."""
        return DateTimeFunctions.hour(column)

    @staticmethod
    def day(column) -> MockColumnOperation:
        """Extract day."""
        return DateTimeFunctions.day(column)

    @staticmethod
    def month(column) -> MockColumnOperation:
        """Extract month."""
        return DateTimeFunctions.month(column)

    @staticmethod
    def year(column) -> MockColumnOperation:
        """Extract year."""
        return DateTimeFunctions.year(column)

    # Conditional functions
    @staticmethod
    def coalesce(*columns) -> MockColumnOperation:
        """Return first non-null value."""
        return ConditionalFunctions.coalesce(*columns)

    @staticmethod
    def isnull(column) -> MockColumnOperation:
        """Check if column is null."""
        return ConditionalFunctions.isnull(column)

    @staticmethod
    def isnotnull(column) -> MockColumnOperation:
        """Check if column is not null."""
        return ConditionalFunctions.isnotnull(column)

    @staticmethod
    def isnan(column) -> MockColumnOperation:
        """Check if column is NaN."""
        return ConditionalFunctions.isnan(column)

    @staticmethod
    def when(condition, value=None) -> MockCaseWhen:
        """Start CASE WHEN expression."""
        if value is not None:
            return ConditionalFunctions.when(condition, value)
        return ConditionalFunctions.when(condition)

    @staticmethod
    def dayofweek(column) -> MockColumnOperation:
        """Extract day of week."""
        return DateTimeFunctions.dayofweek(column)

    @staticmethod
    def dayofyear(column) -> MockColumnOperation:
        """Extract day of year."""
        return DateTimeFunctions.dayofyear(column)

    @staticmethod
    def weekofyear(column) -> MockColumnOperation:
        """Extract week of year."""
        return DateTimeFunctions.weekofyear(column)

    @staticmethod
    def quarter(column) -> MockColumnOperation:
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
    def minute(column) -> MockColumnOperation:
        """Extract minute."""
        return DateTimeFunctions.minute(column)

    @staticmethod
    def second(column) -> MockColumnOperation:
        """Extract second."""
        return DateTimeFunctions.second(column)

    @staticmethod
    def add_months(column, num_months: int) -> MockColumnOperation:
        """Add months to date."""
        return DateTimeFunctions.add_months(column, num_months)

    @staticmethod
    def months_between(column1, column2) -> MockColumnOperation:
        """Calculate months between two dates."""
        return DateTimeFunctions.months_between(column1, column2)

    @staticmethod
    def date_add(column, days: int) -> MockColumnOperation:
        """Add days to date."""
        return DateTimeFunctions.date_add(column, days)

    @staticmethod
    def date_sub(column, days: int) -> MockColumnOperation:
        """Subtract days from date."""
        return DateTimeFunctions.date_sub(column, days)

    @staticmethod
    def nvl(column, default_value) -> MockColumnOperation:
        """Return default if null."""
        if isinstance(column, str):
            column = MockColumn(column)

        operation = MockColumnOperation(column, "nvl", default_value)
        operation.name = f"nvl({column.name}, {default_value})"
        return operation

    @staticmethod
    def nvl2(column, value_if_not_null, value_if_null) -> MockColumnOperation:
        """Return value based on null check."""
        if isinstance(column, str):
            column = MockColumn(column)

        operation = MockColumnOperation(column, "nvl2", (value_if_not_null, value_if_null))
        operation.name = f"nvl2({column.name}, {value_if_not_null}, {value_if_null})"
        return operation

    # Window functions
    @staticmethod
    def row_number() -> MockColumnOperation:
        """Row number window function."""
        # Create a special column for functions without input
        from mock_spark.functions.base import MockColumn

        dummy_column = MockColumn("__row_number__")
        operation = MockColumnOperation(dummy_column, "row_number")
        operation.name = "row_number()"
        operation.function_name = "row_number"
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
    def lag(column, offset: int = 1, default_value=None) -> MockColumnOperation:
        """Lag window function."""
        if isinstance(column, str):
            column = MockColumn(column)

        operation = MockColumnOperation(column, "lag", (offset, default_value))
        operation.name = f"lag({column.name}, {offset})"
        operation.function_name = "lag"
        return operation

    @staticmethod
    def lead(column, offset: int = 1, default_value=None) -> MockColumnOperation:
        """Lead window function."""
        if isinstance(column, str):
            column = MockColumn(column)

        operation = MockColumnOperation(column, "lead", (offset, default_value))
        operation.name = f"lead({column.name}, {offset})"
        operation.function_name = "lead"
        return operation

    @staticmethod
    def nth_value(column, n: int) -> MockColumnOperation:
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
    def desc(column) -> MockColumnOperation:
        """Create descending order column."""
        if isinstance(column, str):
            column = MockColumn(column)

        operation = MockColumnOperation(column, "desc", None, name=f"{column.name} DESC")
        operation.function_name = "desc"
        return operation


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
