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

from typing import Any, Optional, Union, Callable
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
    def max_by(column: Union[MockColumn, str], ord: Union[MockColumn, str]) -> MockAggregateFunction:
        """Value with max of ord column (PySpark 3.1+)."""
        return AggregateFunctions.max_by(column, ord)

    @staticmethod
    def min_by(column: Union[MockColumn, str], ord: Union[MockColumn, str]) -> MockAggregateFunction:
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
    def add_months(column: Union[MockColumn, str], num_months: int) -> MockColumnOperation:
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
        day: Union[MockColumn, int]
    ) -> MockColumnOperation:
        """Construct date from year, month, day (PySpark 3.0+)."""
        return DateTimeFunctions.make_date(year, month, day)

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
        """Return default if null."""
        if isinstance(column, str):
            column = MockColumn(column)

        operation = MockColumnOperation(column, "nvl", default_value)
        operation.name = f"nvl({column.name}, {default_value})"
        return operation

    @staticmethod
    def nvl2(
        column: Union[MockColumn, str], value_if_not_null: Any, value_if_null: Any
    ) -> MockColumnOperation:
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

        operation = MockColumnOperation(column, "desc", None, name=f"{column.name} DESC")
        operation.function_name = "desc"
        return operation

    # Array functions
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
    def array_position(column: Union[MockColumn, str], value: Any) -> MockColumnOperation:
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
    def slice(column: Union[MockColumn, str], start: int, length: int) -> MockColumnOperation:
        """Extract array slice."""
        return ArrayFunctions.slice(column, start, length)

    @staticmethod
    def element_at(column: Union[MockColumn, str], index: int) -> MockColumnOperation:
        """Get element at index."""
        return ArrayFunctions.element_at(column, index)

    @staticmethod
    def array_append(column: Union[MockColumn, str], element: Any) -> MockColumnOperation:
        """Append element to array."""
        return ArrayFunctions.array_append(column, element)

    @staticmethod
    def array_prepend(column: Union[MockColumn, str], element: Any) -> MockColumnOperation:
        """Prepend element to array."""
        return ArrayFunctions.array_prepend(column, element)

    @staticmethod
    def array_insert(column: Union[MockColumn, str], pos: int, value: Any) -> MockColumnOperation:
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
    def map_contains_key(column: Union[MockColumn, str], key: Any) -> MockColumnOperation:
        """Check if map contains key."""
        return MapFunctions.map_contains_key(column, key)

    @staticmethod
    def map_from_entries(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Convert array of structs to map."""
        return MapFunctions.map_from_entries(column)

    @staticmethod
    def map_filter(column: Union[MockColumn, str], function: Callable[[Any, Any], bool]) -> MockColumnOperation:
        """Filter map entries with predicate."""
        return MapFunctions.map_filter(column, function)

    @staticmethod
    def transform_keys(column: Union[MockColumn, str], function: Callable[[Any, Any], Any]) -> MockColumnOperation:
        """Transform map keys with function."""
        return MapFunctions.transform_keys(column, function)

    @staticmethod
    def transform_values(column: Union[MockColumn, str], function: Callable[[Any, Any], Any]) -> MockColumnOperation:
        """Transform map values with function."""
        return MapFunctions.transform_values(column, function)

    # Struct functions (PySpark 3.2+)
    @staticmethod
    def struct(*cols: Union[MockColumn, str]) -> MockColumnOperation:
        """Create a struct column from given columns."""
        if not cols:
            raise ValueError("struct requires at least one column")
        
        # Use first column as base
        base_col = cols[0] if isinstance(cols[0], MockColumn) else MockColumn(str(cols[0]))
        
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
        base_col = cols[1] if isinstance(cols[1], MockColumn) else MockColumn(str(cols[1]))
        
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

    # Timezone functions (PySpark 3.2+)
    @staticmethod
    def convert_timezone(sourceTz: str, targetTz: str, sourceTs: Union[MockColumn, str]) -> MockColumnOperation:
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
        len: Union[MockColumn, int] = -1
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
    def assert_true(condition: Union[MockColumn, MockColumnOperation]) -> MockColumnOperation:
        """Assert condition is true."""
        return ConditionalFunctions.assert_true(condition)

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
