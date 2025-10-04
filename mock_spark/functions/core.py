"""
Core functions module for Mock Spark.

This module provides the main F namespace and re-exports all function classes
for backward compatibility with the original functions.py structure.
"""

from typing import Any, Optional
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
    def upper(column) -> MockColumnOperation:
        """Convert string to uppercase."""
        return StringFunctions.upper(column)

    @staticmethod
    def lower(column) -> MockColumnOperation:
        """Convert string to lowercase."""
        return StringFunctions.lower(column)

    @staticmethod
    def length(column) -> MockColumnOperation:
        """Get string length."""
        return StringFunctions.length(column)

    @staticmethod
    def trim(column) -> MockColumnOperation:
        """Trim whitespace."""
        return StringFunctions.trim(column)

    @staticmethod
    def ltrim(column) -> MockColumnOperation:
        """Trim left whitespace."""
        return StringFunctions.ltrim(column)

    @staticmethod
    def rtrim(column) -> MockColumnOperation:
        """Trim right whitespace."""
        return StringFunctions.rtrim(column)

    @staticmethod
    def regexp_replace(column, pattern: str, replacement: str) -> MockColumnOperation:
        """Replace regex pattern."""
        return StringFunctions.regexp_replace(column, pattern, replacement)

    @staticmethod
    def split(column, delimiter: str) -> MockColumnOperation:
        """Split string by delimiter."""
        return StringFunctions.split(column, delimiter)

    @staticmethod
    def substring(column, start: int, length: Optional[int] = None) -> MockColumnOperation:
        """Extract substring."""
        return StringFunctions.substring(column, start, length)

    @staticmethod
    def concat(*columns) -> MockColumnOperation:
        """Concatenate strings."""
        return StringFunctions.concat(*columns)

    # Math functions
    @staticmethod
    def abs(column) -> MockColumnOperation:
        """Get absolute value."""
        return MathFunctions.abs(column)

    @staticmethod
    def round(column, scale: int = 0) -> MockColumnOperation:
        """Round to decimal places."""
        return MathFunctions.round(column, scale)

    @staticmethod
    def ceil(column) -> MockColumnOperation:
        """Round up."""
        return MathFunctions.ceil(column)

    @staticmethod
    def floor(column) -> MockColumnOperation:
        """Round down."""
        return MathFunctions.floor(column)

    @staticmethod
    def sqrt(column) -> MockColumnOperation:
        """Square root."""
        return MathFunctions.sqrt(column)

    @staticmethod
    def exp(column) -> MockColumnOperation:
        """Exponential."""
        return MathFunctions.exp(column)

    @staticmethod
    def log(column, base: Optional[float] = None) -> MockColumnOperation:
        """Logarithm."""
        return MathFunctions.log(column, base)

    @staticmethod
    def pow(column, exponent) -> MockColumnOperation:
        """Power."""
        return MathFunctions.pow(column, exponent)

    @staticmethod
    def sin(column) -> MockColumnOperation:
        """Sine."""
        return MathFunctions.sin(column)

    @staticmethod
    def cos(column) -> MockColumnOperation:
        """Cosine."""
        return MathFunctions.cos(column)

    @staticmethod
    def tan(column) -> MockColumnOperation:
        """Tangent."""
        return MathFunctions.tan(column)

    # Aggregate functions
    @staticmethod
    def count(column=None) -> MockAggregateFunction:
        """Count values."""
        return AggregateFunctions.count(column)

    @staticmethod
    def sum(column) -> MockAggregateFunction:
        """Sum values."""
        return AggregateFunctions.sum(column)

    @staticmethod
    def avg(column) -> MockAggregateFunction:
        """Average values."""
        return AggregateFunctions.avg(column)

    @staticmethod
    def max(column) -> MockAggregateFunction:
        """Maximum value."""
        return AggregateFunctions.max(column)

    @staticmethod
    def min(column) -> MockAggregateFunction:
        """Minimum value."""
        return AggregateFunctions.min(column)

    @staticmethod
    def first(column) -> MockAggregateFunction:
        """First value."""
        return AggregateFunctions.first(column)

    @staticmethod
    def last(column) -> MockAggregateFunction:
        """Last value."""
        return AggregateFunctions.last(column)

    @staticmethod
    def collect_list(column) -> MockAggregateFunction:
        """Collect values into list."""
        return AggregateFunctions.collect_list(column)

    @staticmethod
    def collect_set(column) -> MockAggregateFunction:
        """Collect unique values into set."""
        return AggregateFunctions.collect_set(column)

    @staticmethod
    def stddev(column) -> MockAggregateFunction:
        """Standard deviation."""
        return AggregateFunctions.stddev(column)

    @staticmethod
    def variance(column) -> MockAggregateFunction:
        """Variance."""
        return AggregateFunctions.variance(column)

    @staticmethod
    def skewness(column) -> MockAggregateFunction:
        """Skewness."""
        return AggregateFunctions.skewness(column)

    @staticmethod
    def kurtosis(column) -> MockAggregateFunction:
        """Kurtosis."""
        return AggregateFunctions.kurtosis(column)

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
    def isnan(column) -> MockColumnOperation:
        """Check if column is NaN."""
        return ConditionalFunctions.isnan(column)

    @staticmethod
    def when(condition) -> MockCaseWhen:
        """Start CASE WHEN expression."""
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

    # Conditional functions
    @staticmethod
    def when(condition, value) -> MockCaseWhen:
        """Start CASE WHEN expression."""
        return MockCaseWhen().when(condition, value)

    @staticmethod
    def coalesce(*columns) -> MockColumnOperation:
        """Return first non-null value."""
        if not columns:
            raise ValueError("At least one column must be provided")
        
        # Use the first column as base and chain coalesce operations
        base_column = MockColumn(columns[0]) if isinstance(columns[0], str) else columns[0]
        operation = MockColumnOperation(base_column, "coalesce", list(columns[1:]))
        column_names = [col.name if hasattr(col, 'name') else str(col) for col in columns]
        operation.name = f"coalesce({', '.join(column_names)})"
        return operation

    @staticmethod
    def isnull(column) -> MockColumnOperation:
        """Check if null."""
        if isinstance(column, str):
            column = MockColumn(column)
        return column.isnull()

    @staticmethod
    def isnotnull(column) -> MockColumnOperation:
        """Check if not null."""
        if isinstance(column, str):
            column = MockColumn(column)
        return column.isnotnull()

    @staticmethod
    def isnan(column) -> MockColumnOperation:
        """Check if NaN."""
        if isinstance(column, str):
            column = MockColumn(column)
        
        operation = MockColumnOperation(column, "isnan")
        operation.name = f"isnan({column.name})"
        return operation

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
