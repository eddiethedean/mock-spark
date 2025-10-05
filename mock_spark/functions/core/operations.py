"""
Column operations for Mock Spark.

This module provides arithmetic, comparison, and logical operations
for MockColumn and MockColumnOperation classes.
"""

from typing import Any, Union
from .column import MockColumn, MockColumnOperation


class ColumnOperations:
    """Mixin class for column operations."""

    def __add__(self, other: Any) -> MockColumnOperation:
        """Addition operation."""
        if isinstance(other, MockColumn):
            return MockColumnOperation(self, "+", other)
        return MockColumnOperation(self, "+", other)

    def __sub__(self, other: Any) -> MockColumnOperation:
        """Subtraction operation."""
        if isinstance(other, MockColumn):
            return MockColumnOperation(self, "-", other)
        return MockColumnOperation(self, "-", other)

    def __mul__(self, other: Any) -> MockColumnOperation:
        """Multiplication operation."""
        if isinstance(other, MockColumn):
            return MockColumnOperation(self, "*", other)
        return MockColumnOperation(self, "*", other)

    def __truediv__(self, other: Any) -> MockColumnOperation:
        """Division operation."""
        if isinstance(other, MockColumn):
            return MockColumnOperation(self, "/", other)
        return MockColumnOperation(self, "/", other)

    def __mod__(self, other: Any) -> MockColumnOperation:
        """Modulo operation."""
        if isinstance(other, MockColumn):
            return MockColumnOperation(self, "%", other)
        return MockColumnOperation(self, "%", other)

    def __and__(self, other: Any) -> MockColumnOperation:
        """Logical AND operation."""
        if isinstance(other, MockColumn):
            return MockColumnOperation(self, "&", other)
        return MockColumnOperation(self, "&", other)

    def __or__(self, other: Any) -> MockColumnOperation:
        """Logical OR operation."""
        if isinstance(other, MockColumn):
            return MockColumnOperation(self, "|", other)
        return MockColumnOperation(self, "|", other)

    def __invert__(self) -> MockColumnOperation:
        """Logical NOT operation."""
        return MockColumnOperation(self, "!", None)

    def __neg__(self) -> MockColumnOperation:
        """Unary minus operation (-column)."""
        return MockColumnOperation(self, "-", None)

    def __eq__(self, other: Any) -> MockColumnOperation:  # type: ignore[override]
        """Equality comparison."""
        if isinstance(other, MockColumn):
            return MockColumnOperation(self, "==", other)
        return MockColumnOperation(self, "==", other)

    def __ne__(self, other: Any) -> MockColumnOperation:  # type: ignore[override]
        """Inequality comparison."""
        if isinstance(other, MockColumn):
            return MockColumnOperation(self, "!=", other)
        return MockColumnOperation(self, "!=", other)

    def __lt__(self, other: Any) -> MockColumnOperation:
        """Less than comparison."""
        if isinstance(other, MockColumn):
            return MockColumnOperation(self, "<", other)
        return MockColumnOperation(self, "<", other)

    def __le__(self, other: Any) -> MockColumnOperation:
        """Less than or equal comparison."""
        if isinstance(other, MockColumn):
            return MockColumnOperation(self, "<=", other)
        return MockColumnOperation(self, "<=", other)

    def __gt__(self, other: Any) -> MockColumnOperation:
        """Greater than comparison."""
        if isinstance(other, MockColumn):
            return MockColumnOperation(self, ">", other)
        return MockColumnOperation(self, ">", other)

    def __ge__(self, other: Any) -> MockColumnOperation:
        """Greater than or equal comparison."""
        if isinstance(other, MockColumn):
            return MockColumnOperation(self, ">=", other)
        return MockColumnOperation(self, ">=", other)


class ComparisonOperations:
    """Mixin class for comparison operations."""

    def isnull(self) -> MockColumnOperation:
        """Check if column value is null."""
        return MockColumnOperation(self, "isnull", None)

    def isnotnull(self) -> MockColumnOperation:
        """Check if column value is not null."""
        return MockColumnOperation(self, "isnotnull", None)

    def isNull(self) -> MockColumnOperation:
        """Check if column value is null (PySpark compatibility)."""
        return self.isnull()

    def isNotNull(self) -> MockColumnOperation:
        """Check if column value is not null (PySpark compatibility)."""
        return self.isnotnull()

    def isin(self, values: list) -> MockColumnOperation:
        """Check if column value is in list of values."""
        return MockColumnOperation(self, "isin", values)

    def between(self, lower: Any, upper: Any) -> MockColumnOperation:
        """Check if column value is between lower and upper bounds."""
        return MockColumnOperation(self, "between", (lower, upper))

    def like(self, pattern: str) -> MockColumnOperation:
        """SQL LIKE pattern matching."""
        return MockColumnOperation(self, "like", pattern)

    def rlike(self, pattern: str) -> MockColumnOperation:
        """Regular expression pattern matching."""
        return MockColumnOperation(self, "rlike", pattern)


class SortOperations:
    """Mixin class for sort operations."""

    def asc(self) -> MockColumnOperation:
        """Ascending sort order."""
        return MockColumnOperation(self, "asc", None)

    def desc(self) -> MockColumnOperation:
        """Descending sort order."""
        return MockColumnOperation(self, "desc", None)


class TypeOperations:
    """Mixin class for type operations."""

    def cast(self, data_type) -> MockColumnOperation:
        """Cast column to different data type."""
        return MockColumnOperation(self, "cast", data_type)


class ConditionalOperations:
    """Mixin class for conditional operations."""

    def when(self, condition: MockColumnOperation, value: Any):
        """Start a CASE WHEN expression."""
        from ..conditional import MockCaseWhen

        return MockCaseWhen(self, condition, value)

    def otherwise(self, value: Any):
        """End a CASE WHEN expression with default value."""
        from ..conditional import MockCaseWhen

        return MockCaseWhen(self, None, value)


class WindowOperations:
    """Mixin class for window operations."""

    def over(self, window_spec):
        """Apply window function over window specification."""
        from ..window_execution import MockWindowFunction

        return MockWindowFunction(self, window_spec)
