"""
Literal values for Mock Spark.

This module provides MockLiteral class for representing literal values
in column expressions and transformations.
"""

from typing import Any, Optional, TYPE_CHECKING
from ...spark_types import MockDataType, StringType
from ...core.interfaces.functions import IColumn

if TYPE_CHECKING:
    from .operations import MockColumnOperation


class MockLiteral(IColumn):
    """Mock literal value for DataFrame operations.

    Represents a literal value that can be used in column expressions
    and transformations, maintaining compatibility with PySpark's lit function.
    """

    def __init__(self, value: Any, data_type: Optional[MockDataType] = None):
        """Initialize MockLiteral.

        Args:
            value: The literal value.
            data_type: Optional data type. Inferred from value if not specified.
        """
        self.value = value
        self.data_type = data_type or self._infer_type(value)
        self.column_type = self.data_type  # Add column_type attribute for compatibility
        # Use the actual value as column name for PySpark compatibility
        # Handle boolean values to match PySpark's lowercase representation
        if isinstance(value, bool):
            self._name = str(value).lower()
        else:
            self._name = str(value)

    @property
    def name(self) -> str:
        """Get literal name."""
        return self._name

    @name.setter
    def name(self, value: str) -> None:
        """Set literal name."""
        self._name = value

    def _infer_type(self, value: Any) -> MockDataType:
        """Infer data type from value.

        Args:
            value: The value to infer type for.

        Returns:
            Inferred MockDataType.
        """
        if value is None:
            return StringType()
        elif isinstance(value, bool):
            from ...spark_types import BooleanType

            return BooleanType(nullable=False)
        elif isinstance(value, int):
            from ...spark_types import IntegerType

            return IntegerType(nullable=False)
        elif isinstance(value, float):
            from ...spark_types import DoubleType

            return DoubleType(nullable=False)
        elif isinstance(value, str):
            return StringType(nullable=False)
        elif isinstance(value, list):
            from ...spark_types import ArrayType

            if value:
                element_type = self._infer_type(value[0])
            else:
                element_type = StringType()
            return ArrayType(element_type)
        elif isinstance(value, dict):
            from ...spark_types import MapType

            return MapType(StringType(), StringType())
        else:
            return StringType()

    def __eq__(self, other: Any) -> "MockColumnOperation":  # type: ignore[override]
        """Equality comparison.

        Note: Returns MockColumnOperation instead of bool for PySpark compatibility.
        """
        from .column import MockColumnOperation

        return MockColumnOperation(self, "==", other)

    def __ne__(self, other: Any) -> "MockColumnOperation":  # type: ignore[override]
        """Inequality comparison.

        Note: Returns MockColumnOperation instead of bool for PySpark compatibility.
        """
        from .column import MockColumnOperation

        return MockColumnOperation(self, "!=", other)

    def __lt__(self, other: Any) -> "MockColumnOperation":
        """Less than comparison."""
        from .column import MockColumnOperation

        return MockColumnOperation(self, "<", other)

    def __le__(self, other: Any) -> "MockColumnOperation":
        """Less than or equal comparison."""
        from .column import MockColumnOperation

        return MockColumnOperation(self, "<=", other)

    def __gt__(self, other: Any) -> "MockColumnOperation":
        """Greater than comparison."""
        from .column import MockColumnOperation

        return MockColumnOperation(self, ">", other)

    def __ge__(self, other: Any) -> "MockColumnOperation":
        """Greater than or equal comparison."""
        from .column import MockColumnOperation

        return MockColumnOperation(self, ">=", other)

    def __add__(self, other: Any) -> "MockColumnOperation":
        """Addition operation."""
        from .column import MockColumnOperation

        return MockColumnOperation(self, "+", other)

    def __sub__(self, other: Any) -> "MockColumnOperation":
        """Subtraction operation."""
        from .column import MockColumnOperation

        return MockColumnOperation(self, "-", other)

    def __mul__(self, other: Any) -> "MockColumnOperation":
        """Multiplication operation."""
        from .column import MockColumnOperation

        return MockColumnOperation(self, "*", other)

    def __truediv__(self, other: Any) -> "MockColumnOperation":
        """Division operation."""
        from .column import MockColumnOperation

        return MockColumnOperation(self, "/", other)

    def __mod__(self, other: Any) -> "MockColumnOperation":
        """Modulo operation."""
        from .column import MockColumnOperation

        return MockColumnOperation(self, "%", other)

    def __and__(self, other: Any) -> "MockColumnOperation":
        """Logical AND operation."""
        from .column import MockColumnOperation

        return MockColumnOperation(self, "&", other)

    def __or__(self, other: Any) -> "MockColumnOperation":
        """Logical OR operation."""
        from .column import MockColumnOperation

        return MockColumnOperation(self, "|", other)

    def __invert__(self) -> "MockColumnOperation":
        """Logical NOT operation."""
        from .column import MockColumnOperation

        return MockColumnOperation(self, "!", None)

    def __neg__(self) -> "MockColumnOperation":
        """Unary minus operation (-literal)."""
        from .column import MockColumnOperation

        return MockColumnOperation(self, "-", None)

    def isnull(self) -> "MockColumnOperation":
        """Check if literal value is null."""
        from .column import MockColumnOperation

        return MockColumnOperation(self, "isnull", None)

    def isnotnull(self) -> "MockColumnOperation":
        """Check if literal value is not null."""
        from .column import MockColumnOperation

        return MockColumnOperation(self, "isnotnull", None)

    def isNull(self) -> "MockColumnOperation":
        """Check if literal value is null (PySpark compatibility)."""
        return self.isnull()

    def isNotNull(self) -> "MockColumnOperation":
        """Check if literal value is not null (PySpark compatibility)."""
        return self.isnotnull()

    def isin(self, values: list) -> "MockColumnOperation":
        """Check if literal value is in list of values."""
        from .column import MockColumnOperation

        return MockColumnOperation(self, "isin", values)

    def between(self, lower: Any, upper: Any) -> "MockColumnOperation":
        """Check if literal value is between lower and upper bounds."""
        from .column import MockColumnOperation

        return MockColumnOperation(self, "between", (lower, upper))

    def like(self, pattern: str) -> "MockColumnOperation":
        """SQL LIKE pattern matching."""
        from .column import MockColumnOperation

        return MockColumnOperation(self, "like", pattern)

    def rlike(self, pattern: str) -> "MockColumnOperation":
        """Regular expression pattern matching."""
        from .column import MockColumnOperation

        return MockColumnOperation(self, "rlike", pattern)

    def alias(self, name: str) -> "MockLiteral":
        """Create an alias for the literal."""
        aliased_literal = MockLiteral(self.value, self.data_type)
        aliased_literal._name = name
        return aliased_literal

    def asc(self) -> "MockColumnOperation":
        """Ascending sort order."""
        from .column import MockColumnOperation

        return MockColumnOperation(self, "asc", None)

    def desc(self) -> "MockColumnOperation":
        """Descending sort order."""
        from .column import MockColumnOperation

        return MockColumnOperation(self, "desc", None)

    def cast(self, data_type: MockDataType) -> "MockColumnOperation":
        """Cast literal to different data type."""
        from .column import MockColumnOperation

        return MockColumnOperation(self, "cast", data_type)

    def when(self, condition: "MockColumnOperation", value: Any) -> Any:
        """Start a CASE WHEN expression."""
        from ..conditional import MockCaseWhen

        return MockCaseWhen(self, condition, value)

    def otherwise(self, value: Any) -> Any:
        """End a CASE WHEN expression with default value."""
        from ..conditional import MockCaseWhen

        return MockCaseWhen(self, None, value)

    def over(self, window_spec: Any) -> Any:
        """Apply window function over window specification."""
        from ..window_execution import MockWindowFunction

        return MockWindowFunction(self, window_spec)
