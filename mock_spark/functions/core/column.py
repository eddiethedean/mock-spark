"""
Column implementation for Mock Spark.

This module provides the MockColumn class for DataFrame column operations,
maintaining compatibility with PySpark's Column interface.
"""

from typing import Any, List, Union, Optional, TYPE_CHECKING
from ...spark_types import MockDataType, StringType
from ...core.interfaces.functions import IColumn

if TYPE_CHECKING:
    from ...window import MockWindowSpec
    from ..conditional import MockCaseWhen
    from ..window_execution import MockWindowFunction


class MockColumn:
    """Mock column expression for DataFrame operations.

    Provides a PySpark-compatible column expression that supports all comparison
    and logical operations. Used for creating complex DataFrame transformations
    and filtering conditions.
    """

    def __init__(self, name: str, column_type: Optional[MockDataType] = None):
        """Initialize MockColumn.

        Args:
            name: Column name.
            column_type: Optional data type. Defaults to StringType if not specified.
        """
        self._name = name
        self._original_column: Optional["MockColumn"] = None
        self._alias_name: Optional[str] = None
        self.column_name = name
        self.column_type = column_type or StringType()
        self.operation = None
        self.operand = None
        self._operations: List["MockColumnOperation"] = []
        # Add expr attribute for PySpark compatibility
        self.expr = f"MockColumn('{name}')"

    @property
    def name(self) -> str:
        """Get the column name (alias if set, otherwise original name)."""
        if hasattr(self, "_alias_name") and self._alias_name is not None:
            return self._alias_name
        return self._name

    @property
    def original_column(self) -> "MockColumn":
        """Get the original column (for aliased columns)."""
        return getattr(self, "_original_column", self)

    def __eq__(self, other: Any) -> "MockColumnOperation":  # type: ignore[override]
        """Equality comparison."""
        if isinstance(other, MockColumn):
            return MockColumnOperation(self, "==", other)
        return MockColumnOperation(self, "==", other)

    def __hash__(self) -> int:
        """Hash method to make MockColumn hashable."""
        return hash((self.name, self.column_type))

    def __ne__(self, other: Any) -> "MockColumnOperation":  # type: ignore[override]
        """Inequality comparison."""
        if isinstance(other, MockColumn):
            return MockColumnOperation(self, "!=", other)
        return MockColumnOperation(self, "!=", other)

    def __lt__(self, other: Any) -> "MockColumnOperation":
        """Less than comparison."""
        if isinstance(other, MockColumn):
            return MockColumnOperation(self, "<", other)
        return MockColumnOperation(self, "<", other)

    def __le__(self, other: Any) -> "MockColumnOperation":
        """Less than or equal comparison."""
        if isinstance(other, MockColumn):
            return MockColumnOperation(self, "<=", other)
        return MockColumnOperation(self, "<=", other)

    def __gt__(self, other: Any) -> "MockColumnOperation":
        """Greater than comparison."""
        if isinstance(other, MockColumn):
            return MockColumnOperation(self, ">", other)
        return MockColumnOperation(self, ">", other)

    def __ge__(self, other: Any) -> "MockColumnOperation":
        """Greater than or equal comparison."""
        if isinstance(other, MockColumn):
            return MockColumnOperation(self, ">=", other)
        return MockColumnOperation(self, ">=", other)

    def __add__(self, other: Any) -> "MockColumnOperation":
        """Addition operation."""
        if isinstance(other, MockColumn):
            return MockColumnOperation(self, "+", other)
        return MockColumnOperation(self, "+", other)

    def __sub__(self, other: Any) -> "MockColumnOperation":
        """Subtraction operation."""
        if isinstance(other, MockColumn):
            return MockColumnOperation(self, "-", other)
        return MockColumnOperation(self, "-", other)

    def __mul__(self, other: Any) -> "MockColumnOperation":
        """Multiplication operation."""
        if isinstance(other, MockColumn):
            return MockColumnOperation(self, "*", other)
        return MockColumnOperation(self, "*", other)

    def __truediv__(self, other: Any) -> "MockColumnOperation":
        """Division operation."""
        if isinstance(other, MockColumn):
            return MockColumnOperation(self, "/", other)
        return MockColumnOperation(self, "/", other)

    def __mod__(self, other: Any) -> "MockColumnOperation":
        """Modulo operation."""
        if isinstance(other, MockColumn):
            return MockColumnOperation(self, "%", other)
        return MockColumnOperation(self, "%", other)

    def __and__(self, other: Any) -> "MockColumnOperation":
        """Logical AND operation."""
        if isinstance(other, MockColumn):
            return MockColumnOperation(self, "&", other)
        return MockColumnOperation(self, "&", other)

    def __or__(self, other: Any) -> "MockColumnOperation":
        """Logical OR operation."""
        if isinstance(other, MockColumn):
            return MockColumnOperation(self, "|", other)
        return MockColumnOperation(self, "|", other)

    def __invert__(self) -> "MockColumnOperation":
        """Logical NOT operation."""
        return MockColumnOperation(self, "!", None)

    def __neg__(self) -> "MockColumnOperation":
        """Unary minus operation (-column)."""
        return MockColumnOperation(self, "-", None)

    def isnull(self) -> "MockColumnOperation":
        """Check if column value is null."""
        return MockColumnOperation(self, "isnull", None)

    def isnotnull(self) -> "MockColumnOperation":
        """Check if column value is not null."""
        return MockColumnOperation(self, "isnotnull", None)

    def isNull(self) -> "MockColumnOperation":
        """Check if column value is null (PySpark compatibility)."""
        return self.isnull()

    def isNotNull(self) -> "MockColumnOperation":
        """Check if column value is not null (PySpark compatibility)."""
        return self.isnotnull()

    def isin(self, values: List[Any]) -> "MockColumnOperation":
        """Check if column value is in list of values."""
        return MockColumnOperation(self, "isin", values)

    def between(self, lower: Any, upper: Any) -> "MockColumnOperation":
        """Check if column value is between lower and upper bounds."""
        return MockColumnOperation(self, "between", (lower, upper))

    def like(self, pattern: str) -> "MockColumnOperation":
        """SQL LIKE pattern matching."""
        return MockColumnOperation(self, "like", pattern)

    def rlike(self, pattern: str) -> "MockColumnOperation":
        """Regular expression pattern matching."""
        return MockColumnOperation(self, "rlike", pattern)

    def alias(self, name: str) -> "MockColumn":
        """Create an alias for the column."""
        aliased_column = MockColumn(name, self.column_type)
        aliased_column._original_column = self
        aliased_column._alias_name = name
        return aliased_column

    def asc(self) -> "MockColumnOperation":
        """Ascending sort order."""
        return MockColumnOperation(self, "asc", None)

    def desc(self) -> "MockColumnOperation":
        """Descending sort order."""
        return MockColumnOperation(self, "desc", None)

    def cast(self, data_type: MockDataType) -> "MockColumnOperation":
        """Cast column to different data type."""
        return MockColumnOperation(self, "cast", data_type)

    def when(self, condition: "MockColumnOperation", value: Any) -> "MockCaseWhen":
        """Start a CASE WHEN expression."""
        from ..conditional import MockCaseWhen

        return MockCaseWhen(self, condition, value)

    def otherwise(self, value: Any) -> "MockCaseWhen":
        """End a CASE WHEN expression with default value."""
        from ..conditional import MockCaseWhen

        return MockCaseWhen(self, None, value)

    def over(self, window_spec: "MockWindowSpec") -> "MockWindowFunction":
        """Apply window function over window specification."""
        from ..window_execution import MockWindowFunction

        return MockWindowFunction(self, window_spec)


class MockColumnOperation(IColumn):
    """Represents a column operation (comparison, arithmetic, etc.).

    This class encapsulates column operations and their operands for evaluation
    during DataFrame operations.
    """

    def __init__(
        self,
        column: Union[MockColumn, "MockColumnOperation", "IColumn", None],
        operation: str,
        value: Any = None,
        name: Optional[str] = None,
    ):
        """Initialize MockColumnOperation.

        Args:
            column: The column being operated on (can be None for some operations).
            operation: The operation being performed.
            value: The value or operand for the operation.
            name: Optional custom name for the operation.
        """
        self.column = column
        self.operation = operation
        self.value = value
        self._name = name or self._generate_name()
        self.function_name = operation

    @property
    def name(self) -> str:
        """Get column name."""
        return self._name

    @name.setter
    def name(self, value: str) -> None:
        """Set column name."""
        self._name = value

    def _generate_name(self) -> str:
        """Generate a name for this operation."""
        # Extract value from MockLiteral if needed
        if hasattr(self.value, "value") and hasattr(self.value, "data_type"):
            # This is a MockLiteral
            value_str = str(self.value.value)
        else:
            value_str = str(self.value)

        if self.operation == "==":
            return f"{self.column.name} = {value_str}"
        elif self.operation == "!=":
            return f"{self.column.name} != {value_str}"
        elif self.operation == "<":
            return f"{self.column.name} < {value_str}"
        elif self.operation == "<=":
            return f"{self.column.name} <= {value_str}"
        elif self.operation == ">":
            return f"{self.column.name} > {value_str}"
        elif self.operation == ">=":
            return f"{self.column.name} >= {value_str}"
        elif self.operation == "+":
            return f"({self.column.name} + {value_str})"
        elif self.operation == "-":
            return f"({self.column.name} - {value_str})"
        elif self.operation == "*":
            return f"({self.column.name} * {value_str})"
        elif self.operation == "/":
            return f"({self.column.name} / {value_str})"
        elif self.operation == "%":
            return f"({self.column.name} % {value_str})"
        elif self.operation == "&":
            return f"({self.column.name} & {value_str})"
        elif self.operation == "|":
            return f"({self.column.name} | {value_str})"
        elif self.operation == "!":
            return f"!{self.column.name}"
        elif self.operation == "isnull":
            return f"{self.column.name} IS NULL"
        elif self.operation == "isnotnull":
            return f"{self.column.name} IS NOT NULL"
        elif self.operation == "isin":
            return f"{self.column.name} IN {self.value}"
        elif self.operation == "between":
            return f"{self.column.name} BETWEEN {self.value[0]} AND {self.value[1]}"
        elif self.operation == "like":
            return f"{self.column.name} LIKE {self.value}"
        elif self.operation == "rlike":
            return f"{self.column.name} RLIKE {self.value}"
        elif self.operation == "asc":
            return f"{self.column.name} ASC"
        elif self.operation == "desc":
            return f"{self.column.name} DESC"
        elif self.operation == "cast":
            return f"CAST({self.column.name} AS {self.value})"
        else:
            return f"{self.column.name} {self.operation} {self.value}"

    def alias(self, name: str) -> "MockColumnOperation":
        """Create an alias for this operation."""
        aliased_operation = MockColumnOperation(self.column, self.operation, self.value, name)
        return aliased_operation

    def __eq__(self, other: Any) -> "MockColumnOperation":
        """Equality comparison."""
        if isinstance(other, MockColumnOperation):
            return MockColumnOperation(self, "==", other)
        return MockColumnOperation(self, "==", other)

    def __ne__(self, other: Any) -> "MockColumnOperation":
        """Inequality comparison."""
        if isinstance(other, MockColumnOperation):
            return MockColumnOperation(self, "!=", other)
        return MockColumnOperation(self, "!=", other)

    def __lt__(self, other: Any) -> "MockColumnOperation":
        """Less than comparison."""
        if isinstance(other, MockColumnOperation):
            return MockColumnOperation(self, "<", other)
        return MockColumnOperation(self, "<", other)

    def __le__(self, other: Any) -> "MockColumnOperation":
        """Less than or equal comparison."""
        if isinstance(other, MockColumnOperation):
            return MockColumnOperation(self, "<=", other)
        return MockColumnOperation(self, "<=", other)

    def __gt__(self, other: Any) -> "MockColumnOperation":
        """Greater than comparison."""
        if isinstance(other, MockColumnOperation):
            return MockColumnOperation(self, ">", other)
        return MockColumnOperation(self, ">", other)

    def __ge__(self, other: Any) -> "MockColumnOperation":
        """Greater than or equal comparison."""
        if isinstance(other, MockColumnOperation):
            return MockColumnOperation(self, ">=", other)
        return MockColumnOperation(self, ">=", other)

    def __add__(self, other: Any) -> "MockColumnOperation":
        """Addition operation."""
        if isinstance(other, MockColumnOperation):
            return MockColumnOperation(self, "+", other)
        return MockColumnOperation(self, "+", other)

    def __sub__(self, other: Any) -> "MockColumnOperation":
        """Subtraction operation."""
        if isinstance(other, MockColumnOperation):
            return MockColumnOperation(self, "-", other)
        return MockColumnOperation(self, "-", other)

    def __mul__(self, other: Any) -> "MockColumnOperation":
        """Multiplication operation."""
        if isinstance(other, MockColumnOperation):
            return MockColumnOperation(self, "*", other)
        return MockColumnOperation(self, "*", other)

    def __truediv__(self, other: Any) -> "MockColumnOperation":
        """Division operation."""
        if isinstance(other, MockColumnOperation):
            return MockColumnOperation(self, "/", other)
        return MockColumnOperation(self, "/", other)

    def __mod__(self, other: Any) -> "MockColumnOperation":
        """Modulo operation."""
        if isinstance(other, MockColumnOperation):
            return MockColumnOperation(self, "%", other)
        return MockColumnOperation(self, "%", other)

    def __and__(self, other: Any) -> "MockColumnOperation":
        """Logical AND operation."""
        if isinstance(other, MockColumnOperation):
            return MockColumnOperation(self, "&", other)
        return MockColumnOperation(self, "&", other)

    def __or__(self, other: Any) -> "MockColumnOperation":
        """Logical OR operation."""
        if isinstance(other, MockColumnOperation):
            return MockColumnOperation(self, "|", other)
        return MockColumnOperation(self, "|", other)

    def __invert__(self) -> "MockColumnOperation":
        """Logical NOT operation."""
        return MockColumnOperation(self, "!", None)

    def __neg__(self) -> "MockColumnOperation":
        """Unary minus operation (-operation)."""
        return MockColumnOperation(self, "-", None)

    def isnull(self) -> "MockColumnOperation":
        """Check if operation result is null."""
        return MockColumnOperation(self, "isnull", None)

    def isnotnull(self) -> "MockColumnOperation":
        """Check if operation result is not null."""
        return MockColumnOperation(self, "isnotnull", None)

    def isNull(self) -> "MockColumnOperation":
        """Check if operation result is null (PySpark compatibility)."""
        return self.isnull()

    def isNotNull(self) -> "MockColumnOperation":
        """Check if operation result is not null (PySpark compatibility)."""
        return self.isnotnull()

    def isin(self, values: List[Any]) -> "MockColumnOperation":
        """Check if operation result is in list of values."""
        return MockColumnOperation(self, "isin", values)

    def between(self, lower: Any, upper: Any) -> "MockColumnOperation":
        """Check if operation result is between lower and upper bounds."""
        return MockColumnOperation(self, "between", (lower, upper))

    def like(self, pattern: str) -> "MockColumnOperation":
        """SQL LIKE pattern matching."""
        return MockColumnOperation(self, "like", pattern)

    def rlike(self, pattern: str) -> "MockColumnOperation":
        """Regular expression pattern matching."""
        return MockColumnOperation(self, "rlike", pattern)

    def alias(self, name: str) -> "MockColumnOperation":
        """Create an alias for this operation."""
        aliased_operation = MockColumnOperation(self.column, self.operation, self.value, name)
        return aliased_operation

    def asc(self) -> "MockColumnOperation":
        """Ascending sort order."""
        return MockColumnOperation(self, "asc", None)

    def desc(self) -> "MockColumnOperation":
        """Descending sort order."""
        return MockColumnOperation(self, "desc", None)

    def cast(self, data_type: MockDataType) -> "MockColumnOperation":
        """Cast operation result to different data type."""
        return MockColumnOperation(self, "cast", data_type)

    def when(self, condition: "MockColumnOperation", value: Any) -> "MockCaseWhen":
        """Start a CASE WHEN expression."""
        from ..conditional import MockCaseWhen

        return MockCaseWhen(self, condition, value)

    def otherwise(self, value: Any) -> "MockCaseWhen":
        """End a CASE WHEN expression with default value."""
        from ..conditional import MockCaseWhen

        return MockCaseWhen(self, None, value)

    def over(self, window_spec: "MockWindowSpec") -> "MockWindowFunction":
        """Apply window function over window specification."""
        from ..window_execution import MockWindowFunction

        return MockWindowFunction(self, window_spec)
