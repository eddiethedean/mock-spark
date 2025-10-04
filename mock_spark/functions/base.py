"""
Base function classes for Mock Spark.

This module contains the base classes and interfaces for all function types
including column expressions, built-in functions, and aggregate functions.
"""

from typing import Any, List, Union, Optional, Callable, TYPE_CHECKING
from dataclasses import dataclass
from mock_spark.spark_types import MockDataType, StringType

if TYPE_CHECKING:
    from mock_spark.window import MockWindowSpec


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

    def isnull(self) -> "MockColumnOperation":
        """Check if column value is null."""
        return MockColumnOperation(self, "isnull", None)

    def isnotnull(self) -> "MockColumnOperation":
        """Check if column value is not null."""
        return MockColumnOperation(self, "isnotnull", None)

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
        from .conditional import MockCaseWhen
        return MockCaseWhen(self, condition, value)

    def otherwise(self, value: Any) -> "MockCaseWhen":
        """End a CASE WHEN expression with default value."""
        from .conditional import MockCaseWhen
        return MockCaseWhen(self, None, value)

    def over(self, window_spec: "MockWindowSpec") -> "MockWindowFunction":
        """Apply window function over window specification."""
        from .window_execution import MockWindowFunction
        return MockWindowFunction(self, window_spec)


class MockColumnOperation:
    """Represents a column operation (comparison, arithmetic, etc.).

    This class encapsulates column operations and their operands for evaluation
    during DataFrame operations.
    """

    def __init__(self, column: MockColumn, operation: str, value: Any = None):
        """Initialize MockColumnOperation.

        Args:
            column: The column being operated on.
            operation: The operation being performed.
            value: The value or operand for the operation.
        """
        self.column = column
        self.operation = operation
        self.value = value
        self.name = self._generate_name()

    def _generate_name(self) -> str:
        """Generate a name for this operation."""
        if self.operation == "==":
            return f"{self.column.name} = {self.value}"
        elif self.operation == "!=":
            return f"{self.column.name} != {self.value}"
        elif self.operation == ">":
            return f"{self.column.name} > {self.value}"
        elif self.operation == ">=":
            return f"{self.column.name} >= {self.value}"
        elif self.operation == "<":
            return f"{self.column.name} < {self.value}"
        elif self.operation == "<=":
            return f"{self.column.name} <= {self.value}"
        elif self.operation == "+":
            return f"{self.column.name} + {self.value}"
        elif self.operation == "-":
            return f"{self.column.name} - {self.value}"
        elif self.operation == "*":
            return f"{self.column.name} * {self.value}"
        elif self.operation == "/":
            return f"{self.column.name} / {self.value}"
        elif self.operation == "%":
            return f"{self.column.name} % {self.value}"
        elif self.operation == "&":
            return f"{self.column.name} & {self.value}"
        elif self.operation == "|":
            return f"{self.column.name} | {self.value}"
        elif self.operation == "!":
            return f"!{self.column.name}"
        elif self.operation == "isnull":
            return f"{self.column.name} IS NULL"
        elif self.operation == "isnotnull":
            return f"{self.column.name} IS NOT NULL"
        elif self.operation == "isin":
            return f"{self.column.name} IN ({', '.join(map(str, self.value))})"
        elif self.operation == "between":
            return f"{self.column.name} BETWEEN {self.value[0]} AND {self.value[1]}"
        elif self.operation == "like":
            return f"{self.column.name} LIKE '{self.value}'"
        elif self.operation == "rlike":
            return f"{self.column.name} RLIKE '{self.value}'"
        elif self.operation == "asc":
            return f"{self.column.name} ASC"
        elif self.operation == "desc":
            return f"{self.column.name} DESC"
        elif self.operation == "cast":
            return f"CAST({self.column.name} AS {self.value})"
        else:
            return f"{self.column.name} {self.operation} {self.value}"

    def over(self, window_spec) -> "MockColumnOperation":
        """Apply window function over window specification."""
        from .window_execution import MockWindowFunction
        return MockWindowFunction(self, window_spec)

    def alias(self, name: str) -> "MockColumnOperation":
        """Create an alias for this operation.

        Args:
            name: The alias name.

        Returns:
            Self for method chaining.
        """
        self.name = name
        return self


class MockLiteral:
    """Represents a literal value in column expressions.

    This class encapsulates literal values that can be used in DataFrame operations.
    """

    def __init__(self, value: Any, data_type: Optional[MockDataType] = None):
        """Initialize MockLiteral.

        Args:
            value: The literal value.
            data_type: Optional data type. Inferred if not specified.
        """
        self.value = value
        self.data_type = data_type or self._infer_type(value)
        self.name = f"lit({value})"

    @property
    def column_type(self) -> MockDataType:
        """Get the column type (alias for data_type for compatibility)."""
        return self.data_type

    def _infer_type(self, value: Any) -> MockDataType:
        """Infer the data type from the value."""
        from mock_spark.spark_types import StringType, IntegerType, LongType, DoubleType, BooleanType
        
        if isinstance(value, bool):
            return BooleanType()
        elif isinstance(value, int):
            if -2147483648 <= value <= 2147483647:
                return IntegerType()
            else:
                return LongType()
        elif isinstance(value, float):
            return DoubleType()
        elif isinstance(value, str):
            return StringType()
        else:
            return StringType()

    def __eq__(self, other: Any) -> "MockColumnOperation":
        """Equality comparison."""
        return MockColumnOperation(self, "==", other)

    def __ne__(self, other: Any) -> "MockColumnOperation":
        """Inequality comparison."""
        return MockColumnOperation(self, "!=", other)

    def __lt__(self, other: Any) -> "MockColumnOperation":
        """Less than comparison."""
        return MockColumnOperation(self, "<", other)

    def __le__(self, other: Any) -> "MockColumnOperation":
        """Less than or equal comparison."""
        return MockColumnOperation(self, "<=", other)

    def __gt__(self, other: Any) -> "MockColumnOperation":
        """Greater than comparison."""
        return MockColumnOperation(self, ">", other)

    def __ge__(self, other: Any) -> "MockColumnOperation":
        """Greater than or equal comparison."""
        return MockColumnOperation(self, ">=", other)

    def __add__(self, other: Any) -> "MockColumnOperation":
        """Addition operation."""
        return MockColumnOperation(self, "+", other)

    def __sub__(self, other: Any) -> "MockColumnOperation":
        """Subtraction operation."""
        return MockColumnOperation(self, "-", other)

    def __mul__(self, other: Any) -> "MockColumnOperation":
        """Multiplication operation."""
        return MockColumnOperation(self, "*", other)

    def __truediv__(self, other: Any) -> "MockColumnOperation":
        """Division operation."""
        return MockColumnOperation(self, "/", other)

    def __mod__(self, other: Any) -> "MockColumnOperation":
        """Modulo operation."""
        return MockColumnOperation(self, "%", other)

    def alias(self, name: str) -> "MockLiteral":
        """Create an alias for this literal.

        Args:
            name: The alias name.

        Returns:
            Self for method chaining.
        """
        self.name = name
        return self


class MockAggregateFunction:
    """Base class for aggregate functions.

    This class provides the base functionality for all aggregate functions
    including count, sum, avg, max, min, etc.
    """

    def __init__(self, column: Union[MockColumn, str, None], function_name: str, data_type: Optional[MockDataType] = None):
        """Initialize MockAggregateFunction.

        Args:
            column: The column to aggregate (None for count(*)).
            function_name: Name of the aggregate function.
            data_type: Optional return data type.
        """
        self.column = column
        self.function_name = function_name
        self.data_type = data_type or StringType()
        self.name = self._generate_name()

    @property
    def column_name(self) -> str:
        """Get the column name for compatibility."""
        if self.column is None:
            return "*"
        elif isinstance(self.column, str):
            return self.column
        else:
            return self.column.name

    def _generate_name(self) -> str:
        """Generate a name for this aggregate function."""
        if self.column is None:
            return f"{self.function_name}(*)"
        elif isinstance(self.column, str):
            return f"{self.function_name}({self.column})"
        else:
            return f"{self.function_name}({self.column.name})"

    def evaluate(self, data: List[dict]) -> Any:
        """Evaluate the aggregate function on the given data.

        Args:
            data: List of data rows to aggregate.

        Returns:
            The aggregated result.
        """
        if self.function_name == "count":
            return self._evaluate_count(data)
        elif self.function_name == "sum":
            return self._evaluate_sum(data)
        elif self.function_name == "avg":
            return self._evaluate_avg(data)
        elif self.function_name == "max":
            return self._evaluate_max(data)
        elif self.function_name == "min":
            return self._evaluate_min(data)
        else:
            return None

    def _evaluate_count(self, data: List[dict]) -> int:
        """Evaluate count function."""
        if self.column is None:
            return len(data)
        else:
            column_name = self.column if isinstance(self.column, str) else self.column.name
            return sum(1 for row in data if row.get(column_name) is not None)

    def _evaluate_sum(self, data: List[dict]) -> Any:
        """Evaluate sum function."""
        if self.column is None:
            return 0
        
        column_name = self.column if isinstance(self.column, str) else self.column.name
        total = 0
        for row in data:
            value = row.get(column_name)
            if value is not None:
                total += value
        return total

    def _evaluate_avg(self, data: List[dict]) -> Any:
        """Evaluate average function."""
        if self.column is None:
            return 0.0
        
        column_name = self.column if isinstance(self.column, str) else self.column.name
        values = [row.get(column_name) for row in data if row.get(column_name) is not None]
        if values:
            return sum(values) / len(values)
        else:
            return None

    def _evaluate_max(self, data: List[dict]) -> Any:
        """Evaluate max function."""
        if self.column is None:
            return None
        
        column_name = self.column if isinstance(self.column, str) else self.column.name
        values = [row.get(column_name) for row in data if row.get(column_name) is not None]
        if values:
            return max(values)
        else:
            return None

    def _evaluate_min(self, data: List[dict]) -> Any:
        """Evaluate min function."""
        if self.column is None:
            return None
        
        column_name = self.column if isinstance(self.column, str) else self.column.name
        values = [row.get(column_name) for row in data if row.get(column_name) is not None]
        if values:
            return min(values)
        else:
            return None

    def over(self, window_spec) -> "MockWindowFunction":
        """Apply window function over window specification."""
        from .window_execution import MockWindowFunction
        return MockWindowFunction(self, window_spec)

    def alias(self, name: str) -> "MockAggregateFunction":
        """Create an alias for this aggregate function.

        Args:
            name: The alias name.

        Returns:
            Self for method chaining.
        """
        self.name = name
        return self
