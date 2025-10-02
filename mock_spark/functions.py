"""
Mock functions for Mock Spark.

This module provides comprehensive mock implementations of PySpark functions
that behave identically to the real PySpark functions. It includes column
expressions, built-in functions, and complex operations for DataFrame
manipulation and analysis.

Key Features:
    - Complete PySpark F.* function compatibility
    - Column expressions with all comparison and logical operations
    - Built-in functions (coalesce, isnull, upper, lower, length, abs, round)
    - Window functions (row_number, rank, etc.)
    - Aggregate functions (count, sum, avg, max, min)
    - Literal value support with proper type inference

Example:
    >>> from mock_spark import F
    >>> # Column expressions
    >>> F.col("name") == "Alice"
    >>> F.col("age") > 25
    >>> # Built-in functions
    >>> F.upper(F.col("name"))
    >>> F.coalesce(F.col("name"), F.lit("Unknown"))
    >>> # Window functions
    >>> F.row_number().over(window)
"""

from typing import Any, List, Union, Optional, Callable, TYPE_CHECKING
from dataclasses import dataclass
from .spark_types import MockDataType, StringType

if TYPE_CHECKING:
    from .window import MockWindowSpec


class MockColumn:
    """Mock column expression for DataFrame operations.

    Provides a PySpark-compatible column expression that supports all comparison
    and logical operations. Used for creating complex DataFrame transformations
    and filtering conditions.

    Attributes:
        _name: Original column name.
        _original_column: Reference to original column for aliasing.
        _alias_name: Alias name if set.
        column_name: Column name for compatibility.
        column_type: Data type of the column.
        operation: Current operation being performed.
        operand: Operand for the operation.
        _operations: List of chained operations.
        expr: String expression for PySpark compatibility.

    Example:
        >>> col = F.col("age")
        >>> col > 25
        >>> col == "Alice"
        >>> col.alias("user_age")
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
        self._operations: List[MockColumnOperation] = []
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

    def __and__(self, other: Any) -> "MockColumnOperation":
        """Logical AND."""
        return MockColumnOperation(self, "and", other)

    def __or__(self, other: Any) -> "MockColumnOperation":
        """Logical OR."""
        if isinstance(other, MockColumnOperation):
            return MockColumnOperation(self, "or", other)
        return MockColumnOperation(self, "or", other)

    def __invert__(self) -> "MockColumnOperation":
        """Logical NOT."""
        return MockColumnOperation(self, "not", None)

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

    def desc(self) -> "MockColumnOperation":
        """Descending order."""
        return MockColumnOperation(self, "desc", None)

    def asc(self) -> "MockColumnOperation":
        """Ascending order."""
        return MockColumnOperation(self, "asc", None)

    def isNull(self) -> "MockColumnOperation":
        """Check if column is null."""
        return MockColumnOperation(self, "isNull", None)

    def isNotNull(self) -> "MockColumnOperation":
        """Check if column is not null."""
        return MockColumnOperation(self, "isNotNull", None)

    def like(self, pattern: str) -> "MockColumnOperation":
        """SQL LIKE pattern matching."""
        return MockColumnOperation(self, "like", pattern)

    def rlike(self, pattern: str) -> "MockColumnOperation":
        """Regex pattern matching."""
        return MockColumnOperation(self, "rlike", pattern)

    def alias(self, name: str) -> "MockColumn":
        """Create an alias for the column.

        Args:
            name: Alias name for the column.

        Returns:
            New MockColumn instance with the alias name.

        Example:
            >>> F.col("user_name").alias("name")
        """
        result = MockColumn(self.name)
        result._original_column = self  # Store reference to original
        result._alias_name = name
        return result

    def isin(self, values: List[Any]) -> "MockColumnOperation":
        """Check if column value is in list."""
        return MockColumnOperation(self, "isin", values)

    def between(self, lower: Any, upper: Any) -> "MockColumnOperation":
        """Check if column value is between bounds."""
        return MockColumnOperation(self, "between", (lower, upper))

    def cast(self, data_type: Any) -> "MockColumnOperation":
        """Cast column to data type."""
        return MockColumnOperation(self, "cast", data_type)

    def when(self, condition: Any, value: Any) -> "MockColumnOperation":
        """CASE WHEN condition."""
        return MockColumnOperation(self, "when", (condition, value))

    def otherwise(self, value: Any) -> "MockColumnOperation":
        """CASE WHEN ... ELSE."""
        return MockColumnOperation(self, "otherwise", value)

    def __repr__(self) -> str:
        return f"MockColumn('{self.name}')"

    def __hash__(self) -> int:
        """Hash for use in sets and dictionaries."""
        return hash((self._name, self._alias_name))


class MockColumnOperation:
    """Represents a column operation."""

    name: str  # Type annotation for mypy

    def __init__(
        self, column: Union[MockColumn, "MockColumnOperation"], operation: str, value: Any = None
    ):
        self.column = column
        self.operation = operation
        self.value = value
        self.precision: Optional[int] = None  # For round function
        # Add operand attribute for compatibility
        self.operand = self.value
        # Add name property for compatibility with select method
        # Store the name for the operation
        column_name: str = self.column.name if hasattr(self.column, "name") else str(self.column)
        if operation in ["+", "-", "*", "/", "%"]:
            self.name = f"({column_name} {operation} {value})"
        elif operation in ["upper", "lower", "length"]:
            self.name = f"{operation}({column_name})"
        else:
            self.name = f"{operation}({column_name})"
        if self.value is None:
            self.expr = f"MockColumnOperation({self.column}, '{self.operation}')"
        else:
            self.expr = f"MockColumnOperation({self.column}, '{self.operation}', {self.value})"

    def __and__(self, other: Any) -> "MockColumnOperation":
        """Logical AND."""
        return MockColumnOperation(self, "and", other)

    def __or__(self, other: Any) -> "MockColumnOperation":
        """Logical OR."""
        return MockColumnOperation(self, "or", other)

    def __invert__(self) -> "MockColumnOperation":
        """Logical NOT."""
        return MockColumnOperation(self.column, "not", None)

    def alias(self, name: str) -> "MockColumnOperation":
        """Create an alias for the column operation."""
        result = MockColumnOperation(self.column, self.operation, self.value)
        result.name = name
        return result

    def __repr__(self) -> str:
        if self.value is None:
            return f"MockColumnOperation({self.column}, '{self.operation}')"
        return f"MockColumnOperation({self.column}, '{self.operation}', {self.value})"

    def __eq__(self, other: Any) -> bool:
        """Equality comparison."""
        if not isinstance(other, MockColumnOperation):
            return False
        # Use id comparison for columns to avoid MockColumnOperation comparison
        column_eq = id(self.column) == id(other.column)
        operation_eq = self.operation == other.operation
        value_eq = self.value == other.value
        return column_eq and operation_eq and value_eq

    def __lt__(self, other: Any) -> bool:
        """Less than comparison for sorting."""
        if not isinstance(other, MockColumnOperation):
            return NotImplemented
        # Compare by operation name, then column name
        if self.operation != other.operation:
            return self.operation < other.operation
        return str(self.column) < str(other.column)

    def __hash__(self) -> int:
        """Hash for use in sets and dictionaries."""
        return hash((self.column, self.operation, self.value))


class MockFunctions:
    """Mock functions module providing PySpark-compatible function implementations.

    This class provides static methods that mirror PySpark's functions module,
    offering complete API compatibility for DataFrame operations. Includes
    column expressions, built-in functions, aggregate functions, and window functions.

    Example:
        >>> F = MockFunctions
        >>> F.col("name")
        >>> F.upper(F.col("name"))
        >>> F.count(F.col("id"))
        >>> F.row_number()
    """

    @staticmethod
    def col(name: str, column_type: Optional[MockDataType] = None) -> MockColumn:
        """Create a column reference.

        Args:
            name: Column name.
            column_type: Optional data type for the column.

        Returns:
            MockColumn instance for DataFrame operations.

        Example:
            >>> F.col("age")
            >>> F.col("name", StringType())
        """
        return MockColumn(name, column_type)

    @staticmethod
    def lit(value: Any, column_type: Optional[MockDataType] = None) -> "MockLiteral":
        """Create a literal value.

        Args:
            value: Literal value (string, number, boolean, etc.).
            column_type: Optional data type. Inferred from value if not provided.

        Returns:
            MockLiteral instance for use in DataFrame operations.

        Example:
            >>> F.lit("Hello")
            >>> F.lit(42)
            >>> F.lit(True)
        """
        return MockLiteral(value, column_type)

    @staticmethod
    def count(column: Union[str, MockColumn] = "*") -> "MockAggregateFunction":
        """Count function for aggregation.

        Args:
            column: Column to count. Use "*" to count all rows.

        Returns:
            MockAggregateFunction for use in groupBy operations.

        Example:
            >>> df.groupBy("department").agg(F.count("*"))
            >>> df.groupBy("department").agg(F.count("employee_id"))
        """
        if isinstance(column, str):
            return MockAggregateFunction("count", column)
        return MockAggregateFunction("count", column.name)

    @staticmethod
    def sum(column: Union[str, MockColumn]) -> "MockAggregateFunction":
        """Sum function."""
        if isinstance(column, str):
            return MockAggregateFunction("sum", column)
        return MockAggregateFunction("sum", column.name)

    @staticmethod
    def avg(column: Union[str, MockColumn]) -> "MockAggregateFunction":
        """Average function."""
        if isinstance(column, str):
            return MockAggregateFunction("avg", column)
        return MockAggregateFunction("avg", column.name)

    @staticmethod
    def max(column: Union[str, MockColumn]) -> "MockAggregateFunction":
        """Max function."""
        if isinstance(column, str):
            return MockAggregateFunction("max", column)
        return MockAggregateFunction("max", column.name)

    @staticmethod
    def min(column: Union[str, MockColumn]) -> "MockAggregateFunction":
        """Min function."""
        if isinstance(column, str):
            return MockAggregateFunction("min", column)
        return MockAggregateFunction("min", column.name)

    @staticmethod
    def countDistinct(column: Union[str, MockColumn]) -> "MockAggregateFunction":
        """Count distinct function."""
        if isinstance(column, str):
            return MockAggregateFunction("count(DISTINCT", column)
        return MockAggregateFunction("count(DISTINCT", column.name)

    @staticmethod
    def abs(column: Union[str, MockColumn]) -> MockColumnOperation:
        """Absolute value function."""
        if isinstance(column, str):
            column = MockColumn(column)
        operation = MockColumnOperation(column, "abs")
        return operation

    @staticmethod
    def round(column: Union[str, MockColumn], scale: int = 0) -> MockColumnOperation:
        """Round function."""
        if isinstance(column, str):
            column = MockColumn(column)
        operation = MockColumnOperation(column, "round", scale)
        operation.precision = scale  # Store precision for evaluation
        # Update the name to include the precision parameter to match PySpark
        if scale == 0:
            operation.name = f"round({column.name})"
        else:
            operation.name = f"round({column.name}, {scale})"
        return operation

    @staticmethod
    def when(condition: MockColumnOperation, value: Any) -> MockColumn:
        """CASE WHEN condition."""
        return MockColumn(f"when({condition}, {value})")

    @staticmethod
    def current_timestamp() -> MockColumn:
        """Current timestamp function."""
        return MockColumn("current_timestamp()")

    @staticmethod
    def current_date() -> MockColumn:
        """Current date function."""
        return MockColumn("current_date()")

    @staticmethod
    def to_date(column: Union[str, MockColumn], format: Optional[str] = None) -> MockColumn:
        """Convert to date function."""
        if isinstance(column, str):
            column = MockColumn(column)
        if format:
            return MockColumn(f"to_date({column.name}, '{format}')")
        return MockColumn(f"to_date({column.name})")

    @staticmethod
    def to_timestamp(column: Union[str, MockColumn], format: Optional[str] = None) -> MockColumn:
        """Convert to timestamp function."""
        if isinstance(column, str):
            column = MockColumn(column)
        if format:
            return MockColumn(f"to_timestamp({column.name}, '{format}')")
        return MockColumn(f"to_timestamp({column.name})")

    @staticmethod
    def hour(column: Union[str, MockColumn]) -> MockColumn:
        """Extract hour function."""
        if isinstance(column, str):
            column = MockColumn(column)
        return MockColumn(f"hour({column.name})")

    @staticmethod
    def day(column: Union[str, MockColumn]) -> MockColumn:
        """Extract day function."""
        if isinstance(column, str):
            column = MockColumn(column)
        return MockColumn(f"day({column.name})")

    @staticmethod
    def month(column: Union[str, MockColumn]) -> MockColumn:
        """Extract month function."""
        if isinstance(column, str):
            column = MockColumn(column)
        return MockColumn(f"month({column.name})")

    @staticmethod
    def year(column: Union[str, MockColumn]) -> MockColumn:
        """Extract year function."""
        if isinstance(column, str):
            column = MockColumn(column)
        return MockColumn(f"year({column.name})")

    @staticmethod
    def concat(*columns: Union[str, MockColumn]) -> MockColumn:
        """Concatenate columns function."""
        col_names = []
        for col in columns:
            if isinstance(col, str):
                col_names.append(col)
            else:
                col_names.append(col.name)
        return MockColumn(f"concat({', '.join(col_names)})")

    @staticmethod
    def substring(column: Union[str, MockColumn], pos: int, len: int) -> MockColumn:
        """Substring function."""
        if isinstance(column, str):
            column = MockColumn(column)
        return MockColumn(f"substring({column.name}, {pos}, {len})")

    @staticmethod
    def upper(column: Union[str, MockColumn]) -> MockColumnOperation:
        """Uppercase function."""
        if isinstance(column, str):
            column = MockColumn(column)
        return MockColumnOperation(column, "upper")

    @staticmethod
    def lower(column: Union[str, MockColumn]) -> MockColumnOperation:
        """Lowercase function."""
        if isinstance(column, str):
            column = MockColumn(column)
        return MockColumnOperation(column, "lower")

    @staticmethod
    def trim(column: Union[str, MockColumn]) -> MockColumnOperation:
        """Trim function."""
        if isinstance(column, str):
            column = MockColumn(column)
        return MockColumnOperation(column, "trim")

    @staticmethod
    def length(column: Union[str, MockColumn]) -> MockColumnOperation:
        """Length function."""
        if isinstance(column, str):
            column = MockColumn(column)
        return MockColumnOperation(column, "length")

    @staticmethod
    def coalesce(*columns: Union[str, MockColumn]) -> MockColumn:
        """Coalesce function to return first non-null value.

        Args:
            *columns: Columns to evaluate in order.

        Returns:
            MockColumn with coalesce expression.

        Example:
            >>> F.coalesce(F.col("name"), F.lit("Unknown"))
            >>> F.coalesce(F.col("first_name"), F.col("last_name"), F.lit("N/A"))
        """
        col_names = []
        for col in columns:
            if isinstance(col, str):
                col_names.append(col)
            else:
                col_names.append(col.name)
        return MockColumn(f"coalesce({', '.join(col_names)})")

    @staticmethod
    def isnan(column: Union[str, MockColumn]) -> MockColumn:
        """Check if NaN function."""
        if isinstance(column, str):
            column = MockColumn(column)
        return MockColumn(f"isnan({column.name})")

    @staticmethod
    def isnull(column: Union[str, MockColumn]) -> MockColumn:
        """Check if null function."""
        if isinstance(column, str):
            column = MockColumn(column)
        return MockColumn(f"isnull({column.name})")

    @staticmethod
    def expr(expression: str) -> MockColumn:
        """Create a column from a SQL expression."""
        return MockColumn(f"expr({expression})")

    @staticmethod
    def row_number() -> "MockWindowFunction":
        """Row number window function."""
        return MockWindowFunction("row_number")

    @staticmethod
    def rank() -> "MockWindowFunction":
        """Rank window function."""
        return MockWindowFunction("rank")

    @staticmethod
    def dense_rank() -> "MockWindowFunction":
        """Dense rank window function."""
        return MockWindowFunction("dense_rank")


# Create the functions module instance
F = MockFunctions()

# Export commonly used functions
__all__ = [
    "MockColumn",
    "MockColumnOperation",
    "MockFunctions",
    "F",
    "col",
    "lit",
    "count",
    "sum",
    "avg",
    "max",
    "min",
    "countDistinct",
    "abs",
    "round",
    "when",
    "current_timestamp",
    "current_date",
    "to_date",
    "to_timestamp",
    "hour",
    "day",
    "month",
    "year",
    "concat",
    "substring",
    "upper",
    "lower",
    "trim",
    "length",
    "coalesce",
    "isnan",
    "isnull",
]


# Additional classes for compatibility with tests
class MockLiteral:
    """Mock literal value."""

    column_type: MockDataType  # Type annotation for mypy

    def __init__(self, value: Any, column_type: Optional[MockDataType] = None):
        """Initialize MockLiteral."""
        self.value = value
        # Use the correct type based on the value
        from .spark_types import (
            convert_python_type_to_mock_type,
            IntegerType,
            BooleanType,
            MockDataType,
        )

        if column_type is None:
            if isinstance(value, bool):
                self.column_type = BooleanType()
            elif isinstance(value, int):
                self.column_type = IntegerType()
            else:
                self.column_type = convert_python_type_to_mock_type(type(value))
        else:
            self.column_type = column_type
        # Add name attribute to match PySpark behavior - use the actual value as column name
        # PySpark uses lowercase for boolean literals
        if isinstance(value, bool):
            self.name = str(value).lower()
        else:
            self.name = str(value)

    def alias(self, name: str) -> "MockLiteral":
        """Create an alias for the literal."""
        result = MockLiteral(self.value, self.column_type)
        result.name = name
        return result

    def __repr__(self) -> str:
        """String representation."""
        return f"MockLiteral({self.value})"


class MockAggregateFunction:
    """Mock aggregate function."""

    def __init__(self, function_name: str, column_name: Optional[str] = None):
        """Initialize MockAggregateFunction."""
        self.function_name = function_name
        self.column_name = column_name

    def __repr__(self) -> str:
        """String representation."""
        if self.column_name:
            if self.function_name == "count(DISTINCT":
                return f"MockAggregateFunction({self.function_name} {self.column_name}))"
            else:
                return f"MockAggregateFunction({self.function_name}({self.column_name}))"
        else:
            return f"MockAggregateFunction({self.function_name}())"


class MockWindowFunction:
    """Mock window function."""

    def __init__(self, function_name: str, column_name: Optional[str] = None):
        """Initialize MockWindowFunction."""
        self.function_name = function_name
        self.column_name = column_name
        self._window_spec: Optional["MockWindowSpec"] = None
        self._alias: Optional[str] = None

    def over(self, window_spec: "MockWindowSpec") -> "MockWindowFunction":
        """Apply window specification."""
        # Create a new instance with the window spec
        new_func = MockWindowFunction(self.function_name, self.column_name)
        new_func._window_spec = window_spec
        return new_func

    def alias(self, name: str) -> "MockWindowFunction":
        """Create an alias for the window function."""
        new_func = MockWindowFunction(self.function_name, self.column_name)
        new_func._window_spec = self._window_spec
        new_func._alias = name
        return new_func

    @property
    def name(self) -> str:
        """Get the column name for this window function."""
        if hasattr(self, "_alias") and self._alias is not None:
            return self._alias
        return f"{self.function_name}()"

    def __repr__(self) -> str:
        """String representation."""
        if self.column_name:
            return f"MockWindowFunction({self.function_name}({self.column_name}))"
        else:
            return f"MockWindowFunction({self.function_name}())"


# Create function aliases for easy access
col = F.col
lit = F.lit
count = F.count
sum = F.sum
avg = F.avg
max = F.max
min = F.min
countDistinct = F.countDistinct
abs = F.abs
round = F.round
row_number = F.row_number
when = F.when
current_timestamp = F.current_timestamp
current_date = F.current_date
to_date = F.to_date
to_timestamp = F.to_timestamp
hour = F.hour
day = F.day
month = F.month
year = F.year
concat = F.concat
substring = F.substring
upper = F.upper
lower = F.lower
trim = F.trim
length = F.length
coalesce = F.coalesce
isnan = F.isnan
isnull = F.isnull
