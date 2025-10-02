"""
Mock functions for Mock Spark.

This module provides mock implementations of PySpark functions that behave
identically to the real PySpark functions, particularly F.col expressions.
"""

from typing import Any, List, Union, Optional, Callable
from dataclasses import dataclass
from .spark_types import MockDataType, StringType


class MockColumn:
    """Mock column expression for DataFrame operations."""
    
    def __init__(self, name: str, column_type: Optional[MockDataType] = None):
        self.name = name
        self.column_name = name
        self.column_type = column_type or StringType()
        self.operation = None
        self.operand = None
        self._operations: List[MockColumnOperation] = []
        # Add expr attribute for PySpark compatibility
        self.expr = f"MockColumn('{name}')"
    
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
    
    def isin(self, values: List[Any]) -> "MockColumnOperation":
        """Check if column value is in list."""
        return MockColumnOperation(self, "isin", values)
    
    def between(self, lower: Any, upper: Any) -> "MockColumnOperation":
        """Check if column value is between bounds."""
        return MockColumnOperation(self, "between", (lower, upper))
    
    def alias(self, name: str) -> "MockColumnOperation":
        """Create column alias."""
        return MockColumnOperation(self, "alias", name)
    
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


class MockColumnOperation:
    """Represents a column operation."""
    
    def __init__(self, column: MockColumn, operation: str, value: Any = None):
        self.column = column
        self.operation = operation
        self.value = value
        # Add operand attribute for compatibility
        self.operand = self.value
        # Add name property for compatibility with select method
        if operation in ["+", "-", "*", "/", "%"]:
            self.name = f"({self.column.name} {operation} {value})"
        elif operation in ["upper", "lower", "length"]:
            self.name = f"{operation}({self.column.name})"
        else:
            self.name = f"{operation}({self.column.name})"
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


class MockFunctions:
    """Mock functions module (equivalent to pyspark.sql.functions)."""
    
    @staticmethod
    def col(name: str, column_type: Optional[MockDataType] = None) -> MockColumn:
        """Create a column reference."""
        return MockColumn(name, column_type)
    
    @staticmethod
    def lit(value: Any, column_type: Optional[MockDataType] = None) -> "MockLiteral":
        """Create a literal value."""
        return MockLiteral(value, column_type)
    
    @staticmethod
    def count(column: Union[str, MockColumn] = "*") -> "MockAggregateFunction":
        """Count function."""
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
            return MockAggregateFunction("countDistinct", column)
        return MockAggregateFunction("countDistinct", column.name)
    
    @staticmethod
    def abs(column: Union[str, MockColumn]) -> MockColumn:
        """Absolute value function."""
        if isinstance(column, str):
            column = MockColumn(column)
        return MockColumn(f"abs({column.name})")
    
    @staticmethod
    def round(column: Union[str, MockColumn], scale: int = 0) -> MockColumn:
        """Round function."""
        if isinstance(column, str):
            column = MockColumn(column)
        return MockColumn(f"round({column.name}, {scale})")
    
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
    def upper(column: Union[str, MockColumn]) -> MockColumn:
        """Uppercase function."""
        if isinstance(column, str):
            column = MockColumn(column)
        return MockColumn(f"upper({column.name})")
    
    @staticmethod
    def lower(column: Union[str, MockColumn]) -> MockColumn:
        """Lowercase function."""
        if isinstance(column, str):
            column = MockColumn(column)
        return MockColumn(f"lower({column.name})")
    
    @staticmethod
    def trim(column: Union[str, MockColumn]) -> MockColumn:
        """Trim function."""
        if isinstance(column, str):
            column = MockColumn(column)
        return MockColumn(f"trim({column.name})")
    
    @staticmethod
    def length(column: Union[str, MockColumn]) -> MockColumn:
        """Length function."""
        if isinstance(column, str):
            column = MockColumn(column)
        return MockColumn(f"length({column.name})")
    
    @staticmethod
    def coalesce(*columns: Union[str, MockColumn]) -> MockColumn:
        """Coalesce function."""
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
    
    def __init__(self, value: Any, column_type: Optional[MockDataType] = None):
        """Initialize MockLiteral."""
        self.value = value
        # Use the correct type based on the value
        if column_type is None:
            from .spark_types import convert_python_type_to_mock_type, IntegerType
            if isinstance(value, int):
                self.column_type = IntegerType()
            else:
                self.column_type = convert_python_type_to_mock_type(type(value))
        else:
            self.column_type = column_type
        # Add name attribute to match PySpark behavior - use the actual value as column name
        self.name = str(value)
    
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
            return f"MockAggregateFunction({self.function_name}({self.column_name}))"
        else:
            return f"MockAggregateFunction({self.function_name}())"


class MockWindowFunction:
    """Mock window function."""
    
    def __init__(self, function_name: str, column_name: Optional[str] = None):
        """Initialize MockWindowFunction."""
        self.function_name = function_name
        self.column_name = column_name
    
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
