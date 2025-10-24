"""
Column implementation for Mock Spark.

This module provides the MockColumn class for DataFrame column operations,
maintaining compatibility with PySpark's Column interface.
"""

from typing import Any, List, Optional, TYPE_CHECKING
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

    def __str__(self) -> str:
        """Return string representation of column for SQL generation."""
        return self.name

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

    def contains(self, literal: str) -> "MockColumnOperation":
        """Check if column contains the literal string."""
        return MockColumnOperation(self, "contains", literal)

    def startswith(self, literal: str) -> "MockColumnOperation":
        """Check if column starts with the literal string."""
        return MockColumnOperation(self, "startswith", literal)

    def endswith(self, literal: str) -> "MockColumnOperation":
        """Check if column ends with the literal string."""
        return MockColumnOperation(self, "endswith", literal)

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

    def count(self) -> "MockColumnOperation":
        """Count non-null values in this column.

        Returns:
            MockColumnOperation representing the count operation.
        """
        return MockColumnOperation(self, "count", None)


class MockColumnOperation(IColumn):
    """Represents a column operation (comparison, arithmetic, etc.).

    This class encapsulates column operations and their operands for evaluation
    during DataFrame operations.
    """

    def __init__(
        self,
        column: Any,  # Can be MockColumn, MockColumnOperation, IColumn, mixin, or None
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
        self._alias_name: Optional[str] = None
        self.function_name = operation
        self.return_type: Optional[Any] = None  # Type hint for return type

    @property
    def name(self) -> str:
        """Get column name."""
        # If there's an alias, use it
        if hasattr(self, "_alias_name") and self._alias_name:
            return self._alias_name
        # For datetime and comparison operations, use the SQL representation
        if self.operation in [
            "hour",
            "minute",
            "second",
            "year",
            "month",
            "day",
            "dayofmonth",
            "dayofweek",
            "dayofyear",
            "weekofyear",
            "quarter",
            "to_date",
            "to_timestamp",
            "==",
            "!=",
            "<",
            ">",
            "<=",
            ">=",
        ]:
            return str(self)
        return self._name

    @name.setter
    def name(self, value: str) -> None:
        """Set column name."""
        self._name = value

    def __str__(self) -> str:
        """Generate SQL representation of this operation."""
        # For datetime functions, generate proper SQL
        if self.operation in ["hour", "minute", "second"]:
            return f"extract({self.operation} from TRY_CAST({self.column.name} AS TIMESTAMP))"
        elif self.operation in ["year", "month", "day", "dayofmonth"]:
            part = "day" if self.operation == "dayofmonth" else self.operation
            return f"extract({part} from TRY_CAST({self.column.name} AS DATE))"
        elif self.operation in ["dayofweek", "dayofyear", "weekofyear", "quarter"]:
            return (
                f"extract({self.operation} from TRY_CAST({self.column.name} AS DATE))"
            )
        elif self.operation in ["to_date", "to_timestamp"]:
            if self.value is not None:
                return f"STRPTIME({self.column.name}, '{self.value}')"
            else:
                target_type = "DATE" if self.operation == "to_date" else "TIMESTAMP"
                return f"TRY_CAST({self.column.name} AS {target_type})"
        elif self.operation in ["==", "!=", "<", ">", "<=", ">="]:
            # For comparison operations, generate proper SQL
            left = (
                str(self.column)
                if hasattr(self.column, "__str__")
                else self.column.name
            )
            right = str(self.value) if self.value is not None else "NULL"
            return f"({left} {self.operation} {right})"
        elif self.operation == "cast":
            # For cast operations, use the generated name which handles proper SQL syntax
            return self._generate_name()
        else:
            # For other operations, use the generated name
            return self._generate_name()

    def _generate_name(self) -> str:
        """Generate a name for this operation."""
        # Extract value from MockLiteral if needed
        if hasattr(self.value, "value") and hasattr(self.value, "data_type"):
            # This is a MockLiteral
            value_str = str(self.value.value)
        else:
            value_str = str(self.value)

        # Handle column reference - use str() to get proper SQL for MockColumnOperation
        if self.column is None:
            # For functions without column input (like current_date, current_timestamp)
            return self.operation + "()"
        # Handle MockColumn objects properly
        if hasattr(self.column, "name"):
            column_ref = self.column.name
        else:
            column_ref = str(self.column)

        if self.operation == "==":
            return f"{column_ref} = {value_str}"
        elif self.operation == "!=":
            return f"{column_ref} != {value_str}"
        elif self.operation == "<":
            return f"{column_ref} < {value_str}"
        elif self.operation == "<=":
            return f"{column_ref} <= {value_str}"
        elif self.operation == ">":
            return f"{column_ref} > {value_str}"
        elif self.operation == ">=":
            return f"{column_ref} >= {value_str}"
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
            # Map PySpark type names to DuckDB/SQL type names
            type_mapping = {
                "int": "INTEGER",
                "integer": "INTEGER",
                "long": "BIGINT",
                "bigint": "BIGINT",
                "double": "DOUBLE",
                "float": "FLOAT",
                "string": "VARCHAR",
                "varchar": "VARCHAR",
                "boolean": "BOOLEAN",
                "bool": "BOOLEAN",
                "date": "DATE",
                "timestamp": "TIMESTAMP",
            }
            if isinstance(self.value, str):
                sql_type = type_mapping.get(self.value.lower(), self.value.upper())
            else:
                # If value is a MockDataType, use its SQL representation
                sql_type = str(self.value)
            return f"CAST({self.column.name} AS {sql_type})"
        elif self.operation == "from_unixtime":
            # Handle from_unixtime function properly
            if self.value is not None:
                return f"from_unixtime({self.column.name}, '{self.value}')"
            else:
                return f"from_unixtime({self.column.name})"
        else:
            return f"{self.column.name} {self.operation} {self.value}"

    def alias(self, name: str) -> "MockColumnOperation":
        """Create an alias for this operation."""
        aliased_operation = MockColumnOperation(self.column, self.operation, self.value)
        aliased_operation._alias_name = name
        return aliased_operation

    def __eq__(self, other: Any) -> "MockColumnOperation":  # type: ignore[override]
        """Equality comparison.

        Note: Returns MockColumnOperation instead of bool for PySpark compatibility.
        This allows chaining operations like: (col("a") == 1) & (col("b") == 2)
        """
        if isinstance(other, MockColumnOperation):
            return MockColumnOperation(self, "==", other)
        return MockColumnOperation(self, "==", other)

    def __ne__(self, other: Any) -> "MockColumnOperation":  # type: ignore[override]
        """Inequality comparison.

        Note: Returns MockColumnOperation instead of bool for PySpark compatibility.
        This allows chaining operations like: (col("a") != 1) | (col("b") != 2)
        """
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

    def asc(self) -> "MockColumnOperation":
        """Ascending sort order."""
        return MockColumnOperation(self, "asc", None)

    def desc(self) -> "MockColumnOperation":
        """Descending sort order."""
        return MockColumnOperation(self, "desc", None)

    def cast(self, data_type: MockDataType) -> "MockColumnOperation":
        """Cast operation result to different data type."""
        return MockColumnOperation(self, "cast", data_type)

    def over(self, window_spec: "MockWindowSpec") -> "MockWindowFunction":
        """Apply window function over window specification."""
        from ..window_execution import MockWindowFunction

        return MockWindowFunction(self, window_spec)
