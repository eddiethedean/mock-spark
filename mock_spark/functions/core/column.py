"""
Column implementation for Mock Spark.

This module provides the Column class for DataFrame column operations,
maintaining compatibility with PySpark's Column interface.
"""

from typing import Any, Optional, TYPE_CHECKING
from ...spark_types import DataType, StringType

if TYPE_CHECKING:
    from ...window import WindowSpec
    from ..conditional import CaseWhen
    from ..window_execution import WindowFunction
    from ..base import AggregateFunction


class ColumnOperatorMixin:
    """Mixin providing common operator methods for Column and ColumnOperation."""

    if TYPE_CHECKING:

        @property
        def name(self) -> str: ...

    def _create_operation(self, operation: str, other: Any) -> "ColumnOperation":
        """Create a ColumnOperation with the given operation and other operand.

        Args:
            operation: The operation to perform (e.g., "==", "+", etc.)
            other: The other operand

        Returns:
            ColumnOperation instance
        """
        return ColumnOperation(self, operation, other)

    def __eq__(self, other: Any) -> "ColumnOperation":  # type: ignore[override]
        """Equality comparison."""
        return self._create_operation("==", other)

    def __ne__(self, other: Any) -> "ColumnOperation":  # type: ignore[override]
        """Inequality comparison."""
        return self._create_operation("!=", other)

    def __lt__(self, other: Any) -> "ColumnOperation":
        """Less than comparison."""
        return self._create_operation("<", other)

    def __le__(self, other: Any) -> "ColumnOperation":
        """Less than or equal comparison."""
        return self._create_operation("<=", other)

    def __gt__(self, other: Any) -> "ColumnOperation":
        """Greater than comparison."""
        return self._create_operation(">", other)

    def __ge__(self, other: Any) -> "ColumnOperation":
        """Greater than or equal comparison."""
        return self._create_operation(">=", other)

    def __add__(self, other: Any) -> "ColumnOperation":
        """Addition operation."""
        return self._create_operation("+", other)

    def __sub__(self, other: Any) -> "ColumnOperation":
        """Subtraction operation."""
        return self._create_operation("-", other)

    def __mul__(self, other: Any) -> "ColumnOperation":
        """Multiplication operation."""
        return self._create_operation("*", other)

    def __truediv__(self, other: Any) -> "ColumnOperation":
        """Division operation."""
        return self._create_operation("/", other)

    def __mod__(self, other: Any) -> "ColumnOperation":
        """Modulo operation."""
        return self._create_operation("%", other)

    def __and__(self, other: Any) -> "ColumnOperation":
        """Logical AND operation."""
        return self._create_operation("&", other)

    def __or__(self, other: Any) -> "ColumnOperation":
        """Logical OR operation."""
        return self._create_operation("|", other)

    def __invert__(self) -> "ColumnOperation":
        """Logical NOT operation."""
        return self._create_operation("!", None)

    def __neg__(self) -> "ColumnOperation":
        """Unary minus operation (-column)."""
        return self._create_operation("-", None)

    def isnull(self) -> "ColumnOperation":
        """Check if column value is null."""
        return self._create_operation("isnull", None)

    def isnotnull(self) -> "ColumnOperation":
        """Check if column value is not null."""
        return self._create_operation("isnotnull", None)

    def isNull(self) -> "ColumnOperation":
        """Check if column value is null (PySpark compatibility)."""
        return self.isnull()

    def isNotNull(self) -> "ColumnOperation":
        """Check if column value is not null (PySpark compatibility)."""
        return self.isnotnull()

    def isin(self, values: list[Any]) -> "ColumnOperation":
        """Check if column value is in list of values."""
        return self._create_operation("isin", values)

    def between(self, lower: Any, upper: Any) -> "ColumnOperation":
        """Check if column value is between lower and upper bounds."""
        return self._create_operation("between", (lower, upper))

    def like(self, pattern: str) -> "ColumnOperation":
        """SQL LIKE pattern matching."""
        return self._create_operation("like", pattern)

    def rlike(self, pattern: str) -> "ColumnOperation":
        """Regular expression pattern matching."""
        # Create operation with proper naming format: RLIKE(name, pattern)
        # Note: Pattern is used as-is without quotes to match PySpark format
        pattern_str = str(pattern) if not isinstance(pattern, str) else pattern
        return ColumnOperation(
            self, "rlike", pattern, name=f"RLIKE({self.name}, {pattern_str})"
        )

    def contains(self, literal: str) -> "ColumnOperation":
        """Check if column contains the literal string."""
        return self._create_operation("contains", literal)

    def startswith(self, literal: str) -> "ColumnOperation":
        """Check if column starts with the literal string."""
        return self._create_operation("startswith", literal)

    def endswith(self, literal: str) -> "ColumnOperation":
        """Check if column ends with the literal string."""
        return self._create_operation("endswith", literal)

    def asc(self) -> "ColumnOperation":
        """Ascending sort order."""
        return self._create_operation("asc", None)

    def desc(self) -> "ColumnOperation":
        """Descending sort order."""
        return self._create_operation("desc", None)

    def cast(self, data_type: DataType) -> "ColumnOperation":
        """Cast column to different data type."""
        return self._create_operation("cast", data_type)


class Column(ColumnOperatorMixin):
    """Mock column expression for DataFrame operations.

    Provides a PySpark-compatible column expression that supports all comparison
    and logical operations. Used for creating complex DataFrame transformations
    and filtering conditions.
    """

    def __init__(self, name: str, column_type: Optional[DataType] = None):
        """Initialize Column.

        Args:
            name: Column name.
            column_type: Optional data type. Defaults to StringType if not specified.
        """
        self._name = name
        self._original_column: Optional[Column] = None
        self._alias_name: Optional[str] = None
        self.column_name = name
        self.column_type = column_type or StringType()
        self.operation = None
        self.operand = None
        self._operations: list[ColumnOperation] = []
        # Add expr attribute for PySpark compatibility
        self.expr = f"Column('{name}')"

    @property
    def name(self) -> str:
        """Get the column name (alias if set, otherwise original name)."""
        if hasattr(self, "_alias_name") and self._alias_name is not None:
            return self._alias_name
        return self._name

    @property
    def original_column(self) -> "Column":
        """Get the original column (for aliased columns)."""
        return getattr(self, "_original_column", self)

    def __eq__(self, other: Any) -> "ColumnOperation":  # type: ignore[override]
        """Equality comparison."""
        if isinstance(other, Column):
            return ColumnOperation(self, "==", other)
        return ColumnOperation(self, "==", other)

    def __hash__(self) -> int:
        """Hash method to make Column hashable."""
        return hash((self.name, self.column_type))

    def __str__(self) -> str:
        """Return string representation of column for SQL generation."""
        return self.name

    def alias(self, name: str) -> "Column":
        """Create an alias for the column."""
        aliased_column = Column(name, self.column_type)
        aliased_column._original_column = self
        aliased_column._alias_name = name
        return aliased_column

    def when(self, condition: "ColumnOperation", value: Any) -> "CaseWhen":
        """Start a CASE WHEN expression."""
        from ..conditional import CaseWhen

        return CaseWhen(self, condition, value)

    def otherwise(self, value: Any) -> "CaseWhen":
        """End a CASE WHEN expression with default value."""
        from ..conditional import CaseWhen

        return CaseWhen(self, None, value)

    def over(self, window_spec: "WindowSpec") -> "WindowFunction":
        """Apply window function over window specification."""
        from ..window_execution import WindowFunction

        return WindowFunction(self, window_spec)

    def count(self) -> "ColumnOperation":
        """Count non-null values in this column.

        Returns:
            ColumnOperation representing the count operation.
        """
        return ColumnOperation(self, "count", None)

    def avg(self) -> "AggregateFunction":  # noqa: F821
        """Average values in this column.

        Returns:
            AggregateFunction representing the avg function.
        """
        from ..base import AggregateFunction
        from ...spark_types import DoubleType

        return AggregateFunction(self, "avg", DoubleType())

    def sum(self) -> "AggregateFunction":  # noqa: F821
        """Sum values in this column.

        Returns:
            AggregateFunction representing the sum function.
        """
        from ..base import AggregateFunction
        from ...spark_types import DoubleType

        return AggregateFunction(self, "sum", DoubleType())

    def max(self) -> "AggregateFunction":  # noqa: F821
        """Maximum value in this column.

        Returns:
            AggregateFunction representing the max function.
        """
        from ..base import AggregateFunction
        from ...spark_types import DoubleType

        return AggregateFunction(self, "max", DoubleType())

    def min(self) -> "AggregateFunction":  # noqa: F821
        """Minimum value in this column.

        Returns:
            AggregateFunction representing the min function.
        """
        from ..base import AggregateFunction
        from ...spark_types import DoubleType

        return AggregateFunction(self, "min", DoubleType())

    def stddev(self) -> "AggregateFunction":  # noqa: F821
        """Standard deviation of values in this column.

        Returns:
            AggregateFunction representing the stddev function.
        """
        from ..base import AggregateFunction
        from ...spark_types import DoubleType

        return AggregateFunction(self, "stddev", DoubleType())

    def variance(self) -> "AggregateFunction":  # noqa: F821
        """Variance of values in this column.

        Returns:
            AggregateFunction representing the variance function.
        """
        from ..base import AggregateFunction
        from ...spark_types import DoubleType

        return AggregateFunction(self, "variance", DoubleType())

    def bitwise_not(self) -> "ColumnOperation":
        """Bitwise NOT operation on this column.

        Returns:
            ColumnOperation representing the bitwise_not function.
        """
        # PySpark uses ~column for bitwise_not column names
        return ColumnOperation(self, "bitwise_not", name=f"~{self.name}")


class ColumnOperation(ColumnOperatorMixin):
    """Represents a column operation (comparison, arithmetic, etc.).

    This class encapsulates column operations and their operands for evaluation
    during DataFrame operations.
    """

    def __init__(
        self,
        column: Any,  # Can be Column, ColumnOperation, IColumn, mixin, or None
        operation: str,
        value: Any = None,
        name: Optional[str] = None,
    ):
        """Initialize ColumnOperation.

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
        # If _name was explicitly set (e.g., by datetime functions), use it
        if self._name and self._name != self._generate_name():
            return self._name
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
            part_map = {
                "dayofweek": "dow",
                "dayofyear": "doy",
                "weekofyear": "week",
                "quarter": "quarter",
            }
            part = part_map.get(self.operation, self.operation)

            # PySpark dayofweek returns 1-7 (Sunday=1, Saturday=7)
            # DuckDB DOW returns 0-6 (Sunday=0, Saturday=6) - NOTE: DuckDB backend only
            # Add 1 to dayofweek to match PySpark
            if self.operation == "dayofweek":
                return f"CAST(extract({part} from TRY_CAST({self.column.name} AS DATE)) + 1 AS INTEGER)"
            else:
                return f"CAST(extract({part} from TRY_CAST({self.column.name} AS DATE)) AS INTEGER)"
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
        # Extract value from Literal if needed
        if hasattr(self.value, "value") and hasattr(self.value, "data_type"):
            # This is a Literal
            value_str = str(self.value.value)
        else:
            value_str = str(self.value)

        # Handle column reference - use str() to get proper SQL for ColumnOperation
        if self.column is None:
            # For functions without column input (like current_date, current_timestamp)
            return self.operation + "()"
        # Handle Column objects properly
        if hasattr(self.column, "name"):
            column_ref = self.column.name
        else:
            column_ref = str(self.column)

        if self.operation == "bitwise_not":
            # PySpark uses ~column for bitwise_not
            return f"~{column_ref}"
        elif self.operation == "==":
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
        elif self.operation == "!" or self.operation == "!":
            return f"(CASE WHEN {self.column.name} THEN FALSE ELSE TRUE END)"
        elif self.operation == "isnull":
            return f"{self.column.name} IS NULL"
        elif self.operation == "isnotnull":
            return f"{self.column.name} IS NOT NULL"
        elif self.operation == "isin":
            return f"{self.column.name} IN {self.value}"
        elif self.operation == "between":
            if self.value is None:
                return f"{self.column.name} BETWEEN NULL AND NULL"
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
            # Map PySpark type names to DuckDB/SQL type names (DuckDB backend only)
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
                # If value is a DataType, use its SQL representation
                sql_type = str(self.value)
            return f"CAST({self.column.name} AS {sql_type})"
        elif self.operation == "from_unixtime":
            # Handle from_unixtime function properly
            if self.value is not None:
                return f"from_unixtime({self.column.name}, '{self.value}')"
            else:
                return f"from_unixtime({self.column.name})"
        elif self.operation == "array_sort":
            # Handle array_sort -> LIST_SORT or LIST_REVERSE_SORT
            asc = getattr(self, "value", True)
            if asc:
                return f"LIST_SORT({self.column.name})"
            else:
                return f"LIST_REVERSE_SORT({self.column.name})"
        elif self.operation == "array_reverse":
            return f"LIST_REVERSE({self.column.name})"
        elif self.operation == "array_size":
            return f"LEN({self.column.name})"
        elif self.operation == "array_max":
            return f"LIST_MAX({self.column.name})"
        elif self.operation == "array_min":
            return f"LIST_MIN({self.column.name})"
        elif self.operation == "struct":
            # Generate struct name from columns/literals in value
            if (
                hasattr(self, "value")
                and self.value is not None
                and isinstance(self.value, (list, tuple))
            ):
                col_names = []
                for col in self.value:
                    if hasattr(col, "value") and hasattr(col, "data_type"):
                        # It's a Literal
                        col_names.append(str(col.value))
                    elif hasattr(col, "name"):
                        col_names.append(col.name)
                    else:
                        col_names.append(str(col))
                # Also include the first column if it's not a dummy
                if self.column and self.column.name != "__struct_dummy__":
                    if hasattr(self.column, "value") and hasattr(
                        self.column, "data_type"
                    ):
                        col_names.insert(0, str(self.column.value))
                    else:
                        col_names.insert(
                            0,
                            self.column.name
                            if hasattr(self.column, "name")
                            else str(self.column),
                        )
                return f"struct({', '.join(col_names)})"
            # Fallback to default
            return "struct(...)"
        else:
            return f"{self.column.name} {self.operation} {self.value}"

    def alias(self, name: str) -> "ColumnOperation":
        """Create an alias for this operation."""
        aliased_operation = ColumnOperation(self.column, self.operation, self.value)
        aliased_operation._alias_name = name
        return aliased_operation

    def over(self, window_spec: "WindowSpec") -> "WindowFunction":
        """Apply window function over window specification."""
        from ..window_execution import WindowFunction

        return WindowFunction(self, window_spec)
