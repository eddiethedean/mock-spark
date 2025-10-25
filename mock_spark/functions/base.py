"""
Base function classes for Mock Spark.

This module provides base classes for all function types.
Most classes are imported from core/ modules to avoid duplication.
"""

from typing import Any, Dict, List, Union, Optional, TYPE_CHECKING
from mock_spark.spark_types import MockDataType, StringType

# Import core classes from their canonical locations
from .core.column import MockColumn, MockColumnOperation
from .core.literals import MockLiteral
from .core.lambda_parser import (
    MockLambdaExpression,
    LambdaParser,
    LambdaTranslationError,
)

if TYPE_CHECKING:
    from .window_execution import MockWindowFunction

# Re-export for backward compatibility
__all__ = [
    "MockColumn",
    "MockColumnOperation",
    "MockLiteral",
    "MockAggregateFunction",
    "MockLambdaExpression",
    "LambdaParser",
    "LambdaTranslationError",
]


class MockAggregateFunction:
    """Base class for aggregate functions.

    This class provides the base functionality for all aggregate functions
    including count, sum, avg, max, min, etc.
    """

    def __init__(
        self,
        column: Union[MockColumn, str, None],
        function_name: str,
        data_type: Optional[MockDataType] = None,
    ):
        """Initialize MockAggregateFunction.

        Args:
            column: The column to aggregate (None for count(*)).
            function_name: Name of the aggregate function.
            data_type: Optional return data type.
        """
        self.column = column
        self.function_name = function_name
        self.data_type = self._configure_data_type(data_type)
        self.name = self._generate_name()
        # Optional attributes for specific functions
        self.ord_column: Optional[Union[MockColumn, str]] = None  # For max_by, min_by

    def _configure_data_type(self, data_type: Optional[MockDataType]) -> MockDataType:
        """Configure data type with appropriate nullability based on function type."""
        if not data_type:
            return StringType()

        # Functions that always return non-nullable results in PySpark
        non_nullable_functions = {
            "count",
            "countDistinct",
            "row_number",
            "rank",
            "dense_rank",
            "isNull",
            "isnan",
            "coalesce",
        }

        if self.function_name in non_nullable_functions:
            data_type.nullable = False

        return data_type

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
            # For count(*), PySpark generates just "count", not "count(*)"
            if self.function_name == "count":
                return "count"
            else:
                return f"{self.function_name}(*)"
        elif isinstance(self.column, str):
            # For count("*"), PySpark generates "count(1)", not "count(*)"
            if self.function_name == "count" and self.column == "*":
                return "count(1)"
            elif self.function_name == "countDistinct":
                return f"count(DISTINCT {self.column})"
            else:
                return f"{self.function_name}({self.column})"
        else:
            if self.function_name == "countDistinct":
                return f"count(DISTINCT {self.column.name})"
            else:
                return f"{self.function_name}({self.column.name})"

    def evaluate(self, data: List[Dict[str, Any]]) -> Any:
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

    def _evaluate_count(self, data: List[Dict[str, Any]]) -> int:
        """Evaluate count function."""
        if self.column is None:
            return len(data)
        else:
            column_name = (
                self.column if isinstance(self.column, str) else self.column.name
            )
            return sum(1 for row in data if row.get(column_name) is not None)

    def _evaluate_sum(self, data: List[Dict[str, Any]]) -> Any:
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

    def _evaluate_avg(self, data: List[Dict[str, Any]]) -> Any:
        """Evaluate average function."""
        if self.column is None:
            return 0.0

        column_name = self.column if isinstance(self.column, str) else self.column.name
        values = [
            row.get(column_name) for row in data if row.get(column_name) is not None
        ]
        numeric_values = [v for v in values if isinstance(v, (int, float))]
        if numeric_values:
            return sum(numeric_values) / len(numeric_values)
        else:
            return None

    def _evaluate_max(self, data: List[Dict[str, Any]]) -> Any:
        """Evaluate max function."""
        if self.column is None:
            return None

        column_name = self.column if isinstance(self.column, str) else self.column.name
        values = [
            row.get(column_name) for row in data if row.get(column_name) is not None
        ]
        if values:
            return max(values)  # type: ignore[type-var]
        else:
            return None

    def _evaluate_min(self, data: List[Dict[str, Any]]) -> Any:
        """Evaluate min function."""
        if self.column is None:
            return None

        column_name = self.column if isinstance(self.column, str) else self.column.name
        values = [
            row.get(column_name) for row in data if row.get(column_name) is not None
        ]
        if values:
            return min(values)  # type: ignore[type-var]
        else:
            return None

    def over(self, window_spec: Any) -> "MockWindowFunction":
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
