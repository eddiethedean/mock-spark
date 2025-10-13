"""
Conditional functions for Mock Spark.

This module contains conditional functions including CASE WHEN expressions.

Note: This module now delegates expression evaluation to ExpressionEvaluator
to eliminate code duplication and follow the Single Responsibility Principle.
"""

from typing import Any, List, Union
from mock_spark.functions.base import MockColumn, MockColumnOperation
from mock_spark.core.expression_evaluator import ExpressionEvaluator


class MockCaseWhen:
    """Represents a CASE WHEN expression.

    This class handles complex conditional logic with multiple conditions
    and default values, similar to SQL CASE WHEN statements.
    """

    def __init__(self, column: Any = None, condition: Any = None, value: Any = None):
        """Initialize MockCaseWhen.

        Args:
            column: The column or expression being evaluated.
            condition: The condition for this case.
            value: The value to return if condition is true.
        """
        self.column = column
        self.conditions: List[tuple] = []
        self.default_value: Any = None
        self._evaluator = ExpressionEvaluator()  # Composition for evaluation logic

        if condition is not None and value is not None:
            self.conditions.append((condition, value))

        self.name = "CASE WHEN"

    @property
    def else_value(self) -> Any:
        """Get the else value (alias for default_value for compatibility)."""
        return self.default_value

    @else_value.setter
    def else_value(self, value: Any) -> None:
        """Set the else value (alias for default_value for compatibility)."""
        self.default_value = value

    def when(self, condition: Any, value: Any) -> "MockCaseWhen":
        """Add another WHEN condition.

        Args:
            condition: The condition to check.
            value: The value to return if condition is true.

        Returns:
            Self for method chaining.
        """
        self.conditions.append((condition, value))
        return self

    def otherwise(self, value: Any) -> "MockCaseWhen":
        """Set the default value for the CASE WHEN expression.

        Args:
            value: The default value to return if no conditions match.

        Returns:
            Self for method chaining.
        """
        self.default_value = value
        return self

    def alias(self, name: str) -> "MockCaseWhen":
        """Create an alias for the CASE WHEN expression.

        Args:
            name: The alias name.

        Returns:
            Self for method chaining.
        """
        self.name = name
        return self

    def evaluate(self, row: dict) -> Any:
        """Evaluate the CASE WHEN expression for a given row.

        Args:
            row: The data row to evaluate against.

        Returns:
            The evaluated result.
        """
        # Evaluate conditions in order
        for condition, value in self.conditions:
            if self._evaluate_condition(row, condition):
                return self._evaluate_value(row, value)

        # Return default value if no condition matches
        return self._evaluate_value(row, self.default_value)

    def _evaluate_condition(self, row: dict, condition: Any) -> bool:
        """Evaluate a condition for a given row (delegates to ExpressionEvaluator).

        Args:
            row: The data row to evaluate against.
            condition: The condition to evaluate.

        Returns:
            True if condition is met, False otherwise.
        """
        return self._evaluator._evaluate_case_when_condition(row, condition)

    def _evaluate_value(self, row: dict, value: Any) -> Any:
        """Evaluate a value for a given row (delegates to ExpressionEvaluator).

        Args:
            row: The data row to evaluate against.
            value: The value to evaluate.

        Returns:
            The evaluated value.
        """
        return self._evaluator.evaluate_value(row, value)

    def _get_column_value(self, row: dict, column: Any) -> Any:
        """Get the value of a column from a row (delegates to ExpressionEvaluator).

        Args:
            row: The data row.
            column: The column to get the value for.

        Returns:
            The column value.
        """
        return self._evaluator.get_column_value(row, column)


class ConditionalFunctions:
    """Collection of conditional functions."""

    @staticmethod
    def coalesce(*columns: Union[MockColumn, str, Any]) -> MockColumnOperation:
        """Return the first non-null value from a list of columns.

        Args:
            *columns: Variable number of columns or values to check.

        Returns:
            MockColumnOperation representing the coalesce function.
        """
        # Convert string columns to MockColumn objects
        mock_columns = []
        for col in columns:
            if isinstance(col, str):
                mock_columns.append(MockColumn(col))
            else:
                mock_columns.append(col)

        # Create operation with first column as base
        operation = MockColumnOperation(mock_columns[0], "coalesce", mock_columns[1:])
        operation.name = f"coalesce({', '.join([str(c) for c in mock_columns])})"
        return operation

    @staticmethod
    def isnull(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Check if a column is null.

        Args:
            column: The column to check.

        Returns:
            MockColumnOperation representing the isnull function.
        """
        if isinstance(column, str):
            column = MockColumn(column)

        operation = MockColumnOperation(column, "isnull")
        operation.name = f"isnull({column.name})"
        return operation

    @staticmethod
    def isnotnull(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Check if a column is not null.

        Args:
            column: The column to check.

        Returns:
            MockColumnOperation representing the isnotnull function.
        """
        if isinstance(column, str):
            column = MockColumn(column)

        operation = MockColumnOperation(column, "isnotnull")
        operation.name = f"isnotnull({column.name})"
        return operation

    @staticmethod
    def isnan(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Check if a column is NaN (Not a Number).

        Args:
            column: The column to check.

        Returns:
            MockColumnOperation representing the isnan function.
        """
        if isinstance(column, str):
            column = MockColumn(column)

        operation = MockColumnOperation(column, "isnan")
        operation.name = f"isnan({column.name})"
        return operation

    @staticmethod
    def when(condition: Any, value: Any = None) -> MockCaseWhen:
        """Start a CASE WHEN expression.

        Args:
            condition: The initial condition.
            value: Optional value for the condition.

        Returns:
            MockCaseWhen object for chaining.
        """
        if value is not None:
            return MockCaseWhen(condition=condition, value=value)
        return MockCaseWhen(condition=condition)
