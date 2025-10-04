"""
Conditional functions for Mock Spark.

This module contains conditional functions including CASE WHEN expressions.
"""

from typing import Any, List, Union, Optional
from mock_spark.spark_types import MockDataType, StringType, BooleanType
from mock_spark.functions.base import MockColumn, MockColumnOperation
import math


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
        """Evaluate a condition for a given row.

        Args:
            row: The data row to evaluate against.
            condition: The condition to evaluate.

        Returns:
            True if condition is met, False otherwise.
        """
        if hasattr(condition, 'operation') and hasattr(condition, 'column'):
            # Handle MockColumnOperation
            from mock_spark.functions.base import MockColumnOperation
            if isinstance(condition, MockColumnOperation):
                return self._evaluate_column_operation(row, condition)
        
        # For simple values, check if truthy
        return bool(condition) if condition is not None else False

    def _evaluate_column_operation(self, row: dict, operation) -> bool:
        """Evaluate a column operation.

        Args:
            row: The data row to evaluate against.
            operation: The column operation to evaluate.

        Returns:
            True if operation evaluates to true, False otherwise.
        """
        if operation.operation == "==":
            left_value = self._get_column_value(row, operation.column)
            right_value = self._get_column_value(row, operation.value)
            return left_value == right_value
        elif operation.operation == "!=":
            left_value = self._get_column_value(row, operation.column)
            right_value = self._get_column_value(row, operation.value)
            return left_value != right_value
        elif operation.operation == ">":
            left_value = self._get_column_value(row, operation.column)
            right_value = self._get_column_value(row, operation.value)
            return left_value is not None and right_value is not None and left_value > right_value
        elif operation.operation == ">=":
            left_value = self._get_column_value(row, operation.column)
            right_value = self._get_column_value(row, operation.value)
            return left_value is not None and right_value is not None and left_value >= right_value
        elif operation.operation == "<":
            left_value = self._get_column_value(row, operation.column)
            right_value = self._get_column_value(row, operation.value)
            return left_value is not None and right_value is not None and left_value < right_value
        elif operation.operation == "<=":
            left_value = self._get_column_value(row, operation.column)
            right_value = self._get_column_value(row, operation.value)
            return left_value is not None and right_value is not None and left_value <= right_value
        elif operation.operation == "&":
            left_result = self._evaluate_column_operation(row, operation.column)
            right_result = self._evaluate_column_operation(row, operation.value)
            return left_result and right_result
        elif operation.operation == "|":
            left_result = self._evaluate_column_operation(row, operation.column)
            right_result = self._evaluate_column_operation(row, operation.value)
            return left_result or right_result
        else:
            return False

    def _get_column_value(self, row: dict, column: Any) -> Any:
        """Get the value of a column from a row.

        Args:
            row: The data row.
            column: The column to get the value for.

        Returns:
            The column value.
        """
        if hasattr(column, 'name'):
            return row.get(column.name)
        elif hasattr(column, 'value'):
            return column.value
        else:
            return column

    def _evaluate_value(self, row: dict, value: Any) -> Any:
        """Evaluate a value for a given row.

        Args:
            row: The data row to evaluate against.
            value: The value to evaluate.

        Returns:
            The evaluated value.
        """
        if hasattr(value, 'operation') and hasattr(value, 'column'):
            # Handle MockColumnOperation (e.g., unary minus, arithmetic operations)
            from mock_spark.functions.base import MockColumnOperation
            if isinstance(value, MockColumnOperation):
                return self._evaluate_column_operation_value(row, value)
        elif hasattr(value, 'name'):
            return row.get(value.name)
        elif hasattr(value, 'value'):
            return value.value
        else:
            return value

    def _evaluate_column_operation_value(self, row: dict, operation) -> Any:
        """Evaluate a column operation for a value.

        Args:
            row: The data row.
            operation: The column operation to evaluate.

        Returns:
            The evaluated result.
        """
        if operation.operation == "-" and operation.value is None:
            # Unary minus operation
            left_value = self._get_column_value(row, operation.column)
            if left_value is None:
                return None
            return -left_value
        elif operation.operation == "+" and operation.value is None:
            # Unary plus operation (just return the value)
            return self._get_column_value(row, operation.column)
        elif operation.operation in ["+", "-", "*", "/", "%"]:
            # Binary arithmetic operations
            left_value = self._get_column_value(row, operation.column)
            right_value = self._get_column_value(row, operation.value)
            
            if left_value is None or right_value is None:
                return None
                
            if operation.operation == "+":
                return left_value + right_value
            elif operation.operation == "-":
                return left_value - right_value
            elif operation.operation == "*":
                return left_value * right_value
            elif operation.operation == "/":
                return left_value / right_value if right_value != 0 else None
            elif operation.operation == "%":
                return left_value % right_value if right_value != 0 else None
        else:
            # For other operations, try to get the column value
            return self._get_column_value(row, operation.column)


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
