"""
Conditional functions for Mock Spark.

This module contains conditional functions including CASE WHEN expressions.
"""

from typing import Any, List, Union, TYPE_CHECKING
from mock_spark.functions.base import MockColumn, MockColumnOperation
from mock_spark.core.condition_evaluator import ConditionEvaluator

if TYPE_CHECKING:
    from mock_spark.spark_types import MockDataType


def validate_rule(
    column: Union[MockColumn, str], rule: Union[str, List]
) -> MockColumnOperation:
    """Convert validation rule to column expression.

    Args:
        column: The column to validate.
        rule: Validation rule as string or list.

    Returns:
        MockColumn expression for the validation rule.

    Raises:
        ValueError: If rule is not recognized.
    """
    if isinstance(column, str):
        column = MockColumn(column)

    if isinstance(rule, str):
        # String rules
        if rule == "not_null":
            return column.isNotNull()
        elif rule == "positive":
            return column > 0
        elif rule == "non_negative":
            return column >= 0
        elif rule == "negative":
            return column < 0
        elif rule == "non_positive":
            return column <= 0
        elif rule == "non_zero":
            return column != 0
        elif rule == "zero":
            return column == 0
        else:
            raise ValueError(f"Unknown string validation rule: {rule}")
    elif isinstance(rule, list):
        # List rules: ["operator", arg1, arg2, ...]
        if not rule:
            raise ValueError("Empty rule list")

        op = rule[0]
        if op == "gt":
            if len(rule) < 2:
                raise ValueError("gt rule requires a value")
            return column > rule[1]
        elif op == "gte":
            if len(rule) < 2:
                raise ValueError("gte rule requires a value")
            return column >= rule[1]
        elif op == "lt":
            if len(rule) < 2:
                raise ValueError("lt rule requires a value")
            return column < rule[1]
        elif op == "lte":
            if len(rule) < 2:
                raise ValueError("lte rule requires a value")
            return column <= rule[1]
        elif op == "eq":
            if len(rule) < 2:
                raise ValueError("eq rule requires a value")
            return column == rule[1]
        elif op == "ne":
            if len(rule) < 2:
                raise ValueError("ne rule requires a value")
            return column != rule[1]
        elif op == "between":
            if len(rule) < 3:
                raise ValueError("between rule requires two values")
            return column.between(rule[1], rule[2])
        elif op == "in":
            if len(rule) < 2:
                raise ValueError("in rule requires a list of values")
            return column.isin(rule[1])
        elif op == "not_in":
            if len(rule) < 2:
                raise ValueError("not_in rule requires a list of values")
            return ~column.isin(rule[1])
        elif op == "contains":
            if len(rule) < 2:
                raise ValueError("contains rule requires a value")
            return column.contains(rule[1])
        elif op == "starts_with":
            if len(rule) < 2:
                raise ValueError("starts_with rule requires a value")
            return column.startswith(rule[1])
        elif op == "ends_with":
            if len(rule) < 2:
                raise ValueError("ends_with rule requires a value")
            return column.endswith(rule[1])
        elif op == "regex":
            if len(rule) < 2:
                raise ValueError("regex rule requires a pattern")
            return column.rlike(rule[1])
        else:
            raise ValueError(f"Unknown list validation rule: {op}")
    else:
        raise ValueError(f"Unknown validation rule type: {type(rule)}")


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

    def get_result_type(self) -> "MockDataType":
        """Infer the result type from condition values."""
        from ..spark_types import BooleanType, IntegerType, StringType, DoubleType
        from .core.literals import MockLiteral

        # Check all condition values and default value
        all_values = [v for _, v in self.conditions]
        if self.default_value is not None:
            all_values.append(self.default_value)

        # Check if all values are literals (which are never nullable)
        all_literals = all(
            isinstance(val, MockLiteral) or val is None for val in all_values
        )

        for val in all_values:
            if val is not None:
                if isinstance(val, MockLiteral):
                    # For MockLiteral, create a new instance with correct nullable
                    data_type = val.data_type
                    if isinstance(data_type, BooleanType):
                        return BooleanType(
                            nullable=False
                        )  # Literals are never nullable
                    elif isinstance(data_type, IntegerType):
                        return IntegerType(
                            nullable=False
                        )  # Literals are never nullable
                    elif isinstance(data_type, DoubleType):
                        return DoubleType(nullable=False)  # Literals are never nullable
                    elif isinstance(data_type, StringType):
                        return StringType(nullable=False)  # Literals are never nullable
                    else:
                        # For other types, create with correct nullable
                        return data_type.__class__(
                            nullable=False
                        )  # Literals are never nullable
                elif isinstance(val, bool):
                    return BooleanType(nullable=False)  # Literals are never nullable
                elif isinstance(val, int):
                    return IntegerType(nullable=False)  # Literals are never nullable
                elif isinstance(val, float):
                    return DoubleType(nullable=False)  # Literals are never nullable
                elif isinstance(val, str):
                    return StringType(nullable=False)  # Literals are never nullable
                elif hasattr(val, "operation") and hasattr(val, "column"):
                    # Handle MockColumnOperation - check the operation type
                    from ..spark_types import LongType

                    if val.operation in ["+", "-", "*", "/", "%", "abs"]:
                        # Arithmetic operations return LongType
                        return LongType(nullable=False)
                    elif val.operation in ["round"]:
                        # Round operations return DoubleType
                        return DoubleType(nullable=False)
                    else:
                        # Default to StringType for other operations
                        return StringType(nullable=False)

        # Default to LongType for arithmetic operations, not BooleanType
        return LongType(nullable=not all_literals)

    def _evaluate_condition(self, row: dict, condition: Any) -> bool:
        """Evaluate a condition for a given row.

        Delegates to shared ConditionEvaluator for consistency.

        Args:
            row: The data row to evaluate against.
            condition: The condition to evaluate.

        Returns:
            True if condition is met, False otherwise.
        """
        from mock_spark.core.condition_evaluator import ConditionEvaluator

        return ConditionEvaluator.evaluate_condition(row, condition)

    def _evaluate_value(self, row: dict, value: Any) -> Any:
        """Evaluate a value for a given row.

        Args:
            row: The data row to evaluate against.
            value: The value to evaluate.

        Returns:
            The evaluated value.
        """
        from .core.literals import MockLiteral

        if isinstance(value, MockLiteral):
            # For MockLiteral, return the actual value
            return value.value
        elif hasattr(value, "operation") and hasattr(value, "column"):
            # Handle MockColumnOperation (e.g., unary minus, arithmetic operations)
            from mock_spark.functions.base import MockColumnOperation

            if isinstance(value, MockColumnOperation):
                return self._evaluate_column_operation_value(row, value)
        elif hasattr(value, "name"):
            return row.get(value.name)
        elif hasattr(value, "value"):
            return value.value
        else:
            return value

    def _evaluate_column_operation_value(self, row: dict, operation: Any) -> Any:
        """Evaluate a column operation for a value.

        Args:
            row: The data row.
            operation: The column operation to evaluate.

        Returns:
            The evaluated result.
        """
        if operation.operation == "-" and operation.value is None:
            # Unary minus operation
            left_value = ConditionEvaluator._get_column_value(row, operation.column)
            if left_value is None:
                return None
            return -left_value
        elif operation.operation == "+" and operation.value is None:
            # Unary plus operation (just return the value)
            return ConditionEvaluator._get_column_value(row, operation.column)
        elif operation.operation in ["+", "-", "*", "/", "%"]:
            # Binary arithmetic operations
            left_value = ConditionEvaluator._get_column_value(row, operation.column)
            right_value = ConditionEvaluator._get_column_value(row, operation.value)

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
            return ConditionEvaluator._get_column_value(row, operation.column)


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

    @staticmethod
    def assert_true(
        condition: Union[MockColumn, MockColumnOperation],
    ) -> MockColumnOperation:
        """Assert that a condition is true, raises error if false.

        Args:
            condition: Boolean condition to assert.

        Returns:
            MockColumnOperation representing the assert_true function.

        Example:
            >>> df.select(F.assert_true(F.col("value") > 0))
        """
        return MockColumnOperation(
            condition if isinstance(condition, MockColumn) else condition.column,
            "assert_true",
            condition if not isinstance(condition, MockColumn) else None,
            name=f"assert_true({condition if isinstance(condition, str) else getattr(condition, 'name', 'condition')})",
        )

    # Priority 2: Conditional/Null Functions
    @staticmethod
    def ifnull(
        col1: Union[MockColumn, str], col2: Union[MockColumn, str]
    ) -> MockColumnOperation:
        """Alias for coalesce(col1, col2) - Returns col2 if col1 is null (PySpark 3.5+).

        Args:
            col1: First column.
            col2: Second column (replacement for null).

        Returns:
            MockColumnOperation representing the ifnull function.
        """
        return ConditionalFunctions.coalesce(col1, col2)

    @staticmethod
    def nullif(
        col1: Union[MockColumn, str], col2: Union[MockColumn, str]
    ) -> MockColumnOperation:
        """Returns null if col1 equals col2, otherwise returns col1 (PySpark 3.5+).

        Args:
            col1: First column.
            col2: Second column to compare.

        Returns:
            MockColumnOperation representing the nullif function.
        """
        column1 = MockColumn(col1) if isinstance(col1, str) else col1
        column2 = MockColumn(col2) if isinstance(col2, str) else col2

        return MockColumnOperation(
            column1,
            "nullif",
            value=column2,
            name=f"nullif({column1.name}, {column2.name})",
        )
