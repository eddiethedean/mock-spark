"""
Condition Handler for MockDataFrame operations.

This module provides a centralized handler for all condition evaluation logic,
ensuring consistency and adherence to the Single Responsibility Principle.
"""

from typing import Any, Dict, List, Union, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ...functions import MockColumn, MockColumnOperation
    from .evaluation.expression_evaluator import ExpressionEvaluator


class ConditionHandler:
    """Handles condition evaluation for MockDataFrame operations.

    This class consolidates all condition evaluation logic that was previously
    scattered throughout MockDataFrame, providing a single point of
    responsibility for condition evaluation operations.
    """

    def __init__(self) -> None:
        """Initialize the ConditionHandler."""
        self._expression_evaluator: Optional["ExpressionEvaluator"] = None

    def _get_expression_evaluator(self) -> "ExpressionEvaluator":
        """Lazy initialization of ExpressionEvaluator."""
        if self._expression_evaluator is None:
            from .evaluation.expression_evaluator import ExpressionEvaluator

            self._expression_evaluator = ExpressionEvaluator()
        return self._expression_evaluator

    def apply_condition(
        self, data: List[Dict[str, Any]], condition: "MockColumnOperation"
    ) -> List[Dict[str, Any]]:
        """Apply condition to filter data.

        Args:
            data: List of row dictionaries to filter.
            condition: The condition to apply for filtering.

        Returns:
            Filtered list of row dictionaries.
        """
        filtered_data = []

        for row in data:
            if self.evaluate_condition(row, condition):
                filtered_data.append(row)

        return filtered_data

    def evaluate_condition(
        self, row: Dict[str, Any], condition: Union["MockColumnOperation", "MockColumn"]
    ) -> bool:
        """Evaluate condition for a single row.

        Delegates to ExpressionEvaluator for consistency.

        Args:
            row: Dictionary representing a single row.
            condition: The condition to evaluate.

        Returns:
            Boolean result of the condition evaluation.
        """
        return self._get_expression_evaluator().evaluate_condition(row, condition)

    def evaluate_column_expression(
        self, row: Dict[str, Any], column_expression: Any
    ) -> Any:
        """Evaluate a column expression for a single row.

        Args:
            row: Dictionary representing a single row.
            column_expression: The column expression to evaluate.

        Returns:
            Result of the column expression evaluation.
        """
        return self._get_expression_evaluator().evaluate_expression(
            row, column_expression
        )

    def evaluate_case_when(self, row: Dict[str, Any], case_when_obj: Any) -> Any:
        """Evaluate CASE WHEN expression for a row.

        Args:
            row: Dictionary representing a single row.
            case_when_obj: The CASE WHEN object to evaluate.

        Returns:
            Result of the CASE WHEN evaluation.
        """
        # Use the MockCaseWhen's own evaluate method
        if hasattr(case_when_obj, "evaluate"):
            return case_when_obj.evaluate(row)

        # Fallback to manual evaluation
        for condition, value in case_when_obj.conditions:
            if self._evaluate_case_when_condition(row, condition):
                return self._get_expression_evaluator().evaluate_expression(row, value)

        if case_when_obj.else_value is not None:
            return self._get_expression_evaluator().evaluate_expression(
                row, case_when_obj.else_value
            )

        return None

    def _evaluate_case_when_condition(
        self, row: Dict[str, Any], condition: Any
    ) -> bool:
        """Evaluate a CASE WHEN condition for a row.

        Args:
            row: Dictionary representing a single row.
            condition: The condition to evaluate.

        Returns:
            Boolean result of the condition evaluation.
        """
        if hasattr(condition, "operation") and hasattr(condition, "column"):
            # Handle MockColumnOperation conditions
            if condition.operation == ">":
                col_value = (
                    row.get(condition.column.name)
                    if hasattr(condition.column, "name")
                    else row.get(str(condition.column))
                )
                return col_value is not None and col_value > condition.value
            elif condition.operation == ">=":
                col_value = (
                    row.get(condition.column.name)
                    if hasattr(condition.column, "name")
                    else row.get(str(condition.column))
                )
                return col_value is not None and col_value >= condition.value
            elif condition.operation == "<":
                col_value = (
                    row.get(condition.column.name)
                    if hasattr(condition.column, "name")
                    else row.get(str(condition.column))
                )
                return col_value is not None and col_value < condition.value
            elif condition.operation == "<=":
                col_value = (
                    row.get(condition.column.name)
                    if hasattr(condition.column, "name")
                    else row.get(str(condition.column))
                )
                return col_value is not None and col_value <= condition.value
            elif condition.operation == "==":
                col_value = (
                    row.get(condition.column.name)
                    if hasattr(condition.column, "name")
                    else row.get(str(condition.column))
                )
                return bool(col_value == condition.value)
            elif condition.operation == "!=":
                col_value = (
                    row.get(condition.column.name)
                    if hasattr(condition.column, "name")
                    else row.get(str(condition.column))
                )
                return bool(col_value != condition.value)
        return False
