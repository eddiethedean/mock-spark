"""
Unit tests for condition evaluator.
"""

import pytest
from mock_spark.core.condition_evaluator import ConditionEvaluator
from mock_spark.functions import Column, ColumnOperation


@pytest.mark.unit
class TestConditionEvaluator:
    """Test ConditionEvaluator operations."""

    def test_evaluate_condition_column_reference(self):
        """Test evaluating condition with column reference."""
        row = {"active": True, "inactive": None}

        col_active = Column("active")
        result = ConditionEvaluator.evaluate_condition(row, col_active)

        assert result is True

        col_inactive = Column("inactive")
        result = ConditionEvaluator.evaluate_condition(row, col_inactive)

        assert result is False

    def test_evaluate_condition_comparison_equal(self):
        """Test evaluating equality comparison condition."""
        row = {"age": 25}

        col_age = Column("age")
        operation = ColumnOperation(col_age, "==", 25)

        result = ConditionEvaluator.evaluate_condition(row, operation)

        assert result is True

    def test_evaluate_condition_comparison_not_equal(self):
        """Test evaluating not equal comparison condition."""
        row = {"age": 25}

        col_age = Column("age")
        operation = ColumnOperation(col_age, "!=", 30)

        result = ConditionEvaluator.evaluate_condition(row, operation)

        assert result is True

    def test_evaluate_condition_comparison_less_than(self):
        """Test evaluating less than comparison condition."""
        row = {"age": 25}

        col_age = Column("age")
        operation = ColumnOperation(col_age, "<", 30)

        result = ConditionEvaluator.evaluate_condition(row, operation)

        assert result is True

    def test_evaluate_condition_comparison_greater_than(self):
        """Test evaluating greater than comparison condition."""
        row = {"age": 25}

        col_age = Column("age")
        operation = ColumnOperation(col_age, ">", 20)

        result = ConditionEvaluator.evaluate_condition(row, operation)

        assert result is True

    def test_evaluate_condition_with_null(self):
        """Test evaluating condition with null values."""
        row = {"age": None}

        col_age = Column("age")
        operation = ColumnOperation(col_age, "==", 25)

        result = ConditionEvaluator.evaluate_condition(row, operation)

        assert result is False

    def test_evaluate_condition_boolean_value(self):
        """Test evaluating condition with boolean value."""
        row = {}

        result = ConditionEvaluator.evaluate_condition(row, True)
        assert result is True

        result = ConditionEvaluator.evaluate_condition(row, False)
        assert result is False

    def test_evaluate_condition_none_value(self):
        """Test evaluating condition with None value."""
        row = {}

        result = ConditionEvaluator.evaluate_condition(row, None)

        assert result is False

    def test_evaluate_expression_arithmetic(self):
        """Test evaluating arithmetic expression."""
        row = {"a": 5, "b": 3}

        col_a = Column("a")
        col_b = Column("b")
        operation = ColumnOperation(col_a, "+", col_b)

        result = ConditionEvaluator.evaluate_expression(row, operation)

        assert result == 8

    def test_evaluate_expression_with_null(self):
        """Test evaluating expression with null values."""
        row = {"a": 5, "b": None}

        col_a = Column("a")
        col_b = Column("b")
        operation = ColumnOperation(col_a, "+", col_b)

        result = ConditionEvaluator.evaluate_expression(row, operation)

        assert result is None

    def test_evaluate_expression_division_by_zero(self):
        """Test evaluating division by zero returns None."""
        row = {"a": 5, "b": 0}

        col_a = Column("a")
        col_b = Column("b")
        operation = ColumnOperation(col_a, "/", col_b)

        result = ConditionEvaluator.evaluate_expression(row, operation)

        assert result is None

    def test_evaluate_expression_modulo_by_zero(self):
        """Test evaluating modulo by zero returns None."""
        row = {"a": 5, "b": 0}

        col_a = Column("a")
        col_b = Column("b")
        operation = ColumnOperation(col_a, "%", col_b)

        result = ConditionEvaluator.evaluate_expression(row, operation)

        assert result is None

    def test_evaluate_expression_cast_to_int(self):
        """Test evaluating cast operation to int."""
        row = {"value": "42"}

        col_value = Column("value")
        operation = ColumnOperation(col_value, "cast", "int")

        result = ConditionEvaluator.evaluate_expression(row, operation)

        assert result == 42

    def test_evaluate_expression_cast_to_double(self):
        """Test evaluating cast operation to double."""
        row = {"value": "3.14"}

        col_value = Column("value")
        operation = ColumnOperation(col_value, "cast", "double")

        result = ConditionEvaluator.evaluate_expression(row, operation)

        assert result == 3.14

    def test_evaluate_expression_cast_to_string(self):
        """Test evaluating cast operation to string."""
        row = {"value": 42}

        col_value = Column("value")
        operation = ColumnOperation(col_value, "cast", "string")

        result = ConditionEvaluator.evaluate_expression(row, operation)

        assert result == "42"

    def test_evaluate_expression_cast_with_null(self):
        """Test evaluating cast operation with null value."""
        row = {"value": None}

        col_value = Column("value")
        operation = ColumnOperation(col_value, "cast", "int")

        result = ConditionEvaluator.evaluate_expression(row, operation)

        assert result is None
