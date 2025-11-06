"""
Unit tests for expression evaluator.
"""

import pytest
from mock_spark.dataframe.evaluation.expression_evaluator import ExpressionEvaluator
from mock_spark.functions import Column, ColumnOperation
from mock_spark.functions.conditional import CaseWhen
from mock_spark.functions.core.literals import Literal


@pytest.mark.unit
class TestExpressionEvaluator:
    """Test ExpressionEvaluator operations."""

    def test_evaluate_arithmetic_addition(self):
        """Test evaluating addition operation."""
        evaluator = ExpressionEvaluator()
        row = {"a": 5, "b": 3}

        # Create a column operation: a + b
        col_a = Column("a")
        col_b = Column("b")
        operation = ColumnOperation(col_a, "+", col_b)

        result = evaluator.evaluate_expression(row, operation)

        assert result == 8

    def test_evaluate_arithmetic_subtraction(self):
        """Test evaluating subtraction operation."""
        evaluator = ExpressionEvaluator()
        row = {"a": 5, "b": 3}

        col_a = Column("a")
        col_b = Column("b")
        operation = ColumnOperation(col_a, "-", col_b)

        result = evaluator.evaluate_expression(row, operation)

        assert result == 2

    def test_evaluate_arithmetic_multiplication(self):
        """Test evaluating multiplication operation."""
        evaluator = ExpressionEvaluator()
        row = {"a": 5, "b": 3}

        col_a = Column("a")
        col_b = Column("b")
        operation = ColumnOperation(col_a, "*", col_b)

        result = evaluator.evaluate_expression(row, operation)

        assert result == 15

    def test_evaluate_arithmetic_division(self):
        """Test evaluating division operation."""
        evaluator = ExpressionEvaluator()
        row = {"a": 10, "b": 2}

        col_a = Column("a")
        col_b = Column("b")
        operation = ColumnOperation(col_a, "/", col_b)

        result = evaluator.evaluate_expression(row, operation)

        assert result == 5.0

    def test_evaluate_arithmetic_with_null(self):
        """Test arithmetic operations with null values."""
        evaluator = ExpressionEvaluator()
        row = {"a": 5, "b": None}

        col_a = Column("a")
        col_b = Column("b")
        operation = ColumnOperation(col_a, "+", col_b)

        result = evaluator.evaluate_expression(row, operation)

        assert result is None

    def test_evaluate_comparison_equal(self):
        """Test evaluating equality comparison."""
        evaluator = ExpressionEvaluator()
        row = {"a": 5, "b": 5}

        col_a = Column("a")
        col_b = Column("b")
        operation = ColumnOperation(col_a, "==", col_b)

        result = evaluator.evaluate_expression(row, operation)

        assert result is True

    def test_evaluate_comparison_not_equal(self):
        """Test evaluating not equal comparison."""
        evaluator = ExpressionEvaluator()
        row = {"a": 5, "b": 3}

        col_a = Column("a")
        col_b = Column("b")
        operation = ColumnOperation(col_a, "!=", col_b)

        result = evaluator.evaluate_expression(row, operation)

        assert result is True

    def test_evaluate_comparison_less_than(self):
        """Test evaluating less than comparison."""
        evaluator = ExpressionEvaluator()
        row = {"a": 3, "b": 5}

        col_a = Column("a")
        col_b = Column("b")
        operation = ColumnOperation(col_a, "<", col_b)

        result = evaluator.evaluate_expression(row, operation)

        assert result is True

    def test_evaluate_column_reference(self):
        """Test evaluating simple column reference."""
        evaluator = ExpressionEvaluator()
        row = {"name": "Alice", "age": 25}

        col = Column("name")
        result = evaluator.evaluate_expression(row, col)

        assert result == "Alice"

    def test_evaluate_literal(self):
        """Test evaluating literal value."""
        evaluator = ExpressionEvaluator()
        row = {}

        literal = Literal(42)
        result = evaluator.evaluate_expression(row, literal)

        assert result == 42

    def test_evaluate_case_when(self):
        """Test evaluating case when expression."""
        evaluator = ExpressionEvaluator()
        row = {"age": 25}

        col_age = Column("age")
        case_when = CaseWhen()
        case_when.when(col_age > 30, "Senior")
        case_when.when(col_age > 20, "Mid")
        case_when.otherwise("Junior")

        result = evaluator.evaluate_expression(row, case_when)

        assert result == "Mid"

    def test_evaluate_case_when_default(self):
        """Test case when with default value."""
        evaluator = ExpressionEvaluator()
        row = {"age": 15}

        col_age = Column("age")
        case_when = CaseWhen()
        case_when.when(col_age > 30, "Senior")
        case_when.otherwise("Junior")

        result = evaluator.evaluate_expression(row, case_when)

        assert result == "Junior"

    def test_evaluate_direct_value(self):
        """Test evaluating direct value (not an expression)."""
        evaluator = ExpressionEvaluator()
        row = {}

        result = evaluator.evaluate_expression(row, 42)

        assert result == 42

    def test_evaluate_unary_minus(self):
        """Test evaluating unary minus operation."""
        evaluator = ExpressionEvaluator()
        row = {"a": 5}

        col_a = Column("a")
        operation = ColumnOperation(col_a, "-", None)

        result = evaluator.evaluate_expression(row, operation)

        assert result == -5

    def test_evaluate_unary_minus_with_null(self):
        """Test unary minus with null value."""
        evaluator = ExpressionEvaluator()
        row = {"a": None}

        col_a = Column("a")
        operation = ColumnOperation(col_a, "-", None)

        result = evaluator.evaluate_expression(row, operation)

        assert result is None
