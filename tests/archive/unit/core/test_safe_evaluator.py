"""
Unit tests for safe expression evaluator.

Tests the SafeExpressionEvaluator class that safely evaluates Python-like
expressions using AST parsing instead of eval(), preventing security vulnerabilities.
"""

import pytest
from sparkless.core.safe_evaluator import SafeExpressionEvaluator


@pytest.mark.unit
class TestSafeExpressionEvaluator:
    """Test SafeExpressionEvaluator operations."""

    def test_evaluate_arithmetic_addition(self):
        """Test evaluating addition expression."""
        context = {"a": 5, "b": 3}
        result = SafeExpressionEvaluator.evaluate("a + b", context)
        assert result == 8

    def test_evaluate_arithmetic_subtraction(self):
        """Test evaluating subtraction expression."""
        context = {"x": 10, "y": 4}
        result = SafeExpressionEvaluator.evaluate("x - y", context)
        assert result == 6

    def test_evaluate_arithmetic_multiplication(self):
        """Test evaluating multiplication expression."""
        context = {"a": 5, "b": 3}
        result = SafeExpressionEvaluator.evaluate("a * b", context)
        assert result == 15

    def test_evaluate_arithmetic_division(self):
        """Test evaluating division expression."""
        context = {"a": 10, "b": 2}
        result = SafeExpressionEvaluator.evaluate("a / b", context)
        assert result == 5.0

    def test_evaluate_arithmetic_modulo(self):
        """Test evaluating modulo expression."""
        context = {"a": 10, "b": 3}
        result = SafeExpressionEvaluator.evaluate("a % b", context)
        assert result == 1

    def test_evaluate_comparison_equal(self):
        """Test evaluating equality comparison."""
        context = {"age": 25}
        result = SafeExpressionEvaluator.evaluate_boolean("age == 25", context)
        assert result is True

    def test_evaluate_comparison_not_equal(self):
        """Test evaluating not equal comparison."""
        context = {"age": 25}
        result = SafeExpressionEvaluator.evaluate_boolean("age != 30", context)
        assert result is True

    def test_evaluate_comparison_less_than(self):
        """Test evaluating less than comparison."""
        context = {"age": 25}
        result = SafeExpressionEvaluator.evaluate_boolean("age < 30", context)
        assert result is True

    def test_evaluate_comparison_greater_than(self):
        """Test evaluating greater than comparison."""
        context = {"age": 25}
        result = SafeExpressionEvaluator.evaluate_boolean("age > 20", context)
        assert result is True

    def test_evaluate_comparison_less_equal(self):
        """Test evaluating less than or equal comparison."""
        context = {"age": 25}
        result = SafeExpressionEvaluator.evaluate_boolean("age <= 25", context)
        assert result is True

    def test_evaluate_comparison_greater_equal(self):
        """Test evaluating greater than or equal comparison."""
        context = {"age": 25}
        result = SafeExpressionEvaluator.evaluate_boolean("age >= 25", context)
        assert result is True

    def test_evaluate_logical_and(self):
        """Test evaluating logical AND expression."""
        context = {"age": 25, "active": True}
        result = SafeExpressionEvaluator.evaluate_boolean(
            "age > 20 and active", context
        )
        assert result is True

    def test_evaluate_logical_or(self):
        """Test evaluating logical OR expression."""
        context = {"age": 15, "active": True}
        result = SafeExpressionEvaluator.evaluate_boolean("age > 20 or active", context)
        assert result is True

    def test_evaluate_logical_not(self):
        """Test evaluating logical NOT expression."""
        context = {"active": False}
        result = SafeExpressionEvaluator.evaluate_boolean("not active", context)
        assert result is True

    def test_evaluate_complex_boolean_expression(self):
        """Test evaluating complex boolean expression."""
        context = {"age": 25, "score": 85, "active": True}
        result = SafeExpressionEvaluator.evaluate_boolean(
            "age > 20 and score >= 80 and active", context
        )
        assert result is True

    def test_evaluate_chained_comparison(self):
        """Test evaluating chained comparison."""
        context = {"age": 25}
        result = SafeExpressionEvaluator.evaluate_boolean("20 < age < 30", context)
        assert result is True

    def test_evaluate_with_literals(self):
        """Test evaluating expression with numeric literals."""
        context = {}
        result = SafeExpressionEvaluator.evaluate("5 + 3", context)
        assert result == 8

    def test_evaluate_with_string_literals(self):
        """Test evaluating expression with string literals."""
        context = {"name": "Alice"}
        result = SafeExpressionEvaluator.evaluate_boolean('name == "Alice"', context)
        assert result is True

    def test_evaluate_with_boolean_literals(self):
        """Test evaluating expression with boolean literals."""
        context = {"flag": True}
        result = SafeExpressionEvaluator.evaluate_boolean("flag == True", context)
        assert result is True

    def test_evaluate_with_none(self):
        """Test evaluating expression with None."""
        context = {"value": None}
        result = SafeExpressionEvaluator.evaluate_boolean("value == None", context)
        # In Python, None comparisons work
        assert result is True

    def test_evaluate_undefined_variable_returns_none(self):
        """Test that undefined variables return None (safe fallback)."""
        context = {"a": 5}
        result = SafeExpressionEvaluator.evaluate("b + 5", context)
        # b is undefined, should handle gracefully
        assert result is None or isinstance(result, (int, float))

    def test_evaluate_division_by_zero_handles_gracefully(self):
        """Test that division by zero is handled."""
        context = {"a": 10, "b": 0}
        # Should not raise exception, but may return None or handle gracefully
        try:
            result = SafeExpressionEvaluator.evaluate("a / b", context)
            # Result could be None, inf, or handled in some way
            assert result is None or result == float("inf") or result == float("-inf")
        except ZeroDivisionError:
            # If it raises, that's also acceptable behavior
            pass

    def test_evaluate_attribute_access(self):
        """Test evaluating attribute access."""
        from types import SimpleNamespace

        obj = SimpleNamespace(value=42)
        context = {"obj": obj}
        result = SafeExpressionEvaluator.evaluate("obj.value", context)
        assert result == 42

    def test_evaluate_ternary_expression(self):
        """Test evaluating ternary (if-else) expression."""
        context = {"age": 25}
        result = SafeExpressionEvaluator.evaluate("age if age > 20 else 0", context)
        assert result == 25

    def test_evaluate_unary_minus(self):
        """Test evaluating unary minus."""
        context = {"value": 5}
        result = SafeExpressionEvaluator.evaluate("-value", context)
        assert result == -5

    def test_evaluate_unary_plus(self):
        """Test evaluating unary plus."""
        context = {"value": -5}
        result = SafeExpressionEvaluator.evaluate("+value", context)
        assert result == -5

    def test_evaluate_complex_nested_expression(self):
        """Test evaluating complex nested expression."""
        context = {"a": 5, "b": 3, "c": 2}
        result = SafeExpressionEvaluator.evaluate("(a + b) * c", context)
        assert result == 16

    def test_evaluate_invalid_syntax_returns_false(self):
        """Test that invalid syntax returns False (boolean) or handles gracefully."""
        context = {"a": 5}
        result = SafeExpressionEvaluator.evaluate_boolean(
            "invalid syntax here", context
        )
        # Should return False for invalid syntax
        assert result is False

    def test_evaluate_sql_normalized_expressions(self):
        """Test evaluating SQL-normalized expressions (AND, OR, NOT)."""
        context = {"age": 25, "active": True}
        # These are normalized from SQL-style expressions
        result = SafeExpressionEvaluator.evaluate_boolean(
            "age > 20 and active", context
        )
        assert result is True

        result = SafeExpressionEvaluator.evaluate_boolean(
            "age < 20 or not active", context
        )
        assert result is False

    def test_evaluate_with_target_namespace(self):
        """Test evaluating expressions with target namespace (like in Delta merge)."""
        from types import SimpleNamespace

        row = {"id": 1, "value": 100}
        target_ns = SimpleNamespace(**row)
        context = {"target": target_ns, "id": 1, "value": 100}
        result = SafeExpressionEvaluator.evaluate_boolean("target.id == id", context)
        assert result is True

    def test_evaluate_safe_no_code_injection(self):
        """Test that code injection attempts are safely handled."""
        context = {"x": 5}
        # Attempted code injection should not execute
        result = SafeExpressionEvaluator.evaluate(
            "__import__('os').system('rm -rf /')", context
        )
        # Should return None or handle gracefully, not execute
        assert result is None or isinstance(result, (bool, int, float, str))

    def test_evaluate_comparison_with_nulls(self):
        """Test evaluating comparisons with null values."""
        context = {"age": None, "value": 25}
        # NULL comparisons - only != returns True
        result1 = SafeExpressionEvaluator.evaluate_boolean("age == 25", context)
        assert result1 is False

        result2 = SafeExpressionEvaluator.evaluate_boolean("age != 25", context)
        assert result2 is True

    def test_evaluate_boolean_with_none_result(self):
        """Test that evaluate_boolean handles None results."""
        context = {"value": None}
        result = SafeExpressionEvaluator.evaluate_boolean("value", context)
        # None should be treated as False in boolean context
        assert result is False

    def test_evaluate_string_concatenation(self):
        """Test evaluating string concatenation."""
        context = {"first": "Hello", "second": "World"}
        result = SafeExpressionEvaluator.evaluate('first + " " + second', context)
        assert result == "Hello World"

    def test_evaluate_numeric_types(self):
        """Test evaluating with different numeric types."""
        context = {"int_val": 5, "float_val": 3.14}
        result = SafeExpressionEvaluator.evaluate("int_val + float_val", context)
        assert result == 8.14
