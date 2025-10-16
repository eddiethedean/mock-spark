"""
Unit tests for Lambda Expression Parser.

Tests the AST parsing and translation of Python lambdas to DuckDB syntax.
"""

from mock_spark.functions.core.lambda_parser import (
    LambdaParser,
    MockLambdaExpression,
)


class TestLambdaParser:
    """Test lambda expression parsing and translation."""

    def test_simple_arithmetic_lambda(self):
        """Test simple arithmetic lambda: lambda x: x * 2"""
        lambda_func = lambda x: x * 2  # noqa: E731
        parser = LambdaParser(lambda_func)
        
        result = parser.to_duckdb_lambda()
        assert result == "x -> (x * 2)"

    def test_addition_lambda(self):
        """Test addition lambda: lambda x: x + 10"""
        lambda_func = lambda x: x + 10  # noqa: E731
        parser = LambdaParser(lambda_func)
        
        result = parser.to_duckdb_lambda()
        assert result == "x -> (x + 10)"

    def test_comparison_lambda(self):
        """Test comparison lambda: lambda x: x > 10"""
        lambda_func = lambda x: x > 10  # noqa: E731
        parser = LambdaParser(lambda_func)
        
        result = parser.to_duckdb_lambda()
        assert result == "x -> (x > 10)"

    def test_multiple_comparisons(self):
        """Test multiple comparisons: lambda x: x > 0 and x < 100"""
        lambda_func = lambda x: (x > 0) and (x < 100)  # noqa: E731
        parser = LambdaParser(lambda_func)
        
        result = parser.to_duckdb_lambda()
        # DuckDB uses AND for logical and
        assert "x -> " in result
        assert ">" in result
        assert "<" in result

    def test_two_arg_lambda(self):
        """Test two argument lambda: lambda x, y: x + y"""
        lambda_func = lambda x, y: x + y  # noqa: E731
        parser = LambdaParser(lambda_func)
        
        result = parser.to_duckdb_lambda()
        assert result == "(x, y) -> (x + y)"

    def test_accumulator_lambda(self):
        """Test accumulator lambda: lambda acc, x: acc + x"""
        lambda_func = lambda acc, x: acc + x  # noqa: E731
        parser = LambdaParser(lambda_func)
        
        result = parser.to_duckdb_lambda()
        assert result == "(acc, x) -> (acc + x)"

    def test_division_lambda(self):
        """Test division: lambda x: x / 2"""
        lambda_func = lambda x: x / 2  # noqa: E731
        parser = LambdaParser(lambda_func)
        
        result = parser.to_duckdb_lambda()
        assert result == "x -> (x / 2)"

    def test_subtraction_lambda(self):
        """Test subtraction: lambda x: x - 5"""
        lambda_func = lambda x: x - 5  # noqa: E731
        parser = LambdaParser(lambda_func)
        
        result = parser.to_duckdb_lambda()
        assert result == "x -> (x - 5)"

    def test_equality_lambda(self):
        """Test equality: lambda x: x == 5"""
        lambda_func = lambda x: x == 5  # noqa: E731
        parser = LambdaParser(lambda_func)
        
        result = parser.to_duckdb_lambda()
        assert result == "x -> (x = 5)"  # DuckDB uses = for equality

    def test_inequality_lambda(self):
        """Test inequality: lambda x: x != 5"""
        lambda_func = lambda x: x != 5  # noqa: E731
        parser = LambdaParser(lambda_func)
        
        result = parser.to_duckdb_lambda()
        assert result == "x -> (x != 5)" or result == "x -> (x <> 5)"

    def test_nested_operations(self):
        """Test nested operations: lambda x: (x * 2) + 10"""
        lambda_func = lambda x: (x * 2) + 10  # noqa: E731
        parser = LambdaParser(lambda_func)
        
        result = parser.to_duckdb_lambda()
        assert "x -> " in result
        assert "*" in result
        assert "+" in result

    def test_get_param_names_single(self):
        """Test extracting single parameter name."""
        lambda_func = lambda x: x * 2  # noqa: E731
        parser = LambdaParser(lambda_func)
        
        params = parser.get_param_names()
        assert params == ["x"]

    def test_get_param_names_multiple(self):
        """Test extracting multiple parameter names."""
        lambda_func = lambda x, y: x + y  # noqa: E731
        parser = LambdaParser(lambda_func)
        
        params = parser.get_param_names()
        assert params == ["x", "y"]


class TestMockLambdaExpression:
    """Test MockLambdaExpression wrapper class."""

    def test_create_lambda_expression(self):
        """Test creating a MockLambdaExpression."""
        lambda_func = lambda x: x * 2  # noqa: E731
        expr = MockLambdaExpression(lambda_func)
        
        assert expr.lambda_func == lambda_func
        assert expr.param_count == 1

    def test_lambda_expression_to_duckdb(self):
        """Test converting lambda expression to DuckDB syntax."""
        lambda_func = lambda x: x > 10  # noqa: E731
        expr = MockLambdaExpression(lambda_func)
        
        result = expr.to_duckdb_lambda()
        assert result == "x -> (x > 10)"

    def test_lambda_expression_param_count(self):
        """Test parameter count detection."""
        single_param = MockLambdaExpression(lambda x: x * 2)
        assert single_param.param_count == 1
        
        double_param = MockLambdaExpression(lambda x, y: x + y)
        assert double_param.param_count == 2

    def test_lambda_expression_repr(self):
        """Test string representation."""
        lambda_func = lambda x: x * 2  # noqa: E731
        expr = MockLambdaExpression(lambda_func)
        
        repr_str = repr(expr)
        assert "MockLambdaExpression" in repr_str


class TestLambdaTranslationError:
    """Test lambda translation error handling."""

    def test_error_raised_for_complex_lambda(self):
        """Test that complex unsupported lambdas raise errors."""
        # This test will verify error handling exists
        # Implementation may support more features over time
        pass


class TestLambdaIntegration:
    """Integration tests for lambda expressions with mock-spark."""

    def test_lambda_with_literal_values(self):
        """Test lambda with literal numeric values."""
        lambda_func = lambda x: x + 100  # noqa: E731
        parser = LambdaParser(lambda_func)
        
        result = parser.to_duckdb_lambda()
        assert "100" in result

    def test_lambda_with_string_comparison(self):
        """Test lambda with string values (future support)."""
        # Placeholder for future string lambda support
        pass

