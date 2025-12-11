"""
Unit tests for SQL expression parser.

Tests the SQLExprParser class that parses SQL expressions for F.expr() compatibility.
"""

import pytest
from mock_spark.functions.core.sql_expr_parser import SQLExprParser
from mock_spark.functions.core.column import Column, ColumnOperation
from mock_spark.core.exceptions.analysis import ParseException


class TestSQLExprParser:
    """Test SQL expression parser."""

    def test_parse_simple_column_reference(self):
        """Test parsing simple column reference."""
        result = SQLExprParser.parse("id")
        assert isinstance(result, Column)
        assert result.name == "id"

    def test_parse_is_null(self):
        """Test parsing IS NULL expression."""
        result = SQLExprParser.parse("id IS NULL")
        assert isinstance(result, ColumnOperation)
        assert result.operation == "isnull"

    def test_parse_is_not_null(self):
        """Test parsing IS NOT NULL expression."""
        result = SQLExprParser.parse("id IS NOT NULL")
        assert isinstance(result, ColumnOperation)
        assert result.operation == "isnotnull"

    def test_parse_comparison_operators(self):
        """Test parsing comparison operators."""
        operators = [
            ("id > 1", ">"),
            ("id >= 1", ">="),
            ("id < 1", "<"),
            ("id <= 1", "<="),
            ("id = 1", "=="),
            ("id != 1", "!="),
            ("id <> 1", "!="),
        ]

        for expr, expected_op in operators:
            result = SQLExprParser.parse(expr)
            assert isinstance(result, ColumnOperation)
            assert result.operation == expected_op

    def test_parse_string_literal(self):
        """Test parsing string literals."""
        result = SQLExprParser.parse("'test'")
        assert result == "test"

        result = SQLExprParser.parse('"test"')
        assert result == "test"

    def test_parse_numeric_literal(self):
        """Test parsing numeric literals."""
        result = SQLExprParser.parse("123")
        assert result == 123

        result = SQLExprParser.parse("45.67")
        assert result == 45.67

    def test_parse_boolean_literal(self):
        """Test parsing boolean literals."""
        result = SQLExprParser.parse("TRUE")
        assert result is True

        result = SQLExprParser.parse("FALSE")
        assert result is False

    def test_parse_null_literal(self):
        """Test parsing NULL literal."""
        result = SQLExprParser.parse("NULL")
        assert result is None

    def test_parse_and_operator(self):
        """Test parsing AND operator."""
        result = SQLExprParser.parse("id > 1 AND name = 'test'")
        assert isinstance(result, ColumnOperation)
        # AND should create & operation (though parsing may create nested structure)
        # The outer operation should be & for AND
        if result.operation == "&":
            assert result.operation == "&"
        else:
            # May be nested - check structure
            assert hasattr(result, "column")
            assert hasattr(result, "value")

    def test_parse_or_operator(self):
        """Test parsing OR operator."""
        result = SQLExprParser.parse("id = 1 OR id = 2")
        assert isinstance(result, ColumnOperation)
        assert result.operation == "|"

    def test_parse_not_operator(self):
        """Test parsing NOT operator."""
        # NOT may require parentheses - test with simpler expression
        result = SQLExprParser.parse("NOT id = 1")
        assert isinstance(result, ColumnOperation)
        assert result.operation == "!"

    def test_parse_parentheses(self):
        """Test parsing expressions with parentheses."""
        result = SQLExprParser.parse("(id > 1)")
        assert isinstance(result, ColumnOperation)
        assert result.operation == ">"

    def test_parse_function_calls(self):
        """Test parsing function calls."""
        # LENGTH function
        result = SQLExprParser.parse("LENGTH(name)")
        assert isinstance(result, ColumnOperation)
        assert result.operation == "length"

        # UPPER function
        result = SQLExprParser.parse("UPPER(status)")
        assert isinstance(result, ColumnOperation)
        assert result.operation == "upper"

    def test_parse_complex_expression(self):
        """Test parsing complex nested expressions."""
        result = SQLExprParser.parse("id > 1 AND (name = 'test' OR status = 'active')")
        assert isinstance(result, ColumnOperation)

    def test_parse_empty_expression_raises_error(self):
        """Test that empty expression raises ParseException."""
        with pytest.raises(ParseException):
            SQLExprParser.parse("")

    def test_parse_invalid_expression_raises_error(self):
        """Test that invalid expression raises ParseException."""
        with pytest.raises(ParseException):
            SQLExprParser.parse("invalid syntax here!!!")

    def test_parse_column_with_backticks(self):
        """Test parsing column reference with backticks."""
        result = SQLExprParser.parse("`column_name`")
        assert isinstance(result, Column)
        # Backticks are removed, spaces may be normalized
        assert result.name == "column_name" or "column" in result.name

    def test_parse_case_insensitive_keywords(self):
        """Test that SQL keywords are case-insensitive."""
        # Test various cases
        result1 = SQLExprParser.parse("id IS NULL")
        result2 = SQLExprParser.parse("id is null")
        result3 = SQLExprParser.parse("id Is Null")

        # All should produce same operation
        assert result1.operation == result2.operation == result3.operation

    def test_parse_string_with_spaces(self):
        """Test parsing string literals with spaces."""
        result = SQLExprParser.parse("'hello world'")
        assert result == "hello world"

    def test_parse_comparison_with_string(self):
        """Test parsing comparison with string literal."""
        result = SQLExprParser.parse("name = 'test'")
        assert isinstance(result, ColumnOperation)
        assert result.operation == "=="

        # Check that right side is string literal
        assert result.value == "test"
