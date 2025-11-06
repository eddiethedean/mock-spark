"""
Unit tests for DuckDB error translation.

Tests error mapping from DuckDB exceptions to MockSpark exceptions.
"""

import pytest

# Skip if error_translator module doesn't exist
try:
    from mock_spark.backend.duckdb.error_translator import DuckDBErrorTranslator
except ImportError:
    pytest.skip(
        "error_translator module not available in this version", allow_module_level=True
    )
from mock_spark.core.exceptions.operation import (
    MockSparkOperationError,
    MockSparkSQLGenerationError,
    MockSparkQueryExecutionError,
    SparkColumnNotFoundError,
    MockSparkTypeMismatchError,
)


@pytest.mark.fast
class TestDuckDBErrorTranslator:
    """Test DuckDB error translation to MockSpark exceptions."""

    def test_translate_syntax_error(self):
        """Test translating DuckDB syntax errors."""
        error = Exception("Parser Error: syntax error near 'SELECT'")
        context = {"operation": "select", "column": "id", "sql": "SELECT * FROM table"}

        result = DuckDBErrorTranslator.translate_error(error, context)

        assert isinstance(result, MockSparkOperationError)
        assert "SQL syntax error" in str(result)
        assert "select" in str(result).lower()

    def test_translate_column_not_found_error(self):
        """Test translating column not found errors."""
        error = Exception("Catalog Error: Table column 'missing_col' not found")
        context = {
            "operation": "select",
            "available_columns": ["id", "name", "age"],
        }

        result = DuckDBErrorTranslator.translate_error(error, context)

        assert isinstance(result, SparkColumnNotFoundError)
        assert "missing_col" in str(result).lower() or "missing" in str(result).lower()

    def test_translate_type_mismatch_error(self):
        """Test translating type mismatch errors."""
        error = Exception("Type Error: column type mismatch in operation")
        context = {
            "operation": "filter",
            "expected_type": "INTEGER",
            "actual_type": "VARCHAR",
            "column": "age",
        }

        result = DuckDBErrorTranslator.translate_error(error, context)

        assert isinstance(result, MockSparkTypeMismatchError)
        assert "filter" in str(result).lower()
        assert "age" in str(result).lower()

    def test_translate_function_not_found_error(self):
        """Test translating function not found errors."""
        error = Exception("Catalog Error: function 'unknown_func' not found")
        context = {"operation": "select", "sql_fragment": "SELECT unknown_func(col)"}

        result = DuckDBErrorTranslator.translate_error(error, context)

        assert isinstance(result, MockSparkSQLGenerationError)
        assert "select" in str(result).lower()

    def test_translate_generic_error(self):
        """Test translating generic DuckDB errors."""
        error = Exception("Unknown DuckDB Error")
        context = {"sql": "SELECT * FROM table", "operation": "query"}

        result = DuckDBErrorTranslator.translate_error(error, context)

        assert isinstance(result, MockSparkQueryExecutionError)
        assert "Unknown DuckDB Error" in str(result)

    def test_translate_parser_error(self):
        """Test translating parser errors."""
        error = Exception("Parser Error: unexpected token")
        context = {"operation": "union", "column": ""}

        result = DuckDBErrorTranslator.translate_error(error, context)

        assert isinstance(result, MockSparkOperationError)
        assert "syntax error" in str(result).lower()

    def test_translate_catalog_error(self):
        """Test translating catalog/table errors."""
        error = Exception("Catalog Error: table 'missing_table' not found")
        context = {"operation": "read", "available_columns": []}

        result = DuckDBErrorTranslator.translate_error(error, context)

        # Should return a generic query execution error for unknown catalog errors
        assert isinstance(
            result, (MockSparkQueryExecutionError, MockSparkOperationError)
        )

    def test_translate_with_minimal_context(self):
        """Test error translation with minimal context."""
        error = Exception("Error occurred")
        context = {}

        result = DuckDBErrorTranslator.translate_error(error, context)

        # Should handle minimal context gracefully
        assert isinstance(
            result, (MockSparkQueryExecutionError, MockSparkOperationError)
        )

    def test_translate_binder_error(self):
        """Test translating binder errors (column binding issues)."""
        error = Exception('Binder Error: Table "t1" does not have column "missing_col"')
        context = {
            "operation": "join",
            "available_columns": ["id", "name"],
        }

        result = DuckDBErrorTranslator.translate_error(error, context)

        # Binder error may translate to QueryExecutionError or ColumnNotFoundError
        assert isinstance(
            result,
            (
                SparkColumnNotFoundError,
                MockSparkOperationError,
                MockSparkQueryExecutionError,
            ),
        )

    def test_translate_with_error_message_patterns(self):
        """Test that error translation handles various error message patterns."""
        test_cases = [
            (Exception("syntax error near line 10"), "MockSparkOperationError"),
            (
                Exception("Column 'test' not found in schema"),
                "SparkColumnNotFoundError",
            ),
            (Exception("type mismatch error"), "MockSparkTypeMismatchError"),
            (Exception("function not found error"), "MockSparkSQLGenerationError"),
        ]

        for error, expected_error_type in test_cases:
            context = {
                "operation": "test",
                "available_columns": ["col1", "col2"],
            }
            result = DuckDBErrorTranslator.translate_error(error, context)
            assert result.__class__.__name__ == expected_error_type
