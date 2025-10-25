"""
Error translation utilities for Mock Spark.

This module provides translation of DuckDB errors to MockSpark exceptions.
"""

import re
from typing import Dict, Any

from ...core.exceptions.operation import (
    MockSparkOperationError,
    MockSparkSQLGenerationError,
    MockSparkQueryExecutionError,
    MockSparkColumnNotFoundError,
    MockSparkTypeMismatchError,
)


class DuckDBErrorTranslator:
    """Translates DuckDB errors to MockSpark exceptions."""

    @staticmethod
    def translate_error(error: Exception, context: Dict[str, Any]) -> Exception:
        """Translate DuckDB errors to helpful MockSpark errors.

        Args:
            error: The original DuckDB exception
            context: Context information about the operation that failed

        Returns:
            Appropriate MockSpark exception
        """
        error_msg = str(error).lower()

        if "syntax error" in error_msg or "parser error" in error_msg:
            operation = context.get("operation", "unknown")
            column = context.get("column", "unknown")
            return MockSparkOperationError(
                operation=operation,
                column=column,
                issue="SQL syntax error in generated query",
                suggestion="Check column types and operation compatibility",
            )
        elif "column" in error_msg and "not found" in error_msg:
            # Extract column name from error message
            match = re.search(r"column ['\"]([^'\"]+)['\"]", error_msg)
            column_name = match.group(1) if match else "unknown"
            available_columns = context.get("available_columns", [])
            return MockSparkColumnNotFoundError(column_name, available_columns)
        elif "type" in error_msg and (
            "mismatch" in error_msg or "incompatible" in error_msg
        ):
            operation = context.get("operation", "unknown")
            return MockSparkTypeMismatchError(
                operation=operation,
                expected_type=context.get("expected_type", "unknown"),
                actual_type=context.get("actual_type", "unknown"),
                column=context.get("column", ""),
            )
        elif "function" in error_msg and "not found" in error_msg:
            operation = context.get("operation", "unknown")
            return MockSparkSQLGenerationError(
                operation=operation,
                sql_fragment=context.get("sql_fragment", ""),
                error=str(error),
            )
        else:
            # Generic query execution error
            return MockSparkQueryExecutionError(
                sql=context.get("sql", ""), error=str(error), context=context
            )
