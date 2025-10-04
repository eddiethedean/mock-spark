"""
Error handling for Mock Spark.

This module provides comprehensive error handling that matches PySpark's
exception hierarchy for maximum compatibility. Includes all major PySpark
exceptions and helper functions for consistent error reporting.

Key Features:
    - Complete PySpark exception hierarchy
    - AnalysisException for SQL analysis errors
    - IllegalArgumentException for invalid arguments
    - ParseException for SQL parsing errors
    - QueryExecutionException for execution errors
    - Helper functions for common error scenarios

Example:
    >>> from mock_spark.errors import AnalysisException
    >>> raise AnalysisException("Column 'unknown' does not exist")
    AnalysisException: Column 'unknown' does not exist
"""

from typing import Any


# Define all exceptions as fallbacks to avoid import issues
class MockException(Exception):
    """Base mock exception for all Mock Spark errors.

    Provides the foundation for all exceptions in the Mock Spark error hierarchy.
    Includes stackTrace support for PySpark compatibility.

    Args:
        message: Error message describing the issue.
        stackTrace: Optional stack trace information.
    """

    def __init__(self, message: str, stackTrace: Any = None):
        super().__init__(message)


class AnalysisException(MockException):
    """Exception raised for SQL analysis errors.

    Raised when SQL queries or DataFrame operations fail due to analysis
    errors such as column not found, invalid syntax, or type mismatches.

    Example:
        >>> raise AnalysisException("Column 'unknown' does not exist")
    """

    pass


class IllegalArgumentException(MockException):
    """Exception raised for invalid arguments.

    Raised when invalid arguments are passed to functions or methods,
    such as incorrect data types or invalid parameter values.

    Example:
        >>> raise IllegalArgumentException("Invalid data type provided")
    """

    pass


class ParseException(MockException):
    """Exception raised for SQL parsing errors.

    Raised when SQL queries cannot be parsed due to syntax errors
    or invalid SQL constructs.

    Example:
        >>> raise ParseException("Invalid SQL syntax")
    """

    pass


class QueryExecutionException(MockException):
    """Exception raised for query execution errors.

    Raised when SQL queries or DataFrame operations fail during
    execution due to runtime errors or data issues.

    Example:
        >>> raise QueryExecutionException("Failed to execute query")
    """

    pass


class SparkUpgradeException(MockException):
    """Mock Spark upgrade exception."""

    pass


class StreamingQueryException(MockException):
    """Mock streaming query exception."""

    pass


class TempTableAlreadyExistsException(MockException):
    """Mock temp table already exists exception."""

    pass


class UnsupportedOperationException(MockException):
    """Mock unsupported operation exception."""

    pass


class PySparkException(MockException):
    """Mock PySpark exception."""

    pass


class PySparkValueError(MockException):
    """Mock PySpark value error."""

    pass


class PySparkTypeError(MockException):
    """Mock PySpark type error."""

    pass


class PySparkAttributeError(MockException):
    """Mock PySpark attribute error."""

    pass


class PySparkRuntimeError(MockException):
    """Mock PySpark runtime error."""

    pass


PYSPARK_AVAILABLE = True


def raise_table_not_found(table_name: str) -> None:
    """Raise table not found error."""
    raise AnalysisException(f"Table or view not found: {table_name}")


def raise_column_not_found(column_name: str) -> None:
    """Raise column not found error."""
    raise AnalysisException(f"Column '{column_name}' does not exist")


def raise_schema_not_found(schema_name: str) -> None:
    """Raise schema not found error."""
    raise AnalysisException(f"Database '{schema_name}' not found")


def raise_invalid_argument(param_name: str, value: str, expected: str) -> None:
    """Raise invalid argument error."""
    raise IllegalArgumentException(
        f"Invalid value for parameter '{param_name}': {value}. Expected: {expected}"
    )


def raise_unsupported_operation(operation: str) -> None:
    """Raise unsupported operation error."""
    raise UnsupportedOperationException(f"Operation '{operation}' is not supported in mock mode")


def raise_parse_error(sql: str, error: str) -> None:
    """Raise parse error."""
    raise ParseException(f"Error parsing SQL: {sql}. {error}")


def raise_query_execution_error(error: str) -> None:
    """Raise query execution error."""
    raise QueryExecutionException(f"Query execution failed: {error}")


def raise_type_error(expected_type: str, actual_type: str) -> None:
    """Raise type error."""
    raise PySparkTypeError(f"Expected {expected_type}, got {actual_type}")


def raise_value_error(message: str) -> None:
    """Raise value error."""
    raise PySparkValueError(message)


def raise_runtime_error(message: str) -> None:
    """Raise runtime error."""
    raise PySparkRuntimeError(message)


# Export commonly used exceptions
__all__ = [
    "AnalysisException",
    "IllegalArgumentException",
    "ParseException",
    "QueryExecutionException",
    "SparkUpgradeException",
    "StreamingQueryException",
    "TempTableAlreadyExistsException",
    "UnsupportedOperationException",
    "PySparkException",
    "PySparkValueError",
    "PySparkTypeError",
    "PySparkAttributeError",
    "PySparkRuntimeError",
    "raise_table_not_found",
    "raise_column_not_found",
    "raise_schema_not_found",
    "raise_invalid_argument",
    "raise_unsupported_operation",
    "raise_parse_error",
    "raise_query_execution_error",
    "raise_type_error",
    "raise_value_error",
    "raise_runtime_error",
]
