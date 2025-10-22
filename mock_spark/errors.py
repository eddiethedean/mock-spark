"""
Error handling for Mock Spark.

DEPRECATED: This module re-exports exceptions from core.exceptions for backward compatibility.
New code should import directly from mock_spark.core.exceptions.

This module provides comprehensive error handling that matches PySpark's
exception hierarchy for maximum compatibility.

Example:
    >>> from mock_spark.errors import AnalysisException
    >>> raise AnalysisException("Column 'unknown' does not exist")
"""

# Re-export from core.exceptions for backward compatibility
from .core.exceptions.base import MockException, MockSparkException
from .core.exceptions.analysis import (
    AnalysisException,
    ParseException,
    SchemaException,
    ColumnNotFoundException,
    TableNotFoundException,
    DatabaseNotFoundException,
    TypeMismatchException,
)
from .core.exceptions.execution import (
    QueryExecutionException,
    SparkUpgradeException,
    StreamingQueryException,
    TempTableAlreadyExistsException,
    UnsupportedOperationException,
    ResourceException,
    MemoryException,
)
from .core.exceptions.validation import (
    IllegalArgumentException,
    PySparkValueError,
    PySparkTypeError,
    ValidationException,
)
from .core.exceptions.runtime import (
    PySparkRuntimeError,
    PySparkAttributeError,
    ConfigurationException,
)

# Alias for PySparkException (same as MockSparkException)
PySparkException = MockSparkException

# Mark as available
PYSPARK_AVAILABLE = True


# Helper functions for common error scenarios
def raise_table_not_found(table_name: str) -> None:
    """Raise table not found error."""
    raise TableNotFoundException(f"Table or view not found: {table_name}")


def raise_column_not_found(column_name: str) -> None:
    """Raise column not found error."""
    raise ColumnNotFoundException(f"Column '{column_name}' does not exist")


def raise_schema_not_found(schema_name: str) -> None:
    """Raise schema not found error."""
    raise DatabaseNotFoundException(f"Database '{schema_name}' not found")


def raise_invalid_argument(param_name: str, value: str, expected: str) -> None:
    """Raise invalid argument error."""
    raise IllegalArgumentException(
        f"Invalid value for parameter '{param_name}': {value}. Expected: {expected}"
    )


def raise_unsupported_operation(operation: str) -> None:
    """Raise unsupported operation error."""
    raise UnsupportedOperationException(
        f"Operation '{operation}' is not supported in mock mode"
    )


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
    # Base exceptions
    "MockException",
    "MockSparkException",
    "PySparkException",
    # Analysis exceptions
    "AnalysisException",
    "ParseException",
    "SchemaException",
    "ColumnNotFoundException",
    "TableNotFoundException",
    "DatabaseNotFoundException",
    "TypeMismatchException",
    # Execution exceptions
    "QueryExecutionException",
    "SparkUpgradeException",
    "StreamingQueryException",
    "TempTableAlreadyExistsException",
    "UnsupportedOperationException",
    "ResourceException",
    "MemoryException",
    # Validation exceptions
    "IllegalArgumentException",
    "PySparkValueError",
    "PySparkTypeError",
    "ValidationException",
    # Runtime exceptions
    "PySparkRuntimeError",
    "PySparkAttributeError",
    "ConfigurationException",
    # Helper functions
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
    # Flags
    "PYSPARK_AVAILABLE",
]
