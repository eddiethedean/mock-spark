"""
Error handling for Mock Spark.

This module provides real PySpark error integration for maximum compatibility.
"""

from typing import Any

# Define all exceptions as fallbacks to avoid import issues
class MockException(Exception):
    """Base mock exception."""
    def __init__(self, message: str, stackTrace: Any = None):
        super().__init__(message)

class AnalysisException(MockException):
    """Mock analysis exception."""
    pass

class IllegalArgumentException(MockException):
    """Mock illegal argument exception."""
    pass

class ParseException(MockException):
    """Mock parse exception."""
    pass

class QueryExecutionException(MockException):
    """Mock query execution exception."""
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
    raise IllegalArgumentException(f"Invalid value for parameter '{param_name}': {value}. Expected: {expected}")


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
