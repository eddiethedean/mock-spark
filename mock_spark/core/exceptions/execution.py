"""
Execution exception classes for Mock Spark.

This module provides exception classes for execution-related errors,
including query execution, runtime errors, and system failures.
"""

from typing import Any, Optional
from .base import MockSparkException


class QueryExecutionException(MockSparkException):
    """Exception raised for query execution errors.

    Raised when SQL queries or DataFrame operations fail during
    execution due to runtime errors or data issues.

    Args:
        message: Error message describing the execution error.
        stackTrace: Optional stack trace information.

    Example:
        >>> raise QueryExecutionException("Failed to execute query")
    """

    def __init__(self, message: str, stackTrace: Optional[Any] = None):
        super().__init__(message, stackTrace)


class SparkUpgradeException(MockSparkException):
    """Exception raised for Spark upgrade issues.

    Raised when there are compatibility issues during Spark
    version upgrades or migrations.

    Args:
        message: Error message describing the upgrade issue.
        stackTrace: Optional stack trace information.

    Example:
        >>> raise SparkUpgradeException("Incompatible Spark version")
    """

    def __init__(self, message: str, stackTrace: Optional[Any] = None):
        super().__init__(message, stackTrace)


class StreamingQueryException(QueryExecutionException):
    """Exception raised for streaming query errors.

    Raised when streaming queries fail during execution
    or encounter runtime issues.

    Args:
        message: Error message describing the streaming error.
        stackTrace: Optional stack trace information.

    Example:
        >>> raise StreamingQueryException("Streaming query failed")
    """

    def __init__(self, message: str, stackTrace: Optional[Any] = None):
        super().__init__(message, stackTrace)


class TempTableAlreadyExistsException(QueryExecutionException):
    """Exception raised when a temporary table already exists.

    Raised when attempting to create a temporary table that
    already exists in the session.

    Args:
        table_name: Name of the table that already exists.
        message: Optional custom error message.
        stackTrace: Optional stack trace information.

    Example:
        >>> raise TempTableAlreadyExistsException("temp_table")
    """

    def __init__(self, table_name: str, message: Optional[str] = None, stackTrace: Optional[Any] = None):
        if message is None:
            message = f"Temporary table '{table_name}' already exists"
        super().__init__(message, stackTrace)
        self.table_name = table_name


class UnsupportedOperationException(QueryExecutionException):
    """Exception raised for unsupported operations.

    Raised when attempting to perform an operation that is
    not supported in the current context.

    Args:
        operation: Name of the unsupported operation.
        message: Optional custom error message.
        stackTrace: Optional stack trace information.

    Example:
        >>> raise UnsupportedOperationException("unsupported_operation")
    """

    def __init__(self, operation: str, message: Optional[str] = None, stackTrace: Optional[Any] = None):
        if message is None:
            message = f"Operation '{operation}' is not supported"
        super().__init__(message, stackTrace)
        self.operation = operation


class ResourceException(QueryExecutionException):
    """Exception raised for resource-related errors.

    Raised when there are issues with resource allocation,
    memory, or system resources.

    Args:
        message: Error message describing the resource error.
        stackTrace: Optional stack trace information.

    Example:
        >>> raise ResourceException("Insufficient memory")
    """

    def __init__(self, message: str, stackTrace: Optional[Any] = None):
        super().__init__(message, stackTrace)


class MemoryException(ResourceException):
    """Exception raised for memory-related errors.

    Raised when there are memory allocation or memory limit
    issues during execution.

    Args:
        message: Error message describing the memory error.
        stackTrace: Optional stack trace information.

    Example:
        >>> raise MemoryException("Out of memory")
    """

    def __init__(self, message: str, stackTrace: Optional[Any] = None):
        super().__init__(message, stackTrace)
