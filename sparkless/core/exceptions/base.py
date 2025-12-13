"""
Base exception classes for Sparkless.

This module provides the foundational exception classes that form the
base of the Sparkless exception hierarchy.
"""

from typing import Any, Optional


class MockException(Exception):
    """Base exception for all Sparkless errors.

    Provides the foundation for all exceptions in the Sparkless error hierarchy.
    Includes stackTrace support for PySpark compatibility.

    Args:
        message: Error message describing the issue.
        stackTrace: Optional stack trace information.
    """

    def __init__(self, message: str, stackTrace: Optional[Any] = None):
        super().__init__(message)
        self.message = message
        self.stackTrace = stackTrace

    def __str__(self) -> str:
        return self.message

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.message!r})"


class SparkException(MockException):
    """Base exception for all Spark-related errors.

    This is the root exception for all Spark-specific errors in Sparkless.
    All other Spark exceptions should inherit from this class.

    Args:
        message: Error message describing the issue.
        stackTrace: Optional stack trace information.
    """

    def __init__(self, message: str, stackTrace: Optional[Any] = None):
        super().__init__(message, stackTrace)


class DataException(MockException):
    """Base exception for data-related errors.

    This exception is used for errors related to data processing,
    validation, or transformation operations.

    Args:
        message: Error message describing the issue.
        stackTrace: Optional stack trace information.
    """

    def __init__(self, message: str, stackTrace: Optional[Any] = None):
        super().__init__(message, stackTrace)


class ConfigurationExceptionBase(MockException):
    """Base exception for configuration-related errors (legacy - use ConfigurationException from runtime instead).

    This exception is used for errors related to configuration
    management and validation.

    Args:
        message: Error message describing the issue.
        stackTrace: Optional stack trace information.
    """

    def __init__(self, message: str, stackTrace: Optional[Any] = None):
        super().__init__(message, stackTrace)


class StorageException(MockException):
    """Base exception for storage-related errors.

    This exception is used for errors related to data storage,
    persistence, and retrieval operations.

    Args:
        message: Error message describing the issue.
        stackTrace: Optional stack trace information.
    """

    def __init__(self, message: str, stackTrace: Optional[Any] = None):
        super().__init__(message, stackTrace)
