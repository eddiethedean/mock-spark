"""
Runtime exception classes for Mock Spark.

This module provides exception classes for runtime-related errors,
including system errors, attribute errors, and runtime failures.
"""

from typing import Any, Optional
from .base import SparkException


class PySparkRuntimeError(SparkException):
    """PySpark-compatible RuntimeError.

    Raised when a runtime error occurs during execution
    of Spark operations.

    Args:
        message: Error message describing the runtime error.
        stackTrace: Optional stack trace information.

    Example:
        >>> raise PySparkRuntimeError("Runtime error occurred")
    """

    def __init__(self, message: str, stackTrace: Optional[Any] = None):
        super().__init__(message, stackTrace)


class PySparkAttributeError(SparkException):
    """PySpark-compatible AttributeError.

    Raised when an attribute reference or assignment fails
    on a Spark object.

    Args:
        object_name: Name of the object that lacks the attribute.
        attribute_name: Name of the missing attribute.
        message: Optional custom error message.
        stackTrace: Optional stack trace information.

    Example:
        >>> raise PySparkAttributeError("DataFrame", "unknown_method")
    """

    def __init__(
        self,
        object_name: str,
        attribute_name: str,
        message: Optional[str] = None,
        stackTrace: Optional[Any] = None,
    ):
        if message is None:
            message = f"'{object_name}' object has no attribute '{attribute_name}'"
        super().__init__(message, stackTrace)
        self.object_name = object_name
        self.attribute_name = attribute_name


class SystemException(PySparkRuntimeError):
    """Exception raised for system-level errors.

    Raised when there are system-level issues that prevent
    normal operation of Spark components.

    Args:
        message: Error message describing the system error.
        stackTrace: Optional stack trace information.

    Example:
        >>> raise SystemException("System resource unavailable")
    """

    def __init__(self, message: str, stackTrace: Optional[Any] = None):
        super().__init__(message, stackTrace)


class ConfigurationException(PySparkRuntimeError):
    """Exception raised for configuration errors.

    Raised when there are issues with configuration
    management or invalid configuration values.

    Args:
        config_key: Configuration key that caused the error.
        config_value: Configuration value that caused the error.
        message: Optional custom error message.
        stackTrace: Optional stack trace information.

    Example:
        >>> raise ConfigurationException("spark.memory", "invalid_value")
    """

    def __init__(
        self,
        config_key: str,
        config_value: Any,
        message: Optional[str] = None,
        stackTrace: Optional[Any] = None,
    ):
        if message is None:
            message = (
                f"Invalid configuration value '{config_value}' for key '{config_key}'"
            )
        super().__init__(message, stackTrace)
        self.config_key = config_key
        self.config_value = config_value


class InitializationException(PySparkRuntimeError):
    """Exception raised for initialization errors.

    Raised when there are issues during initialization
    of Spark components or sessions.

    Args:
        component: Name of the component that failed to initialize.
        message: Optional custom error message.
        stackTrace: Optional stack trace information.

    Example:
        >>> raise InitializationException("SparkSession")
    """

    def __init__(
        self,
        component: str,
        message: Optional[str] = None,
        stackTrace: Optional[Any] = None,
    ):
        if message is None:
            message = f"Failed to initialize {component}"
        super().__init__(message, stackTrace)
        self.component = component


class ShutdownException(PySparkRuntimeError):
    """Exception raised for shutdown errors.

    Raised when there are issues during shutdown
    of Spark components or sessions.

    Args:
        component: Name of the component that failed to shutdown.
        message: Optional custom error message.
        stackTrace: Optional stack trace information.

    Example:
        >>> raise ShutdownException("SparkSession")
    """

    def __init__(
        self,
        component: str,
        message: Optional[str] = None,
        stackTrace: Optional[Any] = None,
    ):
        if message is None:
            message = f"Failed to shutdown {component}"
        super().__init__(message, stackTrace)
        self.component = component


class TimeoutException(PySparkRuntimeError):
    """Exception raised for timeout errors.

    Raised when operations exceed their timeout limits
    or when waiting for resources times out.

    Args:
        operation: Name of the operation that timed out.
        timeout_seconds: Timeout value in seconds.
        message: Optional custom error message.
        stackTrace: Optional stack trace information.

    Example:
        >>> raise TimeoutException("query_execution", 30)
    """

    def __init__(
        self,
        operation: str,
        timeout_seconds: int,
        message: Optional[str] = None,
        stackTrace: Optional[Any] = None,
    ):
        if message is None:
            message = (
                f"Operation '{operation}' timed out after {timeout_seconds} seconds"
            )
        super().__init__(message, stackTrace)
        self.operation = operation
        self.timeout_seconds = timeout_seconds
