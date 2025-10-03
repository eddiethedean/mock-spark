"""
Validation exception classes for Mock Spark.

This module provides exception classes for validation-related errors,
including argument validation, type checking, and input validation.
"""

from typing import Any, Optional
from .base import MockSparkException


class IllegalArgumentException(MockSparkException):
    """Exception raised for invalid arguments.

    Raised when invalid arguments are passed to functions or methods,
    such as incorrect data types or invalid parameter values.

    Args:
        message: Error message describing the invalid argument.
        stackTrace: Optional stack trace information.

    Example:
        >>> raise IllegalArgumentException("Invalid data type provided")
    """

    def __init__(self, message: str, stackTrace: Optional[Any] = None):
        super().__init__(message, stackTrace)


class PySparkValueError(IllegalArgumentException):
    """PySpark-compatible ValueError.

    Raised when a value is inappropriate for the operation
    or function being performed.

    Args:
        message: Error message describing the value error.
        stackTrace: Optional stack trace information.

    Example:
        >>> raise PySparkValueError("Invalid value provided")
    """

    def __init__(self, message: str, stackTrace: Optional[Any] = None):
        super().__init__(message, stackTrace)


class PySparkTypeError(IllegalArgumentException):
    """PySpark-compatible TypeError.

    Raised when an operation or function is applied to an object
    of inappropriate type.

    Args:
        message: Error message describing the type error.
        stackTrace: Optional stack trace information.

    Example:
        >>> raise PySparkTypeError("Invalid type provided")
    """

    def __init__(self, message: str, stackTrace: Optional[Any] = None):
        super().__init__(message, stackTrace)


class ValidationException(IllegalArgumentException):
    """Exception raised for validation errors.

    Raised when data validation fails during processing
    or transformation operations.

    Args:
        message: Error message describing the validation error.
        stackTrace: Optional stack trace information.

    Example:
        >>> raise ValidationException("Data validation failed")
    """

    def __init__(self, message: str, stackTrace: Optional[Any] = None):
        super().__init__(message, stackTrace)


class InputValidationException(ValidationException):
    """Exception raised for input validation errors.

    Raised when input data fails validation checks
    before processing.

    Args:
        field_name: Name of the field that failed validation.
        value: Value that failed validation.
        message: Optional custom error message.
        stackTrace: Optional stack trace information.

    Example:
        >>> raise InputValidationException("age", -5, "Age must be positive")
    """

    def __init__(self, field_name: str, value: Any, message: Optional[str] = None, stackTrace: Optional[Any] = None):
        if message is None:
            message = f"Validation failed for field '{field_name}' with value {value}"
        super().__init__(message, stackTrace)
        self.field_name = field_name
        self.value = value


class RangeValidationException(ValidationException):
    """Exception raised for range validation errors.

    Raised when a value is outside the expected range
    for a parameter or field.

    Args:
        field_name: Name of the field that failed range validation.
        value: Value that failed validation.
        min_value: Minimum allowed value.
        max_value: Maximum allowed value.
        message: Optional custom error message.
        stackTrace: Optional stack trace information.

    Example:
        >>> raise RangeValidationException("age", 150, 0, 120)
    """

    def __init__(self, field_name: str, value: Any, min_value: Any, max_value: Any, 
                 message: Optional[str] = None, stackTrace: Optional[Any] = None):
        if message is None:
            message = f"Value {value} for field '{field_name}' is outside range [{min_value}, {max_value}]"
        super().__init__(message, stackTrace)
        self.field_name = field_name
        self.value = value
        self.min_value = min_value
        self.max_value = max_value


class RequiredFieldException(ValidationException):
    """Exception raised for missing required fields.

    Raised when a required field is missing from input data
    or configuration.

    Args:
        field_name: Name of the required field that is missing.
        message: Optional custom error message.
        stackTrace: Optional stack trace information.

    Example:
        >>> raise RequiredFieldException("name")
    """

    def __init__(self, field_name: str, message: Optional[str] = None, stackTrace: Optional[Any] = None):
        if message is None:
            message = f"Required field '{field_name}' is missing"
        super().__init__(message, stackTrace)
        self.field_name = field_name


class FormatValidationException(ValidationException):
    """Exception raised for format validation errors.

    Raised when data format validation fails, such as
    invalid date formats or string patterns.

    Args:
        field_name: Name of the field that failed format validation.
        value: Value that failed validation.
        expected_format: Expected format pattern.
        message: Optional custom error message.
        stackTrace: Optional stack trace information.

    Example:
        >>> raise FormatValidationException("date", "2023-13-01", "YYYY-MM-DD")
    """

    def __init__(self, field_name: str, value: Any, expected_format: str, 
                 message: Optional[str] = None, stackTrace: Optional[Any] = None):
        if message is None:
            message = f"Value {value} for field '{field_name}' does not match format '{expected_format}'"
        super().__init__(message, stackTrace)
        self.field_name = field_name
        self.value = value
        self.expected_format = expected_format
