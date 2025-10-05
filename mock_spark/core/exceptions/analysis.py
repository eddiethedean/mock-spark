"""
Analysis exception classes for Mock Spark.

This module provides exception classes for analysis-related errors,
including SQL parsing, query analysis, and schema validation errors.
"""

from typing import Any, Optional
from .base import MockSparkException


class AnalysisException(MockSparkException):
    """Exception raised for SQL analysis errors.

    Raised when SQL queries or DataFrame operations fail due to analysis
    errors such as column not found, invalid syntax, or type mismatches.

    Args:
        message: Error message describing the analysis error.
        stackTrace: Optional stack trace information.

    Example:
        >>> raise AnalysisException("Column 'unknown' does not exist")
    """

    def __init__(self, message: str, stackTrace: Optional[Any] = None):
        super().__init__(message, stackTrace)


class ParseException(AnalysisException):
    """Exception raised for SQL parsing errors.

    Raised when SQL queries fail to parse due to syntax errors
    or invalid SQL constructs.

    Args:
        message: Error message describing the parsing error.
        stackTrace: Optional stack trace information.

    Example:
        >>> raise ParseException("Invalid SQL syntax")
    """

    def __init__(self, message: str, stackTrace: Optional[Any] = None):
        super().__init__(message, stackTrace)


class SchemaException(AnalysisException):
    """Exception raised for schema-related errors.

    Raised when schema operations fail due to invalid schema
    definitions or schema mismatches.

    Args:
        message: Error message describing the schema error.
        stackTrace: Optional stack trace information.

    Example:
        >>> raise SchemaException("Schema mismatch detected")
    """

    def __init__(self, message: str, stackTrace: Optional[Any] = None):
        super().__init__(message, stackTrace)


class ColumnNotFoundException(AnalysisException):
    """Exception raised when a column is not found.

    Raised when attempting to access a column that doesn't exist
    in the DataFrame or table.

    Args:
        column_name: Name of the column that was not found.
        message: Optional custom error message.
        stackTrace: Optional stack trace information.

    Example:
        >>> raise ColumnNotFoundException("unknown_column")
    """

    def __init__(
        self,
        column_name: str,
        message: Optional[str] = None,
        stackTrace: Optional[Any] = None,
    ):
        if message is None:
            message = f"Column '{column_name}' does not exist"
        super().__init__(message, stackTrace)
        self.column_name = column_name


class TableNotFoundException(AnalysisException):
    """Exception raised when a table is not found.

    Raised when attempting to access a table that doesn't exist
    in the catalog.

    Args:
        table_name: Name of the table that was not found.
        message: Optional custom error message.
        stackTrace: Optional stack trace information.

    Example:
        >>> raise TableNotFoundException("unknown_table")
    """

    def __init__(
        self,
        table_name: str,
        message: Optional[str] = None,
        stackTrace: Optional[Any] = None,
    ):
        if message is None:
            message = f"Table '{table_name}' does not exist"
        super().__init__(message, stackTrace)
        self.table_name = table_name


class DatabaseNotFoundException(AnalysisException):
    """Exception raised when a database is not found.

    Raised when attempting to access a database that doesn't exist
    in the catalog.

    Args:
        database_name: Name of the database that was not found.
        message: Optional custom error message.
        stackTrace: Optional stack trace information.

    Example:
        >>> raise DatabaseNotFoundException("unknown_database")
    """

    def __init__(
        self,
        database_name: str,
        message: Optional[str] = None,
        stackTrace: Optional[Any] = None,
    ):
        if message is None:
            message = f"Database '{database_name}' does not exist"
        super().__init__(message, stackTrace)
        self.database_name = database_name


class TypeMismatchException(AnalysisException):
    """Exception raised for type mismatch errors.

    Raised when there's a type mismatch between expected and actual
    data types in operations.

    Args:
        expected_type: Expected data type.
        actual_type: Actual data type.
        message: Optional custom error message.
        stackTrace: Optional stack trace information.

    Example:
        >>> raise TypeMismatchException("string", "integer")
    """

    def __init__(
        self,
        expected_type: str,
        actual_type: str,
        message: Optional[str] = None,
        stackTrace: Optional[Any] = None,
    ):
        if message is None:
            message = f"Type mismatch: expected {expected_type}, got {actual_type}"
        super().__init__(message, stackTrace)
        self.expected_type = expected_type
        self.actual_type = actual_type
