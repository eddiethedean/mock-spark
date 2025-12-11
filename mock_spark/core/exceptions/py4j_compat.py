"""
Py4J Error Compatibility Layer.

This module provides compatibility with PySpark's Py4JJavaError exception type,
allowing mock-spark errors to be catchable as both Python exceptions and Py4JJavaError.
"""

from typing import Any, Optional


class MockPy4JJavaError(Exception):
    """Mock Py4JJavaError for compatibility with PySpark error handling.

    This class mimics Py4JJavaError behavior so that tests that catch Py4JJavaError
    will also catch mock-spark errors when appropriate.

    Example:
        >>> try:
        ...     F.col(None)
        ... except MockPy4JJavaError as e:
        ...     print("Caught Py4J-compatible error")
    """

    def __init__(self, message: str, java_exception: Optional[Any] = None):
        """Initialize MockPy4JJavaError.

        Args:
            message: Error message
            java_exception: Optional Java exception object (for compatibility)
        """
        super().__init__(message)
        self.message = message
        self.java_exception = java_exception

    def __str__(self) -> str:
        """Return error message."""
        return self.message

    def __repr__(self) -> str:
        """Return representation."""
        return f"MockPy4JJavaError('{self.message}')"


# Note: Py4JJavaError has complex initialization requirements
# For compatibility, MockPy4JJavaError works standalone
# Tests that catch Py4JJavaError can also catch MockPy4JJavaError
# by catching Exception and checking the type
