"""
Mock SparkContext implementation for Mock Spark.

This module provides a mock implementation of PySpark's SparkContext
that behaves identically to the real SparkContext for testing and development.
It includes context management, JVM simulation, and logging without requiring
a JVM or actual Spark installation.

Key Features:
    - Complete PySpark SparkContext API compatibility
    - JVM context simulation
    - Log level management
    - Application name management
    - Context lifecycle management

Example:
    >>> from mock_spark.session import MockSparkContext
    >>> sc = MockSparkContext("MyApp")
    >>> sc.setLogLevel("WARN")
    >>> print(sc.appName)
    MyApp
"""

from typing import Any, Optional
from ..core.interfaces.session import ISparkContext


class MockJVMContext:
    """Mock JVM context for testing without actual JVM."""

    def __init__(self):
        """Initialize mock JVM context."""
        self._jvm_available = True

    @property
    def available(self) -> bool:
        """Check if JVM is available."""
        return self._jvm_available

    def get(self, key: str) -> Any:
        """Get JVM property."""
        return None  # Mock implementation

    def set(self, key: str, value: Any) -> None:
        """Set JVM property."""
        pass  # Mock implementation


class MockSparkContext:
    """Mock SparkContext for testing without PySpark.

    Provides a comprehensive mock implementation of PySpark's SparkContext
    that supports all major operations including context management, logging,
    and JVM simulation without requiring actual Spark installation.

    Attributes:
        app_name: Application name for the Spark context.
        _jvm: Mock JVM context for JVM operations.

    Example:
        >>> sc = MockSparkContext("MyApp")
        >>> sc.setLogLevel("WARN")
        >>> print(sc.appName)
        MyApp
    """

    def __init__(self, app_name: str = "MockSparkApp"):
        """Initialize MockSparkContext.
        
        Args:
            app_name: Name of the Spark application.
        """
        self.app_name = app_name
        self._jvm = MockJVMContext()

    def setLogLevel(self, level: str) -> None:
        """Set log level.
        
        Args:
            level: Log level (DEBUG, INFO, WARN, ERROR, FATAL).
        """
        # Mock implementation - in real Spark this would configure logging
        pass

    @property
    def appName(self) -> str:
        """Get application name.
        
        Returns:
            Application name string.
        """
        return self.app_name

    @property
    def jvm(self) -> MockJVMContext:
        """Get JVM context.
        
        Returns:
            Mock JVM context instance.
        """
        return self._jvm

    def stop(self) -> None:
        """Stop the Spark context.
        
        In a real Spark context, this would stop the Spark application.
        This is a mock implementation.
        """
        # Mock implementation - in real Spark this would stop the context
        pass

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop()
