"""
Session builder implementation for Mock Spark.

This module provides the MockSparkSessionBuilder class for creating
SparkSession instances using the builder pattern, maintaining compatibility
with PySpark's SparkSession.builder interface.
"""

from typing import Any, Dict, Union
from .session import MockSparkSession


class MockSparkSessionBuilder:
    """Mock SparkSession builder."""

    def __init__(self):
        """Initialize builder."""
        self._app_name = "MockSparkApp"
        self._config = {}

    def appName(self, name: str) -> "MockSparkSessionBuilder":
        """Set app name.

        Args:
            name: Application name.

        Returns:
            Self for method chaining.
        """
        self._app_name = name
        return self

    def master(self, master: str) -> "MockSparkSessionBuilder":
        """Set master URL.

        Args:
            master: Master URL.

        Returns:
            Self for method chaining.
        """
        return self

    def config(
        self, key_or_pairs: Union[str, Dict[str, Any]], value: Any = None
    ) -> "MockSparkSessionBuilder":
        """Set configuration.

        Args:
            key_or_pairs: Configuration key or dictionary of key-value pairs.
            value: Configuration value (if key_or_pairs is a string).

        Returns:
            Self for method chaining.
        """
        if isinstance(key_or_pairs, str):
            self._config[key_or_pairs] = value
        else:
            self._config.update(key_or_pairs)
        return self

    def getOrCreate(self) -> MockSparkSession:
        """Get or create session.

        Returns:
            MockSparkSession instance.
        """
        # Return existing singleton if present; otherwise create and cache
        if MockSparkSession._singleton_session is None:
            session = MockSparkSession(self._app_name)
            for key, value in self._config.items():
                session.conf.set(key, value)
            MockSparkSession._singleton_session = session
        return MockSparkSession._singleton_session
