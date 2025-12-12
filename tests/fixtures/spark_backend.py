"""
Backend abstraction layer for unified PySpark and mock-spark testing.

Provides factory functions and utilities to create SparkSession instances
from either PySpark or mock-spark based on configuration.
"""

import os
import pytest
from typing import Optional, Any
from enum import Enum


class BackendType(Enum):
    """Backend type enumeration."""

    MOCK = "mock"
    PYSPARK = "pyspark"
    BOTH = "both"


def get_backend_from_env() -> Optional[BackendType]:
    """Get backend type from environment variable.

    Returns:
        BackendType if set, None otherwise.
    """
    backend_str = os.getenv("MOCK_SPARK_TEST_BACKEND", "").lower()
    if not backend_str:
        return None

    try:
        return BackendType(backend_str)
    except ValueError:
        return None


def get_backend_from_marker(request: pytest.FixtureRequest) -> Optional[BackendType]:
    """Get backend type from pytest marker.

    Args:
        request: Pytest fixture request object.

    Returns:
        BackendType if marker present, None otherwise.
    """
    marker = request.node.get_closest_marker("backend")
    if marker is None:
        return None

    backend_str = marker.args[0] if marker.args else None
    if backend_str is None:
        return None

    try:
        return BackendType(backend_str.lower())
    except ValueError:
        return None


def get_backend_type(request: Optional[pytest.FixtureRequest] = None) -> BackendType:
    """Get backend type from marker, environment, or default to mock.

    Args:
        request: Optional pytest fixture request for marker checking.

    Returns:
        BackendType to use for test execution.
    """
    # Check marker first (highest priority)
    if request is not None:
        try:
            marker_backend = get_backend_from_marker(request)
            if marker_backend is not None:
                return marker_backend
        except (AttributeError, TypeError):
            # Request might not have node attribute in some contexts
            pass

    # Check environment variable
    env_backend = get_backend_from_env()
    if env_backend is not None:
        return env_backend

    # Default to mock-spark
    return BackendType.MOCK


class SparkBackend:
    """Backend abstraction for creating SparkSession instances."""

    @staticmethod
    def create_mock_spark_session(app_name: str = "test_app", **kwargs: Any) -> Any:
        """Create a mock-spark SparkSession.

        Args:
            app_name: Application name for the session.
            **kwargs: Additional arguments for SparkSession creation.

        Returns:
            mock-spark SparkSession instance.
        """
        from mock_spark import SparkSession

        return SparkSession(app_name, **kwargs)

    @staticmethod
    def create_pyspark_session(app_name: str = "test_app", **kwargs: Any) -> Any:
        """Create a PySpark SparkSession.

        Args:
            app_name: Application name for the session.
            **kwargs: Additional arguments for SparkSession creation.

        Returns:
            PySpark SparkSession instance.

        Raises:
            ImportError: If PySpark is not available.
            RuntimeError: If PySpark session creation fails.
        """
        try:
            from pyspark.sql import SparkSession as PySparkSession
            from pyspark import SparkConf
        except ImportError:
            raise ImportError(
                "PySpark is not available. Install with: pip install pyspark"
            )

        # Configure PySpark for testing
        conf = SparkConf()
        conf.set("spark.master", "local[1]")
        conf.set("spark.driver.bindAddress", "127.0.0.1")
        conf.set("spark.driver.host", "127.0.0.1")
        conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
        conf.set("spark.ui.enabled", "false")
        conf.setAppName(app_name)
        conf.set("spark.sql.adaptive.enabled", "false")
        conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")

        # Set environment variables for PySpark
        import sys

        python_executable = sys.executable
        os.environ.setdefault("PYSPARK_PYTHON", python_executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", python_executable)
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

        # Apply any additional config from kwargs
        for key, value in kwargs.items():
            if key.startswith("spark."):
                conf.set(key, str(value))

        try:
            session = PySparkSession.builder.config(conf=conf).getOrCreate()
            # Test that session works
            session.createDataFrame([{"test": 1}]).collect()
            return session
        except Exception as e:
            raise RuntimeError(f"Failed to create PySpark session: {e}") from e

    @staticmethod
    def create_session(
        app_name: str = "test_app",
        backend: Optional[BackendType] = None,
        request: Optional[pytest.FixtureRequest] = None,
        **kwargs: Any,
    ) -> Any:
        """Create a SparkSession based on backend configuration.

        Args:
            app_name: Application name for the session.
            backend: Explicit backend type to use.
            request: Optional pytest fixture request for marker checking.
            **kwargs: Additional arguments for SparkSession creation.

        Returns:
            SparkSession instance (mock-spark or PySpark).

        Raises:
            ValueError: If backend is BOTH (use create_sessions_for_comparison instead).
        """
        if backend is None:
            backend = get_backend_type(request)

        if backend == BackendType.BOTH:
            raise ValueError(
                "BackendType.BOTH requires create_sessions_for_comparison()"
            )

        if backend == BackendType.PYSPARK:
            return SparkBackend.create_pyspark_session(app_name, **kwargs)
        else:
            return SparkBackend.create_mock_spark_session(app_name, **kwargs)

    @staticmethod
    def create_sessions_for_comparison(
        app_name: str = "test_app", **kwargs: Any
    ) -> tuple[Any, Any]:
        """Create both mock-spark and PySpark sessions for comparison.

        Args:
            app_name: Application name for the sessions.
            **kwargs: Additional arguments for SparkSession creation.

        Returns:
            Tuple of (mock_spark_session, pyspark_session).

        Raises:
            ImportError: If PySpark is not available.
        """
        mock_session = SparkBackend.create_mock_spark_session(app_name, **kwargs)
        try:
            pyspark_session = SparkBackend.create_pyspark_session(app_name, **kwargs)
        except ImportError:
            pyspark_session = None

        return mock_session, pyspark_session


def create_spark_session(
    app_name: str = "test_app",
    backend: Optional[BackendType] = None,
    request: Optional[pytest.FixtureRequest] = None,
    **kwargs: Any,
) -> Any:
    """Convenience function to create a SparkSession.

    Args:
        app_name: Application name for the session.
        backend: Explicit backend type to use.
        request: Optional pytest fixture request for marker checking.
        **kwargs: Additional arguments for SparkSession creation.

    Returns:
        SparkSession instance.
    """
    return SparkBackend.create_session(
        app_name=app_name, backend=backend, request=request, **kwargs
    )
