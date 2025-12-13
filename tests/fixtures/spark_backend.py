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
        from sparkless import SparkSession

        return SparkSession(app_name, **kwargs)

    @staticmethod
    def create_pyspark_session(
        app_name: str = "test_app", enable_delta: bool = True, **kwargs: Any
    ) -> Any:
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

        # Set environment variables for PySpark
        import sys

        python_executable = sys.executable
        os.environ.setdefault("PYSPARK_PYTHON", python_executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", python_executable)
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

        try:
            import uuid
            # Get worker ID from pytest-xdist for better isolation in parallel tests
            worker_id = os.environ.get("PYTEST_XDIST_WORKER", "gw0")
            # Use unique app name, warehouse dir, and master URL for better isolation
            # PySpark matches sessions based on master URL, so we need unique master URLs
            # CRITICAL: Include process ID to ensure isolation across pytest-xdist workers
            import os as os_module
            process_id = os_module.getpid()
            unique_id = f"{worker_id}_{process_id}_{uuid.uuid4().hex[:8]}"
            unique_app_name = f"{app_name}_{unique_id}"
            unique_warehouse = f"/tmp/spark-warehouse-{unique_id}"
            # Use a unique master URL to force a new SparkContext
            # This ensures PySpark doesn't reuse sessions across tests
            unique_master = f"local[1]"
            
            # Stop any existing SparkSession to ensure clean configuration
            # This is critical for parallel test execution
            # PySpark's getOrCreate() reuses sessions even with different warehouse dirs,
            # so we must stop and clear the singleton before creating a new session
            try:
                # Try to get and stop the active session
                active_session = PySparkSession.getActiveSession()
                if active_session is not None:
                    active_session.stop()
                    # Wait a bit for the session to fully stop
                    import time
                    time.sleep(0.1)
            except (AttributeError, Exception):
                pass
            
            try:
                existing_session = PySparkSession._instantiatedSession
                if existing_session is not None:
                    existing_session.stop()
                    PySparkSession._instantiatedSession = None
                    # Wait a bit for the session to fully stop
                    import time
                    time.sleep(0.1)
            except (AttributeError, Exception):
                pass
            
            # Clear any cached SparkContext references
            # This helps ensure a fresh session is created
            # PySpark maintains a global context registry that can cause session reuse
            try:
                from pyspark import SparkContext
                # Stop all active contexts to force new ones
                if hasattr(SparkContext, '_active_spark_context'):
                    active_ctx = SparkContext._active_spark_context
                    if active_ctx is not None:
                        try:
                            active_ctx.stop()
                            import time
                            time.sleep(0.1)
                        except:
                            pass
                    SparkContext._active_spark_context = None
            except (AttributeError, Exception):
                pass
            
            # Build SparkSession with Delta Lake if enabled
            if enable_delta:
                try:
                    from delta import configure_spark_with_delta_pip
                    # configure_spark_with_delta_pip must be called on a fresh builder
                    # It adds the Delta JARs and configures extensions/catalog
                    builder = configure_spark_with_delta_pip(
                        PySparkSession.builder
                        .master(unique_master)
                        .appName(unique_app_name)
                        .config("spark.driver.bindAddress", "127.0.0.1")
                        .config("spark.driver.host", "127.0.0.1")
                        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
                        .config("spark.ui.enabled", "false")
                        .config("spark.sql.adaptive.enabled", "false")
                        .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
                        .config("spark.sql.warehouse.dir", unique_warehouse)
                        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                    )
                    # Apply any additional config from kwargs
                    for key, value in kwargs.items():
                        if key.startswith("spark."):
                            builder = builder.config(key, str(value))
                    session = builder.getOrCreate()
                    # CRITICAL FIX: Verify warehouse directory is correct
                    # PySpark's getOrCreate() may reuse a session with wrong warehouse dir
                    actual_warehouse = session.conf.get("spark.sql.warehouse.dir")
                    expected_warehouse = unique_warehouse
                    # PySpark may add "file:" prefix
                    if not (actual_warehouse == expected_warehouse or 
                            actual_warehouse == f"file:{expected_warehouse}"):
                        # Session was reused with wrong warehouse - force stop and recreate
                        session.stop()
                        PySparkSession._instantiatedSession = None
                        # Stop the SparkContext to force a new one
                        try:
                            from pyspark import SparkContext
                            if hasattr(SparkContext, '_active_spark_context'):
                                ctx = SparkContext._active_spark_context
                                if ctx is not None:
                                    ctx.stop()
                                SparkContext._active_spark_context = None
                        except:
                            pass
                        import time
                        time.sleep(0.2)
                        # Recreate the session - this time it should use the correct warehouse
                        session = builder.getOrCreate()
                except ImportError:
                    # Delta Lake not available, continue without it
                    # unique_id already defined above
                    builder = PySparkSession.builder.master(unique_master).appName(unique_app_name)
                    builder = builder.config("spark.driver.bindAddress", "127.0.0.1")
                    builder = builder.config("spark.driver.host", "127.0.0.1")
                    builder = builder.config("spark.sql.execution.arrow.pyspark.enabled", "false")
                    builder = builder.config("spark.ui.enabled", "false")
                    builder = builder.config("spark.sql.adaptive.enabled", "false")
                    builder = builder.config("spark.sql.adaptive.coalescePartitions.enabled", "false")
                    builder = builder.config("spark.sql.warehouse.dir", unique_warehouse)
                    for key, value in kwargs.items():
                        if key.startswith("spark."):
                            builder = builder.config(key, str(value))
                    session = builder.getOrCreate()
                    # Verify warehouse directory
                    actual_warehouse = session.conf.get("spark.sql.warehouse.dir")
                    expected_warehouse = unique_warehouse
                    if not (actual_warehouse == expected_warehouse or 
                            actual_warehouse == f"file:{expected_warehouse}"):
                        session.stop()
                        PySparkSession._instantiatedSession = None
                        try:
                            from pyspark import SparkContext
                            if hasattr(SparkContext, '_active_spark_context'):
                                ctx = SparkContext._active_spark_context
                                if ctx is not None:
                                    ctx.stop()
                                SparkContext._active_spark_context = None
                        except:
                            pass
                        import time
                        time.sleep(0.2)
                        session = builder.getOrCreate()
            else:
                # unique_id already defined above
                builder = PySparkSession.builder.master(unique_master).appName(unique_app_name)
                builder = builder.config("spark.driver.bindAddress", "127.0.0.1")
                builder = builder.config("spark.driver.host", "127.0.0.1")
                builder = builder.config("spark.sql.execution.arrow.pyspark.enabled", "false")
                builder = builder.config("spark.ui.enabled", "false")
                builder = builder.config("spark.sql.adaptive.enabled", "false")
                builder = builder.config("spark.sql.adaptive.coalescePartitions.enabled", "false")
                builder = builder.config("spark.sql.warehouse.dir", unique_warehouse)
                for key, value in kwargs.items():
                    if key.startswith("spark."):
                        builder = builder.config(key, str(value))
                session = builder.getOrCreate()
                # Verify warehouse directory
                actual_warehouse = session.conf.get("spark.sql.warehouse.dir")
                expected_warehouse = unique_warehouse
                if not (actual_warehouse == expected_warehouse or 
                        actual_warehouse == f"file:{expected_warehouse}"):
                    session.stop()
                    PySparkSession._instantiatedSession = None
                    try:
                        from pyspark import SparkContext
                        if hasattr(SparkContext, '_active_spark_context'):
                            ctx = SparkContext._active_spark_context
                            if ctx is not None:
                                ctx.stop()
                            SparkContext._active_spark_context = None
                    except:
                        pass
                    import time
                    time.sleep(0.2)
                    session = builder.getOrCreate()
            
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
