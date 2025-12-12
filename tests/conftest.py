"""
Global pytest configuration for mock-spark tests.

This configuration ensures proper resource cleanup to prevent test leaks.
Supports both mock-spark and PySpark backends for unified testing.
"""

import contextlib
import gc
import os
import pytest

# Prevent numpy crashes on macOS ARM chips with Python 3.9
os.environ["VECLIB_MAXIMUM_THREADS"] = "1"


@pytest.fixture(scope="function", autouse=True)
def cleanup_after_each_test():
    """Automatically clean up resources after each test.

    This fixture runs after every test to ensure backend connections
    and other resources are properly cleaned up, preventing test leaks.
    """
    yield
    # Force garbage collection to trigger __del__ methods
    gc.collect()


@pytest.fixture
def mock_spark_session():
    """Create a SparkSession with automatic cleanup."""
    from mock_spark import SparkSession

    session = SparkSession("test_app")
    yield session
    # Explicitly clean up
    with contextlib.suppress(BaseException):
        session.stop()
    gc.collect()


@pytest.fixture
def isolated_session():
    """Create an isolated SparkSession for tests requiring isolation."""
    from mock_spark import SparkSession
    import uuid

    # Use unique name to ensure isolation
    session_name = f"test_isolated_{uuid.uuid4().hex[:8]}"
    session = SparkSession(session_name)
    yield session
    with contextlib.suppress(BaseException):
        session.stop()
    gc.collect()


@pytest.fixture
def spark(request):
    """Unified SparkSession fixture that works with both mock-spark and PySpark.

    Backend selection priority:
    1. pytest marker: @pytest.mark.backend('mock'|'pyspark'|'both')
    2. Environment variable: MOCK_SPARK_TEST_BACKEND
    3. Default: mock-spark

    Examples:
        # Use mock-spark (default)
        def test_something(spark):
            df = spark.createDataFrame([{"id": 1}])

        # Force PySpark
        @pytest.mark.backend('pyspark')
        def test_with_pyspark(spark):
            df = spark.createDataFrame([{"id": 1}])

        # Compare both
        @pytest.mark.backend('both')
        def test_comparison(mock_spark_session, pyspark_session):
            # Both sessions available
    """
    from tests.fixtures.spark_backend import (
        SparkBackend,
        BackendType,
        get_backend_type,
    )

    # Handle backward compatibility - request may not be available in all contexts
    try:
        backend = get_backend_type(request)
    except (AttributeError, TypeError):
        # Fallback for backward compatibility
        backend = BackendType.MOCK

    if backend == BackendType.BOTH:
        # For comparison mode, return mock-spark by default
        # Tests should use mock_spark_session and pyspark_session fixtures
        backend = BackendType.MOCK

    session = SparkBackend.create_session(
        app_name="test_app",
        backend=backend,
        request=request if hasattr(request, "node") else None,
    )
    yield session

    # Cleanup
    with contextlib.suppress(BaseException):
        session.stop()
    gc.collect()


@pytest.fixture
def spark_backend(request):
    """Get the current backend type being used.

    Returns:
        BackendType enum value.
    """
    from tests.fixtures.spark_backend import get_backend_type

    try:
        return get_backend_type(request)
    except (AttributeError, TypeError):
        # Fallback for backward compatibility
        from tests.fixtures.spark_backend import BackendType

        return BackendType.MOCK


@pytest.fixture
def pyspark_session(request):
    """Create a PySpark SparkSession for comparison testing.

    Skips test if PySpark is not available.
    """
    from tests.fixtures.spark_backend import SparkBackend

    try:
        session = SparkBackend.create_pyspark_session("test_app")
        yield session
        with contextlib.suppress(BaseException):
            session.stop()
        gc.collect()
    except (ImportError, RuntimeError) as e:
        pytest.skip(f"PySpark not available: {e}")


@pytest.fixture
def mock_spark():
    """Provide mock spark session for compatibility tests."""
    from mock_spark import SparkSession

    session = SparkSession("test_app")
    yield session
    with contextlib.suppress(BaseException):
        session.stop()
    gc.collect()


@pytest.fixture
def temp_file_storage_path():
    """Provide a temporary directory for file storage backend tests.

    This fixture ensures that file storage backends created in tests
    use temporary directories that are automatically cleaned up,
    preventing test artifacts from being created in the repository root.
    """
    import tempfile
    import os

    with tempfile.TemporaryDirectory() as tmp_dir:
        # Create a subdirectory for the storage path
        storage_path = os.path.join(tmp_dir, "test_storage")
        yield storage_path
        # Cleanup is handled by TemporaryDirectory context manager


def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "delta: mark test as requiring Delta Lake (may be skipped)"
    )
    config.addinivalue_line(
        "markers", "performance: mark test as a performance benchmark"
    )
    config.addinivalue_line(
        "markers",
        "compatibility: mark test as compatibility test using expected outputs",
    )
    config.addinivalue_line(
        "markers", "unit: mark test as unit test (no external dependencies)"
    )
    config.addinivalue_line(
        "markers", "timeout: mark tests that rely on pytest-timeout"
    )
    config.addinivalue_line(
        "markers",
        "backend(mock|pyspark|both): mark test to run with specific backend(s)",
    )
