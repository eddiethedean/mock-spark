"""
Global pytest configuration for mock-spark tests.

This configuration ensures proper resource cleanup to prevent test leaks.
No PySpark dependencies - tests use expected outputs for compatibility validation.
"""

import contextlib
import gc
import os
import pytest

# Prevent numpy crashes on macOS ARM chips with Python 3.8
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
def spark(mock_spark_session):
    """Alias for mock_spark_session to match common test patterns."""
    return mock_spark_session


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
