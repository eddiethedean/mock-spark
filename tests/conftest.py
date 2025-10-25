"""
Global pytest configuration for mock-spark tests.

This configuration ensures proper resource cleanup to prevent test leaks.
No PySpark dependencies - tests use expected outputs for compatibility validation.
"""

import os
import pytest
import gc

# Prevent numpy crashes on macOS ARM chips with Python 3.8
os.environ["VECLIB_MAXIMUM_THREADS"] = "1"


@pytest.fixture(scope="function", autouse=True)
def cleanup_after_each_test():
    """Automatically clean up resources after each test.

    This fixture runs after every test to ensure DuckDB connections
    and other resources are properly cleaned up, preventing test leaks.
    """
    yield
    # Force garbage collection to trigger __del__ methods
    gc.collect()


@pytest.fixture
def mock_spark_session():
    """Create a MockSparkSession with automatic cleanup."""
    from mock_spark import MockSparkSession

    session = MockSparkSession("test_app")
    yield session
    # Explicitly clean up
    try:
        session.stop()
    except:  # noqa: E722
        pass
    gc.collect()


@pytest.fixture
def isolated_session():
    """Create an isolated MockSparkSession for tests requiring isolation."""
    from mock_spark import MockSparkSession
    import uuid

    # Use unique name to ensure isolation
    session_name = f"test_isolated_{uuid.uuid4().hex[:8]}"
    session = MockSparkSession(session_name)
    yield session
    try:
        session.stop()
    except:  # noqa: E722
        pass
    gc.collect()


@pytest.fixture
def spark(mock_spark_session):
    """Alias for mock_spark_session to match common test patterns."""
    return mock_spark_session


@pytest.fixture
def mock_spark():
    """Provide mock spark session for compatibility tests."""
    from mock_spark import MockSparkSession

    session = MockSparkSession("test_app")
    yield session
    try:
        session.stop()
    except:  # noqa: E722
        pass
    gc.collect()


def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "delta: mark test as requiring Delta Lake (may be skipped)"
    )
    config.addinivalue_line(
        "markers", "performance: mark test as a performance benchmark"
    )
    config.addinivalue_line(
        "markers", "compatibility: mark test as compatibility test using expected outputs"
    )
    config.addinivalue_line(
        "markers", "unit: mark test as unit test (no external dependencies)"
    )
