"""
Global pytest configuration for mock-spark tests.

This configuration ensures proper resource cleanup to prevent test leaks.
"""

import pytest
import gc


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
    except:
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
    except:
        pass
    gc.collect()
