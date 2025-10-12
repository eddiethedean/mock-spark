"""Pytest configuration for integration tests."""

import pytest
import gc
from typing import Iterator


@pytest.fixture(scope="function")
def spark():
    """Create a MockSparkSession for integration tests."""
    from mock_spark import MockSparkSession
    
    session = MockSparkSession("integration_test")
    yield session
    
    # Cleanup
    try:
        session.stop()
    except Exception:
        pass
    gc.collect()


@pytest.fixture(scope="function")
def temp_dir(tmp_path):
    """Provide a temporary directory for file operations."""
    return tmp_path

