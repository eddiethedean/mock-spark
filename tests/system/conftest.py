"""Pytest configuration for system tests."""

import pytest
import gc
from typing import Iterator


@pytest.fixture(scope="function")
def spark():
    """Create a MockSparkSession for system tests."""
    from mock_spark import MockSparkSession
    
    session = MockSparkSession("system_test")
    yield session
    
    # Cleanup
    try:
        session.stop()
    except Exception:
        pass
    gc.collect()


@pytest.fixture(scope="function")
def data_dir(tmp_path):
    """Provide a temporary directory for test data files."""
    data_path = tmp_path / "data"
    data_path.mkdir()
    return data_path


@pytest.fixture(scope="function")
def output_dir(tmp_path):
    """Provide a temporary directory for output files."""
    output_path = tmp_path / "output"
    output_path.mkdir()
    return output_path

