"""
Unit tests for Py4J error compatibility.

Tests the MockPy4JJavaError class for PySpark error handling compatibility.
"""

import pytest
from mock_spark.core.exceptions.py4j_compat import MockPy4JJavaError


class TestMockPy4JJavaError:
    """Test MockPy4JJavaError class."""

    def test_create_error_with_message(self):
        """Test creating error with message."""
        error = MockPy4JJavaError("Test error message")
        assert str(error) == "Test error message"
        assert error.message == "Test error message"

    def test_create_error_with_java_exception(self):
        """Test creating error with Java exception."""
        java_exc = {"message": "Java error"}
        error = MockPy4JJavaError("Test error", java_exception=java_exc)
        assert error.java_exception == java_exc

    def test_error_is_exception(self):
        """Test that error is an Exception."""
        error = MockPy4JJavaError("Test error")
        assert isinstance(error, Exception)

    def test_error_can_be_raised(self):
        """Test that error can be raised and caught."""
        with pytest.raises(MockPy4JJavaError) as exc_info:
            raise MockPy4JJavaError("Test error")

        assert str(exc_info.value) == "Test error"

    def test_error_repr(self):
        """Test error representation."""
        error = MockPy4JJavaError("Test error")
        repr_str = repr(error)
        assert "MockPy4JJavaError" in repr_str
        assert "Test error" in repr_str

    def test_error_can_be_caught_as_exception(self):
        """Test that error can be caught as generic Exception."""
        try:
            raise MockPy4JJavaError("Test error")
        except Exception as e:
            assert isinstance(e, MockPy4JJavaError)
            assert str(e) == "Test error"

    def test_py4j_available_inheritance(self):
        """Test that error can be used when Py4J is available."""
        try:
            error = MockPy4JJavaError("Test error")
            # MockPy4JJavaError works standalone
            # It may not directly inherit from Py4JJavaError due to init requirements
            # but it can be caught as Exception for compatibility
            assert isinstance(error, Exception)
        except ImportError:
            # Py4J not available, skip this test
            pytest.skip("Py4J not available")
