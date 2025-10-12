"""Unit tests for runtime exceptions."""

import pytest
from mock_spark.core.exceptions.runtime import (
    RuntimeException,
    ArithmeticException,
    CastException,
    NullPointerException,
    IndexOutOfBoundsException
)


def test_runtime_exception_creation():
    """Test RuntimeException can be created and raised."""
    with pytest.raises(RuntimeException) as exc_info:
        raise RuntimeException("Runtime error")
    
    assert "Runtime error" in str(exc_info.value)


def test_arithmetic_exception():
    """Test ArithmeticException."""
    with pytest.raises(ArithmeticException) as exc_info:
        raise ArithmeticException("Division by zero")
    
    assert "Division" in str(exc_info.value)
    assert isinstance(exc_info.value, RuntimeException)


def test_arithmetic_exception_with_operation():
    """Test ArithmeticException with operation details."""
    exc = ArithmeticException("Invalid operation", operation="divide")
    assert "divide" in str(exc)


def test_cast_exception():
    """Test CastException."""
    with pytest.raises(CastException) as exc_info:
        raise CastException("Cannot cast string to int")
    
    assert "cast" in str(exc_info.value).lower()
    assert isinstance(exc_info.value, RuntimeException)


def test_cast_exception_with_types():
    """Test CastException with type information."""
    exc = CastException("Cast failed", from_type="string", to_type="integer")
    assert "string" in str(exc) and "integer" in str(exc)


def test_null_pointer_exception():
    """Test NullPointerException."""
    with pytest.raises(NullPointerException) as exc_info:
        raise NullPointerException("Null reference")
    
    assert "Null" in str(exc_info.value)
    assert isinstance(exc_info.value, RuntimeException)


def test_null_pointer_with_field():
    """Test NullPointerException with field name."""
    exc = NullPointerException("Null field access", field="user_id")
    assert "user_id" in str(exc)


def test_index_out_of_bounds_exception():
    """Test IndexOutOfBoundsException."""
    with pytest.raises(IndexOutOfBoundsException) as exc_info:
        raise IndexOutOfBoundsException("Index out of range")
    
    assert "Index" in str(exc_info.value)
    assert isinstance(exc_info.value, RuntimeException)


def test_index_out_of_bounds_with_details():
    """Test IndexOutOfBoundsException with index details."""
    exc = IndexOutOfBoundsException("Out of bounds", index=10, size=5)
    assert "10" in str(exc) or "5" in str(exc)


def test_runtime_exception_inheritance():
    """Test runtime exception inheritance."""
    exc = ArithmeticException("test")
    assert isinstance(exc, RuntimeException)
    assert isinstance(exc, Exception)

