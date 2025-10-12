"""Unit tests for validation exceptions."""

import pytest
from mock_spark.core.exceptions.validation import (
    ValidationException,
    SchemaValidationException,
    DataTypeValidationException,
    ConstraintViolationException
)


def test_validation_exception_creation():
    """Test ValidationException can be created and raised."""
    with pytest.raises(ValidationException) as exc_info:
        raise ValidationException("Validation failed")
    
    assert "Validation failed" in str(exc_info.value)


def test_schema_validation_exception():
    """Test SchemaValidationException."""
    with pytest.raises(SchemaValidationException) as exc_info:
        raise SchemaValidationException("Schema mismatch")
    
    assert "Schema" in str(exc_info.value)
    assert isinstance(exc_info.value, ValidationException)


def test_schema_validation_with_details():
    """Test SchemaValidationException with schema details."""
    exc = SchemaValidationException(
        "Schema incompatible",
        expected="struct<id:int>",
        actual="struct<id:string>"
    )
    assert "struct" in str(exc)


def test_data_type_validation_exception():
    """Test DataTypeValidationException."""
    with pytest.raises(DataTypeValidationException) as exc_info:
        raise DataTypeValidationException("Invalid data type")
    
    assert "data type" in str(exc_info.value).lower()
    assert isinstance(exc_info.value, ValidationException)


def test_data_type_validation_with_types():
    """Test DataTypeValidationException with type info."""
    exc = DataTypeValidationException(
        "Type mismatch",
        expected_type="integer",
        actual_type="string"
    )
    assert "integer" in str(exc) or "string" in str(exc)


def test_constraint_violation_exception():
    """Test ConstraintViolationException."""
    with pytest.raises(ConstraintViolationException) as exc_info:
        raise ConstraintViolationException("Constraint violated")
    
    assert "Constraint" in str(exc_info.value)
    assert isinstance(exc_info.value, ValidationException)


def test_constraint_violation_with_constraint():
    """Test ConstraintViolationException with constraint name."""
    exc = ConstraintViolationException(
        "Violation",
        constraint="NOT NULL"
    )
    assert "NOT NULL" in str(exc)


def test_validation_exception_inheritance():
    """Test validation exception inheritance."""
    exc = SchemaValidationException("test")
    assert isinstance(exc, ValidationException)
    assert isinstance(exc, Exception)

