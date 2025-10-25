"""
Unit tests for Data Validation module.

Tests DataValidator class for validation and coercion functionality.
"""

import pytest
from mock_spark.core.data_validation import (
    DataValidator,
    validate_data,
    coerce_data,
)
from mock_spark.spark_types import (
    MockStructType,
    MockStructField,
    StringType,
    LongType,
    DoubleType,
    BooleanType,
)
from mock_spark.core.exceptions.validation import IllegalArgumentException


# Test Fixtures
@pytest.fixture
def simple_schema():
    """Simple schema with basic types."""
    return MockStructType(
        [
            MockStructField("name", StringType()),
            MockStructField("age", LongType()),
            MockStructField("salary", DoubleType()),
            MockStructField("active", BooleanType()),
        ]
    )


@pytest.fixture
def valid_data():
    """Valid data matching simple schema."""
    return [
        {"name": "Alice", "age": 30, "salary": 50000.0, "active": True},
        {"name": "Bob", "age": 25, "salary": 45000.0, "active": False},
    ]


class TestDataValidatorInit:
    """Test DataValidator initialization."""

    def test_init_with_defaults(self, simple_schema):
        """Test initialization with default parameters."""
        validator = DataValidator(simple_schema)
        assert validator.schema == simple_schema
        assert validator.validation_mode == "relaxed"
        assert validator.enable_coercion is True

    def test_init_strict_mode(self, simple_schema):
        """Test initialization in strict mode."""
        validator = DataValidator(
            simple_schema, validation_mode="strict", enable_coercion=False
        )
        assert validator.validation_mode == "strict"
        assert validator.enable_coercion is False


class TestValidation:
    """Test data validation."""

    def test_validate_valid_data_strict(self, simple_schema, valid_data):
        """Test validation passes for valid data in strict mode."""
        validator = DataValidator(simple_schema, validation_mode="strict")
        validator.validate(valid_data)  # Should not raise

    def test_validate_relaxed_mode_skips(self, simple_schema):
        """Test relaxed mode skips validation."""
        invalid_data = [{"name": "Alice"}]  # Missing fields
        validator = DataValidator(simple_schema, validation_mode="relaxed")
        validator.validate(invalid_data)  # Should not raise in relaxed mode

    def test_validate_missing_field(self, simple_schema):
        """Test validation fails for missing required field."""
        invalid_data = [{"name": "Alice", "age": 30}]  # Missing salary, active
        validator = DataValidator(simple_schema, validation_mode="strict")

        with pytest.raises(IllegalArgumentException) as exc_info:
            validator.validate(invalid_data)
        assert "Missing required field 'salary'" in str(exc_info.value)

    def test_validate_unexpected_field(self, simple_schema):
        """Test validation fails for unexpected field."""
        invalid_data = [
            {
                "name": "Alice",
                "age": 30,
                "salary": 50000.0,
                "active": True,
                "extra": "value",
            }
        ]
        validator = DataValidator(simple_schema, validation_mode="strict")

        with pytest.raises(IllegalArgumentException) as exc_info:
            validator.validate(invalid_data)
        assert "Unexpected field 'extra'" in str(exc_info.value)

    def test_validate_type_mismatch(self, simple_schema):
        """Test validation fails for type mismatch."""
        invalid_data = [
            {
                "name": "Alice",
                "age": "thirty",  # Should be int
                "salary": 50000.0,
                "active": True,
            }
        ]
        validator = DataValidator(simple_schema, validation_mode="strict")

        with pytest.raises(IllegalArgumentException) as exc_info:
            validator.validate(invalid_data)
        assert "Type mismatch for field 'age'" in str(exc_info.value)

    def test_validate_allows_nulls(self, simple_schema):
        """Test validation allows null values."""
        data_with_nulls = [
            {"name": "Alice", "age": None, "salary": None, "active": None}
        ]
        validator = DataValidator(simple_schema, validation_mode="strict")
        validator.validate(data_with_nulls)  # Should not raise

    def test_validate_non_dict_row(self, simple_schema):
        """Test validation fails for non-dict rows in strict mode."""
        invalid_data = [["Alice", 30, 50000.0, True]]
        validator = DataValidator(simple_schema, validation_mode="strict")

        with pytest.raises(IllegalArgumentException) as exc_info:
            validator.validate(invalid_data)
        assert "Strict mode requires dict rows" in str(exc_info.value)

    def test_validate_numeric_widening(self, simple_schema):
        """Test validation allows numeric widening (int->float)."""
        data = [
            {
                "name": "Alice",
                "age": 30,
                "salary": 50000,  # int instead of float - should be OK
                "active": True,
            }
        ]
        validator = DataValidator(simple_schema, validation_mode="strict")
        validator.validate(data)  # Should not raise


class TestCoercion:
    """Test data coercion."""

    def test_coerce_strings_to_numbers(self, simple_schema):
        """Test coercing string values to numeric types."""
        data = [{"name": "Alice", "age": "30", "salary": "50000.0", "active": "true"}]
        validator = DataValidator(simple_schema, enable_coercion=True)
        coerced = validator.coerce(data)

        assert coerced[0]["age"] == 30
        assert coerced[0]["salary"] == 50000.0
        assert coerced[0]["active"] is True

    def test_coerce_numbers_to_strings(self):
        """Test coercing numeric values to strings."""
        schema = MockStructType(
            [
                MockStructField("id", StringType()),
                MockStructField("value", StringType()),
            ]
        )
        data = [{"id": 123, "value": 45.67}]
        validator = DataValidator(schema, enable_coercion=True)
        coerced = validator.coerce(data)

        assert coerced[0]["id"] == "123"
        assert coerced[0]["value"] == "45.67"

    def test_coerce_boolean_from_strings(self, simple_schema):
        """Test coercing string values to booleans."""
        data = [
            {"name": "Alice", "age": 30, "salary": 50000.0, "active": "true"},
            {"name": "Bob", "age": 25, "salary": 45000.0, "active": "false"},
            {"name": "Charlie", "age": 35, "salary": 55000.0, "active": "1"},
            {"name": "David", "age": 28, "salary": 48000.0, "active": "0"},
        ]
        validator = DataValidator(simple_schema, enable_coercion=True)
        coerced = validator.coerce(data)

        assert coerced[0]["active"] is True
        assert coerced[1]["active"] is False
        assert coerced[2]["active"] is True
        assert coerced[3]["active"] is False

    def test_coerce_handles_nulls(self, simple_schema):
        """Test coercion preserves null values."""
        data = [{"name": None, "age": None, "salary": None, "active": None}]
        validator = DataValidator(simple_schema, enable_coercion=True)
        coerced = validator.coerce(data)

        assert coerced[0]["name"] is None
        assert coerced[0]["age"] is None
        assert coerced[0]["salary"] is None
        assert coerced[0]["active"] is None

    def test_coerce_disabled(self, simple_schema):
        """Test coercion is skipped when disabled."""
        data = [{"name": "Alice", "age": "30", "salary": "50000.0", "active": "true"}]
        validator = DataValidator(simple_schema, enable_coercion=False)
        result = validator.coerce(data)

        # Should return original data unchanged
        assert result[0]["age"] == "30"  # Still string
        assert result[0]["salary"] == "50000.0"  # Still string

    def test_coerce_invalid_conversion_returns_original(self, simple_schema):
        """Test coercion returns original value if conversion fails."""
        data = [
            {
                "name": "Alice",
                "age": "not_a_number",
                "salary": 50000.0,
                "active": True,
            }
        ]
        validator = DataValidator(simple_schema, enable_coercion=True)
        coerced = validator.coerce(data)

        # Invalid conversion should preserve original value
        assert coerced[0]["age"] == "not_a_number"

    def test_coerce_non_dict_row(self, simple_schema):
        """Test coercion leaves non-dict rows unchanged."""
        data = [["Alice", 30, 50000.0, True]]
        validator = DataValidator(simple_schema, enable_coercion=True)
        coerced = validator.coerce(data)

        assert coerced[0] == ["Alice", 30, 50000.0, True]


class TestConvenienceFunctions:
    """Test convenience functions."""

    def test_validate_data_function(self, simple_schema, valid_data):
        """Test validate_data convenience function."""
        validate_data(valid_data, simple_schema, mode="strict")  # Should not raise

    def test_validate_data_function_fails(self, simple_schema):
        """Test validate_data raises on invalid data."""
        invalid_data = [{"name": "Alice"}]  # Missing fields

        with pytest.raises(IllegalArgumentException):
            validate_data(invalid_data, simple_schema, mode="strict")

    def test_coerce_data_function(self, simple_schema):
        """Test coerce_data convenience function."""
        data = [{"name": "Alice", "age": "30", "salary": "50000.0", "active": "true"}]
        coerced = coerce_data(data, simple_schema)

        assert coerced[0]["age"] == 30
        assert coerced[0]["salary"] == 50000.0
        assert coerced[0]["active"] is True


class TestEdgeCases:
    """Test edge cases."""

    def test_empty_data(self, simple_schema):
        """Test validator handles empty data."""
        validator = DataValidator(simple_schema, validation_mode="strict")
        validator.validate([])  # Should not raise
        assert validator.coerce([]) == []

    def test_multiple_rows(self, simple_schema):
        """Test validator handles multiple rows."""
        data = [
            {"name": "Alice", "age": 30, "salary": 50000.0, "active": True},
            {"name": "Bob", "age": 25, "salary": 45000.0, "active": False},
            {"name": "Charlie", "age": 35, "salary": 55000.0, "active": True},
        ]
        validator = DataValidator(simple_schema, validation_mode="strict")
        validator.validate(data)  # Should not raise

        coerced = validator.coerce(data)
        assert len(coerced) == 3
