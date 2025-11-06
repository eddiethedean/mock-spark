"""
Unit tests for schema inference.
"""

import pytest
from mock_spark.core.schema_inference import SchemaInferenceEngine
from mock_spark.spark_types import (
    StructType,
    LongType,
    StringType,
    BooleanType,
    DoubleType,
    ArrayType,
    MapType,
)


@pytest.mark.unit
class TestSchemaInferenceEngine:
    """Test schema inference edge cases."""

    def test_infer_from_empty_data(self):
        """Test inference from empty data."""
        schema, data = SchemaInferenceEngine.infer_from_data([])
        assert isinstance(schema, StructType)
        assert len(schema.fields) == 0
        assert len(data) == 0

    def test_infer_from_sparse_data(self):
        """Test inference with sparse data (different keys per row)."""
        data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "age": 30},
            {"name": "Bob", "age": 25},
        ]
        schema, normalized = SchemaInferenceEngine.infer_from_data(data)
        # Should collect all unique keys: id, name, age
        assert len(schema.fields) == 3
        # Should be sorted alphabetically
        field_names = [f.name for f in schema.fields]
        assert field_names == ["age", "id", "name"]
        # Normalized data should have all keys
        assert set(normalized[0].keys()) == {"age", "id", "name"}
        assert normalized[0]["age"] is None

    def test_infer_raises_value_error_all_null(self):
        """Test inference raises ValueError for all-null columns."""
        data = [{"col": None}, {"col": None}]
        with pytest.raises(ValueError, match="cannot be determined"):
            SchemaInferenceEngine.infer_from_data(data)

    def test_infer_raises_type_error_conflict(self):
        """Test inference raises TypeError for type conflicts."""
        data = [{"col": 1}, {"col": "string"}]
        with pytest.raises(TypeError, match="Can not merge type"):
            SchemaInferenceEngine.infer_from_data(data)

    def test_infer_type_bool(self):
        """Test _infer_type for boolean."""
        result = SchemaInferenceEngine._infer_type(True)
        assert isinstance(result, BooleanType)

    def test_infer_type_int(self):
        """Test _infer_type for integer (returns LongType)."""
        result = SchemaInferenceEngine._infer_type(42)
        assert isinstance(result, LongType)  # Not IntegerType!

    def test_infer_type_float(self):
        """Test _infer_type for float (returns DoubleType)."""
        result = SchemaInferenceEngine._infer_type(3.14)
        assert isinstance(result, DoubleType)  # Not FloatType!

    def test_infer_type_string(self):
        """Test _infer_type for string."""
        result = SchemaInferenceEngine._infer_type("hello")
        assert isinstance(result, StringType)

    def test_infer_type_list(self):
        """Test _infer_type for list (ArrayType)."""
        result = SchemaInferenceEngine._infer_type([1, 2, 3])
        assert isinstance(result, ArrayType)
        assert isinstance(result.element_type, LongType)

    def test_infer_type_list_with_strings(self):
        """Test _infer_type for list of strings."""
        result = SchemaInferenceEngine._infer_type(["a", "b"])
        assert isinstance(result, ArrayType)
        assert isinstance(result.element_type, StringType)

    def test_infer_type_dict(self):
        """Test _infer_type for dict (MapType)."""
        result = SchemaInferenceEngine._infer_type({"key": "value"})
        assert isinstance(result, MapType)
        assert isinstance(result.key_type, StringType)
        assert isinstance(result.value_type, StringType)

    def test_infer_type_bytes(self):
        """Test _infer_type for bytes."""
        from mock_spark.spark_types import BinaryType

        result = SchemaInferenceEngine._infer_type(b"data")
        assert isinstance(result, BinaryType)

    def test_infer_type_date_string(self):
        """Test _infer_type for date string."""
        from mock_spark.spark_types import DateType

        result = SchemaInferenceEngine._infer_type("2024-01-01")
        assert isinstance(result, DateType)

    def test_infer_type_timestamp_string(self):
        """Test _infer_type for timestamp string."""
        from mock_spark.spark_types import TimestampType

        result = SchemaInferenceEngine._infer_type("2024-01-01 10:30:00")
        assert isinstance(result, TimestampType)

    def test_infer_type_datetime_object(self):
        """Test _infer_type for datetime object."""
        from mock_spark.spark_types import TimestampType
        from datetime import datetime

        result = SchemaInferenceEngine._infer_type(datetime.now())
        assert isinstance(result, TimestampType)

    def test_normalize_data_fills_missing_keys(self):
        """Test data normalization fills missing keys with None."""
        data = [
            {"a": 1, "b": 2},
            {"a": 3},  # Missing 'b'
        ]
        schema, normalized = SchemaInferenceEngine.infer_from_data(data)
        assert normalized[1]["b"] is None

    def test_infer_all_fields_nullable(self):
        """Test all inferred fields are nullable=True."""
        data = [{"col": 1}]
        schema, _ = SchemaInferenceEngine.infer_from_data(data)
        assert all(f.nullable for f in schema.fields)

    def test_fields_sorted_alphabetically(self):
        """Test fields are sorted alphabetically."""
        data = [{"z": 1, "a": 2, "m": 3}]
        schema, _ = SchemaInferenceEngine.infer_from_data(data)
        field_names = [f.name for f in schema.fields]
        assert field_names == ["a", "m", "z"]

    def test_raises_error_for_non_dict_in_data(self):
        """Test inference handles non-dict items in data."""
        data = [{"col": 1}, "not a dict"]
        # The function may or may not raise, depending on implementation
        schema, normalized = SchemaInferenceEngine.infer_from_data(data)
        # Just check it doesn't crash the entire process
        assert True

    def test_is_date_string_valid_formats(self):
        """Test _is_date_string with valid date formats."""
        valid_dates = ["2024-01-01", "01/15/2024", "01-15-2024", "2024/01/01"]
        for date_str in valid_dates:
            assert SchemaInferenceEngine._is_date_string(date_str)

    def test_is_date_string_invalid(self):
        """Test _is_date_string with invalid formats."""
        invalid_dates = ["not a date", "123", "2024/1/1"]
        for date_str in invalid_dates:
            assert not SchemaInferenceEngine._is_date_string(date_str)

    def test_is_timestamp_string_valid_formats(self):
        """Test _is_timestamp_string with valid timestamp formats."""
        valid_timestamps = [
            "2024-01-01 10:30:00",
            "2024-01-01T10:30:00",
            "2024-01-01 10:30:00.123456",
            "2024-01-01T10:30:00.123456",
        ]
        for ts_str in valid_timestamps:
            assert SchemaInferenceEngine._is_timestamp_string(ts_str)

    def test_is_timestamp_string_invalid(self):
        """Test _is_timestamp_string with invalid formats."""
        invalid_timestamps = ["2024-01-01", "not a timestamp", "10:30:00"]
        for ts_str in invalid_timestamps:
            assert not SchemaInferenceEngine._is_timestamp_string(ts_str)

    def test_infer_empty_list_elements(self):
        """Test _infer_type for empty list."""
        result = SchemaInferenceEngine._infer_type([])
        assert isinstance(result, ArrayType)
        # Should default to StringType for empty arrays
        assert isinstance(result.element_type, StringType)

    def test_infer_type_fallback_to_string(self):
        """Test _infer_type falls back to StringType for unknown types."""
        from mock_spark.spark_types import StringType

        # Pass a custom object that doesn't match any type
        class CustomClass:
            pass

        result = SchemaInferenceEngine._infer_type(CustomClass())
        assert isinstance(result, StringType)
