"""Tests for TypeConverter class."""

from mock_spark.dataframe.casting.type_converter import TypeConverter
from mock_spark.spark_types import (
    ArrayType,
    BooleanType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    StringType,
)


class TestTypeConverter:
    """Test cases for TypeConverter."""

    def test_cast_to_string(self):
        """Test casting to string type."""
        result = TypeConverter.cast_to_type(42, StringType())
        assert result == "42"
        assert isinstance(result, str)

    def test_cast_to_integer(self):
        """Test casting to integer type."""
        result = TypeConverter.cast_to_type("42", IntegerType())
        assert result == 42
        assert isinstance(result, int)

    def test_cast_to_long(self):
        """Test casting to long type."""
        result = TypeConverter.cast_to_type("42", LongType())
        assert result == 42
        assert isinstance(result, int)

    def test_cast_to_float(self):
        """Test casting to float type."""
        result = TypeConverter.cast_to_type("42.5", FloatType())
        assert result == 42.5
        assert isinstance(result, float)

    def test_cast_to_double(self):
        """Test casting to double type."""
        result = TypeConverter.cast_to_type("42.5", DoubleType())
        assert result == 42.5
        assert isinstance(result, float)

    def test_cast_to_boolean(self):
        """Test casting to boolean type."""
        result = TypeConverter.cast_to_type("true", BooleanType())
        assert result is True
        assert isinstance(result, bool)

    def test_cast_to_array(self):
        """Test casting to array type."""
        result = TypeConverter.cast_to_type([1, 2, 3], ArrayType(IntegerType()))
        assert result == [1, 2, 3]
        assert isinstance(result, list)

    def test_cast_to_map(self):
        """Test casting to map type."""
        result = TypeConverter.cast_to_type(
            {"key": "value"}, MapType(StringType(), StringType())
        )
        assert result == {"key": "value"}
        assert isinstance(result, dict)

    def test_cast_none_value(self):
        """Test casting None value."""
        result = TypeConverter.cast_to_type(None, StringType())
        assert result is None

    def test_infer_type_string(self):
        """Test type inference for string."""
        result = TypeConverter.infer_type("hello")
        assert isinstance(result, StringType)

    def test_infer_type_integer(self):
        """Test type inference for integer."""
        result = TypeConverter.infer_type(42)
        assert isinstance(result, LongType)

    def test_infer_type_float(self):
        """Test type inference for float."""
        result = TypeConverter.infer_type(42.5)
        assert isinstance(result, DoubleType)

    def test_infer_type_boolean(self):
        """Test type inference for boolean."""
        result = TypeConverter.infer_type(True)
        assert isinstance(result, BooleanType)

    def test_infer_type_list(self):
        """Test type inference for list."""
        result = TypeConverter.infer_type([1, 2, 3])
        assert isinstance(result, ArrayType)
        assert isinstance(result.element_type, LongType)

    def test_infer_type_empty_list(self):
        """Test type inference for empty list."""
        result = TypeConverter.infer_type([])
        assert isinstance(result, ArrayType)
        assert isinstance(result.element_type, StringType)

    def test_infer_type_dict(self):
        """Test type inference for dictionary."""
        result = TypeConverter.infer_type({"key": "value"})
        assert isinstance(result, MapType)
        assert isinstance(result.key_type, StringType)
        assert isinstance(result.value_type, StringType)

    def test_infer_type_empty_dict(self):
        """Test type inference for empty dictionary."""
        result = TypeConverter.infer_type({})
        assert isinstance(result, MapType)
        assert isinstance(result.key_type, StringType)
        assert isinstance(result.value_type, StringType)

    def test_infer_type_none(self):
        """Test type inference for None."""
        result = TypeConverter.infer_type(None)
        assert isinstance(result, StringType)
