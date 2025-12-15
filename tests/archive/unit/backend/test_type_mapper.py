"""
Unit tests for type mapper.
"""

import pytest
import polars as pl
from sparkless.backend.polars.type_mapper import (
    mock_type_to_polars_dtype,
    polars_dtype_to_mock_type,
)
from sparkless.spark_types import (
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    BooleanType,
    ArrayType,
    MapType,
    StructType,
    StructField,
)


@pytest.mark.unit
class TestTypeMapper:
    """Test type mapping operations."""

    def test_mock_to_polars_string(self):
        """Test converting StringType to Polars."""
        result = mock_type_to_polars_dtype(StringType())
        assert result == pl.Utf8

    def test_mock_to_polars_integer(self):
        """Test converting IntegerType to Polars."""
        result = mock_type_to_polars_dtype(IntegerType())
        assert result == pl.Int32

    def test_mock_to_polars_long(self):
        """Test converting LongType to Polars."""
        result = mock_type_to_polars_dtype(LongType())
        assert result == pl.Int64

    def test_mock_to_polars_double(self):
        """Test converting DoubleType to Polars."""
        result = mock_type_to_polars_dtype(DoubleType())
        assert result == pl.Float64

    def test_mock_to_polars_boolean(self):
        """Test converting BooleanType to Polars."""
        result = mock_type_to_polars_dtype(BooleanType())
        assert result == pl.Boolean

    def test_mock_to_polars_array(self):
        """Test converting ArrayType to Polars."""
        array_type = ArrayType(StringType())
        result = mock_type_to_polars_dtype(array_type)

        assert isinstance(result, pl.List)
        assert result.inner == pl.Utf8

    def test_mock_to_polars_map(self):
        """Test converting MapType to Polars."""
        map_type = MapType(StringType(), IntegerType())
        result = mock_type_to_polars_dtype(map_type)

        assert isinstance(result, pl.Struct)
        assert len(result.fields) == 2

    def test_mock_to_polars_struct(self):
        """Test converting StructType to Polars."""
        struct_type = StructType(
            [
                StructField("name", StringType()),
                StructField("age", IntegerType()),
            ]
        )
        result = mock_type_to_polars_dtype(struct_type)

        assert isinstance(result, pl.Struct)
        assert len(result.fields) == 2

    def test_mock_to_polars_unsupported_raises_error(self):
        """Test converting unsupported type raises ValueError."""

        class UnsupportedType:
            pass

        with pytest.raises(ValueError, match="Unsupported Sparkless type"):
            mock_type_to_polars_dtype(UnsupportedType())

    def test_polars_to_mock_string(self):
        """Test converting Polars Utf8 to StringType."""
        result = polars_dtype_to_mock_type(pl.Utf8)
        assert isinstance(result, StringType)

    def test_polars_to_mock_integer(self):
        """Test converting Polars Int32 to IntegerType."""
        result = polars_dtype_to_mock_type(pl.Int32)
        assert isinstance(result, IntegerType)

    def test_polars_to_mock_long(self):
        """Test converting Polars Int64 to LongType."""
        result = polars_dtype_to_mock_type(pl.Int64)
        assert isinstance(result, LongType)

    def test_polars_to_mock_array(self):
        """Test converting Polars List to ArrayType."""
        list_type = pl.List(pl.Utf8)
        result = polars_dtype_to_mock_type(list_type)

        assert isinstance(result, ArrayType)
        assert isinstance(result.element_type, StringType)

    def test_polars_to_mock_struct(self):
        """Test converting Polars Struct to StructType."""
        struct_type = pl.Struct(
            [
                pl.Field("name", pl.Utf8),
                pl.Field("age", pl.Int32),
            ]
        )
        result = polars_dtype_to_mock_type(struct_type)

        assert isinstance(result, StructType)
        assert len(result.fields) == 2

    def test_polars_to_mock_unsupported_raises_error(self):
        """Test converting unsupported Polars type raises ValueError."""
        # Create an unsupported type (e.g., Duration)
        with pytest.raises(ValueError, match="Unsupported Polars dtype"):
            polars_dtype_to_mock_type(pl.Duration(time_unit="ns"))
