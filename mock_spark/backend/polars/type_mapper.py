"""
Type mapper for converting MockSpark types to Polars dtypes.

This module provides functions to convert MockSpark data types
to Polars data types for DataFrame operations.
"""

from typing import Any
import polars as pl
from mock_spark.spark_types import (
    MockDataType,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    FloatType,
    BooleanType,
    DateType,
    TimestampType,
    TimestampNTZType,
    DecimalType,
    BinaryType,
    ArrayType,
    MapType,
    StructType,
    ShortType,
    ByteType,
    NullType,
)


def mock_type_to_polars_dtype(mock_type: MockDataType) -> pl.DataType:
    """Convert MockSpark type to Polars dtype.

    Args:
        mock_type: MockSpark data type

    Returns:
        Polars data type

    Raises:
        ValueError: If the type is not supported
    """
    if isinstance(mock_type, StringType):
        return pl.Utf8
    elif isinstance(mock_type, IntegerType):
        return pl.Int32
    elif isinstance(mock_type, LongType):
        return pl.Int64
    elif isinstance(mock_type, DoubleType):
        return pl.Float64
    elif isinstance(mock_type, FloatType):
        return pl.Float32
    elif isinstance(mock_type, BooleanType):
        return pl.Boolean
    elif isinstance(mock_type, DateType):
        return pl.Date
    elif isinstance(mock_type, TimestampType):
        return pl.Datetime(time_unit="us")  # Polars uses microseconds
    elif isinstance(mock_type, TimestampNTZType):
        return pl.Datetime(time_unit="us", time_zone=None)
    elif isinstance(mock_type, DecimalType):
        # Polars doesn't have exact decimal type, use Float64
        return pl.Float64
    elif isinstance(mock_type, BinaryType):
        return pl.Binary
    elif isinstance(mock_type, ArrayType):
        element_type = mock_type_to_polars_dtype(mock_type.elementType)
        return pl.List(element_type)
    elif isinstance(mock_type, MapType):
        key_type = mock_type_to_polars_dtype(mock_type.keyType)
        value_type = mock_type_to_polars_dtype(mock_type.valueType)
        return pl.Struct(
            [
                pl.Field("key", key_type),
                pl.Field("value", value_type),
            ]
        )
    elif isinstance(mock_type, StructType):
        # Convert struct fields to Polars struct
        fields = []
        for field in mock_type.fields:
            field_type = mock_type_to_polars_dtype(field.dataType)
            fields.append(pl.Field(field.name, field_type))
        return pl.Struct(fields)
    elif isinstance(mock_type, ShortType):
        return pl.Int16
    elif isinstance(mock_type, ByteType):
        return pl.Int8
    elif isinstance(mock_type, NullType):
        return pl.Null
    else:
        raise ValueError(f"Unsupported MockSpark type: {type(mock_type)}")


def polars_dtype_to_mock_type(polars_dtype: pl.DataType) -> MockDataType:
    """Convert Polars dtype to MockSpark type.

    Args:
        polars_dtype: Polars data type

    Returns:
        MockSpark data type

    Raises:
        ValueError: If the type is not supported
    """
    if polars_dtype == pl.Utf8:
        return StringType()
    elif polars_dtype == pl.Int32:
        return IntegerType()
    elif polars_dtype == pl.Int64:
        return LongType()
    elif polars_dtype == pl.Float64:
        return DoubleType()
    elif polars_dtype == pl.Float32:
        return FloatType()
    elif polars_dtype == pl.Boolean:
        return BooleanType()
    elif polars_dtype == pl.Date:
        return DateType()
    elif isinstance(polars_dtype, pl.Datetime):
        if polars_dtype.time_zone is None:
            return TimestampNTZType()
        return TimestampType()
    elif polars_dtype == pl.Binary:
        return BinaryType()
    elif isinstance(polars_dtype, pl.List):
        element_type = polars_dtype_to_mock_type(polars_dtype.inner)
        return ArrayType(elementType=element_type)
    elif isinstance(polars_dtype, pl.Struct):
        # Convert struct to StructType
        from mock_spark.spark_types import MockStructType, MockStructField

        fields = []
        for field in polars_dtype.fields:
            field_type = polars_dtype_to_mock_type(field.dtype)
            fields.append(MockStructField(field.name, field_type))
        return StructType(fields)
    elif polars_dtype == pl.Int16:
        return ShortType()
    elif polars_dtype == pl.Int8:
        return ByteType()
    elif polars_dtype == pl.Null:
        return NullType()
    else:
        raise ValueError(f"Unsupported Polars dtype: {polars_dtype}")

