"""Type conversion utilities for DataFrame operations."""

from typing import Any

from ...spark_types import (
    ArrayType,
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    TimestampType,
)


class TypeConverter:
    """Handles type conversion operations for DataFrame."""

    @staticmethod
    def cast_to_type(value: Any, target_type: DataType) -> Any:
        """Cast a value to the specified target type."""
        if value is None:
            return None

        if isinstance(target_type, StringType):
            return str(value)
        elif isinstance(target_type, (IntegerType, LongType)):
            return int(value)
        elif isinstance(target_type, (FloatType, DoubleType)):
            return float(value)
        elif isinstance(target_type, BooleanType):
            return bool(value)
        elif isinstance(target_type, DateType):
            # Handle date conversion
            if hasattr(value, "date"):
                return value.date()
            return value
        elif isinstance(target_type, TimestampType):
            # Handle timestamp conversion
            if hasattr(value, "timestamp"):
                return value.timestamp()
            return value
        elif isinstance(target_type, DecimalType):
            # Handle decimal conversion
            from decimal import Decimal

            return Decimal(str(value))
        elif isinstance(target_type, ArrayType):
            # Handle array conversion
            if isinstance(value, (list, tuple)):
                return [
                    TypeConverter.cast_to_type(item, target_type.element_type)
                    for item in value
                ]
            return [value]
        elif isinstance(target_type, MapType):
            # Handle map conversion
            if isinstance(value, dict):
                return {
                    TypeConverter.cast_to_type(
                        k, target_type.key_type
                    ): TypeConverter.cast_to_type(v, target_type.value_type)
                    for k, v in value.items()
                }
            return {value: None}
        else:
            return value

    @staticmethod
    def infer_type(value: Any) -> DataType:
        """Infer the data type of a value."""
        if value is None:
            return StringType()
        elif isinstance(value, bool):
            return BooleanType()
        elif isinstance(value, int):
            return LongType()
        elif isinstance(value, float):
            return DoubleType()
        elif isinstance(value, str):
            return StringType()
        elif isinstance(value, (list, tuple)):
            if value:
                element_type = TypeConverter.infer_type(value[0])
                return ArrayType(element_type)
            return ArrayType(StringType())
        elif isinstance(value, dict):
            if value:
                key_type = TypeConverter.infer_type(next(iter(value.keys())))
                value_type = TypeConverter.infer_type(next(iter(value.values())))
                return MapType(key_type, value_type)
            return MapType(StringType(), StringType())
        else:
            return StringType()
