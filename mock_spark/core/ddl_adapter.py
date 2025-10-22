"""
Adapter to convert spark-ddl-parser output to MockStructType.

This module provides an adapter layer between the standalone spark-ddl-parser
package and mock-spark's internal type system.
"""

from spark_ddl_parser import parse_ddl_schema as parse_ddl
from spark_ddl_parser.types import (
    StructType,
    StructField,
    SimpleType,
    DecimalType,
    ArrayType,
    MapType,
    DataType,
)

from ..spark_types import (
    MockStructType,
    MockStructField,
    MockDataType,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    BooleanType,
    DateType,
    TimestampType,
    BinaryType,
    FloatType,
    ShortType,
    ByteType,
    DecimalType as MockDecimalType,
    ArrayType as MockArrayType,
    MapType as MockMapType,
)


def parse_ddl_schema(ddl_string: str) -> MockStructType:
    """Parse DDL and convert to MockStructType.

    Args:
        ddl_string: DDL schema string (e.g., "id long, name string")

    Returns:
        MockStructType with parsed fields

    Raises:
        ValueError: If DDL string is invalid
    """
    parsed = parse_ddl(ddl_string)
    return _convert_struct_type(parsed)


def _convert_struct_type(struct: StructType) -> MockStructType:
    """Convert StructType to MockStructType.

    Args:
        struct: Parsed StructType from spark-ddl-parser

    Returns:
        MockStructType
    """
    fields = [_convert_field(f) for f in struct.fields]
    return MockStructType(fields)


def _convert_field(field: StructField) -> MockStructField:
    """Convert StructField to MockStructField.

    Args:
        field: Parsed StructField from spark-ddl-parser

    Returns:
        MockStructField
    """
    data_type = _convert_data_type(field.data_type)
    return MockStructField(name=field.name, dataType=data_type, nullable=field.nullable)


def _convert_data_type(data_type: DataType) -> MockDataType:
    """Convert DataType to MockDataType.

    Args:
        data_type: Parsed DataType from spark-ddl-parser

    Returns:
        MockDataType
    """
    if isinstance(data_type, SimpleType):
        return _convert_simple_type(data_type)
    elif isinstance(data_type, DecimalType):
        return MockDecimalType(precision=data_type.precision, scale=data_type.scale)
    elif isinstance(data_type, ArrayType):
        element_type = _convert_data_type(data_type.element_type)
        return MockArrayType(element_type)
    elif isinstance(data_type, MapType):
        key_type = _convert_data_type(data_type.key_type)
        value_type = _convert_data_type(data_type.value_type)
        return MockMapType(key_type, value_type)
    elif isinstance(data_type, StructType):
        return _convert_struct_type(data_type)
    else:
        # Default to string for unknown types
        return StringType()


def _convert_simple_type(simple_type: SimpleType) -> MockDataType:
    """Convert SimpleType to appropriate MockDataType.

    Args:
        simple_type: SimpleType from spark-ddl-parser

    Returns:
        MockDataType instance
    """
    type_mapping = {
        "string": StringType,
        "integer": IntegerType,
        "long": LongType,
        "double": DoubleType,
        "float": FloatType,
        "boolean": BooleanType,
        "date": DateType,
        "timestamp": TimestampType,
        "binary": BinaryType,
        "short": ShortType,
        "byte": ByteType,
    }

    type_class = type_mapping.get(simple_type.type_name, StringType)
    return type_class()
