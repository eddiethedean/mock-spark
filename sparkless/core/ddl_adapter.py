"""
Adapter to convert spark-ddl-parser output to StructType.

This module provides an adapter layer between the standalone spark-ddl-parser
package and sparkless's internal type system.
"""

from spark_ddl_parser import parse_ddl_schema as parse_ddl
from spark_ddl_parser.types import (
    StructType as DDLStructType,
    StructField as DDLStructField,
    SimpleType,
    DecimalType,
    ArrayType,
    MapType,
    DataType as DDLDataType,
)

from ..spark_types import (
    StructType,
    StructField,
    DataType,
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


def parse_ddl_schema(ddl_string: str) -> StructType:
    """Parse DDL and convert to StructType.

    Args:
        ddl_string: DDL schema string (e.g., "id long, name string")

    Returns:
        StructType with parsed fields

    Raises:
        ValueError: If DDL string is invalid
    """
    parsed = parse_ddl(ddl_string)
    return _convert_struct_type(parsed)


def _convert_struct_type(struct: DDLStructType) -> StructType:
    """Convert StructType to StructType.

    Args:
        struct: Parsed StructType from spark-ddl-parser

    Returns:
        StructType
    """
    fields = [_convert_field(f) for f in struct.fields]
    return StructType(fields)


def _convert_field(field: DDLStructField) -> StructField:
    """Convert StructField to StructField.

    Args:
        field: Parsed StructField from spark-ddl-parser

    Returns:
        StructField
    """
    data_type = _convert_data_type(field.data_type)
    return StructField(name=field.name, dataType=data_type, nullable=field.nullable)


def _convert_data_type(data_type: DDLDataType) -> DataType:
    """Convert DataType to DataType.

    Args:
        data_type: Parsed DataType from spark-ddl-parser

    Returns:
        DataType
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
    elif isinstance(data_type, DDLStructType):
        return _convert_struct_type(data_type)
    else:
        # Default to string for unknown types
        return StringType()


def _convert_simple_type(simple_type: SimpleType) -> DataType:
    """Convert SimpleType to appropriate DataType.

    Args:
        simple_type: SimpleType from spark-ddl-parser

    Returns:
        DataType instance
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
