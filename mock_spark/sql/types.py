"""
Mock Spark SQL Types module - PySpark-compatible types interface.

This module provides all PySpark data types, mirroring pyspark.sql.types.

Example:
    >>> from mock_spark.sql.types import StringType, IntegerType, StructType, StructField
    >>> schema = StructType([
    ...     StructField("name", StringType(), True),
    ...     StructField("age", IntegerType(), True)
    ... ])
"""

# Re-export all data types from spark_types
from ..spark_types import (  # noqa: E402
    DataType,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    BooleanType,
    DateType,
    TimestampType,
    DecimalType,
    ArrayType,
    MapType,
    BinaryType,
    NullType,
    FloatType,
    ShortType,
    ByteType,
    StructType,
    StructField,
    Row,
)

__all__ = [
    "DataType",
    "StringType",
    "IntegerType",
    "LongType",
    "DoubleType",
    "BooleanType",
    "DateType",
    "TimestampType",
    "DecimalType",
    "ArrayType",
    "MapType",
    "BinaryType",
    "NullType",
    "FloatType",
    "ShortType",
    "ByteType",
    "StructType",
    "StructField",
    "Row",
]
