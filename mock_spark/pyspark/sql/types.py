"""
PySpark SQL Types module - Drop-in replacement for pyspark.sql.types.

This module provides all PySpark data types by re-exporting from mock_spark.sql.types,
enabling seamless compatibility with PySpark code.

Example:
    >>> from pyspark.sql.types import StringType, IntegerType, StructType, StructField
    >>> schema = StructType([
    ...     StructField("name", StringType(), True),
    ...     StructField("age", IntegerType(), True)
    ... ])
"""

import sys
from types import ModuleType

# Register this module in sys.modules for proper import resolution
sys.modules["pyspark.sql.types"] = sys.modules[__name__]

# Re-export all data types from mock_spark.sql.types
from mock_spark.sql.types import (  # noqa: E402
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
