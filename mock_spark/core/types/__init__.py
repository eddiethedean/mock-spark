"""
Core type definitions for Mock Spark.

This module provides the foundational type definitions that are used
throughout the Mock Spark system for schema, data types, and metadata.
"""

from .schema import ISchema, IStructField, IStructType
from .data_types import IDataType, IStringType, IIntegerType, IBooleanType
from .metadata import IMetadata, ITableMetadata, IFieldMetadata

__all__ = [
    "ISchema",
    "IStructField", 
    "IStructType",
    "IDataType",
    "IStringType",
    "IIntegerType",
    "IBooleanType",
    "IMetadata",
    "ITableMetadata",
    "IFieldMetadata",
]
