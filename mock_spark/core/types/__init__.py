"""
Core type definitions for Mock Spark.

This module provides the foundational type definitions and interfaces that are used
throughout the Mock Spark system for schema management, data types, and metadata handling.
These interfaces ensure consistency and type safety across all Mock Spark components.

Key Features:
    - Schema interfaces (ISchema, IStructField, IStructType)
    - Data type interfaces (IDataType, IStringType, IIntegerType, IBooleanType)
    - Metadata interfaces (IMetadata, ITableMetadata, IFieldMetadata)
    - Type-safe operations and validation
    - Extensible type system for custom data types

Example:
    >>> from mock_spark.core.types import ISchema, IStructField, IDataType
    >>> # These interfaces define the contract for schema and data type implementations
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
