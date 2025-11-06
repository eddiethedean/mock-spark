"""
Storage module for Mock Spark.

This module provides a comprehensive storage system with Polars as the primary
persistent storage backend (v3.0.0+) and in-memory storage for testing. Supports
file-based storage and various serialization formats.

Key Features:
    - Polars as primary persistent storage backend (v3.0.0+, default)
    - In-memory storage for testing
    - File-based storage for data export/import
    - Flexible serialization (JSON, CSV, Parquet)
    - Unified storage interface for consistency
    - Transaction support and data integrity
    - Schema management and validation
    - Table and database operations
    - Storage manager factory for easy backend switching

Example:
    >>> from mock_spark.storage import PolarsStorageManager
    >>> from mock_spark.spark_types import StructType, StructField, StringType, IntegerType
    >>> storage = PolarsStorageManager()
    >>> storage.create_schema("test_db")
    >>> schema = StructType([
    ...     StructField("name", StringType()),
    ...     StructField("age", IntegerType())
    ... ])
    >>> storage.create_table("test_db", "users", schema)
    >>> storage.insert_data("test_db", "users", [{"name": "Alice", "age": 25}])
"""

# Import interfaces from canonical location
from ..core.interfaces.storage import IStorageManager, ITable
from ..core.types.schema import ISchema

# Import backends
from .backends.memory import MemoryStorageManager, MemoryTable, MemorySchema

# Import Polars from backend location (default in v3.0.0+)
from mock_spark.backend.polars import PolarsStorageManager, PolarsTable, PolarsSchema
from .models import (
    MockTableMetadata,
    ColumnDefinition,
    StorageMode,
    StorageOperationResult,
    QueryResult,
)
from .backends.file import FileStorageManager, FileTable, FileSchema

# Import serialization
from .serialization.json import JSONSerializer
from .serialization.csv import CSVSerializer

# Import managers
from .manager import StorageManagerFactory, UnifiedStorageManager

__all__ = [
    # Interfaces
    "IStorageManager",
    "ITable",
    "ISchema",
    # Memory backend
    "MemoryStorageManager",
    "MemoryTable",
    "MemorySchema",
    # Polars backend (default in v3.0.0+)
    "PolarsStorageManager",
    "PolarsTable",
    "PolarsSchema",
    # Storage models (dataclasses)
    "MockTableMetadata",
    "ColumnDefinition",
    "StorageMode",
    "StorageOperationResult",
    "QueryResult",
    # File backend
    "FileStorageManager",
    "FileTable",
    "FileSchema",
    # Serialization
    "JSONSerializer",
    "CSVSerializer",
    # Storage managers
    "StorageManagerFactory",
    "UnifiedStorageManager",
]
