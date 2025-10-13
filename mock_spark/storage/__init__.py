"""
Storage module for Mock Spark.

This module provides a comprehensive storage system with DuckDB as the primary
persistent storage backend and in-memory storage for testing. Supports
file-based storage and various serialization formats.

Key Features:
    - DuckDB as primary persistent storage backend
    - In-memory storage for testing
    - File-based storage for data export/import
    - Flexible serialization (JSON, CSV, Parquet)
    - Unified storage interface for consistency
    - Transaction support and data integrity
    - Schema management and validation
    - Table and database operations
    - Storage manager factory for easy backend switching

Example:
    >>> from mock_spark.storage import DuckDBStorageManager
    >>> from mock_spark.spark_types import MockStructType, MockStructField, StringType, IntegerType
    >>> storage = DuckDBStorageManager()
    >>> storage.create_schema("test_db")
    >>> schema = MockStructType([
    ...     MockStructField("name", StringType()),
    ...     MockStructField("age", IntegerType())
    ... ])
    >>> storage.create_table("test_db", "users", schema)
    >>> storage.insert_data("test_db", "users", [{"name": "Alice", "age": 25}])
"""

# Import interfaces
from .interfaces import IStorageManager, ITable, ISchema

# Import backends
from .backends.memory import MemoryStorageManager, MemoryTable, MemorySchema
# Import DuckDB from new backend location, re-export for backward compatibility
from mock_spark.backend.duckdb import DuckDBStorageManager, DuckDBTable, DuckDBSchema
from .models import (
    MockTableMetadata,
    MockColumnDefinition,
    StorageMode,
    DuckDBTableModel,
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
    # DuckDB backend
    "DuckDBStorageManager",
    "DuckDBTable",
    "DuckDBSchema",
    # Storage models (dataclasses)
    "MockTableMetadata",
    "MockColumnDefinition",
    "StorageMode",
    "DuckDBTableModel",
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
