"""
Storage module for Mock Spark.

This module provides a comprehensive storage system with multiple backends and
serialization options for persisting and retrieving DataFrame data. Supports
in-memory storage, SQLite databases, file-based storage, and various serialization formats.

Key Features:
    - Multiple storage backends (Memory, SQLite, File)
    - Flexible serialization (JSON, CSV, Parquet)
    - Unified storage interface for consistency
    - Transaction support and data integrity
    - Schema management and validation
    - Table and database operations
    - Storage manager factory for easy backend switching

Example:
    >>> from mock_spark.storage import MemoryStorageManager
    >>> from mock_spark.spark_types import MockStructType, MockStructField, StringType, IntegerType
    >>> storage = MemoryStorageManager()
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
from .backends.sqlite import SQLiteStorageManager, SQLiteTable, SQLiteSchema
from .backends.duckdb import DuckDBStorageManager, DuckDBTable, DuckDBSchema
from .models import (
    MockTableMetadata,
    MockColumnDefinition,
    StorageMode,
    DuckDBTableModel,
    StorageOperationResult,
    QueryResult
)
from .backends.file import FileStorageManager, FileTable, FileSchema

# Import serialization
from .serialization.json import JSONSerializer
from .serialization.csv import CSVSerializer

# Import manager
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
    # SQLite backend
    "SQLiteStorageManager",
    "SQLiteTable",
    "SQLiteSchema",
    # DuckDB backend
    "DuckDBStorageManager",
    "DuckDBTable",
    "DuckDBSchema",
    # SQLModel models
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
    # Manager
    "StorageManagerFactory",
    "UnifiedStorageManager",
]
