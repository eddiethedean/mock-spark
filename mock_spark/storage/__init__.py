"""
Storage module for Mock Spark.

This module provides a comprehensive storage system with Polars as the primary
persistent storage backend (v3.0.0+) and in-memory storage for testing. Supports
file-based storage and various serialization formats.

Key Features:
    - Polars as primary persistent storage backend (v3.0.0+, default)
    - DuckDB backend available for backward compatibility (legacy)
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
    >>> from mock_spark.spark_types import MockStructType, MockStructField, StringType, IntegerType
    >>> storage = PolarsStorageManager()
    >>> storage.create_schema("test_db")
    >>> schema = MockStructType([
    ...     MockStructField("name", StringType()),
    ...     MockStructField("age", IntegerType())
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

# Import DuckDB from backend location, re-export for backward compatibility
# Note: DuckDB is optional - install with: pip install duckdb
try:
    from mock_spark.backend.duckdb import DuckDBStorageManager, DuckDBTable, DuckDBSchema
except ImportError:
    # DuckDB not installed - set to None for optional usage
    DuckDBStorageManager = None  # type: ignore
    DuckDBTable = None  # type: ignore
    DuckDBSchema = None  # type: ignore
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
        # Polars backend (default in v3.0.0+)
        "PolarsStorageManager",
        "PolarsTable",
        "PolarsSchema",
        # DuckDB backend (legacy, optional - requires: pip install duckdb)
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
