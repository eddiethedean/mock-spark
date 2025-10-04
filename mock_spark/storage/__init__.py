"""
Storage module.

This module provides a unified storage system with multiple backends and serialization options.
"""

# Import interfaces
from .interfaces import IStorageManager, ITable, ISchema

# Import backends
from .backends.memory import MemoryStorageManager, MemoryTable, MemorySchema
from .backends.sqlite import SQLiteStorageManager, SQLiteTable, SQLiteSchema
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
