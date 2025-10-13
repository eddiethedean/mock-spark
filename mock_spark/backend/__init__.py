"""
Backend module for Mock Spark.

This module provides backend implementations for storage, query execution,
and data materialization. It decouples backend-specific logic from the
core DataFrame and Session modules.

Architecture:
    - protocols.py: Protocol definitions for backend interfaces
    - factory.py: Factory for creating backend instances
    - duckdb/: DuckDB-specific backend implementation

Example:
    >>> from mock_spark.backend.factory import BackendFactory
    >>> storage = BackendFactory.create_storage_backend("duckdb")
    >>> materializer = BackendFactory.create_materializer("duckdb")
"""

from .protocols import (
    QueryExecutor,
    DataMaterializer,
    StorageBackend,
    ExportBackend,
)
from .factory import BackendFactory

__all__ = [
    "QueryExecutor",
    "DataMaterializer",
    "StorageBackend",
    "ExportBackend",
    "BackendFactory",
]
