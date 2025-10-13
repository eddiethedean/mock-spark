"""
DuckDB backend implementation for Mock Spark.

This module provides DuckDB-specific implementations for storage,
query execution, materialization, and export operations.

Components:
    - storage: DuckDB storage backend
    - query_executor: SQLAlchemy-based query execution
    - materializer: DuckDB-based lazy operation materialization
    - export: DataFrame export to DuckDB

Example:
    >>> from mock_spark.backend.duckdb import DuckDBStorageManager
    >>> storage = DuckDBStorageManager(max_memory="1GB")
"""

from .storage import DuckDBStorageManager, DuckDBTable, DuckDBSchema
from .materializer import DuckDBMaterializer
from .query_executor import SQLAlchemyMaterializer
from .export import DuckDBExporter

__all__ = [
    "DuckDBStorageManager",
    "DuckDBTable",
    "DuckDBSchema",
    "DuckDBMaterializer",
    "SQLAlchemyMaterializer",
    "DuckDBExporter",
]

