"""
Polars backend implementation for Sparkless.

This module provides Polars-specific implementations for storage,
query execution, materialization, and export operations.

Components:
    - storage: Polars storage backend
    - materializer: Polars-based lazy operation materialization
    - expression_translator: Translate Column expressions to Polars
    - operation_executor: Execute DataFrame operations using Polars
    - export: DataFrame export to various formats using Polars

Example:
    >>> from sparkless.backend.polars import PolarsStorageManager
    >>> storage = PolarsStorageManager(db_path="sparkless_storage")
"""

from .storage import PolarsStorageManager, PolarsTable, PolarsSchema
from .materializer import PolarsMaterializer
from .export import PolarsExporter

__all__ = [
    "PolarsStorageManager",
    "PolarsTable",
    "PolarsSchema",
    "PolarsMaterializer",
    "PolarsExporter",
]
