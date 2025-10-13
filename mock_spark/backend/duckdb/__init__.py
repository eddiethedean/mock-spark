"""
DuckDB Backend (Deprecated)

This module has been moved to mock_spark.storage.backends.duckdb

For backward compatibility, we re-export from the new location.
Please update your imports.
"""

import warnings

warnings.warn(
    "Importing from mock_spark.backend.duckdb is deprecated. "
    "Use mock_spark.storage.backends.duckdb instead.",
    DeprecationWarning,
    stacklevel=2,
)

# Re-export from new locations
from mock_spark.storage.backends.duckdb import (
    DuckDBStorageManager,
    DuckDBTable,
    DuckDBSchema,
)
from mock_spark.storage.execution.query_executor import SQLAlchemyMaterializer
from mock_spark.storage.execution.materializer import DuckDBMaterializer

__all__ = [
    "DuckDBStorageManager",
    "DuckDBTable",
    "DuckDBSchema",
    "SQLAlchemyMaterializer",
    "DuckDBMaterializer",
]
