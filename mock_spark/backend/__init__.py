"""
Backend module (Deprecated)

This module has been consolidated into mock_spark.storage/ to eliminate
architectural redundancy and improve code organization.

For backward compatibility, we re-export key components from their new locations.
Please update your imports to use mock_spark.storage directly.

Deprecated imports:
    from mock_spark.backend.duckdb import DuckDBStorageManager  # Use storage.backends.duckdb
    from mock_spark.backend.factory import BackendFactory       # Use storage.factory.StorageFactory

New imports:
    from mock_spark.storage.backends.duckdb import DuckDBStorageManager
    from mock_spark.storage.factory import StorageFactory
    # Or simply:
    from mock_spark.storage import DuckDBStorageManager, StorageFactory
"""

import warnings

# Emit deprecation warning when this module is imported
warnings.warn(
    "Importing from mock_spark.backend is deprecated and will be removed in a future version. "
    "Please use mock_spark.storage instead. "
    "See migration guide in documentation.",
    DeprecationWarning,
    stacklevel=2,
)

# Re-export from new locations for backward compatibility
from mock_spark.storage.factory import StorageFactory as BackendFactory

__all__ = ["BackendFactory"]
