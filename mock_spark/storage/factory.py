"""
Unified Storage and Execution Factory

This module consolidates backend/factory.py and storage/manager.py into a single
factory following the SOLID Single Responsibility Principle. All storage backends,
execution engines, and export utilities are created here.

Replaces:
- mock_spark.backend.factory.BackendFactory
- mock_spark.storage.manager.StorageManagerFactory
"""

from typing import Optional, Any
from .interfaces import IStorageManager


class StorageFactory:
    """Unified factory for creating storage backends, execution engines, and exporters.

    This factory consolidates all backend creation logic, replacing the previous
    split between BackendFactory and StorageManagerFactory.

    Example:
        >>> storage = StorageFactory.create_storage("duckdb")
        >>> materializer = StorageFactory.create_materializer("duckdb")
        >>> exporter = StorageFactory.create_exporter("duckdb")
    """

    @staticmethod
    def create_storage(
        backend: str = "duckdb",
        db_path: Optional[str] = None,
        max_memory: str = "1GB",
        allow_disk_spillover: bool = False,
        **kwargs: Any,
    ) -> IStorageManager:
        """Create a storage backend instance.

        Args:
            backend: Type of backend ("duckdb", "memory", "file")
            db_path: Optional database file path
            max_memory: Maximum memory for DuckDB
            allow_disk_spillover: Whether to allow disk spillover
            **kwargs: Additional backend-specific arguments

        Returns:
            Storage backend instance implementing IStorageManager

        Raises:
            ValueError: If backend type is not supported
        """
        if backend == "duckdb":
            from .backends.duckdb import DuckDBStorageManager

            return DuckDBStorageManager(
                db_path=db_path, max_memory=max_memory, allow_disk_spillover=allow_disk_spillover
            )
        elif backend == "memory":
            from .backends.memory import MemoryStorageManager

            return MemoryStorageManager()
        elif backend == "file":
            from .backends.file import FileStorageManager

            base_path = kwargs.get("base_path", "mock_spark_storage")
            return FileStorageManager(base_path)
        else:
            raise ValueError(f"Unsupported backend type: {backend}")

    @staticmethod
    def create_materializer(
        backend: str = "duckdb",
        max_memory: str = "1GB",
        allow_disk_spillover: bool = False,
        **kwargs: Any,
    ) -> Any:
        """Create a data materializer for lazy evaluation.

        Args:
            backend: Type of materializer ("duckdb", "sqlalchemy")
            max_memory: Maximum memory for DuckDB
            allow_disk_spillover: Whether to allow disk spillover
            **kwargs: Additional materializer-specific arguments

        Returns:
            Data materializer instance

        Raises:
            ValueError: If backend type is not supported
        """
        if backend == "duckdb":
            from .execution.materializer import DuckDBMaterializer

            return DuckDBMaterializer(
                max_memory=max_memory, allow_disk_spillover=allow_disk_spillover
            )
        elif backend == "sqlalchemy":
            from .execution.query_executor import SQLAlchemyMaterializer

            engine_url = kwargs.get("engine_url", "duckdb:///:memory:")
            return SQLAlchemyMaterializer(engine_url=engine_url)
        else:
            raise ValueError(f"Unsupported materializer type: {backend}")

    @staticmethod
    def create_exporter(backend: str = "duckdb") -> Any:
        """Create an export backend instance.

        Args:
            backend: Type of export backend ("duckdb")

        Returns:
            Export backend instance

        Raises:
            ValueError: If backend type is not supported
        """
        if backend == "duckdb":
            from .export.duckdb import DuckDBExporter

            return DuckDBExporter()
        else:
            raise ValueError(f"Unsupported export backend type: {backend}")

    # Convenience methods (from old StorageManagerFactory)

    @staticmethod
    def create_memory_manager() -> IStorageManager:
        """Create a memory storage manager.

        Returns:
            Memory storage manager instance.
        """
        return StorageFactory.create_storage("memory")

    @staticmethod
    def create_file_manager(base_path: str = "mock_spark_storage") -> IStorageManager:
        """Create a file storage manager.

        Args:
            base_path: Base path for storage files.

        Returns:
            File storage manager instance.
        """
        return StorageFactory.create_storage("file", base_path=base_path)

    @staticmethod
    def create_duckdb_manager(db_path: Optional[str] = None) -> IStorageManager:
        """Create a DuckDB storage manager.

        Args:
            db_path: Optional path to DuckDB database file. If None, uses in-memory storage.

        Returns:
            DuckDB storage manager instance.
        """
        return StorageFactory.create_storage("duckdb", db_path=db_path)


# Alias for backward compatibility
BackendFactory = StorageFactory
