"""
Backend factory for creating backend instances.

This module provides a centralized factory for creating backend instances,
enabling dependency injection and easier testing.
"""

from typing import Optional, Any, List
from .protocols import StorageBackend, DataMaterializer, ExportBackend


class BackendFactory:
    """Factory for creating backend instances.

    This factory creates backend instances based on the requested type,
    allowing for easy swapping of implementations and testing with mocks.

    Example:
        >>> storage = BackendFactory.create_storage_backend("duckdb")
        >>> materializer = BackendFactory.create_materializer("duckdb")
    """

    @staticmethod
    def create_storage_backend(
        backend_type: str = "duckdb",
        db_path: Optional[str] = None,
        max_memory: str = "1GB",
        allow_disk_spillover: bool = False,
        **kwargs: Any,
    ) -> StorageBackend:
        """Create a storage backend instance.

        Args:
            backend_type: Type of backend ("duckdb", "memory", "file")
            db_path: Optional database file path
            max_memory: Maximum memory for DuckDB
            allow_disk_spillover: Whether to allow disk spillover
            **kwargs: Additional backend-specific arguments

        Returns:
            Storage backend instance

        Raises:
            ValueError: If backend_type is not supported
        """
        if backend_type == "duckdb":
            from .duckdb.storage import DuckDBStorageManager

            return DuckDBStorageManager(
                db_path=db_path,
                max_memory=max_memory,
                allow_disk_spillover=allow_disk_spillover,
            )
        elif backend_type == "memory":
            from mock_spark.storage.backends.memory import MemoryStorageManager

            return MemoryStorageManager()
        elif backend_type == "file":
            from mock_spark.storage.backends.file import FileStorageManager

            base_path = kwargs.get("base_path", "mock_spark_storage")
            return FileStorageManager(base_path)
        else:
            raise ValueError(f"Unsupported backend type: {backend_type}")

    @staticmethod
    def create_materializer(
        backend_type: str = "duckdb",
        max_memory: str = "1GB",
        allow_disk_spillover: bool = False,
        **kwargs: Any,
    ) -> DataMaterializer:
        """Create a data materializer instance.

        Args:
            backend_type: Type of materializer ("duckdb", "sqlalchemy")
            max_memory: Maximum memory for DuckDB
            allow_disk_spillover: Whether to allow disk spillover
            **kwargs: Additional materializer-specific arguments

        Returns:
            Data materializer instance

        Raises:
            ValueError: If backend_type is not supported
        """
        if backend_type == "duckdb":
            from .duckdb.materializer import DuckDBMaterializer

            return DuckDBMaterializer(
                max_memory=max_memory,
                allow_disk_spillover=allow_disk_spillover,
            )
        elif backend_type == "sqlalchemy":
            from .duckdb.query_executor import SQLAlchemyMaterializer

            engine_url = kwargs.get("engine_url", "duckdb:///:memory:")
            return SQLAlchemyMaterializer(engine_url=engine_url)
        elif backend_type == "memory":
            # For memory backend, use DuckDB materializer as fallback
            from .duckdb.materializer import DuckDBMaterializer

            return DuckDBMaterializer(
                max_memory=max_memory,
                allow_disk_spillover=allow_disk_spillover,
            )
        elif backend_type == "file":
            # For file backend, use DuckDB materializer as fallback
            from .duckdb.materializer import DuckDBMaterializer

            return DuckDBMaterializer(
                max_memory=max_memory,
                allow_disk_spillover=allow_disk_spillover,
            )
        else:
            raise ValueError(f"Unsupported materializer type: {backend_type}")

    @staticmethod
    def create_export_backend(backend_type: str = "duckdb") -> ExportBackend:
        """Create an export backend instance.

        Args:
            backend_type: Type of export backend ("duckdb")

        Returns:
            Export backend instance

        Raises:
            ValueError: If backend_type is not supported
        """
        if backend_type == "duckdb":
            from .duckdb.export import DuckDBExporter

            return DuckDBExporter()
        elif backend_type == "memory":
            # For memory backend, use DuckDB exporter as fallback
            from .duckdb.export import DuckDBExporter

            return DuckDBExporter()
        elif backend_type == "file":
            # For file backend, use DuckDB exporter as fallback
            from .duckdb.export import DuckDBExporter

            return DuckDBExporter()
        else:
            raise ValueError(f"Unsupported export backend type: {backend_type}")

    @staticmethod
    def get_backend_type(storage: StorageBackend) -> str:
        """Detect backend type from storage instance.

        Args:
            storage: Storage backend instance

        Returns:
            Backend type string ("duckdb", "memory", "file", etc.)

        Raises:
            ValueError: If backend type cannot be determined
        """
        # Use module path inspection to detect backend type
        module_name = type(storage).__module__

        if "duckdb" in module_name:
            return "duckdb"
        elif "memory" in module_name:
            return "memory"
        elif "file" in module_name:
            return "file"
        else:
            # Fallback: try to match class name
            class_name = type(storage).__name__.lower()
            if "duckdb" in class_name:
                return "duckdb"
            elif "memory" in class_name:
                return "memory"
            elif "file" in class_name:
                return "file"
            else:
                raise ValueError(f"Cannot determine backend type for {type(storage)}")

    @staticmethod
    def list_available_backends() -> List[str]:
        """List all available backend types.

        Returns:
            List of supported backend type strings
        """
        return ["duckdb", "memory", "file"]

    @staticmethod
    def validate_backend_type(backend_type: str) -> None:
        """Validate that a backend type is supported.

        Args:
            backend_type: Backend type to validate

        Raises:
            ValueError: If backend type is not supported
        """
        available_backends = BackendFactory.list_available_backends()
        if backend_type not in available_backends:
            raise ValueError(
                f"Unsupported backend type: {backend_type}. "
                f"Available backends: {', '.join(available_backends)}"
            )
