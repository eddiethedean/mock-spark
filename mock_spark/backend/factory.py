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
        backend_type: str = "polars",
        db_path: Optional[str] = None,
        max_memory: str = "1GB",
        allow_disk_spillover: bool = False,
        **kwargs: Any,
    ) -> StorageBackend:
        """Create a storage backend instance.

        Args:
            backend_type: Type of backend ("polars", "duckdb", "memory", "file")
            db_path: Optional database file path
            max_memory: Maximum memory for DuckDB (ignored for Polars)
            allow_disk_spillover: Whether to allow disk spillover (ignored for Polars)
            **kwargs: Additional backend-specific arguments

        Returns:
            Storage backend instance

        Raises:
            ValueError: If backend_type is not supported
        """
        if backend_type == "polars":
            from .polars.storage import PolarsStorageManager

            return PolarsStorageManager(db_path=db_path)
        elif backend_type == "duckdb":
            try:
                from .duckdb.storage import DuckDBStorageManager
            except ImportError as e:
                raise ImportError(
                    "DuckDB backend requires 'duckdb' package. "
                    "Install with: pip install duckdb"
                ) from e

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
        backend_type: str = "polars",
        max_memory: str = "1GB",
        allow_disk_spillover: bool = False,
        **kwargs: Any,
    ) -> DataMaterializer:
        """Create a data materializer instance.

        Args:
            backend_type: Type of materializer ("polars", "duckdb", "sqlalchemy")
            max_memory: Maximum memory for DuckDB (ignored for Polars)
            allow_disk_spillover: Whether to allow disk spillover (ignored for Polars)
            **kwargs: Additional materializer-specific arguments

        Returns:
            Data materializer instance

        Raises:
            ValueError: If backend_type is not supported
        """
        if backend_type == "polars":
            from .polars.materializer import PolarsMaterializer

            return PolarsMaterializer()
        elif backend_type == "duckdb":
            try:
                from .duckdb.materializer import DuckDBMaterializer
            except ImportError as e:
                raise ImportError(
                    "DuckDB backend requires 'duckdb' package. "
                    "Install with: pip install duckdb"
                ) from e

            return DuckDBMaterializer(
                max_memory=max_memory,
                allow_disk_spillover=allow_disk_spillover,
            )
        elif backend_type == "sqlalchemy":
            try:
                from .duckdb.query_executor import SQLAlchemyMaterializer
            except ImportError as e:
                raise ImportError(
                    "SQLAlchemy materializer with DuckDB requires 'duckdb' package. "
                    "Install with: pip install duckdb"
                ) from e

            engine_url = kwargs.get("engine_url", "duckdb:///:memory:")
            return SQLAlchemyMaterializer(engine_url=engine_url)
        elif backend_type == "memory":
            # For memory backend, use Polars materializer
            from .polars.materializer import PolarsMaterializer

            return PolarsMaterializer()
        elif backend_type == "file":
            # For file backend, use Polars materializer
            from .polars.materializer import PolarsMaterializer

            return PolarsMaterializer()
        else:
            raise ValueError(f"Unsupported materializer type: {backend_type}")

    @staticmethod
    def create_export_backend(backend_type: str = "polars") -> ExportBackend:
        """Create an export backend instance.

        Args:
            backend_type: Type of export backend ("polars", "duckdb")

        Returns:
            Export backend instance

        Raises:
            ValueError: If backend_type is not supported
        """
        if backend_type == "polars":
            from .polars.export import PolarsExporter

            return PolarsExporter()
        elif backend_type == "duckdb":
            try:
                from .duckdb.export import DuckDBExporter
            except ImportError as e:
                raise ImportError(
                    "DuckDB export backend requires 'duckdb' package. "
                    "Install with: pip install duckdb"
                ) from e

            return DuckDBExporter()
        elif backend_type == "memory":
            # For memory backend, use Polars exporter
            from .polars.export import PolarsExporter

            return PolarsExporter()
        elif backend_type == "file":
            # For file backend, use Polars exporter
            from .polars.export import PolarsExporter

            return PolarsExporter()
        else:
            raise ValueError(f"Unsupported export backend type: {backend_type}")

    @staticmethod
    def get_backend_type(storage: StorageBackend) -> str:
        """Detect backend type from storage instance.

        Args:
            storage: Storage backend instance

        Returns:
            Backend type string ("polars", "duckdb", "memory", "file", etc.)

        Raises:
            ValueError: If backend type cannot be determined
        """
        # Use module path inspection to detect backend type
        module_name = type(storage).__module__

        if "polars" in module_name:
            return "polars"
        elif "duckdb" in module_name:
            return "duckdb"
        elif "memory" in module_name:
            return "memory"
        elif "file" in module_name:
            return "file"
        else:
            # Fallback: try to match class name
            class_name = type(storage).__name__.lower()
            if "polars" in class_name:
                return "polars"
            elif "duckdb" in class_name:
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
        return ["polars", "duckdb", "memory", "file"]

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
