"""
Backend factory for creating backend instances.

This module provides a centralized factory for creating backend instances,
enabling dependency injection and easier testing.
"""

from typing import Optional, Any
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
        **kwargs: Any
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
        **kwargs: Any
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
        else:
            raise ValueError(f"Unsupported export backend type: {backend_type}")

