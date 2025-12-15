"""
Unit tests for Catalog storage backend accessor.

Tests the get_storage_backend() public accessor method that provides
proper encapsulation for accessing the storage backend.
"""

import pytest
from sparkless.session.catalog import Catalog
from sparkless.backend.factory import BackendFactory


@pytest.mark.unit
class TestCatalogStorageAccessor:
    """Test Catalog.get_storage_backend() method."""

    def test_get_storage_backend_returns_storage(self):
        """Test that get_storage_backend() returns the storage instance."""
        storage = BackendFactory.create_storage_backend("polars")
        catalog = Catalog(storage)
        
        retrieved_storage = catalog.get_storage_backend()
        assert retrieved_storage is storage

    def test_get_storage_backend_same_instance(self):
        """Test that get_storage_backend() returns the same instance."""
        storage = BackendFactory.create_storage_backend("memory")
        catalog = Catalog(storage)
        
        storage1 = catalog.get_storage_backend()
        storage2 = catalog.get_storage_backend()
        
        assert storage1 is storage2
        assert storage1 is storage

    def test_get_storage_backend_works_with_different_backends(self):
        """Test that get_storage_backend() works with different backend types."""
        # Test with Polars backend
        polars_storage = BackendFactory.create_storage_backend("polars")
        polars_catalog = Catalog(polars_storage)
        assert polars_catalog.get_storage_backend() is polars_storage
        
        # Test with Memory backend
        memory_storage = BackendFactory.create_storage_backend("memory")
        memory_catalog = Catalog(memory_storage)
        assert memory_catalog.get_storage_backend() is memory_storage

    def test_get_storage_backend_allows_operations(self):
        """Test that retrieved storage backend supports operations."""
        storage = BackendFactory.create_storage_backend("polars")
        catalog = Catalog(storage)
        
        retrieved_storage = catalog.get_storage_backend()
        
        # Should be able to use storage operations
        retrieved_storage.create_schema("test_schema")
        assert retrieved_storage.schema_exists("test_schema") is True
        
        # Cleanup
        if hasattr(retrieved_storage, "drop_schema"):
            retrieved_storage.drop_schema("test_schema")

    def test_get_storage_backend_encapsulation(self):
        """Test that get_storage_backend() provides proper encapsulation."""
        storage = BackendFactory.create_storage_backend("polars")
        catalog = Catalog(storage)
        
        # Public accessor should work
        retrieved_storage = catalog.get_storage_backend()
        assert retrieved_storage is not None
        
        # Direct access to _storage should still work (backward compatibility)
        # but get_storage_backend() is the preferred method
        assert catalog._storage is storage

    def test_get_storage_backend_with_sql_executor(self):
        """Test that SQL executor can use get_storage_backend() properly."""
        from sparkless import SparkSession
        
        spark = SparkSession("test")
        
        # SQL executor should be able to access storage through catalog
        storage = spark.catalog.get_storage_backend()
        assert storage is not None
        
        # Should be able to use it for operations
        storage.create_schema("test_db")
        assert storage.schema_exists("test_db") is True
        
        spark.stop()

