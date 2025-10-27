"""
Unit tests for backend factory.
"""

import pytest
from mock_spark.backend.factory import BackendFactory


@pytest.mark.unit
class TestBackendFactory:
    """Test BackendFactory operations."""

    def test_create_storage_backend_duckdb_default(self):
        """Test creating DuckDB storage backend with defaults."""
        backend = BackendFactory.create_storage_backend("duckdb")
        assert backend is not None
        from mock_spark.backend.duckdb.storage import DuckDBStorageManager

        assert isinstance(backend, DuckDBStorageManager)

    def test_create_storage_backend_duckdb_with_path(self):
        """Test creating DuckDB backend with custom path."""
        backend = BackendFactory.create_storage_backend("duckdb", db_path="test.db")
        assert backend is not None

    def test_create_storage_backend_duckdb_with_memory(self):
        """Test creating DuckDB backend with memory limit."""
        backend = BackendFactory.create_storage_backend("duckdb", max_memory="512MB")
        assert backend is not None

    def test_create_storage_backend_memory(self):
        """Test creating memory storage backend."""
        backend = BackendFactory.create_storage_backend("memory")
        assert backend is not None
        from mock_spark.storage.backends.memory import MemoryStorageManager

        assert isinstance(backend, MemoryStorageManager)

    def test_create_storage_backend_file(self):
        """Test creating file storage backend."""
        backend = BackendFactory.create_storage_backend(
            "file", base_path="test_storage"
        )
        assert backend is not None
        from mock_spark.storage.backends.file import FileStorageManager

        assert isinstance(backend, FileStorageManager)

    def test_create_storage_backend_unsupported(self):
        """Test creating unsupported backend raises ValueError."""
        with pytest.raises(ValueError, match="Unsupported backend type"):
            BackendFactory.create_storage_backend("unsupported_type")

    def test_create_materializer_duckdb_default(self):
        """Test creating DuckDB materializer with defaults."""
        materializer = BackendFactory.create_materializer("duckdb")
        assert materializer is not None
        from mock_spark.backend.duckdb.materializer import DuckDBMaterializer

        assert isinstance(materializer, DuckDBMaterializer)

    def test_create_materializer_duckdb_with_memory(self):
        """Test creating DuckDB materializer with memory limit."""
        materializer = BackendFactory.create_materializer("duckdb", max_memory="512MB")
        assert materializer is not None

    def test_create_materializer_sqlalchemy(self):
        """Test creating SQLAlchemy materializer."""
        materializer = BackendFactory.create_materializer(
            "sqlalchemy", engine_url="duckdb:///:memory:"
        )
        assert materializer is not None
        from mock_spark.backend.duckdb.query_executor import SQLAlchemyMaterializer

        assert isinstance(materializer, SQLAlchemyMaterializer)

    def test_create_materializer_unsupported(self):
        """Test creating unsupported materializer raises ValueError."""
        with pytest.raises(ValueError, match="Unsupported materializer type"):
            BackendFactory.create_materializer("unsupported_type")

    def test_create_storage_backend_file_custom_path(self):
        """Test creating file storage with custom base path."""
        backend = BackendFactory.create_storage_backend("file", base_path="custom_path")
        assert backend is not None

    def test_create_export_backend_duckdb(self):
        """Test creating export backend."""
        export_backend = BackendFactory.create_export_backend("duckdb")
        assert export_backend is not None
        from mock_spark.backend.duckdb.export import DuckDBExporter

        assert isinstance(export_backend, DuckDBExporter)

    def test_create_export_backend_unsupported(self):
        """Test creating unsupported export backend raises ValueError."""
        with pytest.raises(ValueError, match="Unsupported export backend type"):
            BackendFactory.create_export_backend("unsupported_type")

    def test_create_export_backend_memory(self):
        """Test creating memory export backend."""
        export_backend = BackendFactory.create_export_backend("memory")
        assert export_backend is not None
        from mock_spark.backend.duckdb.export import DuckDBExporter

        assert isinstance(export_backend, DuckDBExporter)

    def test_create_export_backend_file(self):
        """Test creating file export backend."""
        export_backend = BackendFactory.create_export_backend("file")
        assert export_backend is not None
        from mock_spark.backend.duckdb.export import DuckDBExporter

        assert isinstance(export_backend, DuckDBExporter)

    def test_storage_backend_kwargs(self):
        """Test passing kwargs to storage backend."""
        backend = BackendFactory.create_storage_backend("duckdb", test_param="value")
        assert backend is not None

    def test_materializer_kwargs(self):
        """Test passing kwargs to materializer."""
        materializer = BackendFactory.create_materializer("duckdb", test_param="value")
        assert materializer is not None

    def test_list_available_backends(self):
        """Test list_available_backends."""
        backends = BackendFactory.list_available_backends()
        assert "duckdb" in backends
        assert "memory" in backends
        assert "file" in backends

    def test_validate_backend_type_valid(self):
        """Test validate_backend_type with valid backend."""
        BackendFactory.validate_backend_type("duckdb")  # Should not raise

    def test_validate_backend_type_invalid(self):
        """Test validate_backend_type with invalid backend."""
        with pytest.raises(ValueError, match="Unsupported backend type"):
            BackendFactory.validate_backend_type("invalid")

    def test_get_backend_type_duckdb(self):
        """Test get_backend_type detects DuckDB."""
        backend = BackendFactory.create_storage_backend("duckdb")
        backend_type = BackendFactory.get_backend_type(backend)
        assert backend_type == "duckdb"

    def test_get_backend_type_memory(self):
        """Test get_backend_type detects memory."""
        backend = BackendFactory.create_storage_backend("memory")
        backend_type = BackendFactory.get_backend_type(backend)
        assert backend_type == "memory"

    def test_get_backend_type_file(self):
        """Test get_backend_type detects file."""
        backend = BackendFactory.create_storage_backend("file", base_path="test")
        backend_type = BackendFactory.get_backend_type(backend)
        assert backend_type == "file"

    def test_storage_backend_with_disk_spillover(self):
        """Test creating storage backend with disk spillover."""
        backend = BackendFactory.create_storage_backend(
            "duckdb", allow_disk_spillover=True
        )
        assert backend is not None

    def test_materializer_with_disk_spillover(self):
        """Test creating materializer with disk spillover."""
        materializer = BackendFactory.create_materializer(
            "duckdb", allow_disk_spillover=True
        )
        assert materializer is not None
