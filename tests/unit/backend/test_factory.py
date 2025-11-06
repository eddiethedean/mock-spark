"""
Unit tests for backend factory.
"""

import os
import pytest
import tempfile
import contextlib

# Skip if factory module doesn't exist
try:
    from mock_spark.backend.factory import BackendFactory
except ImportError:
    pytest.skip(
        "backend.factory module not available in this version", allow_module_level=True
    )


@pytest.mark.unit
class TestBackendFactory:
    """Test BackendFactory operations."""

    def test_create_storage_backend_polars_default(self):
        """Test creating Polars storage backend with defaults."""
        backend = BackendFactory.create_storage_backend("polars")
        assert backend is not None
        from mock_spark.backend.polars.storage import PolarsStorageManager

        assert isinstance(backend, PolarsStorageManager)

    def test_create_storage_backend_polars_with_path(self):
        """Test creating Polars backend with custom path."""
        # Use a temporary directory to avoid leaving test files in the repo
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = os.path.join(tmp_dir, "test_storage")
            backend = BackendFactory.create_storage_backend("polars", db_path=db_path)
            assert backend is not None
            # Clean up the backend to close connections
            with contextlib.suppress(AttributeError):
                backend.close()

    def test_create_storage_backend_polars_with_memory(self):
        """Test creating Polars backend with memory limit (ignored but accepted for compatibility)."""
        backend = BackendFactory.create_storage_backend("polars", max_memory="512MB")
        assert backend is not None

    def test_create_storage_backend_memory(self):
        """Test creating memory storage backend."""
        backend = BackendFactory.create_storage_backend("memory")
        assert backend is not None
        from mock_spark.storage.backends.memory import MemoryStorageManager

        assert isinstance(backend, MemoryStorageManager)

    def test_create_storage_backend_file(self):
        """Test creating file storage backend."""
        # Use a temporary directory to avoid leaving test files in the repo
        with tempfile.TemporaryDirectory() as tmp_dir:
            base_path = os.path.join(tmp_dir, "test_storage")
            backend = BackendFactory.create_storage_backend("file", base_path=base_path)
            assert backend is not None
            from mock_spark.storage.backends.file import FileStorageManager

            assert isinstance(backend, FileStorageManager)
            # Explicitly clean up the backend
            with contextlib.suppress(AttributeError):
                backend.close()
            # Verify the directory was created in the temp location, not repo root
            assert os.path.exists(base_path)
            assert not os.path.exists("test_storage")  # Should not exist in repo root

    def test_create_storage_backend_unsupported(self):
        """Test creating unsupported backend raises ValueError."""
        with pytest.raises(ValueError, match="Unsupported backend type"):
            BackendFactory.create_storage_backend("unsupported_type")

    def test_create_materializer_polars_default(self):
        """Test creating Polars materializer with defaults."""
        materializer = BackendFactory.create_materializer("polars")
        assert materializer is not None
        from mock_spark.backend.polars.materializer import PolarsMaterializer

        assert isinstance(materializer, PolarsMaterializer)

    def test_create_materializer_polars_with_memory(self):
        """Test creating Polars materializer with memory limit (ignored but accepted for compatibility)."""
        materializer = BackendFactory.create_materializer("polars", max_memory="512MB")
        assert materializer is not None

    def test_create_materializer_unsupported(self):
        """Test creating unsupported materializer raises ValueError."""
        with pytest.raises(ValueError, match="Unsupported materializer type"):
            BackendFactory.create_materializer("unsupported_type")

    def test_create_storage_backend_file_custom_path(self):
        """Test creating file storage with custom base path."""
        # Use a temporary directory to avoid leaving test files in the repo
        with tempfile.TemporaryDirectory() as tmp_dir:
            base_path = os.path.join(tmp_dir, "custom_path")
            backend = BackendFactory.create_storage_backend("file", base_path=base_path)
            assert backend is not None
            # Explicitly clean up the backend
            with contextlib.suppress(AttributeError):
                backend.close()
            # Verify the directory was created in the temp location, not repo root
            assert os.path.exists(base_path)
            assert not os.path.exists("custom_path")  # Should not exist in repo root

    def test_create_export_backend_polars(self):
        """Test creating Polars export backend."""
        export_backend = BackendFactory.create_export_backend("polars")
        assert export_backend is not None
        from mock_spark.backend.polars.export import PolarsExporter

        assert isinstance(export_backend, PolarsExporter)

    def test_create_export_backend_unsupported(self):
        """Test creating unsupported export backend raises ValueError."""
        with pytest.raises(ValueError, match="Unsupported export backend type"):
            BackendFactory.create_export_backend("unsupported_type")

    def test_create_export_backend_memory(self):
        """Test creating memory export backend uses Polars."""
        export_backend = BackendFactory.create_export_backend("memory")
        assert export_backend is not None
        from mock_spark.backend.polars.export import PolarsExporter

        assert isinstance(export_backend, PolarsExporter)

    def test_create_export_backend_file(self):
        """Test creating file export backend uses Polars."""
        export_backend = BackendFactory.create_export_backend("file")
        assert export_backend is not None
        from mock_spark.backend.polars.export import PolarsExporter

        assert isinstance(export_backend, PolarsExporter)

    def test_storage_backend_kwargs(self):
        """Test passing kwargs to storage backend."""
        backend = BackendFactory.create_storage_backend("polars", test_param="value")
        assert backend is not None

    def test_materializer_kwargs(self):
        """Test passing kwargs to materializer."""
        materializer = BackendFactory.create_materializer("polars", test_param="value")
        assert materializer is not None

    def test_list_available_backends(self):
        """Test list_available_backends."""
        backends = BackendFactory.list_available_backends()
        assert "polars" in backends
        assert "memory" in backends
        assert "file" in backends
        assert "duckdb" not in backends

    def test_validate_backend_type_valid(self):
        """Test validate_backend_type with valid backend."""
        BackendFactory.validate_backend_type("polars")  # Should not raise

    def test_validate_backend_type_invalid(self):
        """Test validate_backend_type with invalid backend."""
        with pytest.raises(ValueError, match="Unsupported backend type"):
            BackendFactory.validate_backend_type("invalid")

    def test_get_backend_type_polars(self):
        """Test get_backend_type detects Polars."""
        backend = BackendFactory.create_storage_backend("polars")
        backend_type = BackendFactory.get_backend_type(backend)
        assert backend_type == "polars"

    def test_get_backend_type_memory(self):
        """Test get_backend_type detects memory."""
        backend = BackendFactory.create_storage_backend("memory")
        backend_type = BackendFactory.get_backend_type(backend)
        assert backend_type == "memory"

    def test_get_backend_type_file(self):
        """Test get_backend_type detects file."""
        # Use a temporary directory to avoid leaving test files in the repo
        with tempfile.TemporaryDirectory() as tmp_dir:
            base_path = os.path.join(tmp_dir, "test")
            backend = BackendFactory.create_storage_backend("file", base_path=base_path)
            backend_type = BackendFactory.get_backend_type(backend)
            assert backend_type == "file"
            # Explicitly clean up the backend
            with contextlib.suppress(AttributeError):
                backend.close()
            # Verify the directory was created in the temp location, not repo root
            assert os.path.exists(base_path)
            assert not os.path.exists("test")  # Should not exist in repo root

    def test_storage_backend_with_disk_spillover(self):
        """Test creating storage backend with disk spillover (ignored but accepted for compatibility)."""
        backend = BackendFactory.create_storage_backend(
            "polars", allow_disk_spillover=True
        )
        assert backend is not None

    def test_materializer_with_disk_spillover(self):
        """Test creating materializer with disk spillover (ignored but accepted for compatibility)."""
        materializer = BackendFactory.create_materializer(
            "polars", allow_disk_spillover=True
        )
        assert materializer is not None
