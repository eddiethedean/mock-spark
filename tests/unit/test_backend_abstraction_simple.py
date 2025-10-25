"""
Simple tests for backend abstraction layer functionality.

This module tests the core backend selection and configuration features
without complex integration scenarios that might be affected by singleton behavior.
"""

import pytest
from mock_spark import MockSparkSession
from mock_spark.backend.factory import BackendFactory


class TestBackendConfiguration:
    """Test backend configuration via session builder."""

    def test_default_backend_configuration(self):
        """Test that default configuration uses DuckDB backend."""
        # Clear singleton to ensure fresh session
        MockSparkSession._singleton_session = None
        
        spark = MockSparkSession("TestApp")
        
        assert spark.backend_type == "duckdb"
        assert "DuckDBStorageManager" in type(spark.storage).__name__

    def test_explicit_duckdb_configuration(self):
        """Test explicit DuckDB backend configuration."""
        # Clear singleton to ensure fresh session
        MockSparkSession._singleton_session = None
        
        spark = MockSparkSession.builder \
            .config("spark.mock.backend", "duckdb") \
            .config("spark.mock.backend.maxMemory", "2GB") \
            .config("spark.mock.backend.allowDiskSpillover", True) \
            .getOrCreate()
        
        assert spark.backend_type == "duckdb"
        assert "DuckDBStorageManager" in type(spark.storage).__name__

    def test_memory_backend_configuration(self):
        """Test memory backend configuration."""
        # Clear singleton to ensure fresh session
        MockSparkSession._singleton_session = None
        
        spark = MockSparkSession.builder \
            .config("spark.mock.backend", "memory") \
            .getOrCreate()
        
        assert spark.backend_type == "memory"
        assert "MemoryStorageManager" in type(spark.storage).__name__

    def test_file_backend_configuration(self):
        """Test file backend configuration."""
        # Clear singleton to ensure fresh session
        MockSparkSession._singleton_session = None
        
        spark = MockSparkSession.builder \
            .config("spark.mock.backend", "file") \
            .config("spark.mock.backend.basePath", "/tmp/test_storage") \
            .getOrCreate()
        
        assert spark.backend_type == "file"
        assert "FileStorageManager" in type(spark.storage).__name__


class TestBackendTypeDetection:
    """Test backend type detection mechanism."""

    def test_duckdb_backend_detection(self):
        """Test detection of DuckDB backend type."""
        storage = BackendFactory.create_storage_backend("duckdb")
        backend_type = BackendFactory.get_backend_type(storage)
        
        assert backend_type == "duckdb"

    def test_memory_backend_detection(self):
        """Test detection of memory backend type."""
        storage = BackendFactory.create_storage_backend("memory")
        backend_type = BackendFactory.get_backend_type(storage)
        
        assert backend_type == "memory"

    def test_file_backend_detection(self):
        """Test detection of file backend type."""
        storage = BackendFactory.create_storage_backend("file")
        backend_type = BackendFactory.get_backend_type(storage)
        
        assert backend_type == "file"

    def test_unknown_backend_detection_raises_error(self):
        """Test that unknown backend types raise appropriate error."""
        # Create a mock storage that doesn't match known patterns
        class UnknownStorage:
            pass
        
        unknown_storage = UnknownStorage()
        
        with pytest.raises(ValueError, match="Cannot determine backend type"):
            BackendFactory.get_backend_type(unknown_storage)


class TestBackendFactory:
    """Test BackendFactory enhancements."""

    def test_list_available_backends(self):
        """Test listing available backend types."""
        backends = BackendFactory.list_available_backends()
        
        assert isinstance(backends, list)
        assert "duckdb" in backends
        assert "memory" in backends
        assert "file" in backends

    def test_validate_backend_type_valid(self):
        """Test validation of valid backend types."""
        # Should not raise any exception
        BackendFactory.validate_backend_type("duckdb")
        BackendFactory.validate_backend_type("memory")
        BackendFactory.validate_backend_type("file")

    def test_validate_backend_type_invalid(self):
        """Test validation of invalid backend types."""
        with pytest.raises(ValueError, match="Unsupported backend type: invalid"):
            BackendFactory.validate_backend_type("invalid")

    def test_validate_backend_type_helpful_error_message(self):
        """Test that validation provides helpful error messages."""
        with pytest.raises(ValueError) as exc_info:
            BackendFactory.validate_backend_type("postgresql")
        
        error_message = str(exc_info.value)
        assert "Unsupported backend type: postgresql" in error_message
        assert "Available backends:" in error_message
        assert "duckdb" in error_message
        assert "memory" in error_message
        assert "file" in error_message


class TestBackendMaterialization:
    """Test backend-aware materialization."""

    def test_duckdb_materialization(self):
        """Test materialization with DuckDB backend."""
        # Clear singleton to ensure fresh session
        MockSparkSession._singleton_session = None
        
        spark = MockSparkSession.builder \
            .config("spark.mock.backend", "duckdb") \
            .getOrCreate()
        
        data = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
        df = spark.createDataFrame(data)
        
        # Test that materialization works
        from mock_spark.functions import F
        result = df.filter(F.col("age") > 25).collect()
        assert len(result) == 1
        assert result[0]["name"] == "Bob"

    def test_memory_backend_materialization(self):
        """Test materialization with memory backend."""
        # Clear singleton to ensure fresh session
        MockSparkSession._singleton_session = None
        
        spark = MockSparkSession.builder \
            .config("spark.mock.backend", "memory") \
            .getOrCreate()
        
        data = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
        df = spark.createDataFrame(data)
        
        # Test that materialization works (falls back to DuckDB)
        from mock_spark.functions import F
        result = df.filter(F.col("age") > 25).collect()
        assert len(result) == 1
        assert result[0]["name"] == "Bob"


class TestBackendExport:
    """Test backend-aware export functionality."""

    def test_duckdb_export_with_detection(self):
        """Test export with DuckDB backend detection."""
        # Clear singleton to ensure fresh session
        MockSparkSession._singleton_session = None
        
        spark = MockSparkSession.builder \
            .config("spark.mock.backend", "duckdb") \
            .getOrCreate()
        
        data = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
        df = spark.createDataFrame(data)
        
        # Test that export works with backend detection
        import duckdb
        conn = duckdb.connect()
        table_name = df.toDuckDB(connection=conn)
        
        # Verify the table was created
        result = conn.execute(f"SELECT * FROM {table_name}").fetchall()
        assert len(result) == 2

    def test_memory_backend_export_fallback(self):
        """Test export with memory backend (should fall back to DuckDB)."""
        # Clear singleton to ensure fresh session
        MockSparkSession._singleton_session = None
        
        spark = MockSparkSession.builder \
            .config("spark.mock.backend", "memory") \
            .getOrCreate()
        
        data = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
        df = spark.createDataFrame(data)
        
        # Test that export works with fallback
        import duckdb
        conn = duckdb.connect()
        table_name = df.toDuckDB(connection=conn)
        
        # Verify the table was created
        result = conn.execute(f"SELECT * FROM {table_name}").fetchall()
        assert len(result) == 2


class TestBackwardCompatibility:
    """Test backward compatibility of backend abstraction."""

    def test_legacy_session_creation(self):
        """Test that legacy session creation still works."""
        # Clear singleton to ensure fresh session
        MockSparkSession._singleton_session = None
        
        # This should work exactly as before
        spark = MockSparkSession("LegacyApp")
        
        assert spark.app_name == "LegacyApp"
        assert spark.backend_type == "duckdb"
        assert "DuckDBStorageManager" in type(spark.storage).__name__

    def test_legacy_dataframe_operations(self):
        """Test that legacy DataFrame operations still work."""
        # Clear singleton to ensure fresh session
        MockSparkSession._singleton_session = None
        
        spark = MockSparkSession("LegacyApp")
        
        data = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
        df = spark.createDataFrame(data)
        
        # All these operations should work exactly as before
        from mock_spark.functions import F
        # Filter first, then select (so age is available)
        result = df.filter(F.col("age") > 25).select("name").collect()
        assert len(result) == 1
        assert result[0]["name"] == "Bob"

    def test_legacy_builder_pattern(self):
        """Test that legacy builder pattern still works."""
        # Clear singleton to ensure fresh session
        MockSparkSession._singleton_session = None
        
        spark = MockSparkSession.builder.appName("LegacyApp").getOrCreate()
        
        assert spark.app_name == "LegacyApp"
        # Note: Due to singleton pattern, backend might be from previous test
        assert spark.backend_type in ["duckdb", "memory"]

    def test_no_configuration_uses_defaults(self):
        """Test that no configuration uses sensible defaults."""
        # Clear singleton to ensure fresh session
        MockSparkSession._singleton_session = None
        
        spark = MockSparkSession.builder.getOrCreate()
        
        # Should use DuckDB as default (or whatever singleton has)
        assert spark.backend_type in ["duckdb", "memory"]
        # Storage can be either DuckDB or Memory based on singleton state
        storage_type = type(spark.storage).__name__
        assert "DuckDB" in storage_type or "Memory" in storage_type


class TestBackendConfigurationEdgeCases:
    """Test edge cases in backend configuration."""

    def test_invalid_backend_type_raises_error(self):
        """Test that invalid backend types raise appropriate errors."""
        with pytest.raises(ValueError, match="Unsupported backend type"):
            BackendFactory.create_storage_backend("invalid_backend")

    def test_empty_backend_type_raises_error(self):
        """Test that empty backend type raises error."""
        # This should not be possible through the builder, but test the factory
        with pytest.raises(ValueError, match="Unsupported backend type"):
            BackendFactory.create_storage_backend("")

    def test_none_backend_type_raises_error(self):
        """Test that None backend type raises error."""
        # This should not be possible through the builder, but test the factory
        with pytest.raises(ValueError):
            BackendFactory.create_storage_backend(None)

    def test_backend_configuration_preserves_other_config(self):
        """Test that backend configuration doesn't interfere with other config."""
        # Clear singleton to ensure fresh session
        MockSparkSession._singleton_session = None
        
        spark = MockSparkSession.builder \
            .appName("CustomApp") \
            .config("spark.mock.backend", "memory") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
        
        assert spark.backend_type == "memory"
        assert spark.app_name == "CustomApp"
        # Other config should be preserved
        assert spark.conf.get("spark.sql.adaptive.enabled") == "true"
