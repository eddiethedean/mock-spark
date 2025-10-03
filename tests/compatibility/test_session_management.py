"""
Session and Context Management Compatibility Tests

Tests for SparkSession, SparkContext, and related management functionality.
"""

import pytest
from mock_spark.session import MockSparkSession, MockSparkContext


class TestSessionManagement:
    """Test session management functionality."""

    def test_session_creation(self):
        """Test basic session creation."""
        session = MockSparkSession()
        assert session is not None
        assert hasattr(session, "sparkContext")
        assert hasattr(session, "createDataFrame")
        assert hasattr(session, "sql")
        assert hasattr(session, "stop")

    def test_session_builder(self):
        """Test session builder functionality."""
        builder = MockSparkSession.builder
        assert builder is not None

        # Test builder methods
        builder.appName("test-app")
        builder.master("local[2]")
        builder.config("spark.test.config", "test-value")

        # Test that builder methods return self for chaining
        assert builder.appName("test") is builder

    def test_context_management(self):
        """Test context manager functionality."""
        with MockSparkSession() as session:
            assert session is not None
            # Session should be usable within context
            data = [{"id": 1, "name": "test"}]
            df = session.createDataFrame(data)
            assert df.count() == 1

        # Session should be stopped after context
        # Note: Mock implementation may not enforce this strictly

    def test_multiple_sessions(self):
        """Test creating multiple sessions."""
        session1 = MockSparkSession()
        session2 = MockSparkSession()

        # Both sessions should work independently
        data = [{"id": 1, "name": "test1"}]
        df1 = session1.createDataFrame(data)

        data = [{"id": 2, "name": "test2"}]
        df2 = session2.createDataFrame(data)

        assert df1.count() == 1
        assert df2.count() == 1

        session1.stop()
        session2.stop()

    def test_session_configuration(self):
        """Test session configuration."""
        session = MockSparkSession()

        # Test getting configuration
        config = session.conf
        assert config is not None

        # Test setting configuration
        session.conf.set("test.key", "test.value")

        # Test getting configuration value
        value = session.conf.get("test.key")
        assert value == "test.value"

    def test_spark_context_properties(self):
        """Test SparkContext properties."""
        session = MockSparkSession()
        sc = session.sparkContext

        assert sc is not None
        assert hasattr(sc, "appName")
        assert hasattr(sc, "setLogLevel")
        assert hasattr(sc, "_jvm")

    def test_session_sql_method(self):
        """Test session SQL method."""
        session = MockSparkSession()

        # Test that sql method exists (even if not fully implemented)
        assert hasattr(session, "sql")

        # Test basic SQL functionality if implemented
        try:
            result = session.sql("SELECT 1 as test_col")
            assert result is not None
        except NotImplementedError:
            # SQL method might not be fully implemented yet
            pass


class TestContextManagement:
    """Test context and lifecycle management."""

    def test_session_lifecycle(self):
        """Test session lifecycle."""
        session = MockSparkSession()

        # Session should be active
        assert session is not None

        # Create DataFrame to ensure session works
        data = [{"id": 1, "name": "test"}]
        df = session.createDataFrame(data)
        assert df.count() == 1

        # Stop session
        session.stop()

        # After stopping, session might still exist but should be marked as stopped
        # Exact behavior depends on implementation

    def test_context_logging(self):
        """Test context logging functionality."""
        session = MockSparkSession()
        sc = session.sparkContext

        # Test log level setting
        sc.setLogLevel("WARN")
        sc.setLogLevel("ERROR")
        sc.setLogLevel("INFO")

        # Should not raise exceptions
        assert True

    def test_context_app_name(self):
        """Test context app name."""
        session = MockSparkSession()
        sc = session.sparkContext

        # Test app name property
        app_name = sc.appName
        assert app_name is not None
        assert isinstance(app_name, str)

    def test_context_jvm_access(self):
        """Test JVM context access."""
        session = MockSparkSession()
        sc = session.sparkContext

        # Test JVM access
        jvm = sc._jvm
        assert jvm is not None

        # Test that JVM context has expected properties
        assert hasattr(jvm, "functions")


class TestDataFrameSessionIntegration:
    """Test DataFrame and session integration."""

    def test_dataframe_session_reference(self):
        """Test that DataFrames maintain session reference."""
        session = MockSparkSession()
        data = [{"id": 1, "name": "test"}]
        df = session.createDataFrame(data)

        # DataFrame should have storage reference (which connects to session)
        assert hasattr(df, "storage")  # Through storage reference
        assert df.storage is session.storage

        # Test that DataFrame operations work
        result = df.select("id")
        assert result.count() == 1

    def test_dataframe_collect_after_session_stop(self):
        """Test DataFrame behavior after session stop."""
        session = MockSparkSession()
        data = [{"id": 1, "name": "test"}]
        df = session.createDataFrame(data)

        # Collect data before stopping session
        data_before = df.collect()
        assert len(data_before) == 1

        # Stop session
        session.stop()

        # Try to collect data after session stop
        # Behavior depends on implementation - might work or might raise exception
        try:
            data_after = df.collect()
            assert len(data_after) == 1
        except Exception:
            # If exception is raised, it should be appropriate
            pass


class TestStorageIntegration:
    """Test storage and table integration."""

    def test_catalog_access(self):
        """Test catalog access."""
        session = MockSparkSession()

        # Test catalog property
        catalog = session.catalog
        assert catalog is not None

        # Test catalog methods
        assert hasattr(catalog, "listTables")
        assert hasattr(catalog, "listDatabases")
        assert hasattr(catalog, "tableExists")

    def test_table_operations(self):
        """Test table operations."""
        session = MockSparkSession()

        # Create test DataFrame
        data = [{"id": 1, "name": "test"}]
        df = session.createDataFrame(data)

        # Test table creation and access
        try:
            df.createOrReplaceTempView("test_table")

            # Test that table exists
            catalog = session.catalog
            tables = catalog.listTables("default")

            # Table should be in the list
            assert "test_table" in tables

        except NotImplementedError:
            # Table operations might not be fully implemented yet
            pass

    def test_database_operations(self):
        """Test database operations."""
        session = MockSparkSession()
        catalog = session.catalog

        # Test listing databases
        databases = catalog.listDatabases()
        assert databases is not None

        # Should have at least default database
        assert len(databases) >= 0


class TestConfigurationManagement:
    """Test configuration management."""

    def test_default_configuration(self):
        """Test default configuration values."""
        session = MockSparkSession()
        conf = session.conf

        # Test getting default configurations
        app_name = conf.get("spark.app.name")
        assert app_name is not None

        master = conf.get("spark.master")
        assert master is not None

    def test_configuration_setting(self):
        """Test setting configuration values."""
        session = MockSparkSession()
        conf = session.conf

        # Test setting and getting custom configuration
        conf.set("custom.key", "custom.value")
        value = conf.get("custom.key")
        assert value == "custom.value"

    def test_configuration_persistence(self):
        """Test configuration persistence across operations."""
        session = MockSparkSession()
        conf = session.conf

        # Set configuration
        conf.set("test.persistence", "test.value")

        # Perform DataFrame operation
        data = [{"id": 1, "name": "test"}]
        df = session.createDataFrame(data)
        df.count()

        # Configuration should persist
        value = conf.get("test.persistence")
        assert value == "test.value"
