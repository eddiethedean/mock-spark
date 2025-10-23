"""
Unit tests for catalog operations.

Tests database management, table operations, and catalog functionality.
"""

import pytest
from datetime import datetime
from mock_spark import MockSparkSession, F
from mock_spark.storage.manager import TableMetadata
from mock_spark.spark_types import MockStructType, MockStructField, StringType, IntegerType


@pytest.mark.fast
class TestCatalogOperations:
    """Test catalog operations and database/table management."""

    @pytest.fixture
    def spark(self):
        """Create a MockSparkSession for testing."""
        return MockSparkSession("test")

    @pytest.fixture
    def sample_data(self):
        """Sample data for testing."""
        return [
            {"id": 1, "name": "Alice", "age": 25, "department": "IT"},
            {"id": 2, "name": "Bob", "age": 30, "department": "HR"},
            {"id": 3, "name": "Charlie", "age": 35, "department": "IT"},
        ]

    def test_database_creation_and_listing(self, spark):
        """Test database creation and listing."""
        catalog = spark.catalog

        # Test initial databases
        initial_dbs = catalog.listDatabases()
        assert len(initial_dbs) >= 1  # Should have at least 'default'

        # Create new database
        catalog.createDatabase("test_db")
        
        # Verify database was created
        dbs = catalog.listDatabases()
        db_names = [db.name for db in dbs]
        assert "test_db" in db_names

    def test_current_database_operations(self, spark):
        """Test current database operations."""
        catalog = spark.catalog

        # Test getting current database
        current_db = catalog.currentDatabase()
        assert current_db is not None
        assert isinstance(current_db, str)

        # Create and switch to new database
        catalog.createDatabase("new_db")
        catalog.setCurrentDatabase("new_db")
        
        # Verify switch worked
        assert catalog.currentDatabase() == "new_db"

    def test_database_switching(self, spark):
        """Test switching between databases."""
        catalog = spark.catalog

        # Create multiple databases
        catalog.createDatabase("db1")
        catalog.createDatabase("db2")

        # Switch to db1
        catalog.setCurrentDatabase("db1")
        assert catalog.currentDatabase() == "db1"

        # Switch to db2
        catalog.setCurrentDatabase("db2")
        assert catalog.currentDatabase() == "db2"

        # Switch back to default
        catalog.setCurrentDatabase("default")
        assert catalog.currentDatabase() == "default"

    def test_table_creation_and_listing(self, spark, sample_data):
        """Test table creation and listing."""
        catalog = spark.catalog

        # Create a DataFrame and save as table
        df = spark.createDataFrame(sample_data)
        df.write.mode("overwrite").saveAsTable("test_table")

        # Verify table exists
        assert catalog.tableExists("test_table")

        # List tables
        tables = catalog.listTables()
        table_names = [table.name for table in tables]
        assert "test_table" in table_names

    def test_qualified_table_names(self, spark, sample_data):
        """Test qualified table names (schema.table)."""
        catalog = spark.catalog

        # Create a new database
        catalog.createDatabase("test_schema")
        catalog.setCurrentDatabase("test_schema")

        # Create DataFrame and save as table
        df = spark.createDataFrame(sample_data)
        df.write.mode("overwrite").saveAsTable("test_table")

        # Test qualified name
        qualified_name = "test_schema.test_table"
        assert catalog.tableExists(qualified_name)

        # Test loading table with qualified name
        loaded_df = spark.table(qualified_name)
        assert loaded_df.count() == 3

    def test_table_metadata_tracking(self, spark, sample_data):
        """Test table metadata tracking."""
        catalog = spark.catalog

        # Create DataFrame and save as table
        df = spark.createDataFrame(sample_data)
        df.write.mode("overwrite").saveAsTable("metadata_test_table")

        # Get table metadata
        tables = catalog.listTables()
        table_names = [table.name for table in tables]
        assert "metadata_test_table" in table_names

    def test_write_modes(self, spark, sample_data):
        """Test different write modes."""
        catalog = spark.catalog

        # Create initial DataFrame
        df1 = spark.createDataFrame(sample_data)
        df1.write.mode("overwrite").saveAsTable("write_test_table")

        # Test error mode (should fail if table exists)
        df2 = spark.createDataFrame([{"id": 4, "name": "David", "age": 40, "department": "Finance"}])
        
        try:
            df2.write.mode("error").saveAsTable("write_test_table")
            assert False, "Error mode should have failed"
        except Exception:
            pass  # Expected to fail

        # Test ignore mode (should not fail, but not write)
        df2.write.mode("ignore").saveAsTable("write_test_table")
        result_df = spark.table("write_test_table")
        assert result_df.count() == 3  # Original data unchanged

        # Test append mode
        df2.write.mode("append").saveAsTable("write_test_table")
        result_df = spark.table("write_test_table")
        assert result_df.count() == 4  # Original + appended

        # Test overwrite mode
        df2.write.mode("overwrite").saveAsTable("write_test_table")
        result_df = spark.table("write_test_table")
        assert result_df.count() == 1  # Only new data

    def test_table_dropping(self, spark, sample_data):
        """Test table dropping."""
        catalog = spark.catalog

        # Create and save table
        df = spark.createDataFrame(sample_data)
        df.write.mode("overwrite").saveAsTable("drop_test_table")

        # Verify table exists
        assert catalog.tableExists("drop_test_table")

        # Drop table
        catalog.dropTable("drop_test_table")

        # Verify table no longer exists
        assert not catalog.tableExists("drop_test_table")

    def test_database_dropping(self, spark):
        """Test database dropping."""
        catalog = spark.catalog

        # Create database
        catalog.createDatabase("drop_test_db")
        assert "drop_test_db" in [db.name for db in catalog.listDatabases()]

        # Drop database
        catalog.dropDatabase("drop_test_db")

        # Verify database no longer exists
        db_names = [db.name for db in catalog.listDatabases()]
        assert "drop_test_db" not in db_names

    def test_table_caching(self, spark, sample_data):
        """Test table caching operations."""
        catalog = spark.catalog

        # Create and save table
        df = spark.createDataFrame(sample_data)
        df.write.mode("overwrite").saveAsTable("cache_test_table")

        # Test caching
        catalog.cacheTable("cache_test_table")
        assert catalog.isCached("cache_test_table")

        # Test uncaching
        catalog.uncacheTable("cache_test_table")
        assert not catalog.isCached("cache_test_table")

    def test_table_refresh(self, spark, sample_data):
        """Test table refresh operations."""
        catalog = spark.catalog

        # Create and save table
        df = spark.createDataFrame(sample_data)
        df.write.mode("overwrite").saveAsTable("refresh_test_table")

        # Test refresh
        catalog.refreshTable("refresh_test_table")
        # Should not raise exception

    def test_multiple_schemas(self, spark, sample_data):
        """Test operations across multiple schemas."""
        catalog = spark.catalog

        # Create multiple databases
        catalog.createDatabase("schema1")
        catalog.createDatabase("schema2")

        # Create tables in different schemas
        df = spark.createDataFrame(sample_data)

        # Switch to schema1 and create table
        catalog.setCurrentDatabase("schema1")
        df.write.mode("overwrite").saveAsTable("table1")

        # Switch to schema2 and create table
        catalog.setCurrentDatabase("schema2")
        df.write.mode("overwrite").saveAsTable("table2")

        # Verify tables exist in correct schemas
        catalog.setCurrentDatabase("schema1")
        assert catalog.tableExists("table1")
        assert not catalog.tableExists("table2")

        catalog.setCurrentDatabase("schema2")
        assert catalog.tableExists("table2")
        assert not catalog.tableExists("table1")

    def test_table_schema_inference(self, spark, sample_data):
        """Test table schema inference."""
        catalog = spark.catalog

        # Create DataFrame with specific schema
        df = spark.createDataFrame(sample_data)
        df.write.mode("overwrite").saveAsTable("schema_test_table")

        # Load table and verify schema
        loaded_df = spark.table("schema_test_table")
        assert "id" in loaded_df.columns
        assert "name" in loaded_df.columns
        assert "age" in loaded_df.columns
        assert "department" in loaded_df.columns

    def test_table_properties(self, spark, sample_data):
        """Test table properties and metadata."""
        catalog = spark.catalog

        # Create DataFrame and save as table
        df = spark.createDataFrame(sample_data)
        df.write.mode("overwrite").saveAsTable("properties_test_table")

        # Get table metadata
        tables = catalog.listTables()
        test_table = next((t for t in tables if t.name == "properties_test_table"), None)
        assert test_table is not None

        # Verify table properties
        assert hasattr(test_table, 'name')
        assert hasattr(test_table, 'database')

    def test_catalog_error_handling(self, spark):
        """Test catalog error handling."""
        catalog = spark.catalog

        # Test setting non-existent database
        try:
            catalog.setCurrentDatabase("non_existent_db")
            assert False, "Should have raised exception"
        except Exception:
            pass  # Expected to fail

        # Test dropping non-existent table
        try:
            catalog.dropTable("non_existent_table")
            assert False, "Should have raised exception"
        except Exception:
            pass  # Expected to fail

    def test_table_loading_with_qualified_names(self, spark, sample_data):
        """Test loading tables with qualified names."""
        catalog = spark.catalog

        # Create database and table
        catalog.createDatabase("qualified_test")
        catalog.setCurrentDatabase("qualified_test")

        df = spark.createDataFrame(sample_data)
        df.write.mode("overwrite").saveAsTable("qualified_table")

        # Test loading with qualified name
        qualified_df = spark.table("qualified_test.qualified_table")
        assert qualified_df.count() == 3

        # Test loading with unqualified name (should work in current database)
        unqualified_df = spark.table("qualified_table")
        assert unqualified_df.count() == 3

    def test_catalog_performance(self, spark):
        """Test catalog performance with multiple operations."""
        catalog = spark.catalog

        # Create multiple databases
        for i in range(5):
            catalog.createDatabase(f"perf_db_{i}")

        # Verify all databases exist
        dbs = catalog.listDatabases()
        db_names = [db.name for db in dbs]
        for i in range(5):
            assert f"perf_db_{i}" in db_names

        # Create multiple tables in one database
        catalog.setCurrentDatabase("perf_db_0")
        for i in range(3):
            df = spark.createDataFrame([{"id": i, "value": f"test_{i}"}])
            df.write.mode("overwrite").saveAsTable(f"perf_table_{i}")

        # Verify all tables exist
        tables = catalog.listTables()
        table_names = [table.name for table in tables]
        for i in range(3):
            assert f"perf_table_{i}" in table_names
