"""
Tests for catalog synchronization fixes.

This module tests that catalog.tableExists() works correctly after saveAsTable().
"""

import pytest
from mock_spark import MockSparkSession


@pytest.mark.unit
class TestCatalogSynchronization:
    """Test catalog synchronization fixes."""

    @pytest.fixture
    def spark(self):
        """Create a MockSparkSession for testing."""
        session = MockSparkSession("catalog_sync_test")
        yield session
        session.stop()

    def test_table_exists_after_save_as_table(self, spark):
        """Test that catalog.tableExists() works after saveAsTable()."""
        test_data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

        df = spark.createDataFrame(test_data)

        # Save as table
        df.write.mode("overwrite").saveAsTable("test_schema.test_table")

        # Verify table exists in catalog
        assert spark.catalog.tableExists("test_schema", "test_table") is True
        assert spark.catalog.tableExists("test_schema.test_table") is True

    def test_table_exists_after_append_mode(self, spark):
        """Test that catalog.tableExists() works after append mode saveAsTable()."""
        test_data1 = [{"id": 1, "name": "Alice"}]
        test_data2 = [{"id": 2, "name": "Bob"}]

        df1 = spark.createDataFrame(test_data1)
        df2 = spark.createDataFrame(test_data2)

        # Create table with overwrite
        df1.write.mode("overwrite").saveAsTable("test_schema.test_table_append")

        # Verify table exists
        assert spark.catalog.tableExists("test_schema", "test_table_append") is True

        # Append to table
        df2.write.mode("append").saveAsTable("test_schema.test_table_append")

        # Verify table still exists after append
        assert spark.catalog.tableExists("test_schema", "test_table_append") is True

        # Verify data was appended
        result_df = spark.table("test_schema.test_table_append")
        assert result_df.count() == 2

    def test_table_exists_with_qualified_name(self, spark):
        """Test that catalog.tableExists() works with qualified table names."""
        test_data = [{"id": 1, "value": "test"}]

        df = spark.createDataFrame(test_data)
        df.write.mode("overwrite").saveAsTable("my_schema.my_table")

        # Test with qualified name
        assert spark.catalog.tableExists("my_schema.my_table") is True

        # Test with separate schema and table
        assert spark.catalog.tableExists("my_table", "my_schema") is True

    def test_table_exists_returns_false_for_nonexistent_table(self, spark):
        """Test that catalog.tableExists() returns False for non-existent tables."""
        assert (
            spark.catalog.tableExists("nonexistent_schema", "nonexistent_table")
            is False
        )
        assert (
            spark.catalog.tableExists("nonexistent_schema.nonexistent_table") is False
        )

    def test_table_list_includes_saved_tables(self, spark):
        """Test that listTables() includes tables created with saveAsTable()."""
        test_data = [{"id": 1, "name": "Test"}]

        df = spark.createDataFrame(test_data)
        df.write.mode("overwrite").saveAsTable("list_test_schema.list_test_table")

        # List tables in schema
        tables = spark.catalog.listTables("list_test_schema")
        table_names = [t.name for t in tables]
        assert "list_test_table" in table_names
