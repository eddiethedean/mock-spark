"""
Unit Tests for SQL DDL Catalog Integration

Tests that SQL DDL commands (CREATE/DROP SCHEMA/DATABASE) properly
update the catalog, matching PySpark behavior.

Critical Issue #2 from IMPROVEMENT_PLAN.md
"""

import pytest
from mock_spark import MockSparkSession


class TestCreateSchemaSQL:
    """Test CREATE SCHEMA/DATABASE via SQL updates catalog."""

    def test_create_schema_updates_catalog(self, spark):
        """Test that CREATE SCHEMA updates catalog.listDatabases()."""
        # Clean up first for parallel test isolation
        spark.sql("DROP SCHEMA IF EXISTS test_schema")
        spark.sql("CREATE SCHEMA test_schema")

        dbs = spark.catalog.listDatabases()
        db_names = [db.name for db in dbs]

        assert "test_schema" in db_names, "Created schema should appear in catalog"

    def test_create_database_updates_catalog(self, spark):
        """Test that CREATE DATABASE updates catalog (DATABASE is synonym for SCHEMA)."""
        # Clean up first for parallel test isolation
        spark.sql("DROP DATABASE IF EXISTS test_database")
        spark.sql("CREATE DATABASE test_database")

        dbs = spark.catalog.listDatabases()
        db_names = [db.name for db in dbs]

        assert "test_database" in db_names, "Created database should appear in catalog"

    def test_create_schema_if_not_exists(self, spark):
        """Test CREATE SCHEMA IF NOT EXISTS is idempotent."""
        spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")
        spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")  # Should not raise error

        dbs = spark.catalog.listDatabases()
        db_names = [db.name for db in dbs]

        assert "test_schema" in db_names
        # Should only appear once (no duplicates)
        assert db_names.count("test_schema") == 1

    def test_create_multiple_schemas(self, spark):
        """Test creating multiple schemas."""
        # Clean up first for parallel test isolation
        spark.sql("DROP SCHEMA IF EXISTS analytics")
        spark.sql("DROP SCHEMA IF EXISTS staging")
        spark.sql("DROP SCHEMA IF EXISTS production")

        spark.sql("CREATE SCHEMA analytics")
        spark.sql("CREATE SCHEMA staging")
        spark.sql("CREATE SCHEMA production")

        dbs = spark.catalog.listDatabases()
        db_names = [db.name for db in dbs]

        assert "analytics" in db_names
        assert "staging" in db_names
        assert "production" in db_names


class TestDropSchemaSQL:
    """Test DROP SCHEMA/DATABASE via SQL updates catalog."""

    def test_drop_schema_updates_catalog(self, spark):
        """Test that DROP SCHEMA removes from catalog."""
        # First create a schema (use IF NOT EXISTS for test isolation)
        spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")
        dbs = spark.catalog.listDatabases()
        assert "test_schema" in [db.name for db in dbs]

        # Now drop it
        spark.sql("DROP SCHEMA test_schema")
        dbs = spark.catalog.listDatabases()
        db_names = [db.name for db in dbs]

        assert "test_schema" not in db_names, "Dropped schema should not appear in catalog"

    def test_drop_database_updates_catalog(self, spark):
        """Test that DROP DATABASE removes from catalog."""
        spark.sql("CREATE DATABASE IF NOT EXISTS test_database")
        assert "test_database" in [db.name for db in spark.catalog.listDatabases()]

        spark.sql("DROP DATABASE test_database")
        db_names = [db.name for db in spark.catalog.listDatabases()]

        assert "test_database" not in db_names

    def test_drop_schema_if_exists(self, spark):
        """Test DROP SCHEMA IF EXISTS doesn't error if schema doesn't exist."""
        # This should not raise an error even if schema doesn't exist
        spark.sql("DROP SCHEMA IF EXISTS nonexistent_schema")

        # Should work without error
        assert True

    def test_drop_then_recreate_schema(self, spark):
        """Test dropping and recreating a schema."""
        # Clean up first if it exists
        spark.sql("DROP SCHEMA IF EXISTS test_schema")
        spark.sql("CREATE SCHEMA test_schema")
        spark.sql("DROP SCHEMA test_schema")
        spark.sql("CREATE SCHEMA test_schema")  # Should work

        dbs = spark.catalog.listDatabases()
        assert "test_schema" in [db.name for db in dbs]


class TestCatalogAPIIntegration:
    """Test that SQL DDL and catalog API work together."""

    def test_sql_create_catalog_list(self, spark):
        """Test schema created via SQL appears in catalog.listDatabases()."""
        # Clean up first for parallel test isolation
        spark.sql("DROP SCHEMA IF EXISTS my_schema")
        spark.sql("CREATE SCHEMA my_schema")

        # Should be visible via catalog API
        dbs = spark.catalog.listDatabases()
        db_names = [db.name for db in dbs]
        assert "my_schema" in db_names

    def test_catalog_create_sql_visible(self, spark):
        """Test schema created via catalog API is visible to SQL."""
        # Clean up first for parallel test isolation
        spark.sql("DROP SCHEMA IF EXISTS catalog_schema")
        spark.catalog.createDatabase("catalog_schema")

        # Should be visible when listing via catalog
        dbs = spark.catalog.listDatabases()
        db_names = [db.name for db in dbs]
        assert "catalog_schema" in db_names

    def test_sql_drop_catalog_reflects(self, spark):
        """Test schema dropped via SQL is removed from catalog."""
        # Clean up first for parallel test isolation
        spark.sql("DROP SCHEMA IF EXISTS test_schema")
        spark.catalog.createDatabase("test_schema")

        # Drop via SQL
        spark.sql("DROP SCHEMA test_schema")

        # Should be gone from catalog
        dbs = spark.catalog.listDatabases()
        db_names = [db.name for db in dbs]
        assert "test_schema" not in db_names

    def test_catalog_drop_sql_reflects(self, spark):
        """Test schema dropped via catalog API is removed."""
        # Use IF NOT EXISTS for test isolation in parallel execution
        spark.sql("DROP SCHEMA IF EXISTS test_schema")
        spark.sql("CREATE SCHEMA test_schema")

        # Drop via catalog API
        spark.catalog.dropDatabase("test_schema")

        # Should be gone
        dbs = spark.catalog.listDatabases()
        db_names = [db.name for db in dbs]
        assert "test_schema" not in db_names


class TestSparkForgePatterns:
    """Test the patterns from SparkForge that were failing."""

    def test_pipeline_builder_create_schema_pattern(self, spark):
        """Test the exact pattern used in SparkForge PipelineBuilder."""
        # This is the pattern that was commented out in SparkForge tests
        schema_name = "pipeline_test_schema"

        # Create schema via SQL
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

        # This assertion was failing in SparkForge - should now work
        dbs = spark.catalog.listDatabases()
        db_names = [db.name for db in dbs]
        assert (
            schema_name in db_names
        ), f"Schema {schema_name} should appear in catalog after SQL CREATE"

    def test_multiple_schema_creation_workflow(self, spark):
        """Test creating multiple schemas as in real workflows."""
        # Clean up first for parallel test isolation
        spark.sql("DROP SCHEMA IF EXISTS bronze")
        spark.sql("DROP SCHEMA IF EXISTS silver")
        spark.sql("DROP SCHEMA IF EXISTS gold")

        # Typical workflow: create schemas for different environments
        spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
        spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
        spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

        dbs = spark.catalog.listDatabases()
        db_names = [db.name for db in dbs]

        assert "bronze" in db_names
        assert "silver" in db_names
        assert "gold" in db_names

    def test_schema_lifecycle_management(self, spark):
        """Test full lifecycle: create, verify, drop, verify."""
        schema_name = "temp_schema"

        # Create
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        assert schema_name in [db.name for db in spark.catalog.listDatabases()]

        # Use it (in real code, would create tables here)
        # ...

        # Drop
        spark.sql(f"DROP SCHEMA IF EXISTS {schema_name}")
        assert schema_name not in [db.name for db in spark.catalog.listDatabases()]


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_create_schema_with_backticks(self, spark):
        """Test schema names with backticks."""
        spark.sql("CREATE SCHEMA IF NOT EXISTS `my_schema`")

        dbs = spark.catalog.listDatabases()
        db_names = [db.name for db in dbs]
        assert "my_schema" in db_names

    def test_case_insensitive_keywords(self, spark):
        """Test that keywords are case-insensitive."""
        # Clean up first for parallel test isolation
        spark.sql("DROP SCHEMA IF EXISTS lowercase_test")
        spark.sql("DROP SCHEMA IF EXISTS uppercase_test")
        spark.sql("DROP SCHEMA IF EXISTS mixedcase_test")

        spark.sql("create schema lowercase_test")
        spark.sql("CREATE SCHEMA uppercase_test")
        spark.sql("CrEaTe ScHeMa mixedcase_test")

        dbs = spark.catalog.listDatabases()
        db_names = [db.name for db in dbs]

        assert "lowercase_test" in db_names
        assert "uppercase_test" in db_names
        assert "mixedcase_test" in db_names

    def test_schema_name_with_underscores(self, spark):
        """Test schema names with underscores."""
        # Clean up first for parallel test isolation
        spark.sql("DROP SCHEMA IF EXISTS my_test_schema_123")
        spark.sql("CREATE SCHEMA my_test_schema_123")

        dbs = spark.catalog.listDatabases()
        db_names = [db.name for db in dbs]
        assert "my_test_schema_123" in db_names


# Fixtures
@pytest.fixture
def spark():
    """Create Mock Spark session with isolated storage."""
    session = MockSparkSession.builder.appName("SQLDDLCatalogTests").getOrCreate()
    yield session
    session.stop()
