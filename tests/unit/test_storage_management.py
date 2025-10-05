"""
Test Storage Management for Mock Spark 1.0.0

Tests the hybrid storage manager and migration utilities.
"""

import pytest
import tempfile
import os
from pathlib import Path
from mock_spark.storage import (
    HybridStorageManager, 
    create_hybrid_manager,
    StorageMigrationTool,
    migrate_sqlite_to_duckdb,
    StorageManagerFactory
)
from mock_spark.spark_types import MockStructType, MockStructField, StringType, IntegerType, DoubleType


class TestHybridStorageManager:
    """Test HybridStorageManager functionality."""

    def test_hybrid_manager_initialization(self):
        """Test hybrid manager initialization."""
        with tempfile.TemporaryDirectory() as temp_dir:
            sqlite_path = os.path.join(temp_dir, "test.db")
            duckdb_path = os.path.join(temp_dir, "test.duckdb")
            
            manager = HybridStorageManager(
                primary_backend="duckdb",
                sqlite_path=sqlite_path,
                duckdb_path=duckdb_path
            )
            
            assert manager.primary_backend == "duckdb"
            assert manager.current_backend.__class__.__name__ == "DuckDBStorageManager"
            assert manager.secondary_backend.__class__.__name__ == "SQLiteStorageManager"
            
            manager.close()

    def test_backend_switching(self):
        """Test switching between backends."""
        with tempfile.TemporaryDirectory() as temp_dir:
            sqlite_path = os.path.join(temp_dir, "test.db")
            duckdb_path = os.path.join(temp_dir, "test.duckdb")
            
            manager = HybridStorageManager(
                primary_backend="duckdb",
                sqlite_path=sqlite_path,
                duckdb_path=duckdb_path
            )
            
            # Switch to SQLite
            manager.switch_primary_backend("sqlite")
            assert manager.primary_backend == "sqlite"
            assert manager.current_backend.__class__.__name__ == "SQLiteStorageManager"
            
            # Switch back to DuckDB
            manager.switch_primary_backend("duckdb")
            assert manager.primary_backend == "duckdb"
            assert manager.current_backend.__class__.__name__ == "DuckDBStorageManager"
            
            manager.close()

    def test_data_migration_between_backends(self):
        """Test data migration between backends."""
        with tempfile.TemporaryDirectory() as temp_dir:
            sqlite_path = os.path.join(temp_dir, "test.db")
            duckdb_path = os.path.join(temp_dir, "test.duckdb")
            
            manager = HybridStorageManager(
                primary_backend="sqlite",
                sqlite_path=sqlite_path,
                duckdb_path=duckdb_path
            )
            
            # Create test data in SQLite (primary)
            schema = MockStructType([
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType()),
                MockStructField("value", DoubleType())
            ])
            
            test_data = [
                {"id": 1, "name": "Alice", "value": 10.5},
                {"id": 2, "name": "Bob", "value": 20.3},
                {"id": 3, "name": "Charlie", "value": 15.7}
            ]
            
            # Create table and insert data in SQLite
            manager.create_table("default", "test_table", schema)
            manager.insert_data("default", "test_table", test_data)
            
            # Migrate to DuckDB
            success = manager.migrate_data("default", "test_table", target_backend="duckdb")
            assert success
            
            # Verify data in DuckDB
            duckdb_data = manager.duckdb_manager.get_data("default", "test_table")
            assert len(duckdb_data) == 3
            assert duckdb_data[0]["name"] == "Alice"
            
            manager.close()

    def test_bulk_migration(self):
        """Test bulk migration of all tables."""
        with tempfile.TemporaryDirectory() as temp_dir:
            sqlite_path = os.path.join(temp_dir, "test.db")
            duckdb_path = os.path.join(temp_dir, "test.duckdb")
            
            manager = HybridStorageManager(
                primary_backend="sqlite",
                sqlite_path=sqlite_path,
                duckdb_path=duckdb_path
            )
            
            # Create multiple tables in SQLite
            schema = MockStructType([
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType())
            ])
            
            # Create table 1
            manager.create_table("default", "table1", schema)
            manager.insert_data("default", "table1", [{"id": 1, "name": "Alice"}])
            
            # Create table 2
            manager.create_table("default", "table2", schema)
            manager.insert_data("default", "table2", [{"id": 2, "name": "Bob"}])
            
            # Migrate all tables
            results = manager.migrate_all_tables("default")
            
            assert len(results) == 2
            assert results["table1"] == True
            assert results["table2"] == True
            
            # Verify tables exist in DuckDB
            duckdb_tables = manager.duckdb_manager.list_tables("default")
            assert "table1" in duckdb_tables
            assert "table2" in duckdb_tables
            
            manager.close()

    def test_backend_info(self):
        """Test getting backend information."""
        with tempfile.TemporaryDirectory() as temp_dir:
            sqlite_path = os.path.join(temp_dir, "test.db")
            duckdb_path = os.path.join(temp_dir, "test.duckdb")
            
            manager = HybridStorageManager(
                primary_backend="duckdb",
                sqlite_path=sqlite_path,
                duckdb_path=duckdb_path
            )
            
            info = manager.get_backend_info()
            
            assert info["primary_backend"] == "duckdb"
            assert info["current_backend"] == "DuckDBStorageManager"
            assert info["secondary_backend"] == "SQLiteStorageManager"
            assert info["sqlite_path"] == sqlite_path
            assert info["duckdb_path"] == duckdb_path
            
            manager.close()

    def test_create_hybrid_manager_factory(self):
        """Test factory function for creating hybrid manager."""
        with tempfile.TemporaryDirectory() as temp_dir:
            sqlite_path = os.path.join(temp_dir, "test.db")
            duckdb_path = os.path.join(temp_dir, "test.duckdb")
            
            manager = create_hybrid_manager(
                primary_backend="duckdb",
                sqlite_path=sqlite_path,
                duckdb_path=duckdb_path
            )
            
            assert isinstance(manager, HybridStorageManager)
            assert manager.primary_backend == "duckdb"
            
            manager.close()


class TestStorageMigrationTool:
    """Test StorageMigrationTool functionality."""

    def test_migration_tool_initialization(self):
        """Test migration tool initialization."""
        with tempfile.TemporaryDirectory() as temp_dir:
            sqlite_path = os.path.join(temp_dir, "source.db")
            duckdb_path = os.path.join(temp_dir, "target.duckdb")
            
            # Create source SQLite database with test data
            sqlite_manager = StorageManagerFactory.create_sqlite_manager(sqlite_path)
            schema = MockStructType([
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType())
            ])
            
            sqlite_manager.create_table("default", "users", schema)
            sqlite_manager.insert_data("default", "users", [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"}
            ])
            sqlite_manager.close()
            
            # Initialize migration tool
            migration_tool = StorageMigrationTool(sqlite_path, duckdb_path)
            
            assert migration_tool.source_type == "sqlite"
            assert migration_tool.target_type == "duckdb"
            assert migration_tool.source_path == sqlite_path
            assert migration_tool.target_path == duckdb_path
            
            migration_tool.close()

    def test_migration_analysis(self):
        """Test migration analysis functionality."""
        with tempfile.TemporaryDirectory() as temp_dir:
            sqlite_path = os.path.join(temp_dir, "source.db")
            duckdb_path = os.path.join(temp_dir, "target.duckdb")
            
            # Create source SQLite database with test data
            sqlite_manager = StorageManagerFactory.create_sqlite_manager(sqlite_path)
            schema = MockStructType([
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType())
            ])
            
            sqlite_manager.create_table("default", "users", schema)
            sqlite_manager.insert_data("default", "users", [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
                {"id": 3, "name": "Charlie"}
            ])
            sqlite_manager.close()
            
            # Analyze migration
            with StorageMigrationTool(sqlite_path, duckdb_path) as migration_tool:
                analysis = migration_tool.analyze_migration()
                
                assert analysis["source_type"] == "sqlite"
                assert analysis["target_type"] == "duckdb"
                assert analysis["total_tables"] == 1
                assert analysis["total_rows"] == 3
                assert "default" in analysis["schemas"]
                assert "users" in analysis["schemas"]["default"]["tables"]
                assert analysis["schemas"]["default"]["tables"]["users"]["row_count"] == 3

    def test_table_migration(self):
        """Test single table migration."""
        with tempfile.TemporaryDirectory() as temp_dir:
            sqlite_path = os.path.join(temp_dir, "source.db")
            duckdb_path = os.path.join(temp_dir, "target.duckdb")
            
            # Create source SQLite database with test data
            sqlite_manager = StorageManagerFactory.create_sqlite_manager(sqlite_path)
            schema = MockStructType([
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType()),
                MockStructField("age", IntegerType())
            ])
            
            test_data = [
                {"id": 1, "name": "Alice", "age": 25},
                {"id": 2, "name": "Bob", "age": 30},
                {"id": 3, "name": "Charlie", "age": 35}
            ]
            
            sqlite_manager.create_table("default", "users", schema)
            sqlite_manager.insert_data("default", "users", test_data)
            sqlite_manager.close()
            
            # Migrate table
            with StorageMigrationTool(sqlite_path, duckdb_path) as migration_tool:
                result = migration_tool.migrate_table("default", "users")
                
                assert result["success"] == True
                assert result["rows_migrated"] == 3
                assert result["error"] is None
                
                # Verify data in target
                target_data = migration_tool.target_manager.get_data("default", "users")
                assert len(target_data) == 3
                assert target_data[0]["name"] == "Alice"

    def test_schema_migration(self):
        """Test schema migration."""
        with tempfile.TemporaryDirectory() as temp_dir:
            sqlite_path = os.path.join(temp_dir, "source.db")
            duckdb_path = os.path.join(temp_dir, "target.duckdb")
            
            # Create source SQLite database with multiple tables
            sqlite_manager = StorageManagerFactory.create_sqlite_manager(sqlite_path)
            schema = MockStructType([
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType())
            ])
            
            # Create multiple tables
            sqlite_manager.create_table("default", "table1", schema)
            sqlite_manager.insert_data("default", "table1", [{"id": 1, "name": "Alice"}])
            
            sqlite_manager.create_table("default", "table2", schema)
            sqlite_manager.insert_data("default", "table2", [{"id": 2, "name": "Bob"}])
            
            sqlite_manager.close()
            
            # Migrate schema
            with StorageMigrationTool(sqlite_path, duckdb_path) as migration_tool:
                result = migration_tool.migrate_schema("default")
                
                assert result["tables_migrated"] == 2
                assert result["tables_failed"] == 0
                assert result["total_rows_migrated"] == 2
                assert len(result["errors"]) == 0

    def test_full_migration(self):
        """Test full database migration."""
        with tempfile.TemporaryDirectory() as temp_dir:
            sqlite_path = os.path.join(temp_dir, "source.db")
            duckdb_path = os.path.join(temp_dir, "target.duckdb")
            
            # Create source SQLite database
            sqlite_manager = StorageManagerFactory.create_sqlite_manager(sqlite_path)
            schema = MockStructType([
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType())
            ])
            
            # Create tables
            for i in range(3):
                table_name = f"table{i+1}"
                sqlite_manager.create_table("default", table_name, schema)
                sqlite_manager.insert_data("default", table_name, [
                    {"id": i+1, "name": f"User{i+1}"}
                ])
            
            sqlite_manager.close()
            
            # Migrate all
            with StorageMigrationTool(sqlite_path, duckdb_path) as migration_tool:
                result = migration_tool.migrate_all()
                
                assert result["schemas_migrated"] == 1
                assert result["schemas_failed"] == 0
                assert result["tables_migrated"] == 3
                assert result["tables_failed"] == 0
                assert result["total_rows_migrated"] == 3

    def test_migration_validation(self):
        """Test migration validation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            sqlite_path = os.path.join(temp_dir, "source.db")
            duckdb_path = os.path.join(temp_dir, "target.duckdb")
            
            # Create source SQLite database
            sqlite_manager = StorageManagerFactory.create_sqlite_manager(sqlite_path)
            schema = MockStructType([
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType())
            ])
            
            sqlite_manager.create_table("default", "users", schema)
            sqlite_manager.insert_data("default", "users", [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"}
            ])
            sqlite_manager.close()
            
            # Migrate and validate
            with StorageMigrationTool(sqlite_path, duckdb_path) as migration_tool:
                # Migrate table
                migration_tool.migrate_table("default", "users")
                
                # Validate migration
                validation = migration_tool.validate_migration("default")
                
                assert validation["tables_validated"] == 1
                assert validation["tables_failed"] == 0
                assert validation["row_count_matches"] == 1
                assert validation["row_count_mismatches"] == 0
                assert len(validation["errors"]) == 0


class TestMigrationConvenienceFunctions:
    """Test convenience functions for migration."""

    def test_migrate_sqlite_to_duckdb_function(self):
        """Test the convenience function for SQLite to DuckDB migration."""
        with tempfile.TemporaryDirectory() as temp_dir:
            sqlite_path = os.path.join(temp_dir, "source.db")
            duckdb_path = os.path.join(temp_dir, "target.duckdb")
            
            # Create source SQLite database
            sqlite_manager = StorageManagerFactory.create_sqlite_manager(sqlite_path)
            schema = MockStructType([
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType())
            ])
            
            sqlite_manager.create_table("default", "users", schema)
            sqlite_manager.insert_data("default", "users", [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"}
            ])
            sqlite_manager.close()
            
            # Use convenience function
            result = migrate_sqlite_to_duckdb(
                sqlite_path=sqlite_path,
                duckdb_path=duckdb_path,
                backup=False,
                validate=True
            )
            
            assert result["success"] == True
            assert result["migration_results"] is not None
            assert result["validation_results"] is not None
            assert result["error"] is None


if __name__ == "__main__":
    pytest.main([__file__])
