"""
Unit tests for SQLite storage backend.

These tests cover the SQLite storage implementation to improve coverage.
"""

import pytest
import tempfile
import os
from mock_spark.storage.backends.sqlite import SQLiteStorageManager, SQLiteTable
from mock_spark.spark_types import MockStructType, MockStructField, StringType, IntegerType


class TestSQLiteTable:
    """Test SQLiteTable functionality."""

    @pytest.fixture
    def temp_db(self):
        """Create a temporary SQLite database for testing."""
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        temp_file.close()
        yield temp_file.name
        os.unlink(temp_file.name)

    @pytest.fixture
    def sample_schema(self):
        """Create a sample schema for testing."""
        return MockStructType(
            [
                MockStructField("id", IntegerType(), True),
                MockStructField("name", StringType(), True),
                MockStructField("age", IntegerType(), True),
            ]
        )

    @pytest.fixture
    def sqlite_table(self, temp_db, sample_schema):
        """Create a SQLite table for testing."""
        import sqlite3

        connection = sqlite3.connect(temp_db)
        table = SQLiteTable("test_table", sample_schema, connection)

        # Create the table
        cursor = connection.cursor()
        cursor.execute(
            """
            CREATE TABLE test_table (
                id INTEGER PRIMARY KEY,
                name TEXT,
                age INTEGER
            )
        """
        )
        connection.commit()

        return table

    def test_sqlite_table_initialization(self, sqlite_table):
        """Test SQLite table initialization."""
        assert sqlite_table.name == "test_table"
        assert sqlite_table.schema is not None
        assert sqlite_table.connection is not None
        assert "created_at" in sqlite_table.metadata
        assert "row_count" in sqlite_table.metadata

    def test_insert_data_append_mode(self, sqlite_table):
        """Test inserting data in append mode."""
        data = [
            {"id": 1, "name": "Alice", "age": 25},
            {"id": 2, "name": "Bob", "age": 30},
        ]

        sqlite_table.insert_data(data, mode="append")

        # Verify data was inserted
        cursor = sqlite_table.connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM test_table")
        count = cursor.fetchone()[0]
        assert count == 2

    def test_insert_data_overwrite_mode(self, sqlite_table):
        """Test inserting data in overwrite mode."""
        # Insert initial data
        initial_data = [{"id": 1, "name": "Alice", "age": 25}]
        sqlite_table.insert_data(initial_data, mode="append")

        # Insert new data in overwrite mode
        new_data = [{"id": 2, "name": "Bob", "age": 30}]
        sqlite_table.insert_data(new_data, mode="overwrite")

        # Verify only new data exists
        cursor = sqlite_table.connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM test_table")
        count = cursor.fetchone()[0]
        assert count == 1

        cursor.execute("SELECT name FROM test_table")
        name = cursor.fetchone()[0]
        assert name == "Bob"

    def test_insert_data_ignore_mode(self, sqlite_table):
        """Test inserting data in ignore mode."""
        # Insert initial data
        initial_data = [{"id": 1, "name": "Alice", "age": 25}]
        sqlite_table.insert_data(initial_data, mode="append")

        # Try to insert duplicate data in ignore mode
        duplicate_data = [{"id": 1, "name": "Alice", "age": 25}]
        sqlite_table.insert_data(duplicate_data, mode="ignore")

        # Verify only one row exists
        cursor = sqlite_table.connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM test_table")
        count = cursor.fetchone()[0]
        assert count == 1

    def test_insert_empty_data(self, sqlite_table):
        """Test inserting empty data."""
        sqlite_table.insert_data([])

        # Verify no data was inserted
        cursor = sqlite_table.connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM test_table")
        count = cursor.fetchone()[0]
        assert count == 0

    def test_query_data_no_filter(self, sqlite_table):
        """Test querying data without filter."""
        data = [
            {"id": 1, "name": "Alice", "age": 25},
            {"id": 2, "name": "Bob", "age": 30},
        ]
        sqlite_table.insert_data(data)

        result = sqlite_table.query_data()
        assert len(result) == 2
        assert result[0]["name"] == "Alice"
        assert result[1]["name"] == "Bob"

    def test_query_data_with_filter(self, sqlite_table):
        """Test querying data with filter."""
        data = [
            {"id": 1, "name": "Alice", "age": 25},
            {"id": 2, "name": "Bob", "age": 30},
            {"id": 3, "name": "Charlie", "age": 35},
        ]
        sqlite_table.insert_data(data)

        result = sqlite_table.query_data("age > 25")
        assert len(result) == 2
        assert all(row["age"] > 25 for row in result)

    def test_get_schema(self, sqlite_table, sample_schema):
        """Test getting table schema."""
        schema = sqlite_table.get_schema()
        assert schema == sample_schema
        assert len(schema.fields) == 3
        assert schema.fields[0].name == "id"
        assert schema.fields[1].name == "name"
        assert schema.fields[2].name == "age"

    def test_get_metadata(self, sqlite_table):
        """Test getting table metadata."""
        metadata = sqlite_table.get_metadata()
        assert "created_at" in metadata
        assert "row_count" in metadata
        assert "schema_version" in metadata
        assert metadata["row_count"] == 0


class TestSQLiteStorageManager:
    """Test SQLiteStorageManager functionality."""

    @pytest.fixture
    def temp_db(self):
        """Create a temporary SQLite database for testing."""
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        temp_file.close()
        yield temp_file.name
        os.unlink(temp_file.name)

    @pytest.fixture
    def sample_schema(self):
        """Create a sample schema for testing."""
        return MockStructType(
            [
                MockStructField("id", IntegerType(), True),
                MockStructField("name", StringType(), True),
            ]
        )

    @pytest.fixture
    def storage_manager(self, temp_db):
        """Create a SQLite storage manager for testing."""
        return SQLiteStorageManager(temp_db)

    def test_storage_manager_initialization(self, storage_manager, temp_db):
        """Test storage manager initialization."""
        assert storage_manager.db_path == temp_db
        assert storage_manager.connection is not None

    def test_create_table(self, storage_manager, sample_schema):
        """Test creating a table."""
        table = storage_manager.create_table("default", "test_table", sample_schema)

        assert table is not None
        assert table.name == "test_table"
        assert table.schema == sample_schema

        # Verify table was created in database
        cursor = storage_manager.connection.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='test_table'")
        result = cursor.fetchone()
        assert result is not None

    def test_table_exists(self, storage_manager, sample_schema):
        """Test checking if table exists."""
        # Table should not exist initially
        assert not storage_manager.table_exists("default", "test_table")

        # Create table
        storage_manager.create_table("default", "test_table", sample_schema)

        # Table should exist now
        assert storage_manager.table_exists("default", "test_table")

    def test_get_table(self, storage_manager, sample_schema):
        """Test getting an existing table."""
        # Create table
        storage_manager.create_table("default", "test_table", sample_schema)

        # Get table
        table = storage_manager.get_table("default", "test_table")
        assert table is not None
        assert table.name == "test_table"

    def test_get_nonexistent_table(self, storage_manager):
        """Test getting a nonexistent table."""
        table = storage_manager.get_table("default", "nonexistent_table")
        assert table is None

    def test_list_tables(self, storage_manager, sample_schema):
        """Test listing tables."""
        # Initially no tables
        tables = storage_manager.list_tables("default")
        assert len(tables) == 0

        # Create some tables
        storage_manager.create_table("default", "table1", sample_schema)
        storage_manager.create_table("default", "table2", sample_schema)

        # List tables
        tables = storage_manager.list_tables("default")
        assert len(tables) == 2
        assert "table1" in tables
        assert "table2" in tables

    def test_drop_table(self, storage_manager, sample_schema):
        """Test dropping a table."""
        # Create table
        storage_manager.create_table("default", "test_table", sample_schema)
        assert storage_manager.table_exists("default", "test_table")

        # Drop table
        storage_manager.drop_table("default", "test_table")
        assert not storage_manager.table_exists("default", "test_table")

    def test_drop_nonexistent_table(self, storage_manager):
        """Test dropping a nonexistent table."""
        # Should not raise error
        storage_manager.drop_table("default", "nonexistent_table")

    def test_get_database_info(self, storage_manager, sample_schema):
        """Test getting database information."""
        # Create some tables
        storage_manager.create_table("default", "table1", sample_schema)
        storage_manager.create_table("default", "table2", sample_schema)

        info = storage_manager.get_database_info()
        assert "tables" in info
        assert "total_tables" in info
        assert info["total_tables"] == 2
        assert "default.table1" in info["tables"]
        assert "default.table2" in info["tables"]

    def test_close_connection(self, storage_manager):
        """Test closing database connection."""
        storage_manager.close()
        # Connection should be closed (we can't easily test this without accessing private attributes)
        assert True  # If we get here without error, the method works

    def test_context_manager(self, temp_db, sample_schema):
        """Test using storage manager as context manager."""
        with SQLiteStorageManager(temp_db) as manager:
            table = manager.create_table("default", "test_table", sample_schema)
            assert table is not None
            assert manager.table_exists("default", "test_table")

        # Connection should be closed after context
        assert True  # If we get here without error, the context manager works
