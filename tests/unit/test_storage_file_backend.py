"""Unit tests for file storage backend."""

import pytest
import tempfile
import os
from mock_spark.storage.backends.file import FileStorageBackend
from mock_spark.spark_types import MockStructType, MockStructField, StringType, IntegerType


def test_file_backend_creation():
    """Test FileStorageBackend can be created."""
    with tempfile.TemporaryDirectory() as tmpdir:
        backend = FileStorageBackend(base_path=tmpdir)
        assert backend is not None


def test_file_backend_base_path():
    """Test FileStorageBackend stores base path."""
    with tempfile.TemporaryDirectory() as tmpdir:
        backend = FileStorageBackend(base_path=tmpdir)
        assert tmpdir in str(backend.base_path)


def test_file_backend_create_table():
    """Test creating a table in file backend."""
    with tempfile.TemporaryDirectory() as tmpdir:
        backend = FileStorageBackend(base_path=tmpdir)
        schema = MockStructType([
            MockStructField("id", IntegerType()),
            MockStructField("name", StringType())
        ])
        
        table = backend.create_table("test_table", schema)
        assert table is not None


def test_file_backend_get_table():
    """Test getting a table from file backend."""
    with tempfile.TemporaryDirectory() as tmpdir:
        backend = FileStorageBackend(base_path=tmpdir)
        schema = MockStructType([MockStructField("id", IntegerType())])
        
        backend.create_table("test_table", schema)
        table = backend.get_table("test_table")
        assert table is not None


def test_file_backend_table_exists():
    """Test checking if table exists."""
    with tempfile.TemporaryDirectory() as tmpdir:
        backend = FileStorageBackend(base_path=tmpdir)
        schema = MockStructType([MockStructField("id", IntegerType())])
        
        assert not backend.table_exists("test_table")
        backend.create_table("test_table", schema)
        assert backend.table_exists("test_table")


def test_file_backend_list_tables():
    """Test listing tables."""
    with tempfile.TemporaryDirectory() as tmpdir:
        backend = FileStorageBackend(base_path=tmpdir)
        schema = MockStructType([MockStructField("id", IntegerType())])
        
        backend.create_table("table1", schema)
        backend.create_table("table2", schema)
        
        tables = backend.list_tables()
        assert "table1" in tables
        assert "table2" in tables


def test_file_backend_drop_table():
    """Test dropping a table."""
    with tempfile.TemporaryDirectory() as tmpdir:
        backend = FileStorageBackend(base_path=tmpdir)
        schema = MockStructType([MockStructField("id", IntegerType())])
        
        backend.create_table("test_table", schema)
        assert backend.table_exists("test_table")
        
        backend.drop_table("test_table")
        assert not backend.table_exists("test_table")


def test_file_backend_insert_data():
    """Test inserting data into table."""
    with tempfile.TemporaryDirectory() as tmpdir:
        backend = FileStorageBackend(base_path=tmpdir)
        schema = MockStructType([
            MockStructField("id", IntegerType()),
            MockStructField("name", StringType())
        ])
        
        table = backend.create_table("test_table", schema)
        data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        table.insert(data)
        
        result = table.select()
        assert len(result) == 2


def test_file_backend_select_data():
    """Test selecting data from table."""
    with tempfile.TemporaryDirectory() as tmpdir:
        backend = FileStorageBackend(base_path=tmpdir)
        schema = MockStructType([MockStructField("id", IntegerType())])
        
        table = backend.create_table("test_table", schema)
        table.insert([{"id": 1}, {"id": 2}])
        
        result = table.select()
        assert len(result) == 2


def test_file_backend_empty_table():
    """Test selecting from empty table."""
    with tempfile.TemporaryDirectory() as tmpdir:
        backend = FileStorageBackend(base_path=tmpdir)
        schema = MockStructType([MockStructField("id", IntegerType())])
        
        table = backend.create_table("test_table", schema)
        result = table.select()
        assert len(result) == 0


def test_file_backend_persistence():
    """Test data persists across backend instances."""
    with tempfile.TemporaryDirectory() as tmpdir:
        schema = MockStructType([MockStructField("id", IntegerType())])
        
        # Create and populate table
        backend1 = FileStorageBackend(base_path=tmpdir)
        table1 = backend1.create_table("test_table", schema)
        table1.insert([{"id": 1}])
        
        # Create new backend instance
        backend2 = FileStorageBackend(base_path=tmpdir)
        table2 = backend2.get_table("test_table")
        result = table2.select()
        assert len(result) == 1


def test_file_backend_multiple_inserts():
    """Test multiple insert operations."""
    with tempfile.TemporaryDirectory() as tmpdir:
        backend = FileStorageBackend(base_path=tmpdir)
        schema = MockStructType([MockStructField("id", IntegerType())])
        
        table = backend.create_table("test_table", schema)
        table.insert([{"id": 1}])
        table.insert([{"id": 2}])
        table.insert([{"id": 3}])
        
        result = table.select()
        assert len(result) == 3


def test_file_backend_clear_table():
    """Test clearing table data."""
    with tempfile.TemporaryDirectory() as tmpdir:
        backend = FileStorageBackend(base_path=tmpdir)
        schema = MockStructType([MockStructField("id", IntegerType())])
        
        table = backend.create_table("test_table", schema)
        table.insert([{"id": 1}, {"id": 2}])
        
        table.clear()
        result = table.select()
        assert len(result) == 0

