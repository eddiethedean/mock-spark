"""
Tests for Mock Storage implementation.
"""

import pytest
from mock_spark.storage import MockStorageManager, MockTable
from mock_spark.spark_types import MockStructType, MockStructField, StringType, IntegerType, DoubleType


class TestMockTable:
    """Test MockTable."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.schema = MockStructType([
            MockStructField("id", IntegerType()),
            MockStructField("name", StringType()),
            MockStructField("age", IntegerType())
        ])
        self.table = MockTable("test_schema", "users", self.schema)
    
    def test_table_creation(self):
        """Test creating MockTable."""
        assert self.table.schema_name == "test_schema"
        assert self.table.table_name == "users"
        assert self.table.schema == self.schema
        assert self.table.data == []
        assert self.table.fqn == "test_schema.users"
    
    def test_insert_data(self):
        """Test inserting data."""
        data = [
            {"id": 1, "name": "Alice", "age": 25},
            {"id": 2, "name": "Bob", "age": 30}
        ]
        
        self.table.insert_data(data)
        assert len(self.table.data) == 2
        assert self.table.data[0]["id"] == 1
        assert self.table.data[0]["name"] == "Alice"
        assert self.table.data[1]["id"] == 2
        assert self.table.data[1]["name"] == "Bob"
    
    def test_insert_data_append(self):
        """Test inserting data in append mode."""
        data1 = [{"id": 1, "name": "Alice", "age": 25}]
        data2 = [{"id": 2, "name": "Bob", "age": 30}]
        
        self.table.insert_data(data1, mode="append")
        assert len(self.table.data) == 1
        
        self.table.insert_data(data2, mode="append")
        assert len(self.table.data) == 2
    
    def test_insert_data_overwrite(self):
        """Test inserting data in overwrite mode."""
        data1 = [{"id": 1, "name": "Alice", "age": 25}]
        data2 = [{"id": 2, "name": "Bob", "age": 30}]
        
        self.table.insert_data(data1, mode="overwrite")
        assert len(self.table.data) == 1
        
        self.table.insert_data(data2, mode="overwrite")
        assert len(self.table.data) == 1
        assert self.table.data[0]["id"] == 2
        assert self.table.data[0]["name"] == "Bob"
    
    def test_query_data(self):
        """Test querying data."""
        data = [
            {"id": 1, "name": "Alice", "age": 25},
            {"id": 2, "name": "Bob", "age": 30},
            {"id": 3, "name": "Charlie", "age": 35}
        ]
        self.table.insert_data(data)
        
        # Query all data
        result = self.table.query_data()
        assert len(result) == 3
        
        # Query with filter
        result = self.table.query_data("age > 30")
        assert len(result) == 1
        assert result[0]["name"] == "Charlie"
    
    def test_get_schema(self):
        """Test getting schema."""
        schema = self.table.get_schema()
        assert schema == self.schema
        assert len(schema) == 3
        assert schema.fieldNames() == ["id", "name", "age"]
    
    def test_get_metadata(self):
        """Test getting metadata."""
        metadata = self.table.get_metadata()
        assert metadata["schema_name"] == "test_schema"
        assert metadata["table_name"] == "users"
        assert metadata["fqn"] == "test_schema.users"
        assert metadata["row_count"] == 0
        assert metadata["column_count"] == 3
    
    def test_get_metadata_with_data(self):
        """Test getting metadata with data."""
        data = [{"id": 1, "name": "Alice", "age": 25}]
        self.table.insert_data(data)
        
        metadata = self.table.get_metadata()
        assert metadata["row_count"] == 1
        assert metadata["column_count"] == 3


class TestMockStorageManager:
    """Test MockStorageManager."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.storage = MockStorageManager()
        self.schema = MockStructType([
            MockStructField("id", IntegerType()),
            MockStructField("name", StringType()),
            MockStructField("age", IntegerType())
        ])
    
    def test_storage_creation(self):
        """Test creating MockStorageManager."""
        assert self.storage.schemas == {}
        assert self.storage.tables == {}
    
    def test_create_schema(self):
        """Test creating schema."""
        self.storage.create_schema("test_schema")
        assert "test_schema" in self.storage.schemas
        assert self.storage.schemas["test_schema"] == []
    
    def test_create_schema_if_not_exists(self):
        """Test creating schema if not exists."""
        # First creation
        self.storage.create_schema("test_schema")
        assert "test_schema" in self.storage.schemas
        
        # Second creation should not fail
        self.storage.create_schema("test_schema")
        assert "test_schema" in self.storage.schemas
    
    def test_schema_exists(self):
        """Test checking if schema exists."""
        assert not self.storage.schema_exists("test_schema")
        
        self.storage.create_schema("test_schema")
        assert self.storage.schema_exists("test_schema")
    
    def test_list_schemas(self):
        """Test listing schemas."""
        assert self.storage.list_schemas() == []
        
        self.storage.create_schema("schema1")
        self.storage.create_schema("schema2")
        
        schemas = self.storage.list_schemas()
        assert "schema1" in schemas
        assert "schema2" in schemas
        assert len(schemas) == 2
    
    def test_create_table(self):
        """Test creating table."""
        self.storage.create_schema("test_schema")
        self.storage.create_table("test_schema", "users", self.schema)
        
        assert "test_schema" in self.storage.tables
        assert "users" in self.storage.tables["test_schema"]
        
        table = self.storage.tables["test_schema"]["users"]
        assert table.schema_name == "test_schema"
        assert table.table_name == "users"
        assert table.schema == self.schema
    
    def test_create_table_if_not_exists(self):
        """Test creating table if not exists."""
        self.storage.create_schema("test_schema")
        
        # First creation
        self.storage.create_table("test_schema", "users", self.schema)
        assert "users" in self.storage.tables["test_schema"]
        
        # Second creation should not fail
        self.storage.create_table("test_schema", "users", self.schema)
        assert "users" in self.storage.tables["test_schema"]
    
    def test_table_exists(self):
        """Test checking if table exists."""
        assert not self.storage.table_exists("test_schema", "users")
        
        self.storage.create_schema("test_schema")
        self.storage.create_table("test_schema", "users", self.schema)
        assert self.storage.table_exists("test_schema", "users")
    
    def test_get_table(self):
        """Test getting table."""
        self.storage.create_schema("test_schema")
        self.storage.create_table("test_schema", "users", self.schema)
        
        table = self.storage.get_table("test_schema", "users")
        assert table is not None
        assert table.schema_name == "test_schema"
        assert table.table_name == "users"
    
    def test_get_table_nonexistent(self):
        """Test getting nonexistent table."""
        table = self.storage.get_table("nonexistent", "table")
        assert table is None
    
    def test_drop_table(self):
        """Test dropping table."""
        self.storage.create_schema("test_schema")
        self.storage.create_table("test_schema", "users", self.schema)
        
        assert self.storage.table_exists("test_schema", "users")
        
        self.storage.drop_table("test_schema", "users")
        assert not self.storage.table_exists("test_schema", "users")
    
    def test_drop_table_nonexistent(self):
        """Test dropping nonexistent table."""
        # Should not raise exception
        self.storage.drop_table("nonexistent", "table")
    
    def test_insert_data(self):
        """Test inserting data into table."""
        self.storage.create_schema("test_schema")
        self.storage.create_table("test_schema", "users", self.schema)
        
        data = [{"id": 1, "name": "Alice", "age": 25}]
        self.storage.insert_data("test_schema", "users", data)
        
        table = self.storage.get_table("test_schema", "users")
        assert len(table.data) == 1
        assert table.data[0]["id"] == 1
        assert table.data[0]["name"] == "Alice"
    
    def test_insert_data_append(self):
        """Test inserting data in append mode."""
        self.storage.create_schema("test_schema")
        self.storage.create_table("test_schema", "users", self.schema)
        
        data1 = [{"id": 1, "name": "Alice", "age": 25}]
        data2 = [{"id": 2, "name": "Bob", "age": 30}]
        
        self.storage.insert_data("test_schema", "users", data1, mode="append")
        self.storage.insert_data("test_schema", "users", data2, mode="append")
        
        table = self.storage.get_table("test_schema", "users")
        assert len(table.data) == 2
    
    def test_insert_data_overwrite(self):
        """Test inserting data in overwrite mode."""
        self.storage.create_schema("test_schema")
        self.storage.create_table("test_schema", "users", self.schema)
        
        data1 = [{"id": 1, "name": "Alice", "age": 25}]
        data2 = [{"id": 2, "name": "Bob", "age": 30}]
        
        self.storage.insert_data("test_schema", "users", data1, mode="overwrite")
        self.storage.insert_data("test_schema", "users", data2, mode="overwrite")
        
        table = self.storage.get_table("test_schema", "users")
        assert len(table.data) == 1
        assert table.data[0]["id"] == 2
        assert table.data[0]["name"] == "Bob"
    
    def test_query_table(self):
        """Test querying table."""
        self.storage.create_schema("test_schema")
        self.storage.create_table("test_schema", "users", self.schema)
        
        data = [
            {"id": 1, "name": "Alice", "age": 25},
            {"id": 2, "name": "Bob", "age": 30},
            {"id": 3, "name": "Charlie", "age": 35}
        ]
        self.storage.insert_data("test_schema", "users", data)
        
        # Query all data
        result = self.storage.query_table("test_schema", "users")
        assert len(result) == 3
        
        # Query with filter
        result = self.storage.query_table("test_schema", "users", "age > 30")
        assert len(result) == 1
        assert result[0]["name"] == "Charlie"
    
    def test_get_table_schema(self):
        """Test getting table schema."""
        self.storage.create_schema("test_schema")
        self.storage.create_table("test_schema", "users", self.schema)
        
        schema = self.storage.get_table_schema("test_schema", "users")
        assert schema == self.schema
        assert len(schema) == 3
        assert schema.fieldNames() == ["id", "name", "age"]
    
    def test_get_table_metadata(self):
        """Test getting table metadata."""
        self.storage.create_schema("test_schema")
        self.storage.create_table("test_schema", "users", self.schema)
        
        metadata = self.storage.get_table_metadata("test_schema", "users")
        assert metadata["schema_name"] == "test_schema"
        assert metadata["table_name"] == "users"
        assert metadata["fqn"] == "test_schema.users"
        assert metadata["row_count"] == 0
        assert metadata["column_count"] == 3
    
    def test_get_table_metadata_with_data(self):
        """Test getting table metadata with data."""
        self.storage.create_schema("test_schema")
        self.storage.create_table("test_schema", "users", self.schema)
        
        data = [{"id": 1, "name": "Alice", "age": 25}]
        self.storage.insert_data("test_schema", "users", data)
        
        metadata = self.storage.get_table_metadata("test_schema", "users")
        assert metadata["row_count"] == 1
        assert metadata["column_count"] == 3
    
    def test_list_tables(self):
        """Test listing tables in schema."""
        self.storage.create_schema("test_schema")
        self.storage.create_table("test_schema", "users", self.schema)
        self.storage.create_table("test_schema", "orders", self.schema)
        
        tables = self.storage.list_tables("test_schema")
        assert "users" in tables
        assert "orders" in tables
        assert len(tables) == 2
    
    def test_list_tables_nonexistent_schema(self):
        """Test listing tables in nonexistent schema."""
        tables = self.storage.list_tables("nonexistent")
        assert tables == []
    
    def test_clear_all(self):
        """Test clearing all data."""
        self.storage.create_schema("test_schema")
        self.storage.create_table("test_schema", "users", self.schema)
        
        data = [{"id": 1, "name": "Alice", "age": 25}]
        self.storage.insert_data("test_schema", "users", data)
        
        assert len(self.storage.schemas) > 0
        assert len(self.storage.tables) > 0
        
        self.storage.clear_all()
        assert len(self.storage.schemas) == 0
        assert len(self.storage.tables) == 0
    
    def test_get_stats(self):
        """Test getting storage statistics."""
        self.storage.create_schema("test_schema")
        self.storage.create_table("test_schema", "users", self.schema)
        
        data = [{"id": 1, "name": "Alice", "age": 25}]
        self.storage.insert_data("test_schema", "users", data)
        
        stats = self.storage.get_stats()
        assert stats["total_schemas"] == 1
        assert stats["total_tables"] == 1
        assert stats["total_rows"] == 1