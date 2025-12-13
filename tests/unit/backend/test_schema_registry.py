"""
Unit tests for Schema Registry.
"""

import os
import json
import pytest
import tempfile
from sparkless.backend.polars.schema_registry import SchemaRegistry
from sparkless.spark_types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    BooleanType,
    ArrayType,
    MapType,
    DecimalType,
)


@pytest.mark.unit
class TestSchemaRegistry:
    """Test SchemaRegistry operations."""

    def test_init(self):
        """Test SchemaRegistry initialization."""
        registry = SchemaRegistry("test_path")
        assert registry.storage_path == "test_path"

    def test_get_schema_path(self):
        """Test _get_schema_path generates correct path."""
        registry = SchemaRegistry("base_path")
        path = registry._get_schema_path("schema1", "table1")
        expected = os.path.join("base_path", "schema1", "table1.schema.json")
        assert path == expected

    def test_save_schema_basic(self):
        """Test saving a basic schema."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            registry = SchemaRegistry(tmp_dir)
            schema = StructType(
                [
                    StructField("id", IntegerType(), False),
                    StructField("name", StringType(), True),
                ]
            )

            registry.save_schema("test_schema", "test_table", schema)

            schema_path = os.path.join(tmp_dir, "test_schema", "test_table.schema.json")
            assert os.path.exists(schema_path)

            with open(schema_path) as f:
                data = json.load(f)

            assert len(data["fields"]) == 2
            assert data["fields"][0]["name"] == "id"
            assert data["fields"][0]["type"] == "IntegerType"
            assert data["fields"][0]["nullable"] is False
            assert data["fields"][1]["name"] == "name"
            assert data["fields"][1]["type"] == "StringType"
            assert data["fields"][1]["nullable"] is True

    def test_save_schema_creates_directory(self):
        """Test that save_schema creates directory if needed."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            registry = SchemaRegistry(tmp_dir)
            schema = StructType([StructField("id", IntegerType())])

            registry.save_schema("new_schema", "new_table", schema)

            schema_dir = os.path.join(tmp_dir, "new_schema")
            assert os.path.isdir(schema_dir)

    def test_save_schema_with_array_type(self):
        """Test saving schema with ArrayType."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            registry = SchemaRegistry(tmp_dir)
            schema = StructType([StructField("tags", ArrayType(StringType()), True)])

            registry.save_schema("test_schema", "test_table", schema)

            schema_path = os.path.join(tmp_dir, "test_schema", "test_table.schema.json")
            with open(schema_path) as f:
                data = json.load(f)

            assert data["fields"][0]["type"] == "ArrayType"
            assert (
                data["fields"][0]["elementType"] == "StringType"
            )  # JSON uses camelCase

    def test_save_schema_with_map_type(self):
        """Test saving schema with MapType."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            registry = SchemaRegistry(tmp_dir)
            schema = StructType(
                [StructField("metadata", MapType(StringType(), StringType()), True)]
            )

            registry.save_schema("test_schema", "test_table", schema)

            schema_path = os.path.join(tmp_dir, "test_schema", "test_table.schema.json")
            with open(schema_path) as f:
                data = json.load(f)

            assert data["fields"][0]["type"] == "MapType"
            assert data["fields"][0]["keyType"] == "StringType"
            assert data["fields"][0]["valueType"] == "StringType"

    def test_save_schema_with_decimal_type(self):
        """Test saving schema with DecimalType."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            registry = SchemaRegistry(tmp_dir)
            schema = StructType([StructField("price", DecimalType(10, 2), True)])

            registry.save_schema("test_schema", "test_table", schema)

            schema_path = os.path.join(tmp_dir, "test_schema", "test_table.schema.json")
            with open(schema_path) as f:
                data = json.load(f)

            assert data["fields"][0]["type"] == "DecimalType"
            assert data["fields"][0]["precision"] == 10
            assert data["fields"][0]["scale"] == 2

    def test_save_schema_in_memory_skips(self):
        """Test that save_schema skips for in-memory storage."""
        registry = SchemaRegistry(":memory:")
        schema = StructType([StructField("id", IntegerType())])

        # Should not raise error, just skip
        registry.save_schema("test_schema", "test_table", schema)

    def test_load_schema_basic(self):
        """Test loading a basic schema."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            registry = SchemaRegistry(tmp_dir)
            original_schema = StructType(
                [
                    StructField("id", IntegerType(), False),
                    StructField("name", StringType(), True),
                ]
            )
            registry.save_schema("test_schema", "test_table", original_schema)

            loaded_schema = registry.load_schema("test_schema", "test_table")

            assert loaded_schema is not None
            assert len(loaded_schema.fields) == 2
            assert loaded_schema.fields[0].name == "id"
            assert isinstance(loaded_schema.fields[0].dataType, IntegerType)
            assert loaded_schema.fields[0].nullable is False
            assert loaded_schema.fields[1].name == "name"
            assert isinstance(loaded_schema.fields[1].dataType, StringType)
            assert loaded_schema.fields[1].nullable is True

    def test_load_schema_not_found(self):
        """Test loading non-existent schema returns None."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            registry = SchemaRegistry(tmp_dir)

            loaded_schema = registry.load_schema("nonexistent", "table")

            assert loaded_schema is None

    def test_load_schema_in_memory_returns_none(self):
        """Test that load_schema returns None for in-memory storage."""
        registry = SchemaRegistry(":memory:")

        result = registry.load_schema("test_schema", "test_table")

        assert result is None

    def test_load_schema_with_array_type(self):
        """Test loading schema with ArrayType."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            registry = SchemaRegistry(tmp_dir)
            original_schema = StructType(
                [StructField("tags", ArrayType(StringType()), True)]
            )
            registry.save_schema("test_schema", "test_table", original_schema)

            loaded_schema = registry.load_schema("test_schema", "test_table")

            assert loaded_schema is not None
            assert isinstance(loaded_schema.fields[0].dataType, ArrayType)
            assert isinstance(loaded_schema.fields[0].dataType.element_type, StringType)

    def test_load_schema_with_map_type(self):
        """Test loading schema with MapType."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            registry = SchemaRegistry(tmp_dir)
            original_schema = StructType(
                [StructField("metadata", MapType(StringType(), IntegerType()), True)]
            )
            registry.save_schema("test_schema", "test_table", original_schema)

            loaded_schema = registry.load_schema("test_schema", "test_table")

            assert loaded_schema is not None
            assert isinstance(loaded_schema.fields[0].dataType, MapType)
            assert isinstance(loaded_schema.fields[0].dataType.key_type, StringType)
            assert isinstance(loaded_schema.fields[0].dataType.value_type, IntegerType)

    def test_load_schema_with_decimal_type(self):
        """Test loading schema with DecimalType."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            registry = SchemaRegistry(tmp_dir)
            original_schema = StructType(
                [StructField("price", DecimalType(10, 2), True)]
            )
            registry.save_schema("test_schema", "test_table", original_schema)

            loaded_schema = registry.load_schema("test_schema", "test_table")

            assert loaded_schema is not None
            assert isinstance(loaded_schema.fields[0].dataType, DecimalType)
            assert loaded_schema.fields[0].dataType.precision == 10
            assert loaded_schema.fields[0].dataType.scale == 2

    def test_load_schema_unknown_type_raises_error(self):
        """Test loading schema with unknown type raises ValueError."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            schema_path = os.path.join(tmp_dir, "test_schema", "test_table.schema.json")
            os.makedirs(os.path.dirname(schema_path), exist_ok=True)

            with open(schema_path, "w") as f:
                json.dump(
                    {
                        "fields": [
                            {"name": "field1", "type": "UnknownType", "nullable": True}
                        ]
                    },
                    f,
                )

            registry = SchemaRegistry(tmp_dir)

            with pytest.raises(ValueError, match="Unknown type"):
                registry.load_schema("test_schema", "test_table")

    def test_schema_exists_true(self):
        """Test schema_exists returns True for existing schema."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            registry = SchemaRegistry(tmp_dir)
            schema = StructType([StructField("id", IntegerType())])
            registry.save_schema("test_schema", "test_table", schema)

            assert registry.schema_exists("test_schema", "test_table") is True

    def test_schema_exists_false(self):
        """Test schema_exists returns False for non-existent schema."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            registry = SchemaRegistry(tmp_dir)

            assert registry.schema_exists("nonexistent", "table") is False

    def test_schema_exists_in_memory_returns_false(self):
        """Test that schema_exists returns False for in-memory storage."""
        registry = SchemaRegistry(":memory:")

        assert registry.schema_exists("test_schema", "test_table") is False

    def test_delete_schema(self):
        """Test deleting a schema file."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            registry = SchemaRegistry(tmp_dir)
            schema = StructType([StructField("id", IntegerType())])
            registry.save_schema("test_schema", "test_table", schema)

            assert registry.schema_exists("test_schema", "test_table") is True

            registry.delete_schema("test_schema", "test_table")

            assert registry.schema_exists("test_schema", "test_table") is False

    def test_delete_schema_nonexistent(self):
        """Test deleting non-existent schema doesn't raise error."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            registry = SchemaRegistry(tmp_dir)

            # Should not raise error
            registry.delete_schema("nonexistent", "table")

    def test_delete_schema_in_memory_skips(self):
        """Test that delete_schema skips for in-memory storage."""
        registry = SchemaRegistry(":memory:")

        # Should not raise error, just skip
        registry.delete_schema("test_schema", "test_table")

    def test_save_and_load_complex_schema(self):
        """Test saving and loading a complex schema with multiple types."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            registry = SchemaRegistry(tmp_dir)
            original_schema = StructType(
                [
                    StructField("id", IntegerType(), False),
                    StructField("name", StringType(), True),
                    StructField("active", BooleanType(), True),
                    StructField("score", DoubleType(), True),
                    StructField("tags", ArrayType(StringType()), True),
                    StructField("metadata", MapType(StringType(), StringType()), True),
                    StructField("price", DecimalType(10, 2), True),
                ]
            )
            registry.save_schema("test_schema", "test_table", original_schema)

            loaded_schema = registry.load_schema("test_schema", "test_table")

            assert loaded_schema is not None
            assert len(loaded_schema.fields) == 7
            assert loaded_schema.fields[0].name == "id"
            assert loaded_schema.fields[1].name == "name"
            assert loaded_schema.fields[2].name == "active"
            assert loaded_schema.fields[3].name == "score"
            assert loaded_schema.fields[4].name == "tags"
            assert loaded_schema.fields[5].name == "metadata"
            assert loaded_schema.fields[6].name == "price"
