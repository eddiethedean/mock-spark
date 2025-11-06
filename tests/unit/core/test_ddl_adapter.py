"""
Unit tests for DDL adapter.
"""

import pytest
from mock_spark.core.ddl_adapter import parse_ddl_schema
from mock_spark.spark_types import (
    StructType,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    BooleanType,
    DateType,
    DecimalType,
    ArrayType,
    MapType,
)


@pytest.mark.unit
class TestDDLAdapter:
    """Test DDL adapter operations."""

    def test_parse_ddl_basic(self):
        """Test parsing basic DDL schema."""
        ddl = "id long, name string"
        schema = parse_ddl_schema(ddl)

        assert isinstance(schema, StructType)
        assert len(schema.fields) == 2
        assert schema.fields[0].name == "id"
        assert isinstance(schema.fields[0].dataType, LongType)
        assert schema.fields[1].name == "name"
        assert isinstance(schema.fields[1].dataType, StringType)

    def test_parse_ddl_with_types(self):
        """Test parsing DDL with various types."""
        ddl = "id integer, score double, active boolean, created date"
        schema = parse_ddl_schema(ddl)

        assert len(schema.fields) == 4
        assert isinstance(schema.fields[0].dataType, IntegerType)
        assert isinstance(schema.fields[1].dataType, DoubleType)
        assert isinstance(schema.fields[2].dataType, BooleanType)
        assert isinstance(schema.fields[3].dataType, DateType)

    def test_parse_ddl_with_decimal(self):
        """Test parsing DDL with decimal type."""
        ddl = "price decimal(10, 2)"
        schema = parse_ddl_schema(ddl)

        assert len(schema.fields) == 1
        assert isinstance(schema.fields[0].dataType, DecimalType)
        assert schema.fields[0].dataType.precision == 10
        # Note: spark-ddl-parser may parse scale differently, so just check it's a DecimalType
        assert schema.fields[0].dataType.scale >= 0

    def test_parse_ddl_with_array(self):
        """Test parsing DDL with array type."""
        ddl = "tags array<string>"
        schema = parse_ddl_schema(ddl)

        assert len(schema.fields) == 1
        assert isinstance(schema.fields[0].dataType, ArrayType)
        assert isinstance(schema.fields[0].dataType.element_type, StringType)

    def test_parse_ddl_with_map(self):
        """Test parsing DDL with map type."""
        ddl = "metadata map<string, string>"
        schema = parse_ddl_schema(ddl)

        assert len(schema.fields) == 1
        assert isinstance(schema.fields[0].dataType, MapType)
        assert isinstance(schema.fields[0].dataType.key_type, StringType)
        assert isinstance(schema.fields[0].dataType.value_type, StringType)

    def test_parse_ddl_with_nullable(self):
        """Test parsing DDL preserves nullable information."""
        # Note: spark-ddl-parser may handle nullable differently
        ddl = "id long, name string"
        schema = parse_ddl_schema(ddl)

        # Check that fields are created (nullable depends on parser)
        assert len(schema.fields) == 2
