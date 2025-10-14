"""
Unit tests for DDL schema parser.

Tests the parse_ddl_schema function to ensure it correctly converts
DDL schema strings to MockStructType objects.
"""

import pytest
from mock_spark.core.ddl_parser import parse_ddl_schema
from mock_spark.spark_types import (
    MockStructType,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    BooleanType,
    DateType,
    TimestampType,
    DecimalType,
    BinaryType,
    FloatType,
    ShortType,
    ByteType,
    ArrayType,
    MapType,
)


class TestDDLSchemaParser:
    """Test DDL schema parser."""

    def test_simple_schema(self):
        """Test parsing simple schema with two fields."""
        schema = parse_ddl_schema("id long, name string")
        
        assert isinstance(schema, MockStructType)
        assert len(schema.fields) == 2
        assert schema.fields[0].name == "id"
        assert isinstance(schema.fields[0].dataType, LongType)
        assert schema.fields[1].name == "name"
        assert isinstance(schema.fields[1].dataType, StringType)

    def test_simple_schema_with_colon(self):
        """Test parsing schema with colon separator."""
        schema = parse_ddl_schema("id:long, name:string")
        
        assert isinstance(schema, MockStructType)
        assert len(schema.fields) == 2
        assert schema.fields[0].name == "id"
        assert isinstance(schema.fields[0].dataType, LongType)
        assert schema.fields[1].name == "name"
        assert isinstance(schema.fields[1].dataType, StringType)

    def test_multiple_fields(self):
        """Test parsing schema with multiple fields."""
        schema = parse_ddl_schema("id long, name string, age int, active boolean")
        
        assert isinstance(schema, MockStructType)
        assert len(schema.fields) == 4
        assert schema.fields[0].name == "id"
        assert isinstance(schema.fields[0].dataType, LongType)
        assert schema.fields[1].name == "name"
        assert isinstance(schema.fields[1].dataType, StringType)
        assert schema.fields[2].name == "age"
        assert isinstance(schema.fields[2].dataType, IntegerType)
        assert schema.fields[3].name == "active"
        assert isinstance(schema.fields[3].dataType, BooleanType)

    def test_all_primitive_types(self):
        """Test parsing all primitive types."""
        schema = parse_ddl_schema(
            "str_field string, int_field int, long_field long, "
            "double_field double, bool_field boolean, date_field date, "
            "timestamp_field timestamp, float_field float, short_field short, "
            "byte_field byte, binary_field binary"
        )
        
        assert isinstance(schema, MockStructType)
        assert len(schema.fields) == 11
        
        # Check each field type
        assert isinstance(schema.fields[0].dataType, StringType)
        assert isinstance(schema.fields[1].dataType, IntegerType)
        assert isinstance(schema.fields[2].dataType, LongType)
        assert isinstance(schema.fields[3].dataType, DoubleType)
        assert isinstance(schema.fields[4].dataType, BooleanType)
        assert isinstance(schema.fields[5].dataType, DateType)
        assert isinstance(schema.fields[6].dataType, TimestampType)
        assert isinstance(schema.fields[7].dataType, FloatType)
        assert isinstance(schema.fields[8].dataType, ShortType)
        assert isinstance(schema.fields[9].dataType, ByteType)
        assert isinstance(schema.fields[10].dataType, BinaryType)

    def test_array_type(self):
        """Test parsing array types."""
        schema = parse_ddl_schema("tags array<string>, scores array<long>")
        
        assert isinstance(schema, MockStructType)
        assert len(schema.fields) == 2
        
        assert isinstance(schema.fields[0].dataType, ArrayType)
        assert isinstance(schema.fields[0].dataType.element_type, StringType)
        
        assert isinstance(schema.fields[1].dataType, ArrayType)
        assert isinstance(schema.fields[1].dataType.element_type, LongType)

    def test_map_type(self):
        """Test parsing map types."""
        schema = parse_ddl_schema("metadata map<string,string>, counts map<string,long>")
        
        assert isinstance(schema, MockStructType)
        assert len(schema.fields) == 2
        
        assert isinstance(schema.fields[0].dataType, MapType)
        assert isinstance(schema.fields[0].dataType.key_type, StringType)
        assert isinstance(schema.fields[0].dataType.value_type, StringType)
        
        assert isinstance(schema.fields[1].dataType, MapType)
        assert isinstance(schema.fields[1].dataType.key_type, StringType)
        assert isinstance(schema.fields[1].dataType.value_type, LongType)

    def test_nested_struct(self):
        """Test parsing nested struct types."""
        schema = parse_ddl_schema("address struct<street:string,city:string,zip:int>")
        
        assert isinstance(schema, MockStructType)
        assert len(schema.fields) == 1
        
        struct_type = schema.fields[0].dataType
        assert isinstance(struct_type, MockStructType)
        assert len(struct_type.fields) == 3
        assert struct_type.fields[0].name == "street"
        assert isinstance(struct_type.fields[0].dataType, StringType)
        assert struct_type.fields[1].name == "city"
        assert isinstance(struct_type.fields[1].dataType, StringType)
        assert struct_type.fields[2].name == "zip"
        assert isinstance(struct_type.fields[2].dataType, IntegerType)

    def test_complex_nested(self):
        """Test parsing complex nested structures."""
        schema = parse_ddl_schema(
            "data struct<id:long,items:array<string>,meta:map<string,int>>"
        )
        
        assert isinstance(schema, MockStructType)
        assert len(schema.fields) == 1
        
        struct_type = schema.fields[0].dataType
        assert isinstance(struct_type, MockStructType)
        assert len(struct_type.fields) == 3
        
        # Check id field
        assert struct_type.fields[0].name == "id"
        assert isinstance(struct_type.fields[0].dataType, LongType)
        
        # Check items array field
        assert struct_type.fields[1].name == "items"
        assert isinstance(struct_type.fields[1].dataType, ArrayType)
        assert isinstance(struct_type.fields[1].dataType.element_type, StringType)
        
        # Check meta map field
        assert struct_type.fields[2].name == "meta"
        assert isinstance(struct_type.fields[2].dataType, MapType)
        assert isinstance(struct_type.fields[2].dataType.key_type, StringType)
        assert isinstance(struct_type.fields[2].dataType.value_type, IntegerType)

    def test_decimal_type(self):
        """Test parsing decimal types with precision and scale."""
        schema = parse_ddl_schema("price decimal(10,2), amount decimal(5,0)")
        
        assert isinstance(schema, MockStructType)
        assert len(schema.fields) == 2
        
        assert isinstance(schema.fields[0].dataType, DecimalType)
        assert schema.fields[0].dataType.precision == 10
        assert schema.fields[0].dataType.scale == 2
        
        assert isinstance(schema.fields[1].dataType, DecimalType)
        assert schema.fields[1].dataType.precision == 5
        assert schema.fields[1].dataType.scale == 0

    def test_struct_wrapper_format(self):
        """Test parsing schema with struct<> wrapper."""
        schema = parse_ddl_schema("struct<id:long,name:string>")
        
        assert isinstance(schema, MockStructType)
        assert len(schema.fields) == 2
        assert schema.fields[0].name == "id"
        assert isinstance(schema.fields[0].dataType, LongType)
        assert schema.fields[1].name == "name"
        assert isinstance(schema.fields[1].dataType, StringType)

    def test_empty_schema(self):
        """Test parsing empty schema."""
        schema = parse_ddl_schema("")
        
        assert isinstance(schema, MockStructType)
        assert len(schema.fields) == 0

    def test_whitespace_handling(self):
        """Test that whitespace is handled correctly."""
        schema = parse_ddl_schema("  id   long  ,  name   string  ")
        
        assert isinstance(schema, MockStructType)
        assert len(schema.fields) == 2
        assert schema.fields[0].name == "id"
        assert schema.fields[1].name == "name"

    def test_mixed_format(self):
        """Test mixing colon and space formats."""
        schema = parse_ddl_schema("id long, name:string, age int")
        
        assert isinstance(schema, MockStructType)
        assert len(schema.fields) == 3
        assert schema.fields[0].name == "id"
        assert schema.fields[1].name == "name"
        assert schema.fields[2].name == "age"

    def test_nested_array_in_struct(self):
        """Test nested array inside struct."""
        schema = parse_ddl_schema("person struct<name:string,hobbies:array<string>>")
        
        assert isinstance(schema, MockStructType)
        struct_type = schema.fields[0].dataType
        assert isinstance(struct_type, MockStructType)
        
        hobbies_field = struct_type.fields[1]
        assert hobbies_field.name == "hobbies"
        assert isinstance(hobbies_field.dataType, ArrayType)
        assert isinstance(hobbies_field.dataType.element_type, StringType)

    def test_nested_map_in_struct(self):
        """Test nested map inside struct."""
        schema = parse_ddl_schema("config struct<name:string,settings:map<string,string>>")
        
        assert isinstance(schema, MockStructType)
        struct_type = schema.fields[0].dataType
        assert isinstance(struct_type, MockStructType)
        
        settings_field = struct_type.fields[1]
        assert settings_field.name == "settings"
        assert isinstance(settings_field.dataType, MapType)
        assert isinstance(settings_field.dataType.key_type, StringType)
        assert isinstance(settings_field.dataType.value_type, StringType)

    def test_array_of_structs(self):
        """Test array of structs."""
        schema = parse_ddl_schema("addresses array<struct<street:string,city:string>>")
        
        assert isinstance(schema, MockStructType)
        array_type = schema.fields[0].dataType
        assert isinstance(array_type, ArrayType)
        
        struct_type = array_type.element_type
        assert isinstance(struct_type, MockStructType)
        assert len(struct_type.fields) == 2

    def test_invalid_array_type(self):
        """Test that invalid array type raises error."""
        with pytest.raises(ValueError, match="Unbalanced angle brackets"):
            parse_ddl_schema("tags array<string")

    def test_invalid_map_type(self):
        """Test that invalid map type raises error."""
        with pytest.raises(ValueError, match="Unbalanced angle brackets"):
            parse_ddl_schema("metadata map<string,string")

    def test_invalid_struct_type(self):
        """Test that invalid struct type raises error."""
        with pytest.raises(ValueError, match="Unbalanced angle brackets"):
            parse_ddl_schema("address struct<street:string,city:string")

    def test_invalid_field_definition(self):
        """Test that invalid field definition raises error."""
        with pytest.raises(ValueError, match="Invalid field definition"):
            parse_ddl_schema("invalid")

    def test_bigint_alias(self):
        """Test that bigint is parsed as LongType."""
        schema = parse_ddl_schema("id bigint")
        
        assert isinstance(schema, MockStructType)
        assert isinstance(schema.fields[0].dataType, LongType)

    def test_integer_alias(self):
        """Test that integer is parsed as IntegerType."""
        schema = parse_ddl_schema("age integer")
        
        assert isinstance(schema, MockStructType)
        assert isinstance(schema.fields[0].dataType, IntegerType)

    def test_bool_alias(self):
        """Test that bool is parsed as BooleanType."""
        schema = parse_ddl_schema("active bool")
        
        assert isinstance(schema, MockStructType)
        assert isinstance(schema.fields[0].dataType, BooleanType)

    def test_smallint_alias(self):
        """Test that smallint is parsed as ShortType."""
        schema = parse_ddl_schema("count smallint")
        
        assert isinstance(schema, MockStructType)
        assert isinstance(schema.fields[0].dataType, ShortType)

    def test_tinyint_alias(self):
        """Test that tinyint is parsed as ByteType."""
        schema = parse_ddl_schema("flag tinyint")
        
        assert isinstance(schema, MockStructType)
        assert isinstance(schema.fields[0].dataType, ByteType)

