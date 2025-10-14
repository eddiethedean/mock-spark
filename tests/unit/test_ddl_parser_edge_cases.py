"""
Edge case tests for DDL schema parser.

Tests unusual but valid DDL schema strings to ensure the parser
handles various edge cases robustly.
"""

import pytest
from mock_spark.core.ddl_parser import parse_ddl_schema
from mock_spark.spark_types import (
    MockStructType,
    MockStructField,
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


class TestDDLParserEdgeCases:
    """Test DDL parser with edge cases."""

    # ==================== Whitespace Variations ====================
    
    def test_whitespace_tabs(self):
        """Test schema with tabs instead of spaces."""
        schema = parse_ddl_schema("id\tlong,\tname\tstring")
        assert len(schema.fields) == 2
        assert schema.fields[0].name == "id"
        assert schema.fields[1].name == "name"

    def test_whitespace_newlines(self):
        """Test schema with newlines."""
        schema = parse_ddl_schema("id\nlong,\nname\nstring")
        assert len(schema.fields) == 2
        assert schema.fields[0].name == "id"
        assert schema.fields[1].name == "name"

    def test_whitespace_carriage_return(self):
        """Test schema with carriage returns."""
        schema = parse_ddl_schema("id\rlong,\rname\rstring")
        assert len(schema.fields) == 2

    def test_whitespace_mixed(self):
        """Test schema with mixed whitespace characters."""
        schema = parse_ddl_schema("id\t \nlong,\t \nname\t \nstring")
        assert len(schema.fields) == 2

    def test_whitespace_leading_trailing(self):
        """Test schema with leading and trailing whitespace."""
        schema = parse_ddl_schema("   id long, name string   ")
        assert len(schema.fields) == 2
        assert schema.fields[0].name == "id"
        assert schema.fields[1].name == "name"

    def test_whitespace_excessive(self):
        """Test schema with excessive whitespace."""
        schema = parse_ddl_schema("id        long    ,    name        string")
        assert len(schema.fields) == 2
        assert schema.fields[0].name == "id"
        assert schema.fields[1].name == "name"

    def test_whitespace_around_colon(self):
        """Test schema with whitespace around colon separator."""
        schema = parse_ddl_schema("id : long, name : string")
        assert len(schema.fields) == 2

    # ==================== Case Sensitivity ====================
    
    def test_case_uppercase(self):
        """Test schema with uppercase types."""
        schema = parse_ddl_schema("id LONG, name STRING")
        assert len(schema.fields) == 2
        assert isinstance(schema.fields[0].dataType, LongType)
        assert isinstance(schema.fields[1].dataType, StringType)

    def test_case_mixed(self):
        """Test schema with mixed case types."""
        schema = parse_ddl_schema("id lOnG, name StRiNg")
        assert len(schema.fields) == 2
        assert isinstance(schema.fields[0].dataType, LongType)
        assert isinstance(schema.fields[1].dataType, StringType)

    def test_case_camelcase(self):
        """Test schema with CamelCase types."""
        schema = parse_ddl_schema("id Long, name String")
        assert len(schema.fields) == 2

    # ==================== Field Name Variations ====================
    
    def test_field_name_single_char(self):
        """Test schema with single character field names."""
        schema = parse_ddl_schema("a string, b int, c long")
        assert len(schema.fields) == 3
        assert schema.fields[0].name == "a"
        assert schema.fields[1].name == "b"
        assert schema.fields[2].name == "c"

    def test_field_name_long(self):
        """Test schema with long field names."""
        long_name = "a" * 100
        schema = parse_ddl_schema(f"{long_name} string")
        assert len(schema.fields) == 1
        assert len(schema.fields[0].name) == 100

    def test_field_name_very_long(self):
        """Test schema with very long field names."""
        very_long_name = "field_" * 100 + "name"
        schema = parse_ddl_schema(f"{very_long_name} string")
        assert len(schema.fields) == 1
        assert len(schema.fields[0].name) > 500

    def test_field_name_numbers(self):
        """Test schema with numeric field names."""
        schema = parse_ddl_schema("field1 string, field2 int, field3 long")
        assert len(schema.fields) == 3

    def test_field_name_underscores(self):
        """Test schema with underscores in field names."""
        schema = parse_ddl_schema("field_name string, another_field int")
        assert len(schema.fields) == 2

    def test_field_name_many_underscores(self):
        """Test schema with many underscores."""
        schema = parse_ddl_schema("a_b_c_d_e string")
        assert len(schema.fields) == 1

    def test_field_name_leading_underscore(self):
        """Test schema with leading underscore."""
        schema = parse_ddl_schema("_private string, _internal int")
        assert len(schema.fields) == 2

    # ==================== Complex Nesting ====================
    
    def test_nesting_5_levels(self):
        """Test schema with 5 levels of nesting."""
        schema = parse_ddl_schema(
            "level1 struct<"
            "level2 struct<"
            "level3 struct<"
            "level4 struct<"
            "level5 string"
            ">"
            ">"
            ">"
            ">"
        )
        assert len(schema.fields) == 1
        struct1 = schema.fields[0].dataType
        struct2 = struct1.fields[0].dataType
        struct3 = struct2.fields[0].dataType
        struct4 = struct3.fields[0].dataType
        assert isinstance(struct4.fields[0].dataType, StringType)

    def test_nesting_10_levels(self):
        """Test schema with 10 levels of nesting."""
        nested = "struct<" * 9 + "value string" + ">" * 9
        schema = parse_ddl_schema(f"data {nested}")
        assert len(schema.fields) == 1

    # ==================== Large Schemas ====================
    
    def test_large_schema_50_fields(self):
        """Test schema with 50 fields."""
        fields = ", ".join(f"field{i} string" for i in range(50))
        schema = parse_ddl_schema(fields)
        assert len(schema.fields) == 50
        assert schema.fields[0].name == "field0"
        assert schema.fields[49].name == "field49"

    def test_large_schema_100_fields(self):
        """Test schema with 100 fields."""
        fields = ", ".join(f"field{i} int" for i in range(100))
        schema = parse_ddl_schema(fields)
        assert len(schema.fields) == 100

    def test_large_schema_200_fields(self):
        """Test schema with 200 fields."""
        fields = ", ".join(f"field{i} long" for i in range(200))
        schema = parse_ddl_schema(fields)
        assert len(schema.fields) == 200

    # ==================== Mixed Separators ====================
    
    def test_mixed_separators_space_and_colon(self):
        """Test schema mixing space and colon separators."""
        schema = parse_ddl_schema("id long, name:string, age int")
        assert len(schema.fields) == 3

    def test_all_colon_separators(self):
        """Test schema with all colon separators."""
        schema = parse_ddl_schema("id:long,name:string,age:int")
        assert len(schema.fields) == 3

    # ==================== Type Aliases ====================
    
    def test_bigint_alias(self):
        """Test bigint alias for long."""
        schema = parse_ddl_schema("id bigint")
        assert isinstance(schema.fields[0].dataType, LongType)

    def test_integer_alias(self):
        """Test integer alias for int."""
        schema = parse_ddl_schema("count integer")
        assert isinstance(schema.fields[0].dataType, IntegerType)

    def test_bool_alias(self):
        """Test bool alias for boolean."""
        schema = parse_ddl_schema("active bool")
        assert isinstance(schema.fields[0].dataType, BooleanType)

    def test_smallint_alias(self):
        """Test smallint alias for short."""
        schema = parse_ddl_schema("count smallint")
        assert isinstance(schema.fields[0].dataType, ShortType)

    def test_tinyint_alias(self):
        """Test tinyint alias for byte."""
        schema = parse_ddl_schema("flag tinyint")
        assert isinstance(schema.fields[0].dataType, ByteType)

    # ==================== Decimal Variations ====================
    
    def test_decimal_no_params(self):
        """Test decimal without parameters."""
        schema = parse_ddl_schema("value decimal")
        assert isinstance(schema.fields[0].dataType, DecimalType)

    def test_decimal_with_precision(self):
        """Test decimal with only precision."""
        # Note: This might not be valid PySpark DDL, but test our parser
        schema = parse_ddl_schema("value decimal(10)")
        assert isinstance(schema.fields[0].dataType, DecimalType)

    def test_decimal_max_precision(self):
        """Test decimal with maximum precision."""
        schema = parse_ddl_schema("value decimal(38,18)")
        assert isinstance(schema.fields[0].dataType, DecimalType)
        assert schema.fields[0].dataType.precision == 38
        assert schema.fields[0].dataType.scale == 18

    def test_decimal_large_scale(self):
        """Test decimal with large scale."""
        schema = parse_ddl_schema("value decimal(10,9)")
        assert schema.fields[0].dataType.scale == 9

    # ==================== Boundary Conditions ====================
    
    def test_single_field(self):
        """Test schema with single field."""
        schema = parse_ddl_schema("id long")
        assert len(schema.fields) == 1
        assert schema.fields[0].name == "id"

    def test_empty_schema(self):
        """Test empty schema."""
        schema = parse_ddl_schema("")
        assert len(schema.fields) == 0

    def test_all_primitive_types(self):
        """Test schema with all primitive types."""
        schema = parse_ddl_schema(
            "str_field string, int_field int, long_field long, "
            "double_field double, bool_field boolean, date_field date, "
            "timestamp_field timestamp, float_field float, short_field short, "
            "byte_field byte, binary_field binary"
        )
        assert len(schema.fields) == 11

    def test_all_complex_types(self):
        """Test schema with all complex types."""
        schema = parse_ddl_schema(
            "arr_field array<string>, "
            "map_field map<string,int>, "
            "struct_field struct<id:long,name:string>"
        )
        assert len(schema.fields) == 3
        assert isinstance(schema.fields[0].dataType, ArrayType)
        assert isinstance(schema.fields[1].dataType, MapType)
        assert isinstance(schema.fields[2].dataType, MockStructType)

    # ==================== Special Characters ====================
    
    def test_unicode_field_names(self):
        """Test schema with unicode characters in field names."""
        # Note: PySpark may not support this, but test our parser
        schema = parse_ddl_schema("café string, naïve int")
        assert len(schema.fields) == 2

    def test_emoji_field_names(self):
        """Test schema with emoji in field names."""
        schema = parse_ddl_schema("user_😀 string, data_📊 long")
        assert len(schema.fields) == 2

    # ==================== Complex Combinations ====================
    
    def test_array_of_structs(self):
        """Test array of structs."""
        schema = parse_ddl_schema(
            "users array<struct<id:long,name:string,age:int>>"
        )
        assert len(schema.fields) == 1
        arr_type = schema.fields[0].dataType
        assert isinstance(arr_type, ArrayType)
        assert isinstance(arr_type.element_type, MockStructType)

    def test_map_of_arrays(self):
        """Test map of arrays."""
        schema = parse_ddl_schema(
            "categories map<string,array<int>>"
        )
        assert len(schema.fields) == 1
        map_type = schema.fields[0].dataType
        assert isinstance(map_type, MapType)
        assert isinstance(map_type.value_type, ArrayType)

    def test_struct_in_array_in_map(self):
        """Test struct in array in map."""
        schema = parse_ddl_schema(
            "data map<string,array<struct<id:long,name:string>>>"
        )
        assert len(schema.fields) == 1

    def test_deeply_nested_mixed(self):
        """Test deeply nested mixed types."""
        schema = parse_ddl_schema(
            "complex struct<"
            "items array<map<string,struct<id:long,values:array<int>>>>"
            ">"
        )
        assert len(schema.fields) == 1

    # ==================== Struct Wrapper Format ====================
    
    def test_struct_wrapper_simple(self):
        """Test struct wrapper format."""
        schema = parse_ddl_schema("struct<id:long,name:string>")
        assert len(schema.fields) == 2

    def test_struct_wrapper_nested(self):
        """Test nested struct wrapper format."""
        schema = parse_ddl_schema(
            "struct<id:long,address:struct<street:string,city:string>>"
        )
        assert len(schema.fields) == 2

    # ==================== Multiple Complex Types ====================
    
    def test_multiple_arrays(self):
        """Test multiple array fields."""
        schema = parse_ddl_schema(
            "tags array<string>, scores array<int>, ids array<long>"
        )
        assert len(schema.fields) == 3
        assert all(isinstance(f.dataType, ArrayType) for f in schema.fields)

    def test_multiple_maps(self):
        """Test multiple map fields."""
        schema = parse_ddl_schema(
            "metadata map<string,string>, counts map<string,int>"
        )
        assert len(schema.fields) == 2
        assert all(isinstance(f.dataType, MapType) for f in schema.fields)

    def test_multiple_structs(self):
        """Test multiple struct fields."""
        schema = parse_ddl_schema(
            "address struct<street:string,city:string>, "
            "contact struct<email:string,phone:string>"
        )
        assert len(schema.fields) == 2
        assert all(isinstance(f.dataType, MockStructType) for f in schema.fields)

    # ==================== Wide Structs ====================
    
    def test_wide_struct_20_fields(self):
        """Test struct with 20 fields."""
        fields = ", ".join(f"field{i}:string" for i in range(20))
        schema = parse_ddl_schema(f"data struct<{fields}>")
        struct_type = schema.fields[0].dataType
        assert len(struct_type.fields) == 20

    def test_wide_struct_50_fields(self):
        """Test struct with 50 fields."""
        fields = ", ".join(f"field{i}:int" for i in range(50))
        schema = parse_ddl_schema(f"data struct<{fields}>")
        struct_type = schema.fields[0].dataType
        assert len(struct_type.fields) == 50

    # ==================== Alternating Types ====================
    
    def test_alternating_simple_complex(self):
        """Test alternating simple and complex types."""
        schema = parse_ddl_schema(
            "id long, "
            "tags array<string>, "
            "name string, "
            "metadata map<string,string>, "
            "age int, "
            "address struct<street:string,city:string>"
        )
        assert len(schema.fields) == 6
        assert isinstance(schema.fields[0].dataType, LongType)
        assert isinstance(schema.fields[1].dataType, ArrayType)
        assert isinstance(schema.fields[2].dataType, StringType)
        assert isinstance(schema.fields[3].dataType, MapType)
        assert isinstance(schema.fields[4].dataType, IntegerType)
        assert isinstance(schema.fields[5].dataType, MockStructType)

    # ==================== Special Type Combinations ====================
    
    def test_date_timestamp_together(self):
        """Test date and timestamp types together."""
        schema = parse_ddl_schema(
            "birth_date date, created_at timestamp, updated_at timestamp"
        )
        assert len(schema.fields) == 3
        assert isinstance(schema.fields[0].dataType, DateType)
        assert isinstance(schema.fields[1].dataType, TimestampType)
        assert isinstance(schema.fields[2].dataType, TimestampType)

    def test_numeric_types_all(self):
        """Test all numeric types together."""
        schema = parse_ddl_schema(
            "tiny tinyint, small smallint, "
            "medium int, large bigint, "
            "float_val float, double_val double, "
            "decimal_val decimal(10,2)"
        )
        assert len(schema.fields) == 7

    def test_binary_with_others(self):
        """Test binary type with other types."""
        schema = parse_ddl_schema(
            "id long, data binary, hash string"
        )
        assert len(schema.fields) == 3
        assert isinstance(schema.fields[1].dataType, BinaryType)

