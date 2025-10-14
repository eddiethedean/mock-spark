"""
Complex scenario tests for DDL schema parser.

Tests sophisticated combinations of nested types and complex structures
to ensure the parser handles all PySpark DDL features correctly.
"""

from mock_spark.core.ddl_parser import parse_ddl_schema
from mock_spark.spark_types import (
    MockStructType,
    ArrayType,
    MapType,
)


class TestDDLParserComplex:
    """Test DDL parser with complex scenarios."""

    # ==================== Arrays of Arrays ====================
    
    def test_array_of_arrays(self):
        """Test array of arrays."""
        schema = parse_ddl_schema("matrix array<array<int>>")
        assert len(schema.fields) == 1
        outer_array = schema.fields[0].dataType
        assert isinstance(outer_array, ArrayType)
        assert isinstance(outer_array.element_type, ArrayType)

    def test_array_of_arrays_3_levels(self):
        """Test 3-level nested arrays."""
        schema = parse_ddl_schema("data array<array<array<string>>>")
        assert len(schema.fields) == 1

    def test_array_of_arrays_5_levels(self):
        """Test 5-level nested arrays."""
        schema = parse_ddl_schema("data array<array<array<array<array<int>>>>>")
        assert len(schema.fields) == 1

    # ==================== Maps of Arrays ====================
    
    def test_map_of_arrays(self):
        """Test map of arrays."""
        schema = parse_ddl_schema("categories map<string,array<int>>")
        assert len(schema.fields) == 1
        map_type = schema.fields[0].dataType
        assert isinstance(map_type, MapType)
        assert isinstance(map_type.value_type, ArrayType)

    def test_map_of_arrays_nested(self):
        """Test map of nested arrays."""
        schema = parse_ddl_schema("data map<string,array<array<long>>>")
        assert len(schema.fields) == 1

    # ==================== Arrays of Maps ====================
    
    def test_array_of_maps(self):
        """Test array of maps."""
        schema = parse_ddl_schema("metadata array<map<string,string>>")
        assert len(schema.fields) == 1
        array_type = schema.fields[0].dataType
        assert isinstance(array_type, ArrayType)
        assert isinstance(array_type.element_type, MapType)

    def test_array_of_maps_nested(self):
        """Test array of nested maps."""
        schema = parse_ddl_schema("data array<map<string,map<string,int>>>")
        assert len(schema.fields) == 1

    # ==================== Maps of Maps ====================
    
    def test_map_of_maps(self):
        """Test map of maps."""
        schema = parse_ddl_schema("nested_map map<string,map<string,int>>")
        assert len(schema.fields) == 1
        outer_map = schema.fields[0].dataType
        assert isinstance(outer_map, MapType)
        assert isinstance(outer_map.value_type, MapType)

    def test_map_of_maps_3_levels(self):
        """Test 3-level nested maps."""
        schema = parse_ddl_schema("data map<string,map<string,map<string,double>>>")
        assert len(schema.fields) == 1

    # ==================== Structs in Arrays in Maps ====================
    
    def test_map_of_array_of_struct_v1(self):
        """Test map of array of struct (variant 1)."""
        schema = parse_ddl_schema(
            "users map<string,array<struct<id:long,name:string,age:int>>>"
        )
        assert len(schema.fields) == 1

    def test_array_of_map_of_struct_v1(self):
        """Test array of map of struct (variant 1)."""
        schema = parse_ddl_schema(
            "data array<map<string,struct<id:long,name:string>>>"
        )
        assert len(schema.fields) == 1

    def test_struct_in_array_in_map(self):
        """Test struct in array in map."""
        schema = parse_ddl_schema(
            "complex map<string,array<struct<id:long,name:string,values:array<int>>>>"
        )
        assert len(schema.fields) == 1

    # ==================== Deeply Nested Mixed Types ====================
    
    def test_deeply_nested_6_levels(self):
        """Test 6 levels of nesting with mixed types."""
        schema = parse_ddl_schema(
            "data struct<"
            "level1:array<"
            "map<string,"
            "struct<"
            "level2:array<"
            "struct<"
            "level3:string"
            ">"
            ">"
            ">"
            ">"
            ">"
            ">"
        )
        assert len(schema.fields) == 1

    def test_deeply_nested_10_levels(self):
        """Test 10 levels of nesting."""
        # Build deeply nested structure
        nested = "string"
        for i in range(10):
            if i % 3 == 0:
                nested = f"struct<level{i}:{nested}>"
            elif i % 3 == 1:
                nested = f"array<{nested}>"
            else:
                nested = f"map<string,{nested}>"
        
        schema = parse_ddl_schema(f"data {nested}")
        assert len(schema.fields) == 1

    # ==================== Very Wide Structs ====================
    
    def test_wide_struct_50_fields(self):
        """Test struct with 50 fields."""
        fields = ", ".join(f"field{i}:string" for i in range(50))
        schema = parse_ddl_schema(f"data struct<{fields}>")
        struct_type = schema.fields[0].dataType
        assert len(struct_type.fields) == 50

    def test_wide_struct_100_fields(self):
        """Test struct with 100 fields."""
        fields = ", ".join(f"field{i}:int" for i in range(100))
        schema = parse_ddl_schema(f"data struct<{fields}>")
        struct_type = schema.fields[0].dataType
        assert len(struct_type.fields) == 100

    # ==================== Schemas with All Type Combinations ====================
    
    def test_all_type_combinations(self):
        """Test schema with all type combinations."""
        schema = parse_ddl_schema(
            "simple_string string, simple_int int, simple_long long, "
            "simple_double double, "
            "array_simple array<string>, array_nested array<array<int>>, "
            "map_simple map<string,string>, map_nested map<string,map<string,int>>, "
            "struct_simple struct<id:long,name:string>, "
            "struct_nested struct<items:array<string>,metadata:map<string,string>>, "
            "array_of_struct array<struct<id:long,name:string>>, "
            "map_of_struct map<string,struct<id:long,name:string>>, "
            "struct_of_array struct<items:array<string>>, "
            "struct_of_map struct<metadata:map<string,string>>, "
            "array_of_map array<map<string,int>>, "
            "map_of_array map<string,array<int>>, "
            "complex_nested map<string,array<struct<id:long,values:array<int>>>>"
        )
        assert len(schema.fields) == 17

    # ==================== Multiple Struct Fields ====================
    
    def test_multiple_struct_fields(self):
        """Test multiple struct fields at same level."""
        schema = parse_ddl_schema(
            "address struct<street:string,city:string>, "
            "contact struct<phone:string,email:string>, "
            "preferences struct<theme:string,notifications:boolean>"
        )
        assert len(schema.fields) == 3
        assert all(isinstance(f.dataType, MockStructType) for f in schema.fields)

    def test_multiple_nested_structs(self):
        """Test multiple nested struct fields."""
        schema = parse_ddl_schema(
            "user struct<id:long,name:string,address:struct<street:string,city:string>>, "
            "company struct<id:long,name:string,address:struct<street:string,city:string>>"
        )
        assert len(schema.fields) == 2

    # ==================== Alternating Simple and Complex Types ====================
    
    def test_alternating_types(self):
        """Test alternating simple and complex types."""
        schema = parse_ddl_schema(
            "id long, "
            "tags array<string>, "
            "name string, "
            "metadata map<string,string>, "
            "age int, "
            "address struct<street:string,city:string>, "
            "score double, "
            "history array<struct<date:string,action:string>>"
        )
        assert len(schema.fields) == 8

    # ==================== Arrays of Maps of Arrays ====================
    
    def test_array_of_map_of_array(self):
        """Test array of map of array."""
        schema = parse_ddl_schema(
            "data array<map<string,array<int>>>"
        )
        assert len(schema.fields) == 1

    def test_map_of_array_of_map(self):
        """Test map of array of map."""
        schema = parse_ddl_schema(
            "data map<string,array<map<string,int>>>"
        )
        assert len(schema.fields) == 1

    # ==================== Structs in Maps in Arrays ====================
    
    def test_array_of_map_of_struct_v2(self):
        """Test array of map of struct (variant 2)."""
        schema = parse_ddl_schema(
            "data array<map<string,struct<id:long,name:string,age:int>>>"
        )
        assert len(schema.fields) == 1

    def test_map_of_array_of_struct_v2(self):
        """Test map of array of struct (variant 2)."""
        schema = parse_ddl_schema(
            "data map<string,array<struct<id:long,name:string,age:int>>>"
        )
        assert len(schema.fields) == 1

    # ==================== Complex Nested with All Types ====================
    
    def test_all_types_nested(self):
        """Test all types nested together."""
        schema = parse_ddl_schema(
            "complex struct<"
            "simple_string:string,"
            "simple_int:int,"
            "array_field:array<string>,"
            "map_field:map<string,int>,"
            "nested_struct:struct<id:long,name:string>,"
            "array_of_struct:array<struct<id:long,name:string>>,"
            "map_of_struct:map<string,struct<id:long,name:string>>,"
            "array_of_map:array<map<string,int>>,"
            "map_of_array:map<string,array<int>>,"
            "deeply_nested:map<string,array<struct<id:long,values:array<int>>>>"
            ">"
        )
        assert len(schema.fields) == 1

    # ==================== Multiple Levels of Nesting ====================
    
    def test_mixed_nesting_5_levels(self):
        """Test 5 levels of mixed nesting."""
        schema = parse_ddl_schema(
            "data array<"
            "map<string,"
            "struct<"
            "items:array<"
            "struct<"
            "value:string"
            ">"
            ">"
            ">"
            ">"
            ">"
        )
        assert len(schema.fields) == 1

    def test_mixed_nesting_8_levels(self):
        """Test 8 levels of mixed nesting."""
        schema = parse_ddl_schema(
            "data struct<"
            "level1:map<string,"
            "array<"
            "struct<"
            "level2:map<string,"
            "array<"
            "struct<"
            "level3:map<string,"
            "array<"
            "struct<"
            "level4:string"
            ">"
            ">"
            ">"
            ">"
            ">"
            ">"
            ">"
            ">"
            ">"
            ">"
        )
        assert len(schema.fields) == 1

    # ==================== Arrays with Complex Element Types ====================
    
    def test_array_of_complex_struct(self):
        """Test array of complex struct."""
        schema = parse_ddl_schema(
            "users array<struct<"
            "id:long,"
            "name:string,"
            "address:struct<street:string,city:string>,"
            "contacts:array<struct<type:string,value:string>>,"
            "metadata:map<string,string>"
            ">>"
        )
        assert len(schema.fields) == 1

    def test_array_of_array_of_struct(self):
        """Test array of array of struct."""
        schema = parse_ddl_schema(
            "matrix array<array<struct<row:int,col:int,value:double>>>"
        )
        assert len(schema.fields) == 1

    # ==================== Maps with Complex Value Types ====================
    
    def test_map_with_complex_values(self):
        """Test map with complex value types."""
        schema = parse_ddl_schema(
            "data map<string,struct<"
            "id:long,"
            "name:string,"
            "tags:array<string>,"
            "metadata:map<string,string>"
            ">>"
        )
        assert len(schema.fields) == 1

    def test_map_of_map_of_struct(self):
        """Test map of map of struct."""
        schema = parse_ddl_schema(
            "nested map<string,map<string,struct<id:long,name:string,value:double>>>"
        )
        assert len(schema.fields) == 1

    # ==================== Structs with All Complex Types ====================
    
    def test_struct_all_complex(self):
        """Test struct containing all complex types."""
        schema = parse_ddl_schema(
            "complex struct<"
            "array_field:array<string>,"
            "map_field:map<string,int>,"
            "nested_struct:struct<id:long,name:string>,"
            "array_of_struct:array<struct<id:long,name:string>>,"
            "map_of_struct:map<string,struct<id:long,name:string>>,"
            "array_of_map:array<map<string,int>>,"
            "map_of_array:map<string,array<int>>,"
            "deeply_nested:map<string,array<struct<id:long,values:array<int>>>>"
            ">"
        )
        assert len(schema.fields) == 1

    # ==================== Very Complex Real-World Scenarios ====================
    
    def test_ecommerce_order_schema(self):
        """Test complex e-commerce order schema."""
        schema = parse_ddl_schema(
            "order_id long, customer struct<"
            "id:long,"
            "name:string,"
            "address:struct<street:string,city:string,zip:string>,"
            "preferences:map<string,string>"
            ">, items array<struct<"
            "product_id:long,"
            "product_name:string,"
            "quantity:int,"
            "price:decimal(10,2),"
            "discount:decimal(5,2),"
            "variants:map<string,string>"
            ">>, shipping struct<"
            "method:string,"
            "tracking:array<string>,"
            "estimated_delivery:timestamp"
            ">, payment struct<"
            "method:string,"
            "amount:decimal(10,2),"
            "transaction_id:string"
            ">, metadata map<string,string>"
        )
        assert len(schema.fields) == 6

    def test_analytics_event_schema(self):
        """Test complex analytics event schema."""
        schema = parse_ddl_schema(
            "event_id long, timestamp timestamp, user struct<"
            "id:long,"
            "profile:struct<name:string,email:string>,"
            "preferences:map<string,string>"
            ">, session struct<"
            "id:string,"
            "start_time:timestamp,"
            "duration:int,"
            "page_views:array<struct<url:string,timestamp:timestamp,duration:int>>"
            ">, properties map<string,string>, "
            "context struct<"
            "device:struct<type:string,os:string,browser:string>,"
            "location:struct<country:string,city:string>,"
            "referrer:string"
            ">"
        )
        assert len(schema.fields) == 6

    def test_ml_feature_schema(self):
        """Test complex ML feature schema."""
        schema = parse_ddl_schema(
            "user_id long, features struct<"
            "numerical:array<double>,"
            "categorical:map<string,string>,"
            "embeddings:array<array<double>>,"
            "metadata:struct<"
            "created_at:timestamp,"
            "model_version:string,"
            "feature_names:array<string>"
            ">"
            ">, labels array<struct<"
            "label_name:string,"
            "label_value:double,"
            "confidence:double"
            ">>, history array<struct<"
            "timestamp:timestamp,"
            "features:array<double>,"
            "prediction:double"
            ">>"
        )
        assert len(schema.fields) == 4

    # ==================== Edge Cases in Complex Structures ====================
    
    def test_empty_array_in_struct(self):
        """Test struct with empty array field."""
        schema = parse_ddl_schema(
            "data struct<"
            "items:array<string>,"
            "metadata:map<string,string>"
            ">"
        )
        assert len(schema.fields) == 1

    def test_single_element_arrays(self):
        """Test arrays with single element type."""
        schema = parse_ddl_schema(
            "data array<struct<id:long>>"
        )
        assert len(schema.fields) == 1

    def test_single_key_map(self):
        """Test map with single key-value type."""
        schema = parse_ddl_schema(
            "data map<string,string>"
        )
        assert len(schema.fields) == 1

    # ==================== Maximum Complexity ====================
    
    def test_maximum_complexity(self):
        """Test maximum complexity schema."""
        schema = parse_ddl_schema(
            "root struct<"
            "level1:map<string,"
            "array<"
            "struct<"
            "level2:map<string,"
            "array<"
            "struct<"
            "level3:map<string,"
            "array<"
            "struct<"
            "level4:map<string,"
            "array<"
            "struct<"
            "level5:string"
            ">"
            ">"
            ">"
            ">"
            ">"
            ">"
            ">"
            ">"
            ">"
            ">"
            ">"
            ">"
            ">"
        )
        assert len(schema.fields) == 1

    # ==================== Real-World Maximum Complexity ====================
    
    def test_real_world_maximum(self):
        """Test real-world maximum complexity schema."""
        schema = parse_ddl_schema(
            "event struct<"
            "id:long,"
            "timestamp:timestamp,"
            "user:struct<"
            "id:long,"
            "profile:struct<name:string,email:string>,"
            "preferences:map<string,string>,"
            "history:array<struct<action:string,timestamp:timestamp>>"
            ">, session:struct<"
            "id:string,"
            "events:array<struct<"
            "type:string,"
            "data:map<string,string>,"
            "nested:struct<"
            "values:array<struct<key:string,value:string>>,"
            "metadata:map<string,array<string>>"
            ">"
            ">>"
            ">, properties:map<string,string>, "
            "context:struct<"
            "device:struct<type:string,os:string>,"
            "location:struct<country:string,city:string>,"
            "tags:array<string>"
            ">"
            ">"
        )
        assert len(schema.fields) == 1

