"""
Regression tests for DDL schema parser.

Tests to prevent known bugs from reappearing and document
the issues that have been fixed.
"""

import pytest
from mock_spark.core.ddl_parser import parse_ddl_schema
from mock_spark.spark_types import MockStructType, LongType, StringType


class TestDDLParserRegression:
    """Regression tests for DDL parser."""

    # ==================== Original Bug ====================
    
    def test_original_bug_schema_stored_as_string(self):
        """Regression test for original bug: schema stored as string.
        
        Bug: DDL schema strings were stored as-is without parsing,
        causing df.columns to fail with AttributeError.
        """
        schema = parse_ddl_schema("id long, name string")
        
        # Schema should be MockStructType, not string
        assert isinstance(schema, MockStructType)
        assert len(schema.fields) == 2
        assert schema.fields[0].name == "id"
        assert schema.fields[1].name == "name"

    def test_original_bug_columns_property(self):
        """Regression test: df.columns should work with DDL schemas."""
        from mock_spark import MockSparkSession
        
        spark = MockSparkSession()
        data = [{'id': 1, 'name': 'test'}]
        df = spark.createDataFrame(data, schema='id long, name string')
        
        # This should not raise AttributeError
        columns = df.columns
        assert columns == ['id', 'name']

    # ==================== Colon Inside Struct Bug ====================
    
    def test_colon_inside_struct_bug(self):
        """Regression test for colon inside struct causing wrong split.
        
        Bug: Colon inside struct definition (e.g., 'street:string') was
        being used to split field name from type, causing incorrect parsing.
        """
        schema = parse_ddl_schema("address struct<street:string,city:string>")
        
        assert len(schema.fields) == 1
        assert schema.fields[0].name == "address"
        struct_type = schema.fields[0].dataType
        assert isinstance(struct_type, MockStructType)
        assert len(struct_type.fields) == 2
        assert struct_type.fields[0].name == "street"
        assert struct_type.fields[1].name == "city"

    def test_colon_in_nested_struct(self):
        """Regression test: colons in nested structs."""
        schema = parse_ddl_schema(
            "user struct<"
            "name:string,"
            "address:struct<street:string,city:string>"
            ">"
        )
        
        assert len(schema.fields) == 1
        user_struct = schema.fields[0].dataType
        assert len(user_struct.fields) == 2
        address_struct = user_struct.fields[1].dataType
        assert len(address_struct.fields) == 2

    # ==================== Decimal Parentheses Bug ====================
    
    def test_decimal_parentheses_bug(self):
        """Regression test for decimal parentheses breaking field split.
        
        Bug: Parentheses in decimal(10,2) were not handled correctly,
        causing field splitting to break on the comma inside parentheses.
        """
        schema = parse_ddl_schema("price decimal(10,2), amount decimal(5,0)")
        
        assert len(schema.fields) == 2
        assert schema.fields[0].name == "price"
        assert schema.fields[0].dataType.precision == 10
        assert schema.fields[0].dataType.scale == 2
        assert schema.fields[1].name == "amount"
        assert schema.fields[1].dataType.precision == 5
        assert schema.fields[1].dataType.scale == 0

    def test_decimal_with_other_fields(self):
        """Regression test: decimal with other field types."""
        schema = parse_ddl_schema(
            "id long, name string, price decimal(10,2), active boolean"
        )
        
        assert len(schema.fields) == 4
        assert schema.fields[0].name == "id"
        assert schema.fields[1].name == "name"
        assert schema.fields[2].name == "price"
        assert schema.fields[3].name == "active"

    # ==================== Whitespace After Type Bug ====================
    
    def test_whitespace_after_type_bug(self):
        """Regression test for whitespace after type name.
        
        Bug: Extra whitespace after type name could cause parsing issues.
        """
        schema = parse_ddl_schema("id long , name string , age int")
        
        assert len(schema.fields) == 3
        assert schema.fields[0].name == "id"
        assert schema.fields[1].name == "name"
        assert schema.fields[2].name == "age"

    def test_tabs_and_spaces(self):
        """Regression test: tabs and spaces mixed."""
        schema = parse_ddl_schema("id\tlong,\tname\tstring")
        
        assert len(schema.fields) == 2
        assert schema.fields[0].name == "id"
        assert schema.fields[1].name == "name"

    # ==================== Mixed Separator Bug ====================
    
    def test_mixed_separators_bug(self):
        """Regression test: mixing space and colon separators.
        
        Bug: Mixing space and colon separators in same schema could
        cause parsing issues.
        """
        schema = parse_ddl_schema("id long, name:string, age int")
        
        assert len(schema.fields) == 3
        assert schema.fields[0].name == "id"
        assert schema.fields[1].name == "name"
        assert schema.fields[2].name == "age"

    # ==================== Type Alias Bug ====================
    
    def test_type_aliases_bug(self):
        """Regression test: type aliases not recognized.
        
        Bug: bigint, integer, bool, smallint, tinyint aliases were
        not recognized.
        """
        schema = parse_ddl_schema(
            "id bigint, count integer, active bool, "
            "short_val smallint, byte_val tinyint"
        )
        
        assert len(schema.fields) == 5
        # All should be parsed correctly
        assert schema.fields[0].name == "id"
        assert schema.fields[1].name == "count"
        assert schema.fields[2].name == "active"
        assert schema.fields[3].name == "short_val"
        assert schema.fields[4].name == "byte_val"

    # ==================== Array Type Bug ====================
    
    def test_array_type_parsing_bug(self):
        """Regression test: array type parsing.
        
        Bug: Array types were not parsed correctly, causing
        element types to be lost.
        """
        schema = parse_ddl_schema("tags array<string>, scores array<int>")
        
        assert len(schema.fields) == 2
        assert schema.fields[0].name == "tags"
        assert schema.fields[1].name == "scores"
        # Element types should be preserved
        assert schema.fields[0].dataType.element_type.__class__.__name__ == "StringType"
        assert schema.fields[1].dataType.element_type.__class__.__name__ == "IntegerType"

    # ==================== Map Type Bug ====================
    
    def test_map_type_parsing_bug(self):
        """Regression test: map type parsing.
        
        Bug: Map types were not parsed correctly, causing key/value
        types to be lost.
        """
        schema = parse_ddl_schema(
            "metadata map<string,string>, counts map<string,int>"
        )
        
        assert len(schema.fields) == 2
        assert schema.fields[0].name == "metadata"
        assert schema.fields[1].name == "counts"
        # Key and value types should be preserved
        assert schema.fields[0].dataType.key_type.__class__.__name__ == "StringType"
        assert schema.fields[0].dataType.value_type.__class__.__name__ == "StringType"
        assert schema.fields[1].dataType.value_type.__class__.__name__ == "IntegerType"

    # ==================== Nested Struct Bug ====================
    
    def test_nested_struct_parsing_bug(self):
        """Regression test: nested struct parsing.
        
        Bug: Nested structs were not parsed correctly, causing
        inner struct fields to be lost.
        """
        schema = parse_ddl_schema(
            "address struct<street:string,city:string,zip:string>"
        )
        
        assert len(schema.fields) == 1
        assert schema.fields[0].name == "address"
        struct_type = schema.fields[0].dataType
        assert isinstance(struct_type, MockStructType)
        assert len(struct_type.fields) == 3
        assert struct_type.fields[0].name == "street"
        assert struct_type.fields[1].name == "city"
        assert struct_type.fields[2].name == "zip"

    # ==================== Complex Nested Bug ====================
    
    def test_complex_nested_parsing_bug(self):
        """Regression test: complex nested structure parsing.
        
        Bug: Complex nested structures (arrays of structs, maps of arrays)
        were not parsed correctly.
        """
        schema = parse_ddl_schema(
            "users array<struct<id:long,name:string>>, "
            "categories map<string,array<int>>"
        )
        
        assert len(schema.fields) == 2
        # First field: array of struct
        assert schema.fields[0].name == "users"
        assert schema.fields[0].dataType.element_type.__class__.__name__ == "MockStructType"
        # Second field: map of array
        assert schema.fields[1].name == "categories"
        assert schema.fields[1].dataType.value_type.__class__.__name__ == "ArrayType"

    # ==================== DataFrame Integration Bug ====================
    
    def test_dataframe_integration_bug(self):
        """Regression test: DataFrame operations with DDL schema.
        
        Bug: DDL schemas couldn't be used with DataFrame operations
        like select(), filter(), etc.
        """
        from mock_spark import MockSparkSession
        from mock_spark.functions import F
        
        spark = MockSparkSession()
        data = [
            {'id': 1, 'name': 'Alice', 'age': 25},
            {'id': 2, 'name': 'Bob', 'age': 30},
        ]
        
        df = spark.createDataFrame(data, schema='id long, name string, age int')
        
        # All these should work without errors
        assert df.count() == 2
        assert df.columns == ['id', 'name', 'age']
        
        # Select should work
        selected = df.select('name', 'age')
        assert selected.columns == ['name', 'age']
        
        # Filter should work
        filtered = df.filter(F.col('age') > 25)
        assert filtered.count() == 1

    # ==================== Schema Comparison Bug ====================
    
    def test_schema_comparison_bug(self):
        """Regression test: DDL schema vs StructType schema.
        
        Bug: DDL schemas and StructType schemas should produce
        equivalent DataFrames.
        """
        from mock_spark import MockSparkSession
        from mock_spark.spark_types import MockStructType, MockStructField
        
        spark = MockSparkSession()
        data = [{'id': 1, 'name': 'test'}]
        
        # Create with DDL schema
        df1 = spark.createDataFrame(data, schema='id long, name string')
        
        # Create with StructType schema
        schema = MockStructType([
            MockStructField('id', LongType()),
            MockStructField('name', StringType())
        ])
        df2 = spark.createDataFrame(data, schema=schema)
        
        # Both should have same columns
        assert df1.columns == df2.columns
        assert df1.count() == df2.count()

    # ==================== Error Message Bug ====================
    
    def test_error_message_bug(self):
        """Regression test: error messages should be informative.
        
        Bug: Error messages were too generic or missing context.
        """
        with pytest.raises(ValueError) as exc_info:
            parse_ddl_schema("id long name string")  # Missing comma
        
        error_msg = str(exc_info.value)
        assert "Invalid field definition" in error_msg or "error" in error_msg.lower()

    # ==================== Edge Case Regressions ====================
    
    def test_empty_schema_regression(self):
        """Regression test: empty schema should not crash."""
        schema = parse_ddl_schema("")
        assert len(schema.fields) == 0

    def test_single_field_regression(self):
        """Regression test: single field schema."""
        schema = parse_ddl_schema("id long")
        assert len(schema.fields) == 1
        assert schema.fields[0].name == "id"

    def test_struct_wrapper_regression(self):
        """Regression test: struct wrapper format."""
        schema = parse_ddl_schema("struct<id:long,name:string>")
        assert len(schema.fields) == 2
        assert schema.fields[0].name == "id"
        assert schema.fields[1].name == "name"

    # ==================== Type Preservation Regression ====================
    
    def test_type_preservation_regression(self):
        """Regression test: types should be preserved correctly.
        
        Bug: Types could be incorrectly parsed or lost during parsing.
        """
        schema = parse_ddl_schema(
            "id long, name string, age int, score double, active boolean"
        )
        
        assert len(schema.fields) == 5
        # Check that types are correct
        assert schema.fields[0].dataType.__class__.__name__ == "LongType"
        assert schema.fields[1].dataType.__class__.__name__ == "StringType"
        assert schema.fields[2].dataType.__class__.__name__ == "IntegerType"
        assert schema.fields[3].dataType.__class__.__name__ == "DoubleType"
        assert schema.fields[4].dataType.__class__.__name__ == "BooleanType"

    # ==================== Nullability Regression ====================
    
    def test_nullability_regression(self):
        """Regression test: all fields should be nullable by default.
        
        Bug: Nullability was not set correctly on parsed fields.
        """
        schema = parse_ddl_schema("id long, name string")
        
        assert len(schema.fields) == 2
        # All fields should be nullable by default (PySpark behavior)
        assert schema.fields[0].nullable == True
        assert schema.fields[1].nullable == True

