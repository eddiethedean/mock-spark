"""
Error handling tests for DDL schema parser.

Tests malformed DDL schema strings to ensure the parser
provides clear error messages and doesn't crash.
"""

import pytest
from mock_spark.core.ddl_parser import parse_ddl_schema


class TestDDLParserErrors:
    """Test DDL parser error handling."""

    # ==================== Unbalanced Brackets ====================
    
    def test_unbalanced_struct_open(self):
        """Test struct with missing closing bracket."""
        with pytest.raises(ValueError):
            parse_ddl_schema("address struct<street:string,city:string")

    def test_unbalanced_struct_close(self):
        """Test struct with extra closing bracket."""
        with pytest.raises(ValueError):
            parse_ddl_schema("address struct<street:string,city:string>>")

    def test_unbalanced_array_open(self):
        """Test array with missing closing bracket."""
        with pytest.raises(ValueError):
            parse_ddl_schema("tags array<string")

    def test_unbalanced_array_close(self):
        """Test array with extra closing bracket."""
        with pytest.raises(ValueError):
            parse_ddl_schema("tags array<string>>")

    def test_unbalanced_map_open(self):
        """Test map with missing closing bracket."""
        with pytest.raises(ValueError):
            parse_ddl_schema("metadata map<string,string")

    def test_unbalanced_map_close(self):
        """Test map with extra closing bracket."""
        with pytest.raises(ValueError):
            parse_ddl_schema("metadata map<string,string>>")

    def test_unbalanced_nested(self):
        """Test nested structures with unbalanced brackets."""
        with pytest.raises(ValueError):
            parse_ddl_schema("data struct<items:array<string")

    # ==================== Invalid Syntax ====================
    
    def test_missing_comma(self):
        """Test schema with missing comma."""
        with pytest.raises(ValueError, match="Invalid field definition"):
            parse_ddl_schema("id long name string")

    def test_missing_type(self):
        """Test schema with missing type."""
        with pytest.raises(ValueError, match="Invalid field definition"):
            parse_ddl_schema("id, name string")

    def test_missing_field_name(self):
        """Test schema with missing field name."""
        with pytest.raises(ValueError, match="Invalid field definition"):
            parse_ddl_schema(":long, name string")

    def test_trailing_comma(self):
        """Test schema with trailing comma."""
        # This might be valid or invalid depending on PySpark
        with pytest.raises(ValueError, match="Invalid field definition"):
            parse_ddl_schema("id long, name string,")

    def test_double_comma(self):
        """Test schema with double comma."""
        with pytest.raises(ValueError, match="Invalid field definition"):
            parse_ddl_schema("id long,, name string")

    def test_comma_at_start(self):
        """Test schema with comma at start."""
        with pytest.raises(ValueError, match="Invalid field definition"):
            parse_ddl_schema(",id long, name string")

    # ==================== Invalid Types ====================
    
    def test_invalid_type(self):
        """Test schema with invalid type."""
        # Should not crash, might return StringType as default
        schema = parse_ddl_schema("id invalidtype")
        assert len(schema.fields) == 1

    def test_empty_array_type(self):
        """Test array with empty type."""
        with pytest.raises(ValueError, match="Invalid array type"):
            parse_ddl_schema("tags array<>")

    def test_map_missing_value_type(self):
        """Test map with missing value type."""
        with pytest.raises(ValueError, match="Invalid map type"):
            parse_ddl_schema("metadata map<string>")

    def test_map_missing_comma(self):
        """Test map with missing comma between key and value."""
        with pytest.raises(ValueError, match="Invalid map type"):
            parse_ddl_schema("metadata map<stringstring>")

    # ==================== Nested Errors ====================
    
    def test_nested_invalid_type(self):
        """Test nested structure with invalid type."""
        # Should not crash
        schema = parse_ddl_schema("data struct<id:invalidtype>")
        assert len(schema.fields) == 1

    def test_nested_unbalanced(self):
        """Test nested structure with unbalanced brackets."""
        with pytest.raises(ValueError):
            parse_ddl_schema("data struct<items:array<string>")

    def test_deeply_nested_unbalanced(self):
        """Test deeply nested structure with unbalanced brackets."""
        with pytest.raises(ValueError):
            parse_ddl_schema(
                "data struct<"
                "items:array<map<string,struct<id:long,name:string>>"
                ">"
            )

    # ==================== Empty Structures ====================
    
    def test_empty_struct(self):
        """Test empty struct."""
        # Empty struct might be valid or invalid
        with pytest.raises(ValueError):
            parse_ddl_schema("empty struct<>")

    def test_empty_array(self):
        """Test empty array."""
        with pytest.raises(ValueError, match="Invalid array type"):
            parse_ddl_schema("tags array<>")

    def test_empty_map(self):
        """Test empty map."""
        with pytest.raises(ValueError, match="Invalid map type"):
            parse_ddl_schema("metadata map<>")

    # ==================== Invalid Decimal ====================
    
    def test_decimal_letters(self):
        """Test decimal with letters instead of numbers."""
        # Should not crash, might parse as invalid
        schema = parse_ddl_schema("value decimal(a,b)")
        assert len(schema.fields) == 1

    def test_decimal_negative_precision(self):
        """Test decimal with negative precision."""
        schema = parse_ddl_schema("value decimal(-1,2)")
        assert len(schema.fields) == 1

    def test_decimal_negative_scale(self):
        """Test decimal with negative scale."""
        schema = parse_ddl_schema("value decimal(10,-2)")
        assert len(schema.fields) == 1

    def test_decimal_scale_greater_than_precision(self):
        """Test decimal with scale greater than precision."""
        schema = parse_ddl_schema("value decimal(5,10)")
        assert len(schema.fields) == 1

    def test_decimal_too_large(self):
        """Test decimal with very large numbers."""
        schema = parse_ddl_schema("value decimal(1000,500)")
        assert len(schema.fields) == 1

    def test_decimal_missing_closing_paren(self):
        """Test decimal with missing closing parenthesis."""
        with pytest.raises(ValueError):
            parse_ddl_schema("value decimal(10,2")

    def test_decimal_extra_paren(self):
        """Test decimal with extra parenthesis."""
        with pytest.raises(ValueError):
            parse_ddl_schema("value decimal(10,2))")

    # ==================== Multiple Colons ====================
    
    def test_double_colon(self):
        """Test field with double colon."""
        # Should parse as field name with colon in it
        schema = parse_ddl_schema("id::long")
        assert len(schema.fields) == 1

    def test_colon_in_middle(self):
        """Test field with colon in middle of name."""
        # This is ambiguous - could be "field:name long" or "field: name long"
        with pytest.raises(ValueError):
            parse_ddl_schema("field:name long")

    def test_multiple_colons(self):
        """Test field with multiple colons."""
        # This is ambiguous - could be "a::b::c long" or "a:: b:: c long"
        with pytest.raises(ValueError):
            parse_ddl_schema("a::b::c long")

    # ==================== Missing Field Names ====================
    
    def test_only_type(self):
        """Test schema with only type, no field name."""
        with pytest.raises(ValueError, match="Invalid field definition"):
            parse_ddl_schema("long")

    def test_multiple_types_no_names(self):
        """Test schema with multiple types but no field names."""
        with pytest.raises(ValueError, match="Invalid field definition"):
            parse_ddl_schema("long, string, int")

    # ==================== Reserved Words ====================
    
    def test_sql_keyword_as_field_name(self):
        """Test SQL keyword as field name."""
        schema = parse_ddl_schema("select string, from string, where int")
        assert len(schema.fields) == 3

    def test_python_keyword_as_field_name(self):
        """Test Python keyword as field name."""
        schema = parse_ddl_schema("class string, def string, import int")
        assert len(schema.fields) == 3

    # ==================== Special Characters ====================
    
    def test_special_chars_in_field_name(self):
        """Test special characters in field name."""
        schema = parse_ddl_schema("field-name string, field.name int")
        assert len(schema.fields) == 2

    def test_quotes_in_field_name(self):
        """Test quotes in field name."""
        # Parser doesn't support quoted field names
        with pytest.raises(ValueError):
            parse_ddl_schema('"field name" string')

    def test_backticks_in_field_name(self):
        """Test backticks in field name."""
        # Parser doesn't support backtick-quoted field names
        with pytest.raises(ValueError):
            parse_ddl_schema("`field name` string")

    # ==================== Edge Cases ====================
    
    def test_only_comma(self):
        """Test schema with only comma."""
        with pytest.raises(ValueError, match="Invalid field definition"):
            parse_ddl_schema(",")

    def test_only_space(self):
        """Test schema with only space."""
        schema = parse_ddl_schema(" ")
        assert len(schema.fields) == 0

    def test_only_colon(self):
        """Test schema with only colon."""
        with pytest.raises(ValueError, match="Invalid field definition"):
            parse_ddl_schema(":")

    def test_mixed_invalid_syntax(self):
        """Test schema with multiple syntax errors."""
        with pytest.raises(ValueError):
            parse_ddl_schema("id long, name, age int, ,")

    def test_nested_invalid_syntax(self):
        """Test nested structure with invalid syntax."""
        with pytest.raises(ValueError):
            parse_ddl_schema("data struct<id:long,name,age:int>")

    # ==================== Type Parsing Errors ====================
    
    def test_incomplete_array(self):
        """Test incomplete array type."""
        with pytest.raises(ValueError):
            parse_ddl_schema("tags array<")

    def test_incomplete_map(self):
        """Test incomplete map type."""
        with pytest.raises(ValueError):
            parse_ddl_schema("metadata map<")

    def test_incomplete_struct(self):
        """Test incomplete struct type."""
        with pytest.raises(ValueError):
            parse_ddl_schema("address struct<")

    def test_incomplete_decimal(self):
        """Test incomplete decimal type."""
        with pytest.raises(ValueError):
            parse_ddl_schema("value decimal(")

    # ==================== Complex Invalid Structures ====================
    
    def test_array_with_struct_unbalanced(self):
        """Test array of struct with unbalanced brackets."""
        with pytest.raises(ValueError):
            parse_ddl_schema(
                "users array<struct<id:long,name:string,age:int>"
            )

    def test_map_with_array_unbalanced(self):
        """Test map of array with unbalanced brackets."""
        with pytest.raises(ValueError):
            parse_ddl_schema(
                "data map<string,array<int>"
            )

    def test_nested_mixed_unbalanced(self):
        """Test nested mixed types with unbalanced brackets."""
        with pytest.raises(ValueError):
            parse_ddl_schema(
                "complex struct<"
                "items:array<map<string,struct<id:long,name:string>>"
                ">"
            )

    # ==================== Whitespace Errors ====================
    
    def test_no_space_after_field_name(self):
        """Test field without space after name."""
        # Should parse as colon format
        schema = parse_ddl_schema("id:long, name:string")
        assert len(schema.fields) == 2

    def test_no_space_before_type(self):
        """Test field without space before type."""
        # This is invalid - can't distinguish field name from type
        with pytest.raises(ValueError):
            parse_ddl_schema("idlong")

    # ==================== Boundary Conditions ====================
    
    def test_very_long_invalid_input(self):
        """Test very long invalid input."""
        long_string = "a" * 10000
        with pytest.raises(ValueError, match="Invalid field definition"):
            parse_ddl_schema(long_string)

    def test_only_brackets(self):
        """Test schema with only brackets."""
        with pytest.raises(ValueError, match="Invalid field definition"):
            parse_ddl_schema("<>")

    def test_only_angle_brackets(self):
        """Test schema with only angle brackets."""
        with pytest.raises(ValueError, match="Invalid field definition"):
            parse_ddl_schema("array<>")

    # ==================== Type Name Errors ====================
    
    def test_type_with_extra_chars(self):
        """Test type with extra characters."""
        schema = parse_ddl_schema("id longextra")
        assert len(schema.fields) == 1

    def test_type_with_numbers(self):
        """Test type with numbers."""
        schema = parse_ddl_schema("id long123")
        assert len(schema.fields) == 1

    def test_type_with_special_chars(self):
        """Test type with special characters."""
        schema = parse_ddl_schema("id long-type")
        assert len(schema.fields) == 1

