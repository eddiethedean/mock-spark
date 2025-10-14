"""
DDL Schema Parser for Mock Spark.

This module provides functionality to parse DDL (Data Definition Language) schema strings
into MockStructType objects, matching PySpark's StructType.fromDDL() behavior.

Example:
    >>> from mock_spark.core.ddl_parser import parse_ddl_schema
    >>> schema = parse_ddl_schema("id long, name string")
    >>> print(schema)
    MockStructType([MockStructField(name='id', dataType=LongType(), nullable=True),
                    MockStructField(name='name', dataType=StringType(), nullable=True)])
"""

import re
from typing import List
from ..spark_types import (
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
    MockDataType,
)


# Type mapping from DDL type names to MockDataType classes
TYPE_MAPPING = {
    "string": StringType,
    "int": IntegerType,
    "integer": IntegerType,
    "long": LongType,
    "bigint": LongType,
    "double": DoubleType,
    "float": FloatType,
    "boolean": BooleanType,
    "bool": BooleanType,
    "date": DateType,
    "timestamp": TimestampType,
    "decimal": DecimalType,
    "binary": BinaryType,
    "short": ShortType,
    "smallint": ShortType,
    "byte": ByteType,
    "tinyint": ByteType,
}


def parse_ddl_schema(ddl_string: str) -> MockStructType:
    """Parse DDL schema string into MockStructType.
    
    Supports PySpark's DDL format:
    - Simple: "id long, name string"
    - With nullability: "id long, name string, age int"
    - Nested: "id long, address struct<street:string,city:string>"
    - Arrays: "tags array<string>"
    - Maps: "metadata map<string,string>"
    
    Args:
        ddl_string: DDL schema string (e.g., "id long, name string")
    
    Returns:
        MockStructType with parsed fields
    
    Raises:
        ValueError: If DDL string is invalid
    
    Example:
        >>> schema = parse_ddl_schema("id long, name string")
        >>> schema.fields[0].name
        'id'
        >>> schema.fields[0].dataType
        LongType(nullable=True)
    """
    if not ddl_string or not ddl_string.strip():
        return MockStructType([])
    
    # Normalize whitespace: replace tabs, newlines, carriage returns with spaces
    ddl_string = ddl_string.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')
    
    # Remove leading/trailing whitespace and "struct<" wrapper if present
    ddl_string = ddl_string.strip()
    if ddl_string.startswith("struct<"):
        ddl_string = ddl_string[7:]
        if ddl_string.endswith(">"):
            ddl_string = ddl_string[:-1]
        else:
            raise ValueError(f"Invalid struct type: struct<{ddl_string}>")
    
    # Validate comma usage (run before bracket validation)
    _validate_comma_usage(ddl_string)
    
    # Validate brackets before splitting fields
    # This catches errors like "tags array<string>>" (extra >)
    _validate_balanced_brackets(ddl_string)
    
    # Split by comma, but be careful with nested structs, arrays, and maps
    field_strings = _split_ddl_fields(ddl_string)
    
    # Validate no empty fields
    if not field_strings:
        raise ValueError("Invalid field definition: empty schema")
    
    fields = []
    
    for field_str in field_strings:
        if not field_str.strip():
            raise ValueError("Invalid field definition: empty field")
        field = _parse_field(field_str.strip())
        fields.append(field)
    
    return MockStructType(fields)


def _validate_balanced_brackets(s: str) -> None:
    """Validate that brackets and parentheses are balanced.
    
    Args:
        s: String to validate
    
    Raises:
        ValueError: If brackets or parentheses are unbalanced
    """
    angle_depth = 0
    paren_depth = 0
    
    for char in s:
        if char == "<":
            angle_depth += 1
        elif char == ">":
            angle_depth -= 1
            if angle_depth < 0:
                raise ValueError("Unbalanced angle brackets: extra '>'")
        elif char == "(":
            paren_depth += 1
        elif char == ")":
            paren_depth -= 1
            if paren_depth < 0:
                raise ValueError("Unbalanced parentheses: extra ')'")
    
    if angle_depth > 0:
        raise ValueError("Unbalanced angle brackets: missing '>'")
    if paren_depth > 0:
        raise ValueError("Unbalanced parentheses: missing ')'")


def _validate_comma_usage(s: str) -> None:
    """Validate comma usage in DDL string.
    
    Checks for:
    - Comma at start
    - Double commas
    - Trailing comma
    
    Args:
        s: String to validate
    
    Raises:
        ValueError: If comma usage is invalid
    """
    if not s:
        return
    
    # Check for comma at start
    if s.strip().startswith(","):
        raise ValueError("Invalid field definition: comma at start")
    
    # Check for trailing comma
    if s.strip().endswith(","):
        raise ValueError("Invalid field definition: trailing comma")
    
    # Check for double commas (outside of brackets/parens)
    angle_depth = 0
    paren_depth = 0
    i = 0
    
    while i < len(s) - 1:
        char = s[i]
        next_char = s[i + 1]
        
        if char == "<":
            angle_depth += 1
        elif char == ">":
            angle_depth -= 1
        elif char == "(":
            paren_depth += 1
        elif char == ")":
            paren_depth -= 1
        elif char == "," and angle_depth == 0 and paren_depth == 0:
            # Check if next non-whitespace character is also a comma
            if next_char == ",":
                raise ValueError("Invalid field definition: double comma")
        
        i += 1


def _split_ddl_fields(ddl_string: str) -> List[str]:
    """Split DDL string into individual field definitions.
    
    Handles nested structures like struct<>, array<>, map<>, and decimal().
    
    Args:
        ddl_string: DDL string with multiple fields
    
    Returns:
        List of field definition strings
    """
    fields = []
    current_field = ""
    angle_depth = 0  # Track < and >
    paren_depth = 0  # Track ( and )
    i = 0
    
    while i < len(ddl_string):
        char = ddl_string[i]
        
        if char == "<":
            angle_depth += 1
            current_field += char
        elif char == ">":
            angle_depth -= 1
            current_field += char
        elif char == "(":
            paren_depth += 1
            current_field += char
        elif char == ")":
            paren_depth -= 1
            current_field += char
        elif char == "," and angle_depth == 0 and paren_depth == 0:
            if current_field.strip():
                fields.append(current_field.strip())
            current_field = ""
        else:
            current_field += char
        
        i += 1
    
    # Add the last field
    if current_field.strip():
        fields.append(current_field.strip())
    
    return fields


def _parse_field(field_str: str) -> MockStructField:
    """Parse a single field definition.
    
    Format: "name type" or "name:type"
    
    Args:
        field_str: Field definition string (e.g., "id long" or "name:string")
    
    Returns:
        MockStructField object
    
    Raises:
        ValueError: If field definition is invalid
    """
    if not field_str or not field_str.strip():
        raise ValueError(f"Invalid field definition: {field_str}")
    
    # Handle both formats: "name type" and "name:type"
    # Check if there's a colon at the top level (not inside brackets/parens)
    has_colon_at_top_level = False
    angle_depth = 0
    paren_depth = 0
    
    for char in field_str:
        if char == "<":
            angle_depth += 1
        elif char == ">":
            angle_depth -= 1
        elif char == "(":
            paren_depth += 1
        elif char == ")":
            paren_depth -= 1
        elif char == ":" and angle_depth == 0 and paren_depth == 0:
            has_colon_at_top_level = True
            break
    
    if has_colon_at_top_level:
        # Split on the first colon at top level
        name_end = None
        angle_depth = 0
        paren_depth = 0
        
        for i, char in enumerate(field_str):
            if char == "<":
                angle_depth += 1
            elif char == ">":
                angle_depth -= 1
            elif char == "(":
                paren_depth += 1
            elif char == ")":
                paren_depth -= 1
            elif char == ":" and angle_depth == 0 and paren_depth == 0:
                name_end = i
                break
        
        if name_end is not None:
            name = field_str[:name_end].strip()
            type_str = field_str[name_end + 1:].strip()
        else:
            raise ValueError(f"Invalid field definition: {field_str}")
    else:
        # Split by whitespace, name is first, rest is type
        # But we need to be careful with complex types like struct<>, array<>, map<>
        # Find the first space that's not inside angle brackets or parentheses
        name_end = None
        angle_depth = 0
        paren_depth = 0
        
        for i, char in enumerate(field_str):
            if char == "<":
                angle_depth += 1
            elif char == ">":
                angle_depth -= 1
            elif char == "(":
                paren_depth += 1
            elif char == ")":
                paren_depth -= 1
            elif char == " " and angle_depth == 0 and paren_depth == 0:
                name_end = i
                break
        
        if name_end is None:
            raise ValueError(f"Invalid field definition: {field_str}")
        
        name = field_str[:name_end].strip()
        type_str = field_str[name_end:].strip()
    
    name = name.strip()
    type_str = type_str.strip()
    
    # Validate name is not empty or whitespace-only
    if not name or name.isspace():
        raise ValueError(f"Invalid field definition: {field_str}")
    
    # Validate type is not empty
    if not type_str:
        raise ValueError(f"Invalid field definition: {field_str}")
    
    # Parse the data type
    data_type = _parse_type(type_str)
    
    # All fields are nullable by default (PySpark behavior)
    return MockStructField(name=name, dataType=data_type, nullable=True)


def _parse_type(type_str: str) -> MockDataType:
    """Parse a type string into a MockDataType.
    
    Supports:
    - Simple types: "string", "long", "int"
    - Arrays: "array<string>", "array<long>"
    - Maps: "map<string,long>"
    - Structs: "struct<name:string,age:int>"
    - Decimal: "decimal(10,2)"
    
    Args:
        type_str: Type string (e.g., "string", "array<long>")
    
    Returns:
        MockDataType instance
    
    Raises:
        ValueError: If type string is invalid
    """
    type_str = type_str.strip()
    
    if not type_str:
        raise ValueError("Invalid type: empty type string")
    
    # Handle decimal with precision and scale (check before struct)
    if type_str.startswith("decimal"):
        match = re.match(r"decimal\((\d+),(\d+)\)", type_str)
        if match:
            precision = int(match.group(1))
            scale = int(match.group(2))
            return DecimalType(precision=precision, scale=scale)
        else:
            # Check for incomplete decimal
            if "(" in type_str and not type_str.endswith(")"):
                raise ValueError(f"Invalid decimal type: {type_str}")
            return DecimalType()  # Default decimal
    
    # Handle arrays
    if type_str.startswith("array<"):
        if not type_str.endswith(">"):
            raise ValueError(f"Invalid array type: {type_str}")
        element_type_str = type_str[6:-1].strip()
        if not element_type_str:
            raise ValueError(f"Invalid array type: {type_str}")
        element_type = _parse_type(element_type_str)
        return ArrayType(element_type)
    
    # Handle maps
    if type_str.startswith("map<"):
        if not type_str.endswith(">"):
            raise ValueError(f"Invalid map type: {type_str}")
        map_content = type_str[4:-1].strip()
        if not map_content:
            raise ValueError(f"Invalid map type: {type_str}")
        
        # Split key and value types
        comma_pos = _find_map_comma(map_content)
        if comma_pos == -1:
            raise ValueError(f"Invalid map type: {type_str}")
        
        key_type_str = map_content[:comma_pos].strip()
        value_type_str = map_content[comma_pos + 1:].strip()
        
        if not key_type_str or not value_type_str:
            raise ValueError(f"Invalid map type: {type_str}")
        
        key_type = _parse_type(key_type_str)
        value_type = _parse_type(value_type_str)
        
        return MapType(key_type, value_type)
    
    # Handle structs
    if type_str.startswith("struct<"):
        if not type_str.endswith(">"):
            raise ValueError(f"Invalid struct type: {type_str}")
        struct_content = type_str[7:-1].strip()
        if not struct_content:
            raise ValueError(f"Invalid struct type: {type_str}")
        
        # Check if struct content is a nested struct (no field name)
        if struct_content.startswith("struct<"):
            # This is a nested struct without a field name
            # Parse it as a type
            nested_type = _parse_type(struct_content)
            # Create a wrapper struct with a single field
            return MockStructType([MockStructField(name="", dataType=nested_type, nullable=True)])
        else:
            # Normal struct with fields
            struct_schema = parse_ddl_schema(struct_content)
            return struct_schema
    
    # Handle simple types
    type_lower = type_str.lower()
    if type_lower in TYPE_MAPPING:
        return TYPE_MAPPING[type_lower]()
    
    # If we get here, the type is not recognized
    # Check if it contains spaces (indicating missing comma)
    # But allow spaces in complex types (struct<>, array<>, map<>)
    if ' ' in type_str and not any(type_str.startswith(prefix) for prefix in ['struct<', 'array<', 'map<']):
        raise ValueError(f"Invalid field definition: {type_str}")
    
    # Default to string if type not recognized
    return StringType()


def _find_map_comma(s: str) -> int:
    """Find the comma that separates map key and value types.
    
    Handles nested generics correctly.
    
    Args:
        s: String to search (e.g., "string,long" or "string,array<long>")
    
    Returns:
        Index of the comma, or -1 if not found
    """
    depth = 0
    for i, char in enumerate(s):
        if char == "<":
            depth += 1
        elif char == ">":
            depth -= 1
        elif char == "," and depth == 0:
            return i
    return -1

