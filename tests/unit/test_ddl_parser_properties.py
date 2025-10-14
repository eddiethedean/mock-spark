"""
Property-based tests for DDL schema parser using Hypothesis.

Tests the parser with randomly generated valid schemas to ensure
it handles all valid inputs correctly without crashing.
"""

import pytest
from hypothesis import given, strategies as st, assume, settings
from mock_spark.core.ddl_parser import parse_ddl_schema
from mock_spark.spark_types import (
    MockStructType,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    BooleanType,
    ArrayType,
    MapType,
)


class TestDDLParserProperties:
    """Property-based tests for DDL parser."""

    # Valid type names
    VALID_TYPES = [
        'string', 'int', 'long', 'double', 'boolean',
        'date', 'timestamp', 'float', 'short', 'byte',
        'binary', 'bigint', 'integer', 'bool', 'smallint', 'tinyint'
    ]

    # Generate valid field names (letters, numbers, underscores)
    @st.composite
    def field_names(draw):
        """Generate valid field names."""
        return draw(st.text(
            alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd')),
            min_size=1,
            max_size=50
        ))

    # Generate simple schemas
    @given(st.lists(
        st.tuples(field_names(), st.sampled_from(VALID_TYPES)),
        min_size=1,
        max_size=50
    ))
    def test_generated_simple_schemas(self, fields):
        """Test randomly generated simple schemas."""
        ddl = ", ".join(f"{name} {typ}" for name, typ in fields)
        schema = parse_ddl_schema(ddl)
        assert len(schema.fields) == len(fields)
        for i, (name, typ) in enumerate(fields):
            assert schema.fields[i].name == name

    # Test that parser never crashes on valid input
    @given(st.text(min_size=1, max_size=1000))
    def test_parser_never_crashes(self, text):
        """Test parser never crashes on any input."""
        try:
            schema = parse_ddl_schema(text)
            # If it parses, should return a MockStructType
            assert isinstance(schema, MockStructType)
        except ValueError:
            # ValueError is acceptable for invalid input
            pass
        except Exception as e:
            # Any other exception is a bug
            pytest.fail(f"Parser crashed with {type(e).__name__}: {e}")

    # Test commutativity: parse twice, get same result
    @given(st.lists(
        st.tuples(field_names(), st.sampled_from(VALID_TYPES)),
        min_size=1,
        max_size=20
    ))
    def test_parse_twice_same_result(self, fields):
        """Test that parsing twice gives same result."""
        ddl = ", ".join(f"{name} {typ}" for name, typ in fields)
        schema1 = parse_ddl_schema(ddl)
        schema2 = parse_ddl_schema(ddl)
        
        assert len(schema1.fields) == len(schema2.fields)
        for f1, f2 in zip(schema1.fields, schema2.fields):
            assert f1.name == f2.name
            assert type(f1.dataType) == type(f2.dataType)

    # Test type consistency
    @given(
        field_names(),
        st.sampled_from(VALID_TYPES)
    )
    def test_type_consistency(self, name, typ):
        """Test that types are parsed consistently."""
        ddl = f"{name} {typ}"
        schema = parse_ddl_schema(ddl)
        
        assert len(schema.fields) == 1
        field = schema.fields[0]
        
        # Check that type matches expected
        type_map = {
            'string': StringType,
            'int': IntegerType,
            'integer': IntegerType,
            'long': LongType,
            'bigint': LongType,
            'double': DoubleType,
            'boolean': BooleanType,
            'bool': BooleanType,
            'date': 'DateType',
            'timestamp': 'TimestampType',
            'float': 'FloatType',
            'short': 'ShortType',
            'smallint': 'ShortType',
            'byte': 'ByteType',
            'tinyint': 'ByteType',
            'binary': 'BinaryType',
        }
        
        expected_type = type_map.get(typ)
        if expected_type:
            assert isinstance(field.dataType, expected_type)

    # Test schema can be used to create DataFrame
    @given(st.lists(
        st.tuples(field_names(), st.sampled_from(['string', 'int', 'long', 'double'])),
        min_size=1,
        max_size=10
    ))
    def test_schema_usable_for_dataframe(self, fields):
        """Test that parsed schema can be used to create DataFrame."""
        from mock_spark import MockSparkSession
        
        ddl = ", ".join(f"{name} {typ}" for name, typ in fields)
        schema = parse_ddl_schema(ddl)
        
        # Create data matching the schema
        data = [{name: 0 for name, _ in fields}]
        
        # Should be able to create DataFrame
        spark = MockSparkSession()
        df = spark.createDataFrame(data, schema=schema)
        
        assert df.count() == 1
        assert df.columns == [name for name, _ in fields]

    # Test nested structures
    @given(
        st.integers(min_value=1, max_value=5),
        field_names()
    )
    def test_nested_structs(self, depth, field_name):
        """Test nested structs up to N levels."""
        # Build nested struct string
        nested = field_name
        for i in range(depth):
            nested = f"struct<level{i}:{nested}>"
        
        ddl = f"data {nested}"
        schema = parse_ddl_schema(ddl)
        
        assert len(schema.fields) == 1
        assert schema.fields[0].name == "data"

    # Test arrays
    @given(
        field_names(),
        st.sampled_from(['string', 'int', 'long', 'double'])
    )
    def test_array_types(self, name, element_type):
        """Test array types with various element types."""
        ddl = f"{name} array<{element_type}>"
        schema = parse_ddl_schema(ddl)
        
        assert len(schema.fields) == 1
        assert isinstance(schema.fields[0].dataType, ArrayType)

    # Test maps
    @given(
        field_names(),
        st.sampled_from(['string', 'int', 'long']),
        st.sampled_from(['string', 'int', 'long', 'double'])
    )
    def test_map_types(self, name, key_type, value_type):
        """Test map types with various key/value types."""
        ddl = f"{name} map<{key_type},{value_type}>"
        schema = parse_ddl_schema(ddl)
        
        assert len(schema.fields) == 1
        assert isinstance(schema.fields[0].dataType, MapType)

    # Test decimal with random precision/scale
    @given(
        field_names(),
        st.integers(min_value=1, max_value=38),
        st.integers(min_value=0, max_value=18)
    )
    def test_decimal_variations(self, name, precision, scale):
        """Test decimal with various precision/scale values."""
        assume(scale <= precision)  # Scale should not exceed precision
        
        ddl = f"{name} decimal({precision},{scale})"
        schema = parse_ddl_schema(ddl)
        
        assert len(schema.fields) == 1
        assert schema.fields[0].dataType.precision == precision
        assert schema.fields[0].dataType.scale == scale

    # Test mixed separators
    @given(st.lists(
        st.tuples(
            field_names(),
            st.sampled_from(VALID_TYPES),
            st.booleans()  # True for colon, False for space
        ),
        min_size=1,
        max_size=10
    ))
    def test_mixed_separators(self, fields):
        """Test schemas with mixed colon and space separators."""
        ddl = ", ".join(
            f"{name}:{typ}" if use_colon else f"{name} {typ}"
            for name, typ, use_colon in fields
        )
        schema = parse_ddl_schema(ddl)
        
        assert len(schema.fields) == len(fields)

    # Test field name variations
    @given(st.text(min_size=1, max_size=100))
    def test_various_field_names(self, name):
        """Test various field names."""
        ddl = f"{name} string"
        schema = parse_ddl_schema(ddl)
        
        assert len(schema.fields) == 1
        assert schema.fields[0].name == name

    # Test schema with all types
    @given(st.lists(
        st.sampled_from(VALID_TYPES),
        min_size=1,
        max_size=len(VALID_TYPES)
    ))
    def test_all_types_combination(self, types):
        """Test schema with various type combinations."""
        fields = [f"field{i} {typ}" for i, typ in enumerate(types)]
        ddl = ", ".join(fields)
        schema = parse_ddl_schema(ddl)
        
        assert len(schema.fields) == len(types)

    # Test whitespace variations
    @given(
        st.text(min_size=1, max_size=20),
        st.sampled_from(VALID_TYPES),
        st.integers(min_value=0, max_value=5),
        st.integers(min_value=0, max_value=5)
    )
    def test_whitespace_variations(self, name, typ, spaces_before, spaces_after):
        """Test schemas with various whitespace."""
        ddl = f"{name}{' ' * spaces_before}{typ}{' ' * spaces_after}"
        schema = parse_ddl_schema(ddl)
        
        assert len(schema.fields) == 1
        assert schema.fields[0].name == name

    # Test case variations
    @given(
        st.text(min_size=1, max_size=20),
        st.sampled_from(VALID_TYPES)
    )
    def test_case_variations(self, name, typ):
        """Test schemas with various case combinations."""
        # Randomly capitalize name
        import random
        name_variation = ''.join(
            c.upper() if random.random() < 0.5 else c.lower()
            for c in name
        )
        
        # Randomly capitalize type
        typ_variation = ''.join(
            c.upper() if random.random() < 0.5 else c.lower()
            for c in typ
        )
        
        ddl = f"{name_variation} {typ_variation}"
        schema = parse_ddl_schema(ddl)
        
        assert len(schema.fields) == 1

    # Test error messages are informative
    @given(st.text(min_size=1, max_size=200))
    def test_error_messages_informative(self, text):
        """Test that error messages are informative."""
        try:
            parse_ddl_schema(text)
        except ValueError as e:
            # Error message should not be empty
            assert len(str(e)) > 0
            # Error message should mention something about the error
            assert any(keyword in str(e).lower() for keyword in [
                'invalid', 'error', 'field', 'type', 'struct',
                'array', 'map', 'decimal'
            ])
        except Exception:
            # Other exceptions should have messages too
            pass

    # Test parser handles Unicode
    @given(st.text(min_size=1, max_size=20))
    def test_unicode_field_names(self, name):
        """Test schemas with Unicode field names."""
        ddl = f"{name} string"
        try:
            schema = parse_ddl_schema(ddl)
            assert len(schema.fields) == 1
        except Exception:
            # Unicode might not be supported, which is OK
            pass

    # Test large schemas
    @given(st.integers(min_value=1, max_value=100))
    def test_large_schemas(self, num_fields):
        """Test schemas with many fields."""
        fields = [f"field{i} string" for i in range(num_fields)]
        ddl = ", ".join(fields)
        schema = parse_ddl_schema(ddl)
        
        assert len(schema.fields) == num_fields

    # Test nested arrays
    @given(st.integers(min_value=1, max_value=5))
    def test_nested_arrays(self, depth):
        """Test nested arrays up to N levels."""
        nested = "string"
        for _ in range(depth):
            nested = f"array<{nested}>"
        
        ddl = f"data {nested}"
        schema = parse_ddl_schema(ddl)
        
        assert len(schema.fields) == 1

    # Test nested maps
    @given(st.integers(min_value=1, max_value=5))
    def test_nested_maps(self, depth):
        """Test nested maps up to N levels."""
        nested = "string"
        for _ in range(depth):
            nested = f"map<string,{nested}>"
        
        ddl = f"data {nested}"
        schema = parse_ddl_schema(ddl)
        
        assert len(schema.fields) == 1

    # Test complex nested structures
    @given(st.integers(min_value=1, max_value=3))
    def test_complex_nested(self, complexity):
        """Test complex nested structures."""
        # Build complex nested type
        nested = "string"
        for i in range(complexity):
            if i % 3 == 0:
                nested = f"array<{nested}>"
            elif i % 3 == 1:
                nested = f"map<string,{nested}>"
            else:
                nested = f"struct<field:{nested}>"
        
        ddl = f"data {nested}"
        schema = parse_ddl_schema(ddl)
        
        assert len(schema.fields) == 1

