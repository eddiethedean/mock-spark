"""
Tests for Mock Types implementation.
"""

import pytest
from mock_spark.spark_types import (
    MockDataType, StringType, IntegerType, LongType, DoubleType, BooleanType,
    DateType, TimestampType, DecimalType, ArrayType, MapType, StructType,
    MockStructType, MockStructField, MockRow
)


class TestMockDataType:
    """Test MockDataType base class."""
    
    def test_data_type_creation(self):
        """Test creating MockDataType."""
        data_type = MockDataType()
        assert data_type is not None
    
    def test_data_type_repr(self):
        """Test string representation."""
        data_type = MockDataType()
        repr_str = repr(data_type)
        assert "MockDataType" in repr_str


class TestStringType:
    """Test StringType."""
    
    def test_string_type_creation(self):
        """Test creating StringType."""
        string_type = StringType()
        assert isinstance(string_type, MockDataType)
    
    def test_string_type_repr(self):
        """Test string representation."""
        string_type = StringType()
        repr_str = repr(string_type)
        assert "StringType" in repr_str


class TestIntegerType:
    """Test IntegerType."""
    
    def test_integer_type_creation(self):
        """Test creating IntegerType."""
        int_type = IntegerType()
        assert isinstance(int_type, MockDataType)
    
    def test_integer_type_repr(self):
        """Test string representation."""
        int_type = IntegerType()
        repr_str = repr(int_type)
        assert "IntegerType" in repr_str


class TestLongType:
    """Test LongType."""
    
    def test_long_type_creation(self):
        """Test creating LongType."""
        long_type = LongType()
        assert isinstance(long_type, MockDataType)
    
    def test_long_type_repr(self):
        """Test string representation."""
        long_type = LongType()
        repr_str = repr(long_type)
        assert "LongType" in repr_str


class TestDoubleType:
    """Test DoubleType."""
    
    def test_double_type_creation(self):
        """Test creating DoubleType."""
        double_type = DoubleType()
        assert isinstance(double_type, MockDataType)
    
    def test_double_type_repr(self):
        """Test string representation."""
        double_type = DoubleType()
        repr_str = repr(double_type)
        assert "DoubleType" in repr_str


class TestBooleanType:
    """Test BooleanType."""
    
    def test_boolean_type_creation(self):
        """Test creating BooleanType."""
        bool_type = BooleanType()
        assert isinstance(bool_type, MockDataType)
    
    def test_boolean_type_repr(self):
        """Test string representation."""
        bool_type = BooleanType()
        repr_str = repr(bool_type)
        assert "BooleanType" in repr_str


class TestDateType:
    """Test DateType."""
    
    def test_date_type_creation(self):
        """Test creating DateType."""
        date_type = DateType()
        assert isinstance(date_type, MockDataType)
    
    def test_date_type_repr(self):
        """Test string representation."""
        date_type = DateType()
        repr_str = repr(date_type)
        assert "DateType" in repr_str


class TestTimestampType:
    """Test TimestampType."""
    
    def test_timestamp_type_creation(self):
        """Test creating TimestampType."""
        timestamp_type = TimestampType()
        assert isinstance(timestamp_type, MockDataType)
    
    def test_timestamp_type_repr(self):
        """Test string representation."""
        timestamp_type = TimestampType()
        repr_str = repr(timestamp_type)
        assert "TimestampType" in repr_str


class TestDecimalType:
    """Test DecimalType."""
    
    def test_decimal_type_creation(self):
        """Test creating DecimalType."""
        decimal_type = DecimalType()
        assert isinstance(decimal_type, MockDataType)
    
    def test_decimal_type_creation_with_precision(self):
        """Test creating DecimalType with precision."""
        decimal_type = DecimalType(precision=10, scale=2)
        assert isinstance(decimal_type, MockDataType)
    
    def test_decimal_type_repr(self):
        """Test string representation."""
        decimal_type = DecimalType()
        repr_str = repr(decimal_type)
        assert "DecimalType" in repr_str


class TestArrayType:
    """Test ArrayType."""
    
    def test_array_type_creation(self):
        """Test creating ArrayType."""
        array_type = ArrayType(StringType())
        assert isinstance(array_type, MockDataType)
        assert isinstance(array_type.element_type, StringType)
    
    def test_array_type_repr(self):
        """Test string representation."""
        array_type = ArrayType(StringType())
        repr_str = repr(array_type)
        assert "ArrayType" in repr_str
        assert "StringType" in repr_str


class TestMapType:
    """Test MapType."""
    
    def test_map_type_creation(self):
        """Test creating MapType."""
        map_type = MapType(StringType(), IntegerType())
        assert isinstance(map_type, MockDataType)
        assert isinstance(map_type.key_type, StringType)
        assert isinstance(map_type.value_type, IntegerType)
    
    def test_map_type_repr(self):
        """Test string representation."""
        map_type = MapType(StringType(), IntegerType())
        repr_str = repr(map_type)
        assert "MapType" in repr_str
        assert "StringType" in repr_str
        assert "IntegerType" in repr_str


class TestStructType:
    """Test StructType."""
    
    def test_struct_type_creation(self):
        """Test creating StructType."""
        struct_type = StructType()
        assert isinstance(struct_type, MockDataType)
        assert struct_type.fields == []
    
    def test_struct_type_creation_with_fields(self):
        """Test creating StructType with fields."""
        fields = [
            MockStructField("id", IntegerType()),
            MockStructField("name", StringType())
        ]
        struct_type = StructType(fields)
        assert len(struct_type.fields) == 2
        assert struct_type.fields[0].name == "id"
        assert struct_type.fields[1].name == "name"
    
    def test_struct_type_repr(self):
        """Test string representation."""
        struct_type = StructType()
        repr_str = repr(struct_type)
        assert "StructType" in repr_str


class TestMockStructType:
    """Test MockStructType."""
    
    def test_mock_struct_type_creation(self):
        """Test creating MockStructType."""
        struct_type = MockStructType()
        assert isinstance(struct_type, StructType)
        assert struct_type.fields == []
    
    def test_mock_struct_type_creation_with_fields(self):
        """Test creating MockStructType with fields."""
        fields = [
            MockStructField("id", IntegerType()),
            MockStructField("name", StringType())
        ]
        struct_type = MockStructType(fields)
        assert len(struct_type.fields) == 2
    
    def test_add_field(self):
        """Test adding field to MockStructType."""
        struct_type = MockStructType()
        field = MockStructField("id", IntegerType())
        struct_type.add_field(field)
        
        assert len(struct_type.fields) == 1
        assert struct_type.fields[0] == field
    
    def test_field_names(self):
        """Test getting field names."""
        fields = [
            MockStructField("id", IntegerType()),
            MockStructField("name", StringType()),
            MockStructField("age", IntegerType())
        ]
        struct_type = MockStructType(fields)
        
        field_names = struct_type.fieldNames()
        assert field_names == ["id", "name", "age"]
    
    def test_get_field_by_name(self):
        """Test getting field by name."""
        fields = [
            MockStructField("id", IntegerType()),
            MockStructField("name", StringType())
        ]
        struct_type = MockStructType(fields)
        
        field = struct_type.get_field_by_name("name")
        assert field is not None
        assert field.name == "name"
        assert isinstance(field.field_type, StringType)
        
        # Test nonexistent field
        field = struct_type.get_field_by_name("nonexistent")
        assert field is None
    
    def test_has_field(self):
        """Test checking if field exists."""
        fields = [
            MockStructField("id", IntegerType()),
            MockStructField("name", StringType())
        ]
        struct_type = MockStructType(fields)
        
        assert struct_type.has_field("id")
        assert struct_type.has_field("name")
        assert not struct_type.has_field("age")
    
    def test_mock_struct_type_repr(self):
        """Test string representation."""
        struct_type = MockStructType()
        repr_str = repr(struct_type)
        assert "MockStructType" in repr_str


class TestMockStructField:
    """Test MockStructField."""
    
    def test_struct_field_creation(self):
        """Test creating MockStructField."""
        field = MockStructField("name", StringType())
        assert field.name == "name"
        assert isinstance(field.field_type, StringType)
        assert field.nullable is True
    
    def test_struct_field_creation_with_nullable(self):
        """Test creating MockStructField with nullable parameter."""
        field = MockStructField("id", IntegerType(), nullable=False)
        assert field.name == "id"
        assert isinstance(field.field_type, IntegerType)
        assert field.nullable is False
    
    def test_struct_field_repr(self):
        """Test string representation."""
        field = MockStructField("name", StringType())
        repr_str = repr(field)
        assert "MockStructField" in repr_str
        assert "name" in repr_str
        assert "StringType" in repr_str


class TestMockRow:
    """Test MockRow."""
    
    def test_mock_row_creation(self):
        """Test creating MockRow."""
        data = {"id": 1, "name": "Alice", "age": 25}
        row = MockRow(data)
        assert row.data == data
    
    def test_mock_row_getitem(self):
        """Test getting item from MockRow."""
        data = {"id": 1, "name": "Alice", "age": 25}
        row = MockRow(data)
        
        assert row["id"] == 1
        assert row["name"] == "Alice"
        assert row["age"] == 25
    
    def test_mock_row_getitem_nonexistent(self):
        """Test getting nonexistent item from MockRow."""
        data = {"id": 1, "name": "Alice"}
        row = MockRow(data)
        
        with pytest.raises(KeyError):
            row["nonexistent"]
    
    def test_mock_row_contains(self):
        """Test checking if key exists in MockRow."""
        data = {"id": 1, "name": "Alice", "age": 25}
        row = MockRow(data)
        
        assert "id" in row
        assert "name" in row
        assert "age" in row
        assert "nonexistent" not in row
    
    def test_mock_row_keys(self):
        """Test getting keys from MockRow."""
        data = {"id": 1, "name": "Alice", "age": 25}
        row = MockRow(data)
        
        keys = list(row.keys())
        assert "id" in keys
        assert "name" in keys
        assert "age" in keys
        assert len(keys) == 3
    
    def test_mock_row_values(self):
        """Test getting values from MockRow."""
        data = {"id": 1, "name": "Alice", "age": 25}
        row = MockRow(data)
        
        values = list(row.values())
        assert 1 in values
        assert "Alice" in values
        assert 25 in values
        assert len(values) == 3
    
    def test_mock_row_items(self):
        """Test getting items from MockRow."""
        data = {"id": 1, "name": "Alice", "age": 25}
        row = MockRow(data)
        
        items = list(row.items())
        assert ("id", 1) in items
        assert ("name", "Alice") in items
        assert ("age", 25) in items
        assert len(items) == 3
    
    def test_mock_row_len(self):
        """Test getting length of MockRow."""
        data = {"id": 1, "name": "Alice", "age": 25}
        row = MockRow(data)
        
        assert len(row) == 3
    
    def test_mock_row_repr(self):
        """Test string representation."""
        data = {"id": 1, "name": "Alice", "age": 25}
        row = MockRow(data)
        
        repr_str = repr(row)
        assert "MockRow" in repr_str
        assert "id" in repr_str
        assert "Alice" in repr_str