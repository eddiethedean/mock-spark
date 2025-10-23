"""
Unit tests for type inference and casting edge cases.

Tests schema inference, type detection, and casting operations
to ensure Mock-Spark handles all PySpark type scenarios correctly.
"""

import pytest
from mock_spark import MockSparkSession, F
from mock_spark.spark_types import (
    MockStructType,
    MockStructField,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    FloatType,
    BooleanType,
    DateType,
    TimestampType,
    TimestampNTZType,
    BinaryType,
    DecimalType,
    ArrayType,
    MapType,
    CharType,
    VarcharType,
    IntervalType,
    YearMonthIntervalType,
    DayTimeIntervalType,
)


class TestTypeInference:
    """Test type inference from Python objects."""

    def test_primitive_type_inference(self, spark):
        """Test inference of primitive types."""
        data = [
            {"string_val": "hello", "int_val": 42, "float_val": 3.14, "bool_val": True},
            {"string_val": "world", "int_val": 100, "float_val": 2.71, "bool_val": False}
        ]
        
        df = spark.createDataFrame(data)
        
        # Check inferred types
        string_field = df.schema.get_field_by_name("string_val")
        int_field = df.schema.get_field_by_name("int_val")
        float_field = df.schema.get_field_by_name("float_val")
        bool_field = df.schema.get_field_by_name("bool_val")
        
        assert isinstance(string_field.dataType, StringType)
        assert isinstance(int_field.dataType, LongType)  # PySpark uses Long for int
        assert isinstance(float_field.dataType, DoubleType)  # PySpark uses Double for float
        assert isinstance(bool_field.dataType, BooleanType)

    def test_date_string_inference(self, spark):
        """Test inference of date strings."""
        data = [
            {"date_str": "2024-01-15", "timestamp_str": "2024-01-15 10:30:00"},
            {"date_str": "2024-01-16", "timestamp_str": "2024-01-16 14:45:00"}
        ]
        
        df = spark.createDataFrame(data)
        
        # Check inferred types
        date_field = df.schema.get_field_by_name("date_str")
        timestamp_field = df.schema.get_field_by_name("timestamp_str")
        
        assert isinstance(date_field.dataType, DateType)
        assert isinstance(timestamp_field.dataType, TimestampType)

    def test_array_type_inference(self, spark):
        """Test inference of array types."""
        data = [
            {"numbers": [1, 2, 3], "strings": ["a", "b", "c"]},
            {"numbers": [4, 5, 6], "strings": ["d", "e", "f"]}
        ]
        
        df = spark.createDataFrame(data)
        
        # Check inferred types
        numbers_field = df.schema.get_field_by_name("numbers")
        strings_field = df.schema.get_field_by_name("strings")
        
        assert isinstance(numbers_field.dataType, ArrayType)
        assert isinstance(strings_field.dataType, ArrayType)
        assert isinstance(numbers_field.dataType.element_type, LongType)
        assert isinstance(strings_field.dataType.element_type, StringType)

    def test_map_type_inference(self, spark):
        """Test inference of map types."""
        data = [
            {"metadata": {"key1": "value1", "key2": "value2"}},
            {"metadata": {"key3": "value3", "key4": "value4"}}
        ]
        
        df = spark.createDataFrame(data)
        
        # Check inferred types
        metadata_field = df.schema.get_field_by_name("metadata")
        assert isinstance(metadata_field.dataType, MapType)
        assert isinstance(metadata_field.dataType.key_type, StringType)
        assert isinstance(metadata_field.dataType.value_type, StringType)

    def test_binary_type_inference(self, spark):
        """Test inference of binary types."""
        data = [
            {"binary_data": b"hello world"},
            {"binary_data": b"test data"}
        ]
        
        df = spark.createDataFrame(data)
        
        # Check inferred types
        binary_field = df.schema.get_field_by_name("binary_data")
        assert isinstance(binary_field.dataType, BinaryType)

    def test_null_handling_inference(self, spark):
        """Test inference with null values."""
        data = [
            {"value": 42, "nullable_value": None},
            {"value": 100, "nullable_value": "test"}
        ]
        
        df = spark.createDataFrame(data)
        
        # Check inferred types
        value_field = df.schema.get_field_by_name("value")
        nullable_field = df.schema.get_field_by_name("nullable_value")
        
        assert isinstance(value_field.dataType, LongType)
        assert isinstance(nullable_field.dataType, StringType)
        assert value_field.nullable
        assert nullable_field.nullable

    def test_mixed_type_conflict(self, spark):
        """Test type conflict detection."""
        data = [
            {"mixed": 42},
            {"mixed": "string"}
        ]
        
        # This should raise a TypeError due to type conflict
        with pytest.raises(TypeError, match="Can not merge type"):
            spark.createDataFrame(data)

    def test_all_null_column(self, spark):
        """Test handling of all-null columns."""
        data = [
            {"value": 42, "null_col": None},
            {"value": 100, "null_col": None}
        ]
        
        # This should raise a ValueError for all-null column
        with pytest.raises(ValueError, match="Some of types cannot be determined"):
            spark.createDataFrame(data)


class TestTypeCasting:
    """Test type casting operations."""

    def test_string_to_numeric_casting(self, spark):
        """Test casting string to numeric types."""
        data = [
            {"string_num": "42", "string_float": "3.14"},
            {"string_num": "100", "string_float": "2.71"}
        ]
        
        df = spark.createDataFrame(data)
        
        # Test casting to integer
        result = df.withColumn("int_val", df.string_num.cast("int"))
        int_field = result.schema.get_field_by_name("int_val")
        assert isinstance(int_field.dataType, IntegerType)
        
        # Test casting to double
        result = df.withColumn("double_val", df.string_float.cast("double"))
        double_field = result.schema.get_field_by_name("double_val")
        assert isinstance(double_field.dataType, DoubleType)

    def test_numeric_to_string_casting(self, spark):
        """Test casting numeric to string."""
        data = [
            {"int_val": 42, "double_val": 3.14},
            {"int_val": 100, "double_val": 2.71}
        ]
        
        df = spark.createDataFrame(data)
        
        # Test casting to string
        result = df.withColumn("int_str", df.int_val.cast("string"))
        result = result.withColumn("double_str", df.double_val.cast("string"))
        
        int_str_field = result.schema.get_field_by_name("int_str")
        double_str_field = result.schema.get_field_by_name("double_str")
        
        assert isinstance(int_str_field.dataType, StringType)
        assert isinstance(double_str_field.dataType, StringType)

    def test_boolean_casting(self, spark):
        """Test boolean casting."""
        data = [
            {"int_val": 1, "string_val": "true"},
            {"int_val": 0, "string_val": "false"}
        ]
        
        df = spark.createDataFrame(data)
        
        # Test casting to boolean
        result = df.withColumn("bool_from_int", df.int_val.cast("boolean"))
        result = result.withColumn("bool_from_string", df.string_val.cast("boolean"))
        
        bool_int_field = result.schema.get_field_by_name("bool_from_int")
        bool_str_field = result.schema.get_field_by_name("bool_from_string")
        
        assert isinstance(bool_int_field.dataType, BooleanType)
        assert isinstance(bool_str_field.dataType, BooleanType)

    def test_date_timestamp_casting(self, spark):
        """Test date and timestamp casting."""
        data = [
            {"date_str": "2024-01-15", "timestamp_str": "2024-01-15 10:30:00"},
            {"date_str": "2024-01-16", "timestamp_str": "2024-01-16 14:45:00"}
        ]
        
        df = spark.createDataFrame(data)
        
        # Test casting to date
        result = df.withColumn("date_val", df.date_str.cast("date"))
        date_field = result.schema.get_field_by_name("date_val")
        assert isinstance(date_field.dataType, DateType)
        
        # Test casting to timestamp
        result = df.withColumn("timestamp_val", df.timestamp_str.cast("timestamp"))
        timestamp_field = result.schema.get_field_by_name("timestamp_val")
        assert isinstance(timestamp_field.dataType, TimestampType)

    def test_decimal_casting(self, spark):
        """Test decimal casting with precision and scale."""
        data = [
            {"value": 3.14159},
            {"value": 2.71828}
        ]
        
        df = spark.createDataFrame(data)
        
        # Test casting to decimal
        result = df.withColumn("decimal_val", df.value.cast("decimal(10,2)"))
        decimal_field = result.schema.get_field_by_name("decimal_val")
        
        assert isinstance(decimal_field.dataType, DecimalType)
        assert decimal_field.dataType.precision == 10
        assert decimal_field.dataType.scale == 2

    def test_array_casting(self, spark):
        """Test array casting."""
        data = [
            {"numbers": [1, 2, 3]},
            {"numbers": [4, 5, 6]}
        ]
        
        df = spark.createDataFrame(data)
        
        # Test casting array elements
        result = df.withColumn("double_numbers", df.numbers.cast("array<double>"))
        double_array_field = result.schema.get_field_by_name("double_numbers")
        
        assert isinstance(double_array_field.dataType, ArrayType)
        assert isinstance(double_array_field.dataType.element_type, DoubleType)

    def test_safe_casting_with_invalid_values(self, spark):
        """Test safe casting with TRY_CAST for invalid values."""
        data = [
            {"value": "42"},
            {"value": "invalid"},
            {"value": "100"}
        ]
        
        df = spark.createDataFrame(data)
        
        # Test safe casting - should not raise exception
        result = df.withColumn("int_val", df.value.cast("int"))
        
        # Should handle invalid values gracefully
        rows = result.collect()
        assert len(rows) == 3
        # Invalid values should be converted to null or handled safely

    def test_complex_casting_expressions(self, spark):
        """Test complex casting expressions."""
        data = [
            {"value": 42.5, "flag": 1},
            {"value": 100.7, "flag": 0}
        ]
        
        df = spark.createDataFrame(data)
        
        # Test complex expressions with casting
        result = df.withColumn("rounded", F.round(df.value.cast("int")))
        result = result.withColumn("flag_bool", df.flag.cast("boolean"))
        
        rounded_field = result.schema.get_field_by_name("rounded")
        flag_bool_field = result.schema.get_field_by_name("flag_bool")
        
        assert isinstance(rounded_field.dataType, LongType)
        assert isinstance(flag_bool_field.dataType, BooleanType)


class TestAdvancedTypeHandling:
    """Test advanced type handling scenarios."""

    def test_interval_types(self, spark):
        """Test interval type handling."""
        from mock_spark.spark_types import IntervalType, YearMonthIntervalType, DayTimeIntervalType
        
        # Test creating schema with interval types
        schema = MockStructType([
            MockStructField("year_month", YearMonthIntervalType()),
            MockStructField("day_time", DayTimeIntervalType()),
            MockStructField("interval", IntervalType())
        ])
        
        data = [
            {"year_month": None, "day_time": None, "interval": None}
        ]
        
        df = spark.createDataFrame(data, schema)
        
        year_month_field = df.schema.get_field_by_name("year_month")
        day_time_field = df.schema.get_field_by_name("day_time")
        interval_field = df.schema.get_field_by_name("interval")
        
        assert isinstance(year_month_field.dataType, YearMonthIntervalType)
        assert isinstance(day_time_field.dataType, DayTimeIntervalType)
        assert isinstance(interval_field.dataType, IntervalType)

    def test_char_varchar_types(self, spark):
        """Test char and varchar type handling."""
        from mock_spark.spark_types import CharType, VarcharType
        
        # Test creating schema with char/varchar types
        schema = MockStructType([
            MockStructField("char_field", CharType(10)),
            MockStructField("varchar_field", VarcharType(255))
        ])
        
        data = [
            {"char_field": "test", "varchar_field": "longer string"}
        ]
        
        df = spark.createDataFrame(data, schema)
        
        char_field = df.schema.get_field_by_name("char_field")
        varchar_field = df.schema.get_field_by_name("varchar_field")
        
        assert isinstance(char_field.dataType, CharType)
        assert isinstance(varchar_field.dataType, VarcharType)
        assert char_field.dataType.length == 10
        assert varchar_field.dataType.length == 255

    def test_timestamp_ntz_type(self, spark):
        """Test timestamp without timezone type."""
        from mock_spark.spark_types import TimestampNTZType
        
        # Test creating schema with timestamp without timezone
        schema = MockStructType([
            MockStructField("timestamp_ntz", TimestampNTZType())
        ])
        
        data = [
            {"timestamp_ntz": "2024-01-15 10:30:00"}
        ]
        
        df = spark.createDataFrame(data, schema)
        
        timestamp_ntz_field = df.schema.get_field_by_name("timestamp_ntz")
        assert isinstance(timestamp_ntz_field.dataType, TimestampNTZType)

    def test_nested_type_casting(self, spark):
        """Test casting with nested types."""
        data = [
            {"nested": {"key1": "value1", "key2": 42}},
            {"nested": {"key3": "value3", "key4": 100}}
        ]
        
        df = spark.createDataFrame(data)
        
        # Test casting map values
        result = df.withColumn("nested_str", df.nested.cast("map<string,string>"))
        nested_str_field = result.schema.get_field_by_name("nested_str")
        
        assert isinstance(nested_str_field.dataType, MapType)
        assert isinstance(nested_str_field.dataType.key_type, StringType)
        assert isinstance(nested_str_field.dataType.value_type, StringType)

    def test_type_promotion(self, spark):
        """Test type promotion in expressions."""
        data = [
            {"int_val": 42, "double_val": 3.14},
            {"int_val": 100, "double_val": 2.71}
        ]
        
        df = spark.createDataFrame(data)
        
        # Test type promotion in arithmetic
        result = df.withColumn("sum", df.int_val + df.double_val)
        sum_field = result.schema.get_field_by_name("sum")
        
        # Should promote to double
        assert isinstance(sum_field.dataType, DoubleType)

    def test_nullable_handling(self, spark):
        """Test nullable field handling."""
        data = [
            {"required": 42, "optional": 100},
            {"required": 100, "optional": None}
        ]
        
        # Create schema with explicit nullable settings
        schema = MockStructType([
            MockStructField("required", LongType(), nullable=False),
            MockStructField("optional", LongType(), nullable=True)
        ])
        
        df = spark.createDataFrame(data, schema)
        
        required_field = df.schema.get_field_by_name("required")
        optional_field = df.schema.get_field_by_name("optional")
        
        assert not required_field.nullable
        assert optional_field.nullable

    def test_schema_evolution(self, spark):
        """Test schema evolution with type changes."""
        # Create initial schema
        schema1 = MockStructType([
            MockStructField("id", LongType()),
            MockStructField("name", StringType())
        ])
        
        data1 = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"}
        ]
        
        df1 = spark.createDataFrame(data1, schema1)
        
        # Create evolved schema with new field
        schema2 = MockStructType([
            MockStructField("id", LongType()),
            MockStructField("name", StringType()),
            MockStructField("age", LongType())
        ])
        
        data2 = [
            {"id": 3, "name": "Charlie", "age": 25},
            {"id": 4, "name": "David", "age": 30}
        ]
        
        df2 = spark.createDataFrame(data2, schema2)
        
        # Test schema merging
        merged_schema = df1.schema.merge_with(df2.schema)
        assert len(merged_schema.fields) == 3
        assert merged_schema.has_field("id")
        assert merged_schema.has_field("name")
        assert merged_schema.has_field("age")


class TestTypeCastingEdgeCases:
    """Test edge cases in type casting."""

    def test_casting_with_null_values(self, spark):
        """Test casting with null values."""
        data = [
            {"value": None, "string_val": "42"},
            {"value": 100, "string_val": None}
        ]
        
        df = spark.createDataFrame(data)
        
        # Test casting null values
        result = df.withColumn("cast_null", df.value.cast("string"))
        result = result.withColumn("cast_string", df.string_val.cast("int"))
        
        # Should handle nulls gracefully
        rows = result.collect()
        assert len(rows) == 2

    def test_casting_with_empty_strings(self, spark):
        """Test casting with empty strings."""
        data = [
            {"empty_str": "", "whitespace_str": "   "},
            {"empty_str": "42", "whitespace_str": "100"}
        ]
        
        df = spark.createDataFrame(data)
        
        # Test casting empty strings
        result = df.withColumn("empty_int", df.empty_str.cast("int"))
        result = result.withColumn("whitespace_int", df.whitespace_str.cast("int"))
        
        # Should handle empty strings gracefully
        rows = result.collect()
        assert len(rows) == 2

    def test_casting_with_overflow_values(self, spark):
        """Test casting with overflow values."""
        data = [
            {"large_int": "999999999999999999999"},
            {"large_float": "1.7976931348623157e+308"}
        ]
        
        df = spark.createDataFrame(data)
        
        # Test casting overflow values
        result = df.withColumn("large_int_cast", df.large_int.cast("int"))
        result = result.withColumn("large_float_cast", df.large_float.cast("double"))
        
        # Should handle overflow gracefully with TRY_CAST
        rows = result.collect()
        assert len(rows) == 2

    def test_casting_with_special_characters(self, spark):
        """Test casting with special characters."""
        data = [
            {"special_str": "42.5€", "unicode_str": "42.5°"},
            {"special_str": "100$", "unicode_str": "100%"}
        ]
        
        df = spark.createDataFrame(data)
        
        # Test casting strings with special characters
        result = df.withColumn("special_cast", df.special_str.cast("double"))
        result = result.withColumn("unicode_cast", df.unicode_str.cast("double"))
        
        # Should handle special characters gracefully
        rows = result.collect()
        assert len(rows) == 2

    def test_casting_with_boolean_edge_cases(self, spark):
        """Test casting with boolean edge cases."""
        data = [
            {"bool_str": "true", "bool_str2": "false", "bool_str3": "1", "bool_str4": "0"},
            {"bool_str": "TRUE", "bool_str2": "FALSE", "bool_str3": "yes", "bool_str4": "no"}
        ]
        
        df = spark.createDataFrame(data)
        
        # Test casting various boolean representations
        result = df.withColumn("bool1", df.bool_str.cast("boolean"))
        result = result.withColumn("bool2", df.bool_str2.cast("boolean"))
        result = result.withColumn("bool3", df.bool_str3.cast("boolean"))
        result = result.withColumn("bool4", df.bool_str4.cast("boolean"))
        
        # Should handle various boolean representations
        rows = result.collect()
        assert len(rows) == 2
