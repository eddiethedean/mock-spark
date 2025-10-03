"""
Unit tests for Mock-Spark data types.

These tests verify all data types work correctly without real PySpark.
"""

import pytest
from datetime import datetime, date
from mock_spark import MockSparkSession
from mock_spark.spark_types import (
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    BooleanType,
    DateType,
    TimestampType,
    DecimalType,
    ArrayType,
    MapType,
    BinaryType,
    NullType,
    FloatType,
    ShortType,
    ByteType,
    MockStructType,
    MockStructField,
)


@pytest.mark.fast
class TestDataTypes:
    """Test Mock-Spark data types."""

    @pytest.fixture
    def spark(self):
        """Create a MockSparkSession for testing."""
        return MockSparkSession("test")

    def test_basic_types(self, spark):
        """Test basic data types."""
        data = [
            {"name": "Alice", "age": 25, "height": 5.6, "active": True},
            {"name": "Bob", "age": 30, "height": 5.8, "active": False},
        ]

        schema = MockStructType(
            [
                MockStructField("name", StringType()),
                MockStructField("age", IntegerType()),
                MockStructField("height", DoubleType()),
                MockStructField("active", BooleanType()),
            ]
        )

        df = spark.createDataFrame(data, schema)
        assert df.count() == 2
        assert df.schema == schema

    def test_numeric_types(self, spark):
        """Test numeric data types."""
        data = [
            {"int_val": 1, "long_val": 2, "float_val": 3.14, "double_val": 2.718},
            {
                "int_val": 10,
                "long_val": 20,
                "float_val": 3.14159,
                "double_val": 2.71828,
            },
        ]

        schema = MockStructType(
            [
                MockStructField("int_val", IntegerType()),
                MockStructField("long_val", LongType()),
                MockStructField("float_val", FloatType()),
                MockStructField("double_val", DoubleType()),
            ]
        )

        df = spark.createDataFrame(data, schema)
        assert df.count() == 2
        assert df.schema == schema

    def test_date_time_types(self, spark):
        """Test date and time data types."""
        data = [
            {
                "birth_date": date(1990, 1, 1),
                "created_at": datetime(2023, 1, 1, 12, 0, 0),
            },
            {
                "birth_date": date(1985, 6, 15),
                "created_at": datetime(2023, 6, 15, 18, 30, 0),
            },
        ]

        schema = MockStructType(
            [
                MockStructField("birth_date", DateType()),
                MockStructField("created_at", TimestampType()),
            ]
        )

        df = spark.createDataFrame(data, schema)
        assert df.count() == 2
        assert df.schema == schema

    def test_decimal_type(self, spark):
        """Test decimal data type."""
        data = [
            {"price": 19.99, "quantity": 2.5},
            {"price": 29.99, "quantity": 1.0},
        ]

        schema = MockStructType(
            [
                MockStructField("price", DecimalType(10, 2)),
                MockStructField("quantity", DecimalType(5, 2)),
            ]
        )

        df = spark.createDataFrame(data, schema)
        assert df.count() == 2
        assert df.schema == schema

    def test_array_type(self, spark):
        """Test array data type."""
        data = [
            {"name": "Alice", "hobbies": ["reading", "swimming", "coding"]},
            {"name": "Bob", "hobbies": ["gaming", "cooking"]},
        ]

        schema = MockStructType(
            [
                MockStructField("name", StringType()),
                MockStructField("hobbies", ArrayType(StringType())),
            ]
        )

        df = spark.createDataFrame(data, schema)
        assert df.count() == 2
        assert df.schema == schema

    def test_map_type(self, spark):
        """Test map data type."""
        data = [
            {"name": "Alice", "scores": {"math": 95, "english": 88, "science": 92}},
            {"name": "Bob", "scores": {"math": 87, "english": 91, "science": 89}},
        ]

        schema = MockStructType(
            [
                MockStructField("name", StringType()),
                MockStructField("scores", MapType(StringType(), IntegerType())),
            ]
        )

        df = spark.createDataFrame(data, schema)
        assert df.count() == 2
        assert df.schema == schema

    def test_binary_type(self, spark):
        """Test binary data type."""
        data = [
            {"name": "Alice", "data": b"binary_data_1"},
            {"name": "Bob", "data": b"binary_data_2"},
        ]

        schema = MockStructType(
            [
                MockStructField("name", StringType()),
                MockStructField("data", BinaryType()),
            ]
        )

        df = spark.createDataFrame(data, schema)
        assert df.count() == 2
        assert df.schema == schema

    def test_null_type(self, spark):
        """Test null data type."""
        data = [
            {"name": "Alice", "optional_field": None},
            {"name": "Bob", "optional_field": None},
        ]

        schema = MockStructType(
            [
                MockStructField("name", StringType()),
                MockStructField("optional_field", NullType()),
            ]
        )

        df = spark.createDataFrame(data, schema)
        assert df.count() == 2
        assert df.schema == schema

    def test_complex_nested_schema(self, spark):
        """Test complex nested schema."""
        data = [
            {
                "id": 1,
                "person": {
                    "name": "Alice",
                    "age": 25,
                    "addresses": [
                        {"street": "123 Main St", "city": "New York"},
                        {"street": "456 Oak Ave", "city": "Boston"},
                    ],
                },
            }
        ]

        address_schema = MockStructType(
            [
                MockStructField("street", StringType()),
                MockStructField("city", StringType()),
            ]
        )

        person_schema = MockStructType(
            [
                MockStructField("name", StringType()),
                MockStructField("age", IntegerType()),
                MockStructField("addresses", ArrayType(address_schema)),
            ]
        )

        schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("person", person_schema),
            ]
        )

        df = spark.createDataFrame(data, schema)
        assert df.count() == 1
        assert df.schema == schema

    def test_type_inference(self, spark):
        """Test automatic type inference."""
        data = [
            {"name": "Alice", "age": 25, "height": 5.6, "active": True},
            {"name": "Bob", "age": 30, "height": 5.8, "active": False},
        ]

        # Create DataFrame without explicit schema
        df = spark.createDataFrame(data)
        assert df.count() == 2

        # Check that types were inferred correctly
        dtypes = df.dtypes
        assert ("name", "string") in dtypes
        assert ("age", "bigint") in dtypes
        assert ("height", "double") in dtypes
        assert ("active", "boolean") in dtypes

    def test_schema_validation(self, spark):
        """Test schema validation."""
        data = [
            {"name": "Alice", "age": 25},
            {"name": "Bob", "age": 30},
        ]

        schema = MockStructType(
            [
                MockStructField("name", StringType()),
                MockStructField("age", IntegerType()),
            ]
        )

        df = spark.createDataFrame(data, schema)
        assert df.schema == schema

        # Test that schema properties work
        assert df.columns == ["name", "age"]
        assert len(df.schema.fields) == 2
        assert df.schema.fields[0].name == "name"
        assert df.schema.fields[0].dataType == StringType()
        assert df.schema.fields[1].name == "age"
        assert df.schema.fields[1].dataType == IntegerType()

    def test_short_and_byte_types(self, spark):
        """Test short and byte data types."""
        data = [
            {"short_val": 1, "byte_val": 2},
            {"short_val": 10, "byte_val": 20},
        ]

        schema = MockStructType(
            [
                MockStructField("short_val", ShortType()),
                MockStructField("byte_val", ByteType()),
            ]
        )

        df = spark.createDataFrame(data, schema)
        assert df.count() == 2
        assert df.schema == schema

    def test_mixed_data_types(self, spark):
        """Test DataFrame with mixed data types."""
        data = [
            {
                "id": 1,
                "name": "Alice",
                "age": 25,
                "salary": 50000.0,
                "active": True,
                "hobbies": ["reading", "swimming"],
                "metadata": {"department": "Engineering", "level": "Senior"},
                "created_at": datetime(2023, 1, 1, 12, 0, 0),
                "data": b"binary_data",
            }
        ]

        schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType()),
                MockStructField("age", IntegerType()),
                MockStructField("salary", DoubleType()),
                MockStructField("active", BooleanType()),
                MockStructField("hobbies", ArrayType(StringType())),
                MockStructField("metadata", MapType(StringType(), StringType())),
                MockStructField("created_at", TimestampType()),
                MockStructField("data", BinaryType()),
            ]
        )

        df = spark.createDataFrame(data, schema)
        assert df.count() == 1
        assert df.schema == schema
