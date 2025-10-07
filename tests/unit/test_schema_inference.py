"""
Unit Tests for Schema Inference

Fast unit tests for schema inference functionality without PySpark dependency.
These tests verify Mock Spark matches real PySpark schema inference behavior.

Based on actual PySpark 3.2.4 behavior captured on 2025-10-07.
"""

import pytest
from mock_spark import MockSparkSession, F
from mock_spark.spark_types import (
    MockStructType,
    MockStructField,
    LongType,
    DoubleType,
    StringType,
    BooleanType,
    IntegerType,
    ArrayType,
    MapType,
    NullType,
)


class TestBasicTypeDetection:
    """Test basic Python type detection matching PySpark behavior."""

    def test_detect_integer_as_long(self, spark):
        """Test that integers are inferred as LongType (not IntegerType)."""
        data = [{"value": 100}]
        df = spark.createDataFrame(data)

        # PySpark infers integers as LongType
        value_field = df.schema.fields[0]
        assert value_field.name == "value"
        assert isinstance(
            value_field.dataType, LongType
        ), f"Expected LongType, got {type(value_field.dataType).__name__}"
        assert value_field.nullable == True, "All inferred fields should be nullable"

    def test_detect_float_as_double(self, spark):
        """Test that floats are inferred as DoubleType."""
        data = [{"value": 95.5}]
        df = spark.createDataFrame(data)

        # PySpark infers floats as DoubleType
        value_field = df.schema.fields[0]
        assert isinstance(
            value_field.dataType, DoubleType
        ), f"Expected DoubleType, got {type(value_field.dataType).__name__}"
        assert value_field.nullable == True

    def test_detect_string(self, spark):
        """Test string inference."""
        data = [{"value": "hello"}]
        df = spark.createDataFrame(data)

        value_field = df.schema.fields[0]
        assert isinstance(
            value_field.dataType, StringType
        ), f"Expected StringType, got {type(value_field.dataType).__name__}"
        assert value_field.nullable == True

    def test_detect_boolean(self, spark):
        """Test boolean inference."""
        data = [{"value": True}]
        df = spark.createDataFrame(data)

        value_field = df.schema.fields[0]
        assert isinstance(
            value_field.dataType, BooleanType
        ), f"Expected BooleanType, got {type(value_field.dataType).__name__}"
        assert value_field.nullable == True

    def test_all_nulls_raises_error(self, spark):
        """Test that all-null columns raise ValueError (matching PySpark)."""
        data = [{"value": None}]

        # PySpark raises: ValueError: Some of types cannot be determined after inferring
        with pytest.raises(ValueError, match="cannot be determined|cannot infer"):
            df = spark.createDataFrame(data)


class TestMultipleRows:
    """Test type inference across multiple rows."""

    def test_consistent_integers(self, spark):
        """Test inference with consistent integers across rows."""
        data = [
            {"id": 1, "count": 100},
            {"id": 2, "count": 200},
            {"id": 3, "count": 300},
        ]
        df = spark.createDataFrame(data)

        # Both should be LongType
        assert len(df.schema.fields) == 2
        for field in df.schema.fields:
            assert isinstance(field.dataType, LongType)
            assert field.nullable == True

    def test_consistent_mixed_types(self, spark):
        """Test inference with multiple consistent types."""
        data = [
            {"id": 1, "name": "Alice", "age": 25, "score": 95.5, "active": True},
            {"id": 2, "name": "Bob", "age": 30, "score": 87.3, "active": False},
        ]
        df = spark.createDataFrame(data)

        # Find each field by name and check type
        fields_by_name = {f.name: f for f in df.schema.fields}

        assert isinstance(fields_by_name["id"].dataType, LongType)
        assert isinstance(fields_by_name["name"].dataType, StringType)
        assert isinstance(fields_by_name["age"].dataType, LongType)
        assert isinstance(fields_by_name["score"].dataType, DoubleType)
        assert isinstance(fields_by_name["active"].dataType, BooleanType)

        # All should be nullable
        for field in df.schema.fields:
            assert field.nullable == True

    def test_with_some_nulls(self, spark):
        """Test that nulls in some rows don't change type inference."""
        data = [
            {"id": 1, "value": 100},
            {"id": 2, "value": None},  # Null in value
            {"id": 3, "value": 200},
        ]
        df = spark.createDataFrame(data)

        # Should infer from non-null values
        fields_by_name = {f.name: f for f in df.schema.fields}
        assert isinstance(
            fields_by_name["value"].dataType, LongType
        ), "Should infer from non-null values"
        assert fields_by_name["value"].nullable == True


class TestTypeConflicts:
    """Test that type conflicts raise errors (matching PySpark)."""

    def test_int_float_conflict_raises_error(self, spark):
        """Test that mixing int and float raises TypeError."""
        data = [
            {"value": 100},  # Integer
            {"value": 95.5},  # Float
        ]

        # PySpark raises: TypeError: Can not merge type LongType and DoubleType
        with pytest.raises(TypeError, match="Can not merge|cannot merge|incompatible"):
            df = spark.createDataFrame(data)

    def test_numeric_string_conflict_raises_error(self, spark):
        """Test that mixing numeric and string raises TypeError."""
        data = [
            {"value": 100},
            {"value": "text"},
        ]

        with pytest.raises(TypeError, match="Can not merge|cannot merge|incompatible"):
            df = spark.createDataFrame(data)

    def test_boolean_int_conflict_raises_error(self, spark):
        """Test that mixing boolean and int raises TypeError."""
        data = [
            {"value": True},
            {"value": 1},
        ]

        # Note: In Python, True == 1, but PySpark treats them as different types
        with pytest.raises(TypeError, match="Can not merge|cannot merge|incompatible"):
            df = spark.createDataFrame(data)


class TestArrayInference:
    """Test array type inference."""

    def test_array_of_strings(self, spark):
        """Test ArrayType(StringType) inference."""
        data = [
            {"id": 1, "tags": ["python", "spark"]},
            {"id": 2, "tags": ["java", "scala"]},
        ]
        df = spark.createDataFrame(data)

        tags_field = [f for f in df.schema.fields if f.name == "tags"][0]
        assert isinstance(tags_field.dataType, ArrayType), "Should be ArrayType"
        assert isinstance(
            tags_field.dataType.element_type, StringType
        ), "Elements should be StringType"
        assert tags_field.nullable == True

    def test_array_of_integers(self, spark):
        """Test ArrayType(LongType) inference."""
        data = [
            {"id": 1, "numbers": [1, 2, 3]},
            {"id": 2, "numbers": [4, 5, 6]},
        ]
        df = spark.createDataFrame(data)

        numbers_field = [f for f in df.schema.fields if f.name == "numbers"][0]
        assert isinstance(numbers_field.dataType, ArrayType)
        assert isinstance(
            numbers_field.dataType.element_type, LongType
        ), "Array elements should be LongType"

    def test_empty_array_with_non_empty(self, spark):
        """Test that empty arrays don't break inference."""
        data = [
            {"tags": []},
            {"tags": ["python"]},
        ]
        df = spark.createDataFrame(data)

        # Should infer from non-empty array
        tags_field = df.schema.fields[0]
        assert isinstance(tags_field.dataType, ArrayType)
        assert isinstance(tags_field.dataType.element_type, StringType)


class TestMapInference:
    """Test map/dict type inference."""

    def test_consistent_dict_as_map(self, spark):
        """Test that dicts with consistent keys are inferred as MapType."""
        data = [
            {"id": 1, "metadata": {"key1": "val1", "key2": "val2"}},
            {"id": 2, "metadata": {"key1": "val3", "key2": "val4"}},
        ]
        df = spark.createDataFrame(data)

        # PySpark infers consistent-key dicts as MapType
        metadata_field = [f for f in df.schema.fields if f.name == "metadata"][0]
        assert isinstance(metadata_field.dataType, MapType), "Consistent dicts should be MapType"
        # MapType should have string keys and string values
        assert metadata_field.dataType.key_type.typeName() == "string"
        assert metadata_field.dataType.value_type.typeName() == "string"


class TestMissingKeys:
    """Test inference when rows have different keys (sparse data)."""

    def test_union_of_keys_across_rows(self, spark):
        """Test that schema includes all keys from all rows."""
        data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "age": 30},
            {"id": 3, "name": "Charlie", "age": 35},
        ]
        df = spark.createDataFrame(data)

        # Should have all three columns
        column_names = set([f.name for f in df.schema.fields])
        assert column_names == {"id", "name", "age"}

        # All should be nullable (since rows don't all have all keys)
        for field in df.schema.fields:
            assert field.nullable == True, f"Field {field.name} should be nullable"

    def test_types_inferred_from_available_values(self, spark):
        """Test that types are inferred from rows that have the values."""
        data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "age": 30},
        ]
        df = spark.createDataFrame(data)

        fields_by_name = {f.name: f for f in df.schema.fields}

        # 'name' only appears in first row (string)
        assert isinstance(fields_by_name["name"].dataType, StringType)

        # 'age' only appears in second row (integer -> LongType)
        assert isinstance(fields_by_name["age"].dataType, LongType)


class TestColumnOrdering:
    """Test column ordering behavior."""

    def test_columns_sorted_alphabetically(self, spark):
        """Test that PySpark sorts columns alphabetically."""
        data = [{"name": "Alice", "age": 25, "id": 1}]
        df = spark.createDataFrame(data)

        # PySpark sorts columns alphabetically
        # Expected order: age, id, name
        assert df.columns == sorted(df.columns), "Columns should be in alphabetical order"


class TestNestedStructures:
    """Test inference of nested structures."""

    def test_nested_dict_as_map(self, spark):
        """Test that nested dicts with consistent keys become MapType."""
        data = [
            {"id": 1, "props": {"country": "US", "region": "West"}},
            {"id": 2, "props": {"country": "UK", "region": "Europe"}},
        ]
        df = spark.createDataFrame(data)

        props_field = [f for f in df.schema.fields if f.name == "props"][0]
        # PySpark infers this as MapType
        assert isinstance(props_field.dataType, MapType)

    def test_nested_dict_different_keys_as_map(self, spark):
        """Test nested dicts with different keys."""
        data = [
            {"id": 1, "address": {"street": "Main St", "city": "NYC"}},
            {"id": 2, "address": {"street": "Oak Ave", "city": "LA"}},
        ]
        df = spark.createDataFrame(data)

        address_field = [f for f in df.schema.fields if f.name == "address"][0]
        # PySpark infers consistent nested dicts as MapType
        assert isinstance(address_field.dataType, MapType)


class TestEdgeCases:
    """Test edge cases."""

    def test_single_row(self, spark):
        """Test inference with only one row."""
        data = [{"id": 1, "name": "Alice", "score": 95.5}]
        df = spark.createDataFrame(data)

        # Should infer types correctly from single row
        fields_by_name = {f.name: f for f in df.schema.fields}
        assert isinstance(fields_by_name["id"].dataType, LongType)
        assert isinstance(fields_by_name["name"].dataType, StringType)
        assert isinstance(fields_by_name["score"].dataType, DoubleType)

    def test_empty_dataframe(self, spark):
        """Test inference with empty data."""
        data = []
        df = spark.createDataFrame(data)

        # Should create empty DataFrame with empty schema
        assert df.count() == 0
        assert len(df.schema.fields) == 0

    def test_large_integer(self, spark):
        """Test that all integers become LongType regardless of size."""
        data = [
            {"small": 1, "big": 9223372036854775807},  # Max long
        ]
        df = spark.createDataFrame(data)

        # Both should be LongType
        for field in df.schema.fields:
            assert isinstance(field.dataType, LongType)


class TestExplicitSchemaOverride:
    """Test that explicit schemas override inference."""

    def test_explicit_schema_used(self, spark):
        """Test that providing explicit schema prevents inference."""
        data = [{"id": 1, "value": 100}]

        # Explicit schema with IntegerType (different from inferred LongType)
        explicit_schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("value", IntegerType()),
            ]
        )

        df = spark.createDataFrame(data, schema=explicit_schema)

        # Should use explicit schema, not inferred LongType
        assert isinstance(
            df.schema.fields[0].dataType, IntegerType
        ), "Should use explicit IntegerType"
        assert isinstance(
            df.schema.fields[1].dataType, IntegerType
        ), "Should use explicit IntegerType"

    def test_no_schema_triggers_inference(self, spark):
        """Test that omitting schema triggers auto-inference."""
        data = [{"id": 1, "value": 100}]

        df = spark.createDataFrame(data)  # No schema provided

        # Should infer LongType (not IntegerType)
        for field in df.schema.fields:
            assert isinstance(field.dataType, LongType), "Should infer as LongType"


class TestInferenceWithOperations:
    """Test that inferred schemas work with DataFrame operations."""

    def test_filter_on_inferred_schema(self, spark):
        """Test filtering works correctly on inferred schema."""
        data = [
            {"id": 1, "value": 100},
            {"id": 2, "value": 200},
            {"id": 3, "value": 150},
        ]
        df = spark.createDataFrame(data)

        # Filter should work
        filtered = df.filter(F.col("value") > 120)

        assert filtered.count() == 2
        rows = filtered.collect()
        values = [row["value"] for row in rows]
        assert all(v > 120 for v in values)

    def test_groupby_with_inference(self, spark):
        """Test groupBy on inferred schema."""
        data = [
            {"category": "A", "value": 100},
            {"category": "A", "value": 200},
            {"category": "B", "value": 150},
        ]
        df = spark.createDataFrame(data)

        result = df.groupBy("category").agg(F.sum("value").alias("total"))
        assert result.count() == 2

    def test_join_inferred_schemas(self, spark):
        """Test joining two DataFrames with inferred schemas."""
        data1 = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        data2 = [{"id": 1, "dept": "Engineering"}, {"id": 2, "dept": "Sales"}]

        df1 = spark.createDataFrame(data1)
        df2 = spark.createDataFrame(data2)

        # Both should have LongType for 'id'
        id_field_1 = [f for f in df1.schema.fields if f.name == "id"][0]
        id_field_2 = [f for f in df2.schema.fields if f.name == "id"][0]
        assert isinstance(id_field_1.dataType, LongType)
        assert isinstance(id_field_2.dataType, LongType)

        # Join should work
        result = df1.join(df2, "id")
        assert result.count() == 2


class TestColumnOrdering:
    """Test column ordering matches PySpark."""

    def test_alphabetical_ordering(self, spark):
        """Test that columns are ordered alphabetically like PySpark."""
        data = [{"zebra": 1, "apple": 2, "middle": 3}]
        df = spark.createDataFrame(data)

        # PySpark sorts columns alphabetically
        expected_order = ["apple", "middle", "zebra"]
        assert df.columns == expected_order, f"Expected {expected_order}, got {df.columns}"

    def test_ordering_with_multiple_rows(self, spark):
        """Test column ordering with keys appearing in different rows."""
        data = [
            {"name": "Alice", "age": 25},
            {"id": 2, "name": "Bob"},
        ]
        df = spark.createDataFrame(data)

        # Should be alphabetically sorted
        expected_order = sorted(["id", "name", "age"])
        assert df.columns == expected_order


class TestSparseMissingData:
    """Test inference with missing keys (sparse data)."""

    def test_all_columns_included(self, spark):
        """Test that all columns from all rows are included."""
        data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "age": 30},
            {"id": 3, "name": "Charlie", "age": 35},
        ]
        df = spark.createDataFrame(data)

        # All three columns should be present
        columns = set(df.columns)
        assert columns == {"id", "name", "age"}

    def test_all_sparse_fields_nullable(self, spark):
        """Test that fields with missing values are nullable."""
        data = [
            {"id": 1, "name": "Alice"},  # missing 'age'
            {"id": 2, "age": 30},  # missing 'name'
        ]
        df = spark.createDataFrame(data)

        # All fields should be nullable
        for field in df.schema.fields:
            assert field.nullable == True, f"Sparse field {field.name} should be nullable"

    def test_type_from_first_occurrence(self, spark):
        """Test that type is inferred from first occurrence of key."""
        data = [
            {"a": "text"},  # 'a' is string
            {"b": 100},  # 'b' is int
            {"a": "more", "b": 200},  # Both present
        ]
        df = spark.createDataFrame(data)

        fields_by_name = {f.name: f for f in df.schema.fields}
        assert isinstance(fields_by_name["a"].dataType, StringType)
        assert isinstance(fields_by_name["b"].dataType, LongType)


# Fixtures
@pytest.fixture
def spark():
    """Create Mock Spark session."""
    session = MockSparkSession.builder.appName("SchemaInferenceUnitTests").getOrCreate()
    yield session
    session.stop()
