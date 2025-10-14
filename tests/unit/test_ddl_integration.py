"""
Integration tests for DDL schema support in DataFrames.

Tests that DDL schema strings work end-to-end in DataFrame operations,
matching the behavior of StructType schemas.
"""

import pytest
from mock_spark import MockSparkSession
from mock_spark.spark_types import (
    MockStructType,
    MockStructField,
    StringType,
    LongType,
    IntegerType,
)


class TestDDLIntegration:
    """Integration tests for DDL schema support."""

    def test_basic_dataframe_creation(self):
        """Test basic DataFrame creation with DDL schema."""
        spark = MockSparkSession()
        data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        
        df = spark.createDataFrame(data, schema="id long, name string")
        
        assert df.count() == 2
        assert df.columns == ["id", "name"]
        assert isinstance(df.schema, MockStructType)
        assert len(df.schema.fields) == 2

    def test_columns_property_works(self):
        """Test that df.columns works with DDL schema (original bug fix)."""
        spark = MockSparkSession()
        data = [{"id": 1, "name": "test"}]
        
        df = spark.createDataFrame(data, schema="id long, name string")
        
        # This should not raise AttributeError
        columns = df.columns
        assert columns == ["id", "name"]

    def test_select_operation(self):
        """Test select operation with DDL schema."""
        spark = MockSparkSession()
        data = [{"id": 1, "name": "Alice", "age": 25}]
        
        df = spark.createDataFrame(data, schema="id long, name string, age int")
        
        result = df.select("name", "age")
        assert result.columns == ["name", "age"]
        assert result.count() == 1

    def test_filter_operation(self):
        """Test filter operation with DDL schema."""
        from mock_spark.functions import F
        
        spark = MockSparkSession()
        data = [
            {"id": 1, "name": "Alice", "age": 25},
            {"id": 2, "name": "Bob", "age": 30},
        ]
        
        df = spark.createDataFrame(data, schema="id long, name string, age int")
        
        filtered = df.filter(F.col("age") > 25)
        assert filtered.count() == 1
        rows = filtered.collect()
        assert rows[0]["name"] == "Bob"

    def test_schema_property(self):
        """Test that schema property returns MockStructType."""
        spark = MockSparkSession()
        data = [{"id": 1, "name": "Alice"}]
        
        df = spark.createDataFrame(data, schema="id long, name string")
        
        schema = df.schema
        assert isinstance(schema, MockStructType)
        assert len(schema.fields) == 2
        assert schema.fields[0].name == "id"
        assert schema.fields[1].name == "name"

    def test_comparison_with_structtype(self):
        """Test that DDL schema behaves identically to StructType schema."""
        spark = MockSparkSession()
        data = [{"id": 1, "name": "Alice", "age": 25}]
        
        # Create DataFrame with DDL schema
        df1 = spark.createDataFrame(data, schema="id long, name string, age int")
        
        # Create DataFrame with StructType schema
        from mock_spark.spark_types import MockStructType
        schema = MockStructType([
            MockStructField("id", LongType()),
            MockStructField("name", StringType()),
            MockStructField("age", IntegerType()),
        ])
        df2 = spark.createDataFrame(data, schema=schema)
        
        # Both should have same columns
        assert df1.columns == df2.columns
        assert df1.columns == ["id", "name", "age"]
        
        # Both should have same count
        assert df1.count() == df2.count()
        
        # Both should have same data
        assert df1.collect() == df2.collect()

    def test_complex_schema_with_arrays(self):
        """Test DataFrame with array types."""
        spark = MockSparkSession()
        data = [{"id": 1, "tags": ["python", "spark"]}]
        
        df = spark.createDataFrame(data, schema="id long, tags array<string>")
        
        assert df.columns == ["id", "tags"]
        assert df.count() == 1
        rows = df.collect()
        assert rows[0]["tags"] == ["python", "spark"]

    def test_complex_schema_with_maps(self):
        """Test DataFrame with map types."""
        spark = MockSparkSession()
        data = [{"id": 1, "metadata": {"key1": "value1", "key2": "value2"}}]
        
        df = spark.createDataFrame(data, schema="id long, metadata map<string,string>")
        
        assert df.columns == ["id", "metadata"]
        assert df.count() == 1

    def test_nested_struct_schema(self):
        """Test DataFrame with nested struct types."""
        spark = MockSparkSession()
        data = [{
            "id": 1,
            "address": {"street": "123 Main St", "city": "New York"}
        }]
        
        df = spark.createDataFrame(
            data,
            schema="id long, address struct<street:string,city:string>"
        )
        
        assert df.columns == ["id", "address"]
        assert df.count() == 1

    def test_show_operation(self):
        """Test show operation with DDL schema."""
        spark = MockSparkSession()
        data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        
        df = spark.createDataFrame(data, schema="id long, name string")
        
        # Should not raise any errors
        df.show()

    def test_to_pandas(self):
        """Test toPandas operation with DDL schema."""
        try:
            import pandas as pd  # noqa: F401
        except ImportError:
            pytest.skip("pandas not installed")
        
        spark = MockSparkSession()
        data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        
        df = spark.createDataFrame(data, schema="id long, name string")
        
        pdf = df.toPandas()
        assert len(pdf) == 2
        assert list(pdf.columns) == ["id", "name"]

    def test_multiple_operations_chain(self):
        """Test chaining multiple operations with DDL schema."""
        from mock_spark.functions import F
        
        spark = MockSparkSession()
        data = [
            {"id": 1, "name": "Alice", "age": 25, "active": True},
            {"id": 2, "name": "Bob", "age": 30, "active": True},
            {"id": 3, "name": "Charlie", "age": 35, "active": False},
        ]
        
        df = spark.createDataFrame(
            data,
            schema="id long, name string, age int, active boolean"
        )
        
        result = df.filter(F.col("active")).select("name", "age")
        
        assert result.count() == 2
        assert result.columns == ["name", "age"]

    def test_groupby_operation(self):
        """Test groupBy operation with DDL schema."""
        spark = MockSparkSession()
        data = [
            {"id": 1, "category": "A", "value": 10},
            {"id": 2, "category": "A", "value": 20},
            {"id": 3, "category": "B", "value": 15},
        ]
        
        df = spark.createDataFrame(
            data,
            schema="id long, category string, value int"
        )
        
        result = df.groupBy("category").sum("value")
        
        assert result.count() == 2
        assert result.columns == ["category", "sum(value)"]

    def test_join_operation(self):
        """Test join operation with DDL schema."""
        spark = MockSparkSession()
        
        data1 = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        data2 = [{"id": 1, "age": 25}, {"id": 2, "age": 30}]
        
        df1 = spark.createDataFrame(data1, schema="id long, name string")
        df2 = spark.createDataFrame(data2, schema="id long, age int")
        
        result = df1.join(df2, "id")
        
        assert result.count() == 2
        assert "name" in result.columns
        assert "age" in result.columns

    def test_union_operation(self):
        """Test union operation with DDL schema."""
        spark = MockSparkSession()
        
        data1 = [{"id": 1, "name": "Alice"}]
        data2 = [{"id": 2, "name": "Bob"}]
        
        df1 = spark.createDataFrame(data1, schema="id long, name string")
        df2 = spark.createDataFrame(data2, schema="id long, name string")
        
        result = df1.union(df2)
        
        assert result.count() == 2

    def test_original_bug_scenario(self):
        """Test the exact scenario from the bug report."""
        from mock_spark.functions import F
        
        spark = MockSparkSession()
        data = [{"id": 1, "name": "test"}]
        
        # This should work now
        df = spark.createDataFrame(data, schema="id long, name string")
        
        # All these should work
        assert df.count() == 1
        assert df.schema is not None
        assert df.columns == ["id", "name"]  # This was failing before
        
        # Additional operations should work
        assert df.select("name").count() == 1
        assert df.filter(F.col("id") == 1).count() == 1

    def test_empty_dataframe_with_ddl(self):
        """Test creating empty DataFrame with DDL schema."""
        spark = MockSparkSession()
        data = []
        
        df = spark.createDataFrame(data, schema="id long, name string")
        
        assert df.count() == 0
        assert df.columns == ["id", "name"]
        assert isinstance(df.schema, MockStructType)

    def test_ddl_with_whitespace(self):
        """Test DDL schema with various whitespace."""
        spark = MockSparkSession()
        data = [{"id": 1, "name": "Alice"}]
        
        # Various whitespace scenarios
        schemas = [
            "id long, name string",
            "  id   long  ,  name   string  ",
            "id:long,name:string",
            "  id  :  long  ,  name  :  string  ",
        ]
        
        for schema in schemas:
            df = spark.createDataFrame(data, schema=schema)
            assert df.columns == ["id", "name"]
            assert df.count() == 1

    def test_all_primitive_types(self):
        """Test DataFrame with all primitive types."""
        spark = MockSparkSession()
        data = [{
            "str_field": "test",
            "int_field": 42,
            "long_field": 123456789,
            "double_field": 3.14,
            "bool_field": True,
        }]
        
        schema = (
            "str_field string, int_field int, long_field long, "
            "double_field double, bool_field boolean"
        )
        
        df = spark.createDataFrame(data, schema=schema)
        
        assert df.columns == ["str_field", "int_field", "long_field", "double_field", "bool_field"]
        assert df.count() == 1

    def test_schema_field_names(self):
        """Test that schema.fieldNames() works with DDL schema."""
        spark = MockSparkSession()
        data = [{"id": 1, "name": "Alice", "age": 25}]
        
        df = spark.createDataFrame(data, schema="id long, name string, age int")
        
        field_names = df.schema.fieldNames()
        assert field_names == ["id", "name", "age"]

    def test_schema_contains(self):
        """Test that schema.contains() works with DDL schema."""
        spark = MockSparkSession()
        data = [{"id": 1, "name": "Alice"}]
        
        df = spark.createDataFrame(data, schema="id long, name string")
        
        assert df.schema.contains("id")
        assert df.schema.contains("name")
        assert not df.schema.contains("age")

    def test_schema_get_field_by_name(self):
        """Test that schema.get_field_by_name() works with DDL schema."""
        spark = MockSparkSession()
        data = [{"id": 1, "name": "Alice"}]
        
        df = spark.createDataFrame(data, schema="id long, name string")
        
        field = df.schema.get_field_by_name("id")
        assert field is not None
        assert field.name == "id"
        assert isinstance(field.dataType, LongType)
        
        assert df.schema.get_field_by_name("nonexistent") is None

