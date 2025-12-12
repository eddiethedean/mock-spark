"""
Tests for Polars backend implementation.

These tests verify that the Polars backend works correctly for storage,
materialization, and expression translation.
"""

import pytest
from mock_spark.backend.polars import (
    PolarsStorageManager,
    PolarsMaterializer,
)
from mock_spark.backend.factory import BackendFactory
from mock_spark.spark_types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)


@pytest.mark.unit
class TestPolarsStorageManager:
    """Test PolarsStorageManager functionality."""

    def test_create_storage_backend(self):
        """Test creating Polars storage backend via factory."""
        storage = BackendFactory.create_storage_backend("polars")
        assert isinstance(storage, PolarsStorageManager)

    def test_create_schema(self):
        """Test schema creation."""
        storage = PolarsStorageManager()
        storage.create_schema("test_schema")
        assert storage.schema_exists("test_schema")

    def test_create_table(self):
        """Test table creation."""
        storage = PolarsStorageManager()
        schema = StructType(
            [
                StructField("name", StringType()),
                StructField("age", IntegerType()),
            ]
        )
        table = storage.create_table("default", "users", schema)
        assert table is not None
        assert storage.table_exists("default", "users")

    def test_insert_and_query_data(self):
        """Test data insertion and querying."""
        storage = PolarsStorageManager()
        schema = StructType(
            [
                StructField("name", StringType()),
                StructField("age", IntegerType()),
            ]
        )
        storage.create_table("default", "users", schema)

        data = [
            {"name": "Alice", "age": 25},
            {"name": "Bob", "age": 30},
        ]
        storage.insert_data("default", "users", data)

        results = storage.query_data("default", "users")
        assert len(results) == 2
        assert results[0]["name"] == "Alice"
        assert results[0]["age"] == 25


@pytest.mark.unit
class TestPolarsMaterializer:
    """Test PolarsMaterializer functionality."""

    def test_create_materializer(self):
        """Test creating Polars materializer via factory."""
        materializer = BackendFactory.create_materializer("polars")
        assert isinstance(materializer, PolarsMaterializer)

    def test_materialize_simple_filter(self):
        """Test materializing a simple filter operation."""
        materializer = PolarsMaterializer()

        data = [
            {"name": "Alice", "age": 25},
            {"name": "Bob", "age": 30},
            {"name": "Charlie", "age": 35},
        ]
        schema = StructType(
            [
                StructField("name", StringType()),
                StructField("age", IntegerType()),
            ]
        )

        from mock_spark.functions.core.column import Column

        # Create filter operation: age > 30
        age_col = Column("age")
        condition = age_col > 30
        operations = [("filter", condition)]

        results = materializer.materialize(data, schema, operations)
        assert len(results) == 1
        assert results[0]["name"] == "Charlie"
        assert results[0]["age"] == 35


@pytest.mark.unit
class TestPolarsBackendIntegration:
    """Test Polars backend integration with SparkSession."""

    def test_session_uses_polars_by_default(self):
        """Test that SparkSession uses Polars backend by default."""
        from mock_spark import SparkSession

        spark = SparkSession("test_app")
        # Check that storage backend is Polars
        backend_type = BackendFactory.get_backend_type(spark._storage)
        assert backend_type == "polars"
        spark.stop()

    def test_create_dataframe_with_polars(self):
        """Test creating DataFrame with Polars backend."""
        from mock_spark import SparkSession

        spark = SparkSession("test_app")
        data = [{"name": "Alice", "age": 25}]
        df = spark.createDataFrame(data)

        # Verify DataFrame works
        assert df.count() == 1
        spark.stop()
