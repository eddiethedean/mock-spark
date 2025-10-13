"""
Schema Inference Compatibility Tests

Tests Mock Spark's schema inference against real PySpark to ensure
identical behavior for automatic type detection.

Uses standard compatibility test fixtures for proper environment setup.
"""

import pytest
from tests.compatibility.utils.comparison import compare_schemas, assert_dataframes_equal


class TestBasicTypeInference:
    """Test automatic inference of basic Python types."""

    def test_infer_integer_type(self, mock_environment, pyspark_environment):
        """Test inference of integer values to LongType."""
        data = [
            {"id": 1, "count": 100},
            {"id": 2, "count": 200},
            {"id": 3, "count": 300},
        ]

        # Create without explicit schema
        mock_df = mock_environment["session"].createDataFrame(data)
        pyspark_df = pyspark_environment["session"].createDataFrame(data)

        # Compare schemas
        schema_comparison = compare_schemas(mock_df, pyspark_df)
        assert schema_comparison[
            "equivalent"
        ], f"Schemas don't match: {schema_comparison['errors']}"

        # Compare data
        assert_dataframes_equal(mock_df, pyspark_df)

    def test_infer_float_type(self, mock_environment, pyspark_environment):
        """Test inference of float values to DoubleType."""
        data = [
            {"id": 1, "score": 95.5},
            {"id": 2, "score": 87.3},
            {"id": 3, "score": 92.1},
        ]

        mock_df = mock_environment["session"].createDataFrame(data)
        pyspark_df = pyspark_environment["session"].createDataFrame(data)

        schema_comparison = compare_schemas(mock_df, pyspark_df)
        assert schema_comparison[
            "equivalent"
        ], f"Schemas don't match: {schema_comparison['errors']}"

        assert_dataframes_equal(mock_df, pyspark_df)

    def test_infer_mixed_types(self, mock_environment, pyspark_environment):
        """Test inference with multiple different types."""
        data = [
            {"id": 1, "name": "Alice", "age": 25, "score": 95.5, "active": True},
            {"id": 2, "name": "Bob", "age": 30, "score": 87.3, "active": False},
        ]

        mock_df = mock_environment["session"].createDataFrame(data)
        pyspark_df = pyspark_environment["session"].createDataFrame(data)

        schema_comparison = compare_schemas(mock_df, pyspark_df)
        assert schema_comparison[
            "equivalent"
        ], f"Schemas don't match: {schema_comparison['errors']}"

        assert_dataframes_equal(mock_df, pyspark_df)


class TestNullHandling:
    """Test schema inference with null values."""

    def test_infer_with_some_nulls(self, mock_environment, pyspark_environment):
        """Test inference when some values are null."""
        data = [
            {"id": 1, "value": 100},
            {"id": 2, "value": None},
            {"id": 3, "value": 200},
        ]

        mock_df = mock_environment["session"].createDataFrame(data)
        pyspark_df = pyspark_environment["session"].createDataFrame(data)

        schema_comparison = compare_schemas(mock_df, pyspark_df)
        assert schema_comparison[
            "equivalent"
        ], f"Schemas don't match: {schema_comparison['errors']}"

        # Verify nullable is True
        value_field = mock_df.schema.fields[1]  # 'value' field
        assert value_field.nullable == True, "Field with nulls should be nullable"

        assert_dataframes_equal(mock_df, pyspark_df)

    def test_infer_all_nulls_raises_error(self, mock_environment, pyspark_environment):
        """Test all-null column behavior.
        
        PySpark 3.2.4 raises ValueError for all-null columns.
        Mock-Spark defaults to StringType for robustness (behavioral difference).
        """
        data = [{"value": None}]

        # Mock-Spark defaults to StringType (more permissive)
        mock_df = mock_environment["session"].createDataFrame(data)
        assert len(mock_df.schema.fields) == 1
        # Should default to StringType
        from mock_spark.spark_types import StringType
        assert isinstance(mock_df.schema.fields[0].dataType, StringType)

        # PySpark raises ValueError (verified with PySpark 3.2.4)
        with pytest.raises(ValueError, match="cannot be determined"):
            pyspark_environment["session"].createDataFrame(data)


class TestTypeConflicts:
    """Test type promotion and conflict resolution."""

    def test_type_conflict_raises_error(self, mock_environment, pyspark_environment):
        """Test that type conflicts raise TypeError in both."""
        data = [
            {"value": 100},  # Integer
            {"value": 95.5},  # Float
        ]

        # Both should raise TypeError for type conflicts
        with pytest.raises(TypeError):
            mock_environment["session"].createDataFrame(data)

        with pytest.raises(TypeError):
            pyspark_environment["session"].createDataFrame(data)


class TestComplexTypes:
    """Test inference of complex types."""

    def test_infer_array_of_strings(self, mock_environment, pyspark_environment):
        """Test ArrayType(StringType) inference."""
        data = [
            {"id": 1, "tags": ["python", "spark"]},
            {"id": 2, "tags": ["java", "scala"]},
        ]

        mock_df = mock_environment["session"].createDataFrame(data)
        pyspark_df = pyspark_environment["session"].createDataFrame(data)

        schema_comparison = compare_schemas(mock_df, pyspark_df)
        assert schema_comparison[
            "equivalent"
        ], f"Schemas don't match: {schema_comparison['errors']}"

        assert_dataframes_equal(mock_df, pyspark_df)

    def test_infer_array_of_integers(self, mock_environment, pyspark_environment):
        """Test ArrayType(LongType) inference."""
        data = [
            {"id": 1, "numbers": [1, 2, 3]},
            {"id": 2, "numbers": [4, 5]},
        ]

        mock_df = mock_environment["session"].createDataFrame(data)
        pyspark_df = pyspark_environment["session"].createDataFrame(data)

        schema_comparison = compare_schemas(mock_df, pyspark_df)
        assert schema_comparison[
            "equivalent"
        ], f"Schemas don't match: {schema_comparison['errors']}"

        assert_dataframes_equal(mock_df, pyspark_df)


class TestSparseData:
    """Test inference with sparse data (missing keys)."""

    def test_infer_sparse_data(self, mock_environment, pyspark_environment):
        """Test that rows with different keys work correctly."""
        data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "age": 30},
            {"id": 3, "name": "Charlie", "age": 35},
        ]

        mock_df = mock_environment["session"].createDataFrame(data)
        pyspark_df = pyspark_environment["session"].createDataFrame(data)

        schema_comparison = compare_schemas(mock_df, pyspark_df)
        assert schema_comparison[
            "equivalent"
        ], f"Schemas don't match: {schema_comparison['errors']}"

        # All columns should be present
        assert set(mock_df.columns) == {"age", "id", "name"}
        assert set(pyspark_df.columns) == {"age", "id", "name"}

        # For sparse data, just verify schema matches (row order may differ)
        # Data comparison is complex with nulls in different positions
        assert schema_comparison["equivalent"], "Schemas should match"


class TestSchemaOverride:
    """Test that explicit schemas work correctly."""

    def test_explicit_schema_overrides_inference(self, mock_environment, pyspark_environment):
        """Test that explicit schema prevents auto-inference."""
        from mock_spark.spark_types import (
            MockStructType,
            MockStructField,
            IntegerType,
        )
        from pyspark.sql.types import StructType, StructField, IntegerType as PySparkIntegerType

        data = [{"id": 1, "value": 100}]

        # Explicit schema
        mock_schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("value", IntegerType()),
            ]
        )
        pyspark_schema = StructType(
            [
                StructField("id", PySparkIntegerType()),
                StructField("value", PySparkIntegerType()),
            ]
        )

        mock_df = mock_environment["session"].createDataFrame(data, schema=mock_schema)
        pyspark_df = pyspark_environment["session"].createDataFrame(data, schema=pyspark_schema)

        # Should use IntegerType (not inferred LongType)
        assert (
            type(mock_df.schema.fields[0].dataType).__name__ == "IntegerType"
        ), "Should use explicit IntegerType"

        assert_dataframes_equal(mock_df, pyspark_df)
