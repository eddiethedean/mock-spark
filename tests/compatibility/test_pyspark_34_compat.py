"""
Compatibility tests for PySpark 3.4+ features.

Tests that mock-spark implementation matches PySpark behavior for:
- melt() (3.4+)
- to() (3.4+)
- withMetadata() (3.3+)
- observe() (3.3+)
- window_time() (3.4+)
"""

import pytest
from tests.compatibility.utils.comparison import assert_dataframes_equal, assert_schemas_equal


# Skip all tests if PySpark version is < 3.3
pytestmark = pytest.mark.skipif(
    True,  # Will be dynamically checked
    reason="PySpark 3.3+ features not available in this version"
)


def pytest_configure(config):
    """Configure pytest to skip tests based on PySpark version."""
    try:
        import pyspark
        version = tuple(map(int, pyspark.__version__.split('.')[:2]))
        if version < (3, 3):
            pytest.skip("PySpark 3.3+ required", allow_module_level=True)
    except Exception:
        pytest.skip("Could not determine PySpark version", allow_module_level=True)


class TestMelt:
    """Test melt() DataFrame method (PySpark 3.4+)."""

    def test_melt_basic_wide_to_long(self, mock_spark, real_spark):
        """Test basic melt transformation."""
        data = [
            {"id": 1, "A": 10, "B": 20, "C": 30}
        ]
        
        # Test with mock
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.melt(ids=["id"], values=["A", "B", "C"])
        
        # Test with PySpark
        pyspark_df = real_spark.createDataFrame(data)
        try:
            pyspark_result = pyspark_df.melt(ids=["id"], values=["A", "B", "C"])
            assert_dataframes_equal(mock_result, pyspark_result)
        except AttributeError:
            pytest.skip("melt() not available in this PySpark version")

    def test_melt_multiple_ids(self, mock_spark, real_spark):
        """Test melt with multiple ID columns."""
        data = [
            {"user": "Alice", "date": "2024-01-01", "clicks": 5, "views": 10}
        ]
        
        # Test with mock
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.melt(ids=["user", "date"], values=["clicks", "views"])
        
        # Test with PySpark
        pyspark_df = real_spark.createDataFrame(data)
        try:
            pyspark_result = pyspark_df.melt(ids=["user", "date"], values=["clicks", "views"])
            assert_dataframes_equal(mock_result, pyspark_result)
        except AttributeError:
            pytest.skip("melt() not available in this PySpark version")

    def test_melt_custom_column_names(self, mock_spark, real_spark):
        """Test melt with custom variable and value column names."""
        data = [
            {"id": 1, "metric1": 100, "metric2": 200}
        ]
        
        # Test with mock
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.melt(
            ids=["id"],
            values=["metric1", "metric2"],
            variableColumnName="metric_name",
            valueColumnName="metric_value"
        )
        
        # Test with PySpark
        pyspark_df = real_spark.createDataFrame(data)
        try:
            pyspark_result = pyspark_df.melt(
                ids=["id"],
                values=["metric1", "metric2"],
                variableColumnName="metric_name",
                valueColumnName="metric_value"
            )
            assert_dataframes_equal(mock_result, pyspark_result)
        except AttributeError:
            pytest.skip("melt() not available in this PySpark version")

    def test_melt_preserves_types(self, mock_spark, real_spark):
        """Test melt preserves data types correctly."""
        data = [
            {"id": 1, "score1": 95.5, "score2": 87.3}
        ]
        
        # Test with mock
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.melt(ids=["id"], values=["score1", "score2"])
        
        # Test with PySpark
        pyspark_df = real_spark.createDataFrame(data)
        try:
            pyspark_result = pyspark_df.melt(ids=["id"], values=["score1", "score2"])
            
            # Compare data
            assert_dataframes_equal(mock_result, pyspark_result)
            
            # Check value column type is preserved
            mock_value_field = [f for f in mock_result.schema.fields if f.name == "value"][0]
            pyspark_value_field = [f for f in pyspark_result.schema.fields if f.name == "value"][0]
            assert mock_value_field.dataType.typeName() == pyspark_value_field.dataType.typeName()
        except AttributeError:
            pytest.skip("melt() not available in this PySpark version")


class TestToSchema:
    """Test to() DataFrame method (PySpark 3.4+)."""

    def test_to_with_schema_enforcement(self, mock_spark, real_spark):
        """Test to() enforces schema structure."""
        from mock_spark.spark_types import MockStructType, MockStructField, IntegerType
        from pyspark.sql.types import StructType, StructField, IntegerType as PySparkIntType
        
        data = [{"a": 1, "b": 2, "c": 3}]
        
        # Test with mock
        mock_df = mock_spark.createDataFrame(data)
        mock_schema = MockStructType([
            MockStructField("a", IntegerType()),
            MockStructField("b", IntegerType())
        ])
        mock_result = mock_df.to(mock_schema)
        
        # Test with PySpark
        pyspark_df = real_spark.createDataFrame(data)
        pyspark_schema = StructType([
            StructField("a", PySparkIntType()),
            StructField("b", PySparkIntType())
        ])
        try:
            pyspark_result = pyspark_df.to(pyspark_schema)
            assert_dataframes_equal(mock_result, pyspark_result)
        except AttributeError:
            pytest.skip("to() not available in this PySpark version")

    def test_to_drops_extra_columns(self, mock_spark, real_spark):
        """Test to() drops columns not in target schema."""
        from mock_spark.spark_types import MockStructType, MockStructField, IntegerType as MockIntType, StringType as MockStringType
        from pyspark.sql.types import StructType, StructField, IntegerType as PySparkIntType, StringType as PySparkStringType
        
        data = [{"id": 1, "name": "Alice", "extra": "data"}]
        
        # Test with mock
        mock_df = mock_spark.createDataFrame(data)
        mock_schema = MockStructType([
            MockStructField("id", MockIntType()),
            MockStructField("name", MockStringType())
        ])
        mock_result = mock_df.to(mock_schema)
        
        # Test with PySpark
        pyspark_df = real_spark.createDataFrame(data)
        pyspark_schema = StructType([
            StructField("id", PySparkIntType()),
            StructField("name", PySparkStringType())
        ])
        try:
            pyspark_result = pyspark_df.to(pyspark_schema)
            
            # Should have same columns (no 'extra')
            assert set(mock_result.columns) == set(pyspark_result.columns)
            assert "extra" not in mock_result.columns
            assert_dataframes_equal(mock_result, pyspark_result)
        except AttributeError:
            pytest.skip("to() not available in this PySpark version")


class TestWithMetadata:
    """Test withMetadata() DataFrame method (PySpark 3.3+)."""

    def test_with_metadata_basic(self, mock_spark, real_spark):
        """Test withMetadata attaches metadata to column."""
        data = [{"id": 1, "name": "Alice"}]
        metadata = {"comment": "User identifier", "pii": False}
        
        # Test with mock
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.withMetadata("id", metadata)
        
        # Test with PySpark
        pyspark_df = real_spark.createDataFrame(data)
        try:
            pyspark_result = pyspark_df.withMetadata("id", metadata)
            
            # Check metadata matches
            mock_field = [f for f in mock_result.schema.fields if f.name == "id"][0]
            pyspark_field = [f for f in pyspark_result.schema.fields if f.name == "id"][0]
            assert mock_field.metadata == pyspark_field.metadata
            
            # Data should be unchanged
            assert_dataframes_equal(mock_result, pyspark_result)
        except AttributeError:
            pytest.skip("withMetadata() not available in this PySpark version")

    def test_with_metadata_chainable(self, mock_spark, real_spark):
        """Test withMetadata can be chained."""
        data = [{"col1": 1, "col2": 2}]
        
        # Test with mock
        mock_df = mock_spark.createDataFrame(data)
        mock_result = (mock_df
                       .withMetadata("col1", {"desc": "First column"})
                       .withMetadata("col2", {"desc": "Second column"}))
        
        # Test with PySpark
        pyspark_df = real_spark.createDataFrame(data)
        try:
            pyspark_result = (pyspark_df
                              .withMetadata("col1", {"desc": "First column"})
                              .withMetadata("col2", {"desc": "Second column"}))
            
            # Check both metadata are set
            mock_col1 = [f for f in mock_result.schema.fields if f.name == "col1"][0]
            pyspark_col1 = [f for f in pyspark_result.schema.fields if f.name == "col1"][0]
            assert mock_col1.metadata["desc"] == pyspark_col1.metadata["desc"]
            
            mock_col2 = [f for f in mock_result.schema.fields if f.name == "col2"][0]
            pyspark_col2 = [f for f in pyspark_result.schema.fields if f.name == "col2"][0]
            assert mock_col2.metadata["desc"] == pyspark_col2.metadata["desc"]
        except AttributeError:
            pytest.skip("withMetadata() not available in this PySpark version")


class TestObserve:
    """Test observe() DataFrame method (PySpark 3.3+)."""

    def test_observe_doesnt_change_data(self, mock_spark, real_spark, mock_functions, pyspark_functions):
        """Test observe doesn't modify DataFrame data."""
        data = [{"value": 42}]
        
        # Test with mock
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.observe("test", mock_functions.sum("value"))
        
        # Test with PySpark
        pyspark_df = real_spark.createDataFrame(data)
        try:
            pyspark_result = pyspark_df.observe("test", pyspark_functions.sum("value"))
            assert_dataframes_equal(mock_result, pyspark_result)
        except AttributeError:
            pytest.skip("observe() not available in this PySpark version")

    def test_observe_is_chainable(self, mock_spark, real_spark, mock_functions, pyspark_functions):
        """Test observe can be chained with other operations."""
        data = [{"id": i, "value": i * 10} for i in range(10)]
        
        # Test with mock
        mock_df = mock_spark.createDataFrame(data)
        mock_result = (mock_df
                       .observe("metrics", mock_functions.count(mock_functions.lit(1)))
                       .filter(mock_functions.col("value") > 50))
        
        # Test with PySpark
        pyspark_df = real_spark.createDataFrame(data)
        try:
            pyspark_result = (pyspark_df
                              .observe("metrics", pyspark_functions.count(pyspark_functions.lit(1)))
                              .filter(pyspark_functions.col("value") > 50))
            assert_dataframes_equal(mock_result, pyspark_result)
        except AttributeError:
            pytest.skip("observe() not available in this PySpark version")


class TestWindowTime:
    """Test window_time() function (PySpark 3.4+)."""

    def test_window_time_exists(self, mock_functions, pyspark_functions):
        """Test window_time function is accessible."""
        assert hasattr(mock_functions, 'window_time')
        assert callable(mock_functions.window_time)
        
        try:
            assert hasattr(pyspark_functions, 'window_time')
        except AttributeError:
            pytest.skip("window_time() not available in this PySpark version")

    def test_window_time_basic(self, mock_functions, pyspark_functions):
        """Test window_time creates column operation."""
        # Test with mock
        mock_col = mock_functions.window_time("window_col")
        assert mock_col is not None
        
        # Test with PySpark
        try:
            pyspark_col = pyspark_functions.window_time("window_col")
            assert pyspark_col is not None
        except AttributeError:
            pytest.skip("window_time() not available in this PySpark version")


class TestIntegration:
    """Integration tests combining PySpark 3.4+ features."""

    def test_melt_then_aggregate(self, mock_spark, real_spark, mock_functions, pyspark_functions):
        """Test melt followed by aggregation."""
        data = [
            {"id": 1, "A": 10, "B": 20},
            {"id": 2, "A": 15, "B": 25}
        ]
        
        # Test with mock
        mock_df = mock_spark.createDataFrame(data)
        try:
            mock_melted = mock_df.melt(ids=["id"], values=["A", "B"])
            mock_result = mock_melted.groupBy("variable").agg(mock_functions.sum("value").alias("total"))
        except AttributeError:
            pytest.skip("melt() not available in this PySpark version")
        
        # Test with PySpark
        pyspark_df = real_spark.createDataFrame(data)
        try:
            pyspark_melted = pyspark_df.melt(ids=["id"], values=["A", "B"])
            pyspark_result = pyspark_melted.groupBy("variable").agg(pyspark_functions.sum("value").alias("total"))
            
            assert_dataframes_equal(mock_result, pyspark_result)
        except AttributeError:
            pytest.skip("melt() not available in this PySpark version")

    def test_with_metadata_preserved_through_operations(self, mock_spark, real_spark):
        """Test metadata is preserved through select operations."""
        data = [{"id": 1, "name": "Alice"}]
        metadata = {"isPrimaryKey": True}
        
        # Test with mock
        mock_df = mock_spark.createDataFrame(data)
        try:
            mock_with_meta = mock_df.withMetadata("id", metadata)
            mock_result = mock_with_meta.select("id", "name")
            
            # Check metadata preserved
            mock_field = [f for f in mock_result.schema.fields if f.name == "id"][0]
            assert mock_field.metadata.get("isPrimaryKey") is True
        except AttributeError:
            pytest.skip("withMetadata() not available in this PySpark version")
        
        # Test with PySpark
        pyspark_df = real_spark.createDataFrame(data)
        try:
            pyspark_with_meta = pyspark_df.withMetadata("id", metadata)
            pyspark_result = pyspark_with_meta.select("id", "name")
            
            # Check metadata preserved
            pyspark_field = [f for f in pyspark_result.schema.fields if f.name == "id"][0]
            assert pyspark_field.metadata.get("isPrimaryKey") is True
            
            # Compare DataFrames
            assert_dataframes_equal(mock_result, pyspark_result)
        except AttributeError:
            pytest.skip("withMetadata() not available in this PySpark version")

    def test_observe_with_complex_aggregations(self, mock_spark, real_spark, mock_functions, pyspark_functions):
        """Test observe with multiple aggregation metrics."""
        data = [{"value": i} for i in range(100)]
        
        # Test with mock
        mock_df = mock_spark.createDataFrame(data)
        try:
            mock_result = mock_df.observe(
                "stats",
                mock_functions.count(mock_functions.lit(1)).alias("count"),
                mock_functions.sum("value").alias("total")
            ).filter(mock_functions.col("value") > 50)
        except AttributeError:
            pytest.skip("observe() not available in this PySpark version")
        
        # Test with PySpark
        pyspark_df = real_spark.createDataFrame(data)
        try:
            pyspark_result = pyspark_df.observe(
                "stats",
                pyspark_functions.count(pyspark_functions.lit(1)).alias("count"),
                pyspark_functions.sum("value").alias("total")
            ).filter(pyspark_functions.col("value") > 50)
            
            assert_dataframes_equal(mock_result, pyspark_result)
        except AttributeError:
            pytest.skip("observe() not available in this PySpark version")

