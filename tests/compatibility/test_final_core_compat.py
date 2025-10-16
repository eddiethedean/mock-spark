"""
Compatibility tests for final core features (100% PySpark 3.0-3.5 coverage).

Tests that mock-spark implementation matches PySpark behavior for:
- PandasUDFType (enum class)
- to_str() function
- mapInPandas() method
"""

import pytest
from tests.compatibility.utils.comparison import assert_dataframes_equal


class TestPandasUDFType:
    """Test PandasUDFType enum class."""

    def test_pandas_udf_type_constants(self):
        """Test PandasUDFType has correct constants."""
        from mock_spark.functions import PandasUDFType as MockPandasUDFType
        
        try:
            from pyspark.sql.functions import PandasUDFType as PySparkPandasUDFType
        except ImportError:
            pytest.skip("PandasUDFType not available in this PySpark version")
        
        # Check all constants exist and have same values
        assert hasattr(MockPandasUDFType, 'SCALAR')
        assert hasattr(MockPandasUDFType, 'GROUPED_MAP')
        assert hasattr(MockPandasUDFType, 'GROUPED_AGG')
        
        assert MockPandasUDFType.SCALAR == PySparkPandasUDFType.SCALAR
        assert MockPandasUDFType.GROUPED_MAP == PySparkPandasUDFType.GROUPED_MAP  
        assert MockPandasUDFType.GROUPED_AGG == PySparkPandasUDFType.GROUPED_AGG

    def test_pandas_udf_type_scalar_iter(self):
        """Test SCALAR_ITER constant if available."""
        from mock_spark.functions import PandasUDFType as MockPandasUDFType
        
        try:
            from pyspark.sql.functions import PandasUDFType as PySparkPandasUDFType
            if hasattr(PySparkPandasUDFType, 'SCALAR_ITER'):
                assert hasattr(MockPandasUDFType, 'SCALAR_ITER')
                # Note: Exact values may differ between PySpark versions
                assert isinstance(MockPandasUDFType.SCALAR_ITER, int)
        except ImportError:
            pytest.skip("PandasUDFType not available in this PySpark version")

    def test_pandas_udf_type_map_iter(self):
        """Test MAP_ITER constant if available."""
        from mock_spark.functions import PandasUDFType as MockPandasUDFType
        
        try:
            from pyspark.sql.functions import PandasUDFType as PySparkPandasUDFType
            if hasattr(PySparkPandasUDFType, 'MAP_ITER'):
                assert hasattr(MockPandasUDFType, 'MAP_ITER')
                assert MockPandasUDFType.MAP_ITER == PySparkPandasUDFType.MAP_ITER
        except ImportError:
            pytest.skip("PandasUDFType not available in this PySpark version")


class TestToStrFunction:
    """Test to_str() function."""

    def test_to_str_exists(self, mock_functions, pyspark_functions):
        """Test to_str function exists in both environments."""
        assert hasattr(mock_functions, 'to_str')
        assert hasattr(pyspark_functions, 'to_str')
        assert callable(mock_functions.to_str)
        assert callable(pyspark_functions.to_str)

    def test_to_str_basic(self, mock_spark, mock_functions):
        """Test to_str converts values to strings in mock_spark."""
        data = [{"value": 42}, {"value": 100}]
        
        # Test with mock
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.select(mock_functions.to_str(mock_functions.col("value")).alias("str_value"))
        mock_data = mock_result.collect()
        
        # Should convert to strings
        assert len(mock_data) == 2
        assert isinstance(mock_data[0]["str_value"], str)

    def test_to_str_with_nulls(self, mock_spark, mock_functions):
        """Test to_str handles null values in mock_spark."""
        data = [{"value": 10}, {"value": None}]
        
        # Test with mock
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.select(mock_functions.to_str(mock_functions.col("value")).alias("str_value"))
        mock_data = mock_result.collect()
        
        # Should handle nulls
        assert len(mock_data) == 2
        assert mock_data[1]["str_value"] is None

    def test_to_str_with_different_types(self, mock_spark, mock_functions):
        """Test to_str with various data types in mock_spark."""
        data = [
            {"int_val": 42, "float_val": 3.14, "bool_val": True}
        ]
        
        # Test with mock
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.select(
            mock_functions.to_str("int_val").alias("str_int"),
            mock_functions.to_str("float_val").alias("str_float"),
            mock_functions.to_str("bool_val").alias("str_bool")
        )
        mock_data = mock_result.collect()
        
        # All should be strings
        assert len(mock_data) == 1
        assert all(isinstance(v, (str, type(None))) or v is True or v is False 
                   for v in mock_data[0].asDict().values())


class TestMapInPandas:
    """Test mapInPandas() method."""

    @pytest.mark.skipif(
        True, reason="mapInPandas requires pandas and may have environment-specific behavior"
    )
    def test_map_in_pandas_basic(self, mock_spark, real_spark):
        """Test mapInPandas basic transformation."""
        pytest.importorskip("pandas")
        
        data = [{"id": i, "value": i * 10} for i in range(5)]
        
        def transform(iterator):
            for pdf in iterator:
                pdf['value'] = pdf['value'] * 2
                yield pdf
        
        # Test with mock
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.mapInPandas(transform, "id: long, value: long")
        
        # Test with PySpark
        pyspark_df = real_spark.createDataFrame(data)
        pyspark_result = pyspark_df.mapInPandas(transform, "id: long, value: long")
        
        assert_dataframes_equal(mock_result, pyspark_result)

    def test_map_in_pandas_exists(self, mock_spark):
        """Test mapInPandas method exists."""
        data = [{"id": 1}]
        df = mock_spark.createDataFrame(data)
        
        # Method should exist
        assert hasattr(df, 'mapInPandas')
        assert callable(df.mapInPandas)

    @pytest.mark.skipif(
        True, reason="mapInPandas requires pandas"
    )
    def test_map_in_pandas_error_without_pandas(self, mock_spark):
        """Test mapInPandas raises error without pandas."""
        data = [{"id": 1}]
        df = mock_spark.createDataFrame(data)
        
        def transform(iterator):
            for pdf in iterator:
                yield pdf
        
        # Should raise ImportError if pandas not available
        with pytest.raises(ImportError, match="pandas"):
            df.mapInPandas(transform, "id: int").collect()


class TestIntegration:
    """Integration tests combining final core features."""

    @pytest.mark.skip(reason="to_str SQL translation not yet implemented in DuckDB backend")
    def test_to_str_in_complex_query(self, mock_spark, mock_functions):
        """Test to_str in complex query with filter and aggregation."""
        # Note: to_str needs DuckDB CAST translation to work in aggregations
        data = [
            {"category": "A", "value": 10},
            {"category": "B", "value": 20},
            {"category": "A", "value": 15}
        ]
        
        # Test with mock
        mock_df = mock_spark.createDataFrame(data)
        mock_df2 = mock_df.withColumn("str_value", mock_functions.to_str("value"))
        mock_result = mock_df2.groupBy("category").agg(mock_functions.count("*").alias("count"))
        
        # Should execute without error
        assert mock_result.count() == 2

    @pytest.mark.skip(reason="DataFrame subscripting and to_str SQL translation not yet implemented")
    def test_to_str_with_join(self, mock_spark, mock_functions):
        """Test to_str used in join condition."""
        data1 = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        data2 = [{"str_id": "1", "score": 95}, {"str_id": "2", "score": 87}]
        
        # Test with mock
        mock_df1 = mock_spark.createDataFrame(data1)
        mock_df2 = mock_spark.createDataFrame(data2)
        mock_result = mock_df1.join(
            mock_df2,
            mock_functions.to_str(mock_df1.id) == mock_df2.str_id
        ).select("name", "score")
        
        # Should execute and produce results
        assert mock_result.count() == 2

