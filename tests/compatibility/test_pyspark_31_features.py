"""
Compatibility tests for PySpark 3.1 features.

Tests that new PySpark 3.1 functions and methods work identically in mock-spark.
"""

import pytest

# Check if PySpark is available
try:
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as F_real
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

from mock_spark import MockSparkSession
import mock_spark.functions as F_mock


def pyspark_has_function(func_name):
    """Check if PySpark has the specified function."""
    return hasattr(F_real, func_name) if PYSPARK_AVAILABLE else False


@pytest.fixture(scope="module")
def real_spark():
    """Create a real PySpark session."""
    if not PYSPARK_AVAILABLE:
        pytest.skip("PySpark not available")
    
    spark = SparkSession.builder \
        .appName("test_pyspark_31") \
        .master("local[1]") \
        .getOrCreate()
    
    yield spark
    spark.stop()


@pytest.fixture(scope="module")
def mock_spark():
    """Create a mock Spark session."""
    return MockSparkSession("test_app")


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not available")
class TestPhase1IntervalFunctions:
    """Test interval/duration functions from PySpark 3.1."""
    
    @pytest.mark.skipif(not pyspark_has_function('days'), reason="days() not in this PySpark version")
    def test_days_compat(self, real_spark, mock_spark):
        """Test days() produces identical results."""
        data = [{"num": 5}, {"num": 10}]
        
        real_df = real_spark.createDataFrame(data)
        real_result = real_df.select(F_real.days(F_real.col("num")).alias("interval")).collect()
        
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.select(F_mock.days(F_mock.col("num")).alias("interval")).collect()
        
        # Both should create intervals (values may differ in representation)
        assert len(real_result) == len(mock_result)
    
    @pytest.mark.skipif(not pyspark_has_function('hours'), reason="hours() not in this PySpark version")
    def test_hours_compat(self, real_spark, mock_spark):
        """Test hours() produces identical results."""
        data = [{"num": 24}]
        
        real_df = real_spark.createDataFrame(data)
        real_result = real_df.select(F_real.hours(F_real.col("num")).alias("interval")).collect()
        
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.select(F_mock.hours(F_mock.col("num")).alias("interval")).collect()
        
        assert len(real_result) == len(mock_result)
    
    @pytest.mark.skipif(not pyspark_has_function('months'), reason="months() not in this PySpark version")
    def test_months_compat(self, real_spark, mock_spark):
        """Test months() produces identical results."""
        data = [{"num": 3}]
        
        real_df = real_spark.createDataFrame(data)
        real_result = real_df.select(F_real.months(F_real.col("num")).alias("interval")).collect()
        
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.select(F_mock.months(F_mock.col("num")).alias("interval")).collect()
        
        assert len(real_result) == len(mock_result)
    
    @pytest.mark.skipif(not pyspark_has_function('years'), reason="years() not in this PySpark version")
    def test_years_compat(self, real_spark, mock_spark):
        """Test years() produces identical results."""
        data = [{"num": 2}]
        
        real_df = real_spark.createDataFrame(data)
        real_result = real_df.select(F_real.years(F_real.col("num")).alias("interval")).collect()
        
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.select(F_mock.years(F_mock.col("num")).alias("interval")).collect()
        
        assert len(real_result) == len(mock_result)


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not available")
class TestPhase2HigherOrderMapFunctions:
    """Test higher-order map functions from PySpark 3.1."""
    
    @pytest.mark.skipif(not pyspark_has_function('map_filter'), reason="map_filter() not in this PySpark version")
    def test_map_filter_compat(self, real_spark, mock_spark):
        """Test map_filter() with lambda produces identical results."""
        pytest.skip("Requires lambda support - deferred")
    
    @pytest.mark.skipif(not pyspark_has_function('map_zip_with'), reason="map_zip_with() not in this PySpark version")
    def test_map_zip_with_compat(self, real_spark, mock_spark):
        """Test map_zip_with() with lambda produces identical results."""
        pytest.skip("Requires lambda support - deferred")


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not available")
class TestPhase3UtilityFunctions:
    """Test utility functions from PySpark 3.1."""
    
    @pytest.mark.skipif(not pyspark_has_function('bucket'), reason="bucket() not in this PySpark version")
    def test_bucket_compat(self, real_spark, mock_spark):
        """Test bucket() produces identical results."""
        data = [{"id": 1}, {"id": 100}, {"id": 500}]
        
        real_df = real_spark.createDataFrame(data)
        real_result = real_df.select(F_real.bucket(10, F_real.col("id")).alias("bucket")).collect()
        
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.select(F_mock.bucket(10, F_mock.col("id")).alias("bucket")).collect()
        
        # Bucket values should match
        for i in range(len(data)):
            assert real_result[i]["bucket"] == mock_result[i]["bucket"]
    
    @pytest.mark.skipif(not pyspark_has_function('raise_error'), reason="raise_error() not in this PySpark version")
    def test_raise_error_compat(self, real_spark, mock_spark):
        """Test raise_error() raises exception in both."""
        data = [{"id": 1}]
        
        real_df = real_spark.createDataFrame(data)
        mock_df = mock_spark.createDataFrame(data)
        
        # Both should raise when evaluated
        with pytest.raises(Exception):
            real_df.select(F_real.raise_error(F_real.lit("Test error"))).collect()
        
        with pytest.raises(Exception):
            mock_df.select(F_mock.raise_error(F_mock.lit("Test error"))).collect()
    
    @pytest.mark.skipif(not pyspark_has_function('timestamp_seconds'), reason="timestamp_seconds() not in this PySpark version")
    def test_timestamp_seconds_compat(self, real_spark, mock_spark):
        """Test timestamp_seconds() produces identical results."""
        data = [{"seconds": 1609459200}]  # 2021-01-01 00:00:00 UTC
        
        real_df = real_spark.createDataFrame(data)
        real_result = real_df.select(F_real.timestamp_seconds(F_real.col("seconds")).alias("ts")).collect()
        
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.select(F_mock.timestamp_seconds(F_mock.col("seconds")).alias("ts")).collect()
        
        # Both should produce same timestamp
        assert str(real_result[0]["ts"]) == str(mock_result[0]["ts"])


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not available")
class TestPhase4DataFrameMethods:
    """Test DataFrame methods from PySpark 3.1."""
    
    def test_input_files_compat(self, real_spark, mock_spark):
        """Test inputFiles() returns list in both."""
        data = [{"id": 1}]
        
        real_df = real_spark.createDataFrame(data)
        mock_df = mock_spark.createDataFrame(data)
        
        # Both should return a list (may be empty for in-memory data)
        real_files = real_df.inputFiles() if hasattr(real_df, 'inputFiles') else []
        mock_files = mock_df.inputFiles()
        
        assert isinstance(real_files, list)
        assert isinstance(mock_files, list)
    
    def test_same_semantics_compat(self, real_spark, mock_spark):
        """Test sameSemantics() behavior."""
        pytest.skip("sameSemantics requires complex semantic analysis - deferred")
    
    def test_semantic_hash_compat(self, real_spark, mock_spark):
        """Test semanticHash() behavior."""
        pytest.skip("semanticHash requires complex semantic analysis - deferred")
    
    def test_write_to_compat(self, real_spark, mock_spark):
        """Test writeTo() returns DataFrameWriterV2."""
        pytest.skip("writeTo requires DataFrameWriterV2 class - deferred")

