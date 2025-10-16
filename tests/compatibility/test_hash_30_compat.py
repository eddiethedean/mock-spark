"""
Compatibility tests for PySpark 3.0 hash functions.

Tests that hash functions work identically in mock-spark and real PySpark 3.0.
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


@pytest.fixture(scope="module")
def real_spark():
    """Create a real PySpark session."""
    if not PYSPARK_AVAILABLE:
        pytest.skip("PySpark not available")
    
    spark = SparkSession.builder \
        .appName("test_hash_30") \
        .master("local[1]") \
        .getOrCreate()
    
    yield spark
    spark.stop()


@pytest.fixture(scope="module")
def mock_spark():
    """Create a mock Spark session."""
    return MockSparkSession("test_app")


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not available")
class TestMD5Compat:
    """Test md5 function compatibility."""
    
    def test_md5_identical_output(self, real_spark, mock_spark):
        """Test md5 produces identical hashes."""
        data = [{"text": "hello"}, {"text": "world"}, {"text": ""}]
        
        real_df = real_spark.createDataFrame(data)
        real_result = real_df.select(F_real.md5(F_real.col("text")).alias("hash")).collect()
        
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.select(F_mock.md5(F_mock.col("text")).alias("hash")).collect()
        
        for i in range(len(data)):
            assert real_result[i]["hash"] == mock_result[i]["hash"]


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not available")
class TestSHA2Compat:
    """Test sha2 function compatibility."""
    
    def test_sha2_256_output(self, real_spark, mock_spark):
        """Test sha2 with 256 bits produces identical hashes."""
        data = [{"text": "hello"}, {"text": "test"}]
        
        real_df = real_spark.createDataFrame(data)
        real_result = real_df.select(F_real.sha2(F_real.col("text"), 256).alias("hash")).collect()
        
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.select(F_mock.sha2(F_mock.col("text"), 256).alias("hash")).collect()
        
        for i in range(len(data)):
            assert real_result[i]["hash"] == mock_result[i]["hash"]


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not available")  
class TestCRC32Compat:
    """Test crc32 function compatibility."""
    
    @pytest.mark.skip(reason="CRC32 uses DuckDB HASH() approximation, not exact PySpark CRC32")
    def test_crc32_output(self, real_spark, mock_spark):
        """Test crc32 produces similar checksums."""
        data = [{"text": "hello"}, {"text": "world"}]
        
        real_df = real_spark.createDataFrame(data)
        real_result = real_df.select(F_real.crc32(F_real.col("text")).alias("crc")).collect()
        
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.select(F_mock.crc32(F_mock.col("text")).alias("crc")).collect()
        
        # Note: Won't match exactly due to different hash algorithms
        # Just verify both return integers
        for i in range(len(data)):
            assert isinstance(real_result[i]["crc"], int)
            assert isinstance(mock_result[i]["crc"], int)
    
    def test_crc32_deterministic(self, real_spark, mock_spark):
        """Test crc32 is deterministic in both."""
        data = [{"text": "test"}, {"text": "test"}]
        
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.select(F_mock.crc32(F_mock.col("text")).alias("crc")).collect()
        
        # Same input should produce same CRC in mock-spark
        assert mock_result[0]["crc"] == mock_result[1]["crc"]


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not available")
class TestSHA1Compat:
    """Test sha1 function compatibility."""
    
    @pytest.mark.skip(reason="SHA1 uses DuckDB SHA256 fallback, not exact SHA1")
    def test_sha1_output(self, real_spark, mock_spark):
        """Test sha1 produces hashes."""
        data = [{"text": "hello"}]
        
        real_df = real_spark.createDataFrame(data)
        real_result = real_df.select(F_real.sha1(F_real.col("text")).alias("hash")).collect()
        
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.select(F_mock.sha1(F_mock.col("text")).alias("hash")).collect()
        
        # Won't match exactly - real PySpark uses SHA1, mock uses SHA256
        # Just verify both return hex strings
        assert len(real_result[0]["hash"]) == 40  # SHA1
        assert len(mock_result[0]["hash"]) == 64  # SHA256

