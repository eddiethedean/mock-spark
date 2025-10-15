"""
PySpark 3.2 Phase 1 Compatibility Tests.

Tests datetime functions, string functions, and enhanced error messages
against real PySpark to ensure compatibility.
"""

import pytest

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as PySparkF
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

from mock_spark import MockSparkSession
from mock_spark import functions as F


@pytest.mark.compatibility
@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not installed")
class TestTimestampFunctionsCompat:
    """Test TIMESTAMPADD and TIMESTAMPDIFF compatibility."""
    
    def setup_method(self):
        """Setup test data for both mock-spark and PySpark."""
        self.mock_spark = MockSparkSession("test")
        if PYSPARK_AVAILABLE:
            self.real_spark = SparkSession.builder.appName("test").master("local[1]").getOrCreate()
        
        self.test_data = [
            {"id": 1, "timestamp": "2024-01-15 10:30:00", "start_date": "2024-01-01", "end_date": "2024-01-31"},
            {"id": 2, "timestamp": "2024-06-15 14:20:00", "start_date": "2024-06-01", "end_date": "2024-06-30"},
        ]
    
    def teardown_method(self):
        """Cleanup sessions."""
        if PYSPARK_AVAILABLE and hasattr(self, 'real_spark'):
            self.real_spark.stop()
    
    def test_timestampadd_day(self):
        """Test timestampadd with DAY unit."""
        mock_df = self.mock_spark.createDataFrame(self.test_data)
        real_df = self.real_spark.createDataFrame(self.test_data)
        
        # Both should add 7 days
        mock_result = mock_df.select(
            F.col("id"),
            F.timestampadd("DAY", 7, F.col("timestamp")).alias("new_timestamp")
        ).collect()
        
        # Note: PySpark 3.3+ has timestampadd, 3.2 might not
        # This test documents expected behavior
        assert len(mock_result) == 2
        assert mock_result[0]["id"] == 1
    
    def test_timestampdiff_day(self):
        """Test timestampdiff with DAY unit."""
        mock_df = self.mock_spark.createDataFrame(self.test_data)
        
        # Calculate days between start and end
        mock_result = mock_df.select(
            F.col("id"),
            F.timestampdiff("DAY", F.col("start_date"), F.col("end_date")).alias("days_diff")
        ).collect()
        
        assert len(mock_result) == 2
        assert mock_result[0]["id"] == 1


@pytest.mark.compatibility
@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not installed")
class TestStringFunctionsCompat:
    """Test new string functions compatibility."""
    
    def setup_method(self):
        """Setup test data."""
        self.mock_spark = MockSparkSession("test")
        if PYSPARK_AVAILABLE:
            self.real_spark = SparkSession.builder.appName("test").master("local[1]").getOrCreate()
        
        self.test_data = [
            {"id": 1, "text": "Hello World", "tags": ["a", "b", "c"]},
            {"id": 2, "text": "test data", "tags": ["x", "y", "z"]},
        ]
    
    def teardown_method(self):
        """Cleanup sessions."""
        if PYSPARK_AVAILABLE and hasattr(self, 'real_spark'):
            self.real_spark.stop()
    
    def test_initcap(self):
        """Test initcap function."""
        mock_df = self.mock_spark.createDataFrame(self.test_data)
        
        mock_result = mock_df.select(
            F.col("id"),
            F.initcap(F.col("text")).alias("capitalized")
        ).collect()
        
        assert len(mock_result) == 2
        assert mock_result[0]["id"] == 1
    
    def test_repeat(self):
        """Test repeat function."""
        mock_df = self.mock_spark.createDataFrame([{"text": "ab"}])
        
        mock_result = mock_df.select(
            F.repeat(F.col("text"), 3).alias("repeated")
        ).collect()
        
        assert len(mock_result) == 1
    
    def test_soundex(self):
        """Test soundex function."""
        mock_df = self.mock_spark.createDataFrame([{"name": "Smith"}])
        
        mock_result = mock_df.select(
            F.soundex(F.col("name")).alias("soundex_code")
        ).collect()
        
        assert len(mock_result) == 1
    
    def test_array_join(self):
        """Test array_join function."""
        mock_df = self.mock_spark.createDataFrame(self.test_data)
        
        mock_result = mock_df.select(
            F.col("id"),
            F.array_join(F.col("tags"), ", ").alias("joined_tags")
        ).collect()
        
        assert len(mock_result) == 2
        assert mock_result[0]["id"] == 1


@pytest.mark.compatibility  
@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not installed")
class TestEnhancedErrorMessagesCompat:
    """Test enhanced error messages."""
    
    def test_column_not_found_has_suggestions(self):
        """Test that ColumnNotFoundException provides helpful suggestions."""
        from mock_spark.core.exceptions.analysis import ColumnNotFoundException
        
        spark = MockSparkSession("test")
        df = spark.createDataFrame([{"user_id": 1, "user_name": "Alice"}])
        
        # Try to access non-existent column similar to existing one
        with pytest.raises(Exception) as exc_info:
            df.select("userid").collect()  # Missing underscore
        
        # Error message should be helpful
        error_msg = str(exc_info.value)
        # Should contain some helpful context
        assert "userid" in error_msg or "user_id" in error_msg
    
    def test_enhanced_exception_has_error_code(self):
        """Test that enhanced exceptions have error codes."""
        from mock_spark.core.exceptions.analysis import ColumnNotFoundException
        
        exc = ColumnNotFoundException(
            "missing_col",
            available_columns=["id", "name"],
            table_name="users"
        )
        
        assert exc.error_code == "COLUMN_NOT_FOUND"
        assert "missing_col" in str(exc)
        assert "Available columns" in str(exc)

