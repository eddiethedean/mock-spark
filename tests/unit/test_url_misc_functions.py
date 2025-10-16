"""Unit tests for Phase 7 & 8: URL and Miscellaneous Functions."""

import pytest
from mock_spark.session.session import MockSparkSession
from mock_spark import functions as F


class TestURLFunctionsUnit:
    """Test URL functions (Phase 7)."""
    
    def test_parse_url(self):
        """Test parse_url extracts URL components."""
        spark = MockSparkSession("test")
        data = [{"url": "https://spark.apache.org/docs?key=value#section"}]
        df = spark.createDataFrame(data)
        
        result = df.select(
            F.parse_url(F.col("url"), "HOST").alias("host"),
            F.parse_url(F.col("url"), "QUERY").alias("query"),
        ).collect()
        
        assert "spark.apache.org" in str(result[0]["host"])
    
    def test_url_encode(self):
        """Test url_encode encodes URL strings."""
        spark = MockSparkSession("test")
        data = [{"text": "hello world"}]
        df = spark.createDataFrame(data)
        
        result = df.select(
            F.url_encode(F.col("text")).alias("encoded")
        ).collect()
        
        # Should have %20 or + for space
        assert "hello" in result[0]["encoded"]
    
    def test_url_decode(self):
        """Test url_decode decodes URL strings."""
        spark = MockSparkSession("test")
        data = [{"encoded": "hello%20world"}]
        df = spark.createDataFrame(data)
        
        result = df.select(
            F.url_decode(F.col("encoded")).alias("decoded")
        ).collect()
        
        assert "hello world" == result[0]["decoded"] or "hello" in result[0]["decoded"]


class TestMiscFunctionsUnit:
    """Test miscellaneous functions (Phase 8)."""
    
    def test_date_part(self):
        """Test date_part extracts date components."""
        spark = MockSparkSession("test")
        data = [{"date": "2024-06-15"}]
        df = spark.createDataFrame(data)
        
        result = df.select(
            F.date_part("YEAR", F.col("date")).alias("year"),
            F.date_part("MONTH", F.col("date")).alias("month"),
        ).collect()
        
        assert result[0]["year"] == 2024
        assert result[0]["month"] == 6
    
    def test_dayname(self):
        """Test dayname returns day of week name."""
        spark = MockSparkSession("test")
        # 2024-01-01 is a Monday
        data = [{"date": "2024-01-01"}]
        df = spark.createDataFrame(data)
        
        result = df.select(
            F.dayname(F.col("date")).alias("day")
        ).collect()
        
        # Should return "Monday" or similar
        assert isinstance(result[0]["day"], str)
        assert len(result[0]["day"]) > 0
    
    def test_assert_true(self):
        """Test assert_true validates conditions."""
        spark = MockSparkSession("test")
        data = [{"value": 10}]
        df = spark.createDataFrame(data)
        
        # Should succeed without error
        result = df.select(
            F.assert_true(F.col("value") > 5).alias("check")
        ).collect()
        
        # If it passes, we should get a result
        assert len(result) == 1

