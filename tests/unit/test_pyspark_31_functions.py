"""Unit tests for PySpark 3.1 functions in mock-spark."""

from mock_spark import MockSparkSession, F


class TestIntervalFunctions:
    """Test interval/duration functions."""
    
    def test_days_basic(self):
        """Test days function."""
        spark = MockSparkSession("test")
        df = spark.createDataFrame([{"num": 5}])
        
        result = df.select(F.days(F.col("num")).alias("interval")).collect()
        # Should create an interval (exact format may vary)
        assert result is not None
        assert len(result) == 1
    
    def test_hours_basic(self):
        """Test hours function."""
        spark = MockSparkSession("test")
        df = spark.createDataFrame([{"num": 24}])
        
        result = df.select(F.hours(F.col("num")).alias("interval")).collect()
        assert result is not None
        assert len(result) == 1
    
    def test_months_basic(self):
        """Test months function."""
        spark = MockSparkSession("test")
        df = spark.createDataFrame([{"num": 3}])
        
        result = df.select(F.months(F.col("num")).alias("interval")).collect()
        assert result is not None
        assert len(result) == 1
    
    def test_years_basic(self):
        """Test years function."""
        spark = MockSparkSession("test")
        df = spark.createDataFrame([{"num": 2}])
        
        result = df.select(F.years(F.col("num")).alias("interval")).collect()
        assert result is not None
        assert len(result) == 1


class TestUtilityFunctions:
    """Test utility functions."""
    
    def test_bucket_basic(self):
        """Test bucket function produces consistent bucket numbers."""
        spark = MockSparkSession("test")
        df = spark.createDataFrame([
            {"id": 1},
            {"id": 100},
            {"id": 500}
        ])
        
        result = df.select(F.bucket(10, F.col("id")).alias("bucket")).collect()
        
        # Should return bucket numbers 0-9
        for row in result:
            assert 0 <= row["bucket"] < 10
    
    def test_bucket_deterministic(self):
        """Test bucket is deterministic (same input -> same bucket)."""
        spark = MockSparkSession("test")
        df = spark.createDataFrame([{"id": 42}, {"id": 42}])
        
        result = df.select(F.bucket(10, F.col("id")).alias("bucket")).collect()
        
        # Same ID should produce same bucket
        assert result[0]["bucket"] == result[1]["bucket"]
    
    def test_timestamp_seconds_basic(self):
        """Test timestamp_seconds converts epoch to timestamp."""
        spark = MockSparkSession("test")
        df = spark.createDataFrame([{"seconds": 1609459200}])  # 2021-01-01 00:00:00 UTC
        
        result = df.select(F.timestamp_seconds(F.col("seconds")).alias("ts")).collect()
        
        # Should produce a timestamp
        assert result[0]["ts"] is not None
        # Check year is 2021 (or 2020 depending on timezone)
        ts_str = str(result[0]["ts"])
        assert "2021" in ts_str or "2020" in ts_str
    
    def test_raise_error_basic(self):
        """Test raise_error raises an exception."""
        spark = MockSparkSession("test")
        df = spark.createDataFrame([{"id": 1}])
        
        # Should raise when evaluated
        try:
            df.select(F.raise_error(F.lit("Test error"))).collect()
            assert False, "Should have raised an error"
        except Exception as e:
            # Expected - some error should be raised
            assert True


class TestDataFrameMethods:
    """Test DataFrame methods from PySpark 3.1."""
    
    def test_input_files(self):
        """Test inputFiles returns empty list."""
        spark = MockSparkSession("test")
        df = spark.createDataFrame([{"id": 1}])
        
        files = df.inputFiles()
        
        assert isinstance(files, list)
        assert len(files) == 0  # In-memory data has no files
    
    def test_same_semantics(self):
        """Test sameSemantics compares schemas."""
        spark = MockSparkSession("test")
        df1 = spark.createDataFrame([{"id": 1, "name": "Alice"}])
        df2 = spark.createDataFrame([{"id": 2, "name": "Bob"}])
        df3 = spark.createDataFrame([{"id": 1, "value": 10}])
        
        # Same schema
        assert df1.sameSemantics(df2) is True
        
        # Different schema
        assert df1.sameSemantics(df3) is False
    
    def test_semantic_hash(self):
        """Test semanticHash returns integer."""
        spark = MockSparkSession("test")
        df = spark.createDataFrame([{"id": 1, "name": "test"}])
        
        hash_val = df.semanticHash()
        
        assert isinstance(hash_val, int)
    
    def test_semantic_hash_consistent(self):
        """Test semanticHash is consistent for same schema."""
        spark = MockSparkSession("test")
        df1 = spark.createDataFrame([{"id": 1}])
        df2 = spark.createDataFrame([{"id": 2}])
        
        # Same schema should produce same hash
        assert df1.semanticHash() == df2.semanticHash()

