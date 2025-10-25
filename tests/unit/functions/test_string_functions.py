"""
Unit tests for string functions.
"""

import pytest
from mock_spark import MockSparkSession, F


@pytest.mark.unit
class TestStringFunctions:
    """Test string functions."""
    
    @pytest.fixture
    def spark(self):
        """Create a MockSparkSession for testing."""
        return MockSparkSession("test")
    
    @pytest.fixture
    def sample_data(self):
        """Sample data for testing."""
        return [
            {"id": 1, "name": "Alice", "email": "alice@example.com"},
            {"id": 2, "name": "Bob", "email": "bob@test.com"},
            {"id": 3, "name": "Charlie", "email": "charlie@company.org"},
        ]
    
    def test_upper_function(self, spark, sample_data):
        """Test upper function."""
        df = spark.createDataFrame(sample_data)
        result = df.select(F.upper("name").alias("name_upper"))
        
        assert result.count() == 3
        assert len(result.columns) == 1
        assert "name_upper" in result.columns
    
    def test_lower_function(self, spark, sample_data):
        """Test lower function."""
        df = spark.createDataFrame(sample_data)
        result = df.select(F.lower("name").alias("name_lower"))
        
        assert result.count() == 3
        assert len(result.columns) == 1
        assert "name_lower" in result.columns
    
    def test_length_function(self, spark, sample_data):
        """Test length function."""
        df = spark.createDataFrame(sample_data)
        result = df.select(F.length("name").alias("name_length"))
        
        assert result.count() == 3
        assert len(result.columns) == 1
        assert "name_length" in result.columns
    
    def test_substring_function(self, spark, sample_data):
        """Test substring function."""
        df = spark.createDataFrame(sample_data)
        result = df.select(F.substring("name", 1, 3).alias("name_sub"))
        
        assert result.count() == 3
        assert len(result.columns) == 1
        assert "name_sub" in result.columns
    
    def test_concat_function(self, spark, sample_data):
        """Test concat function."""
        df = spark.createDataFrame(sample_data)
        result = df.select(F.concat("name", F.lit(" - "), "email").alias("full_info"))
        
        assert result.count() == 3
        assert len(result.columns) == 1
        assert "full_info" in result.columns
    
    def test_split_function(self, spark, sample_data):
        """Test split function."""
        df = spark.createDataFrame(sample_data)
        result = df.select(F.split("email", "@").alias("email_parts"))
        
        assert result.count() == 3
        assert len(result.columns) == 1
        assert "email_parts" in result.columns
    
    def test_regexp_extract_function(self, spark, sample_data):
        """Test regexp_extract function."""
        df = spark.createDataFrame(sample_data)
        result = df.select(F.regexp_extract("email", r"@(.+)", 1).alias("domain"))
        
        assert result.count() == 3
        assert len(result.columns) == 1
        assert "domain" in result.columns
