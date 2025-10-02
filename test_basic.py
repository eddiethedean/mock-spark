#!/usr/bin/env python3
"""
Basic test to verify Mock Spark package functionality.
"""

def test_basic_import():
    """Test that we can import the main components."""
    from mock_spark import MockSparkSession, MockDataFrame, F
    from mock_spark.spark_types import StringType, IntegerType
    
    # Test basic functionality
    spark = MockSparkSession("TestApp")
    assert spark is not None
    
    # Test DataFrame creation
    data = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
    df = spark.createDataFrame(data)
    assert isinstance(df, MockDataFrame)
    
    # Test basic operations
    result = df.filter(F.col("age") > 25).collect()
    assert len(result) == 1
    assert result[0]["name"] == "Bob"
    
    print("✅ All basic tests passed!")

if __name__ == "__main__":
    test_basic_import()
