"""
Compatibility tests for Delta Lake basic write support.

Tests mock-spark against real PySpark to ensure API compatibility.
Based on exploration/delta_basic_write.py findings.

NOTE: These tests require Delta Lake to be installed in PySpark environment.
They will be skipped if Delta is not available. To run these tests, ensure
delta-core JARs are available to PySpark.
"""

import pytest

# Mark all tests in this module as requiring delta
# These tests are currently skipped because they require additional Delta Lake
# configuration in PySpark that goes beyond standard setup. The delta-spark package
# is installed as a test dependency, but these integration tests need more work.
pytestmark = [
    pytest.mark.compatibility,
    pytest.mark.delta,
    pytest.mark.skip(reason="Delta Lake integration tests need additional PySpark configuration"),
]


class TestDeltaWriteCompatibility:
    """Test Delta write compatibility with real PySpark.
    
    These tests compare Mock Spark's Delta Lake implementation with real PySpark Delta.
    Requires Delta Lake JARs to be installed (delta-core_2.12:2.0.0 or similar).
    
    To enable these tests, remove the skip marker and ensure PySpark has Delta Lake:
    - Install delta-spark package
    - Or add delta-core JARs to Spark classpath
    """

    def test_delta_write_save_as_table_basic(self, real_spark, mock_spark):
        """Test basic Delta write compatibility."""
        data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        
        # Create schemas
        real_spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")
        mock_spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")
        
        # Real PySpark
        real_df = real_spark.createDataFrame(data)
        real_df.write.format("delta").mode("overwrite").saveAsTable("test_schema.users")
        real_result = real_spark.table("test_schema.users")
        real_count = real_result.count()
        real_rows = sorted(real_result.collect(), key=lambda r: r["id"])
        
        # Mock Spark
        mock_df = mock_spark.createDataFrame(data)
        mock_df.write.format("delta").mode("overwrite").saveAsTable("test_schema.users")
        mock_result = mock_spark.table("test_schema.users")
        mock_count = mock_result.count()
        mock_rows = sorted(mock_result.collect(), key=lambda r: r["id"])
        
        # Compare
        assert mock_count == real_count
        assert len(mock_rows) == len(real_rows)
        for mock_row, real_row in zip(mock_rows, real_rows):
            assert mock_row["id"] == real_row["id"]
            assert mock_row["name"] == real_row["name"]

    def test_delta_write_modes_compatibility(self, real_spark, mock_spark):
        """Test Delta write modes match real PySpark behavior."""
        # Create schemas
        real_spark.sql("CREATE SCHEMA IF NOT EXISTS test")
        mock_spark.sql("CREATE SCHEMA IF NOT EXISTS test")
        
        # Test overwrite mode
        df1 = [{"id": 1, "value": "first"}]
        real_spark.createDataFrame(df1).write.format("delta").mode("overwrite").saveAsTable("test.modes")
        mock_spark.createDataFrame(df1).write.format("delta").mode("overwrite").saveAsTable("test.modes")
        
        df2 = [{"id": 2, "value": "second"}]
        real_spark.createDataFrame(df2).write.format("delta").mode("overwrite").saveAsTable("test.modes")
        mock_spark.createDataFrame(df2).write.format("delta").mode("overwrite").saveAsTable("test.modes")
        
        real_count = real_spark.table("test.modes").count()
        mock_count = mock_spark.table("test.modes").count()
        assert mock_count == real_count == 1

    def test_delta_write_append_compatibility(self, real_spark, mock_spark):
        """Test Delta append mode compatibility."""
        # Create schemas
        real_spark.sql("CREATE SCHEMA IF NOT EXISTS test")
        mock_spark.sql("CREATE SCHEMA IF NOT EXISTS test")
        
        df1 = [{"id": 1}]
        df2 = [{"id": 2}]
        
        # Real
        real_spark.createDataFrame(df1).write.format("delta").mode("overwrite").saveAsTable("test.append")
        real_spark.createDataFrame(df2).write.format("delta").mode("append").saveAsTable("test.append")
        real_count = real_spark.table("test.append").count()
        
        # Mock
        mock_spark.createDataFrame(df1).write.format("delta").mode("overwrite").saveAsTable("test.append")
        mock_spark.createDataFrame(df2).write.format("delta").mode("append").saveAsTable("test.append")
        mock_count = mock_spark.table("test.append").count()
        
        assert mock_count == real_count == 2

    def test_delta_write_error_mode_raises(self, real_spark, mock_spark):
        """Test that error mode raises exception in both implementations."""
        # Create schemas
        real_spark.sql("CREATE SCHEMA IF NOT EXISTS test")
        mock_spark.sql("CREATE SCHEMA IF NOT EXISTS test")
        
        df1 = [{"id": 1}]
        df2 = [{"id": 2}]
        
        # Create table in both
        real_spark.createDataFrame(df1).write.format("delta").mode("overwrite").saveAsTable("test.error")
        mock_spark.createDataFrame(df1).write.format("delta").mode("overwrite").saveAsTable("test.error")
        
        # Both should raise on error mode
        with pytest.raises(Exception):  # Real raises AnalysisException
            real_spark.createDataFrame(df2).write.format("delta").mode("error").saveAsTable("test.error")
        
        with pytest.raises(Exception):  # Mock should raise same
            mock_spark.createDataFrame(df2).write.format("delta").mode("error").saveAsTable("test.error")

    def test_delta_write_ignore_mode_compatibility(self, real_spark, mock_spark):
        """Test ignore mode leaves table unchanged in both implementations."""
        # Create schemas
        real_spark.sql("CREATE SCHEMA IF NOT EXISTS test")
        mock_spark.sql("CREATE SCHEMA IF NOT EXISTS test")
        
        df1 = [{"id": 1, "value": "original"}]
        df2 = [{"id": 2, "value": "should be ignored"}]
        
        # Real
        real_spark.createDataFrame(df1).write.format("delta").mode("overwrite").saveAsTable("test.ignore")
        real_spark.createDataFrame(df2).write.format("delta").mode("ignore").saveAsTable("test.ignore")
        real_count = real_spark.table("test.ignore").count()
        real_val = real_spark.table("test.ignore").collect()[0]["value"]
        
        # Mock
        mock_spark.createDataFrame(df1).write.format("delta").mode("overwrite").saveAsTable("test.ignore")
        mock_spark.createDataFrame(df2).write.format("delta").mode("ignore").saveAsTable("test.ignore")
        mock_count = mock_spark.table("test.ignore").count()
        mock_val = mock_spark.table("test.ignore").collect()[0]["value"]
        
        assert mock_count == real_count == 1
        assert mock_val == real_val == "original"

