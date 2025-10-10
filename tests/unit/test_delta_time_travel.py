"""
Unit tests for Delta Lake time travel.

Based on exploration/delta_time_travel.py findings:
- versionAsOf allows reading historical versions
- DESCRIBE HISTORY shows version history with timestamps
- Each write operation increments version number
- Versions start at 0 (initial write)
- Can query any historical version that exists
- Querying non-existent version raises error
"""

import pytest
from mock_spark import MockSparkSession
from mock_spark.errors import AnalysisException


class TestDeltaTimeTravel:
    """Test Delta Lake time travel functionality."""

    def test_version_as_of_basic(self):
        """Test reading specific version with versionAsOf."""
        spark = MockSparkSession.builder.appName("test").getOrCreate()
        
        # Version 0: Initial write
        df0 = spark.createDataFrame([{"id": 1, "value": "v0"}])
        df0.write.format("delta").saveAsTable("test.versioned")
        
        # Version 1: Overwrite
        df1 = spark.createDataFrame([{"id": 2, "value": "v1"}])
        df1.write.format("delta").mode("overwrite").saveAsTable("test.versioned")
        
        # Version 2: Another overwrite
        df2 = spark.createDataFrame([{"id": 3, "value": "v2"}])
        df2.write.format("delta").mode("overwrite").saveAsTable("test.versioned")
        
        # Read current version (should be v2)
        current = spark.table("test.versioned")
        assert current.count() == 1
        assert current.collect()[0]["value"] == "v2"
        
        # Read version 0
        v0 = spark.read.format("delta").option("versionAsOf", 0).table("test.versioned")
        assert v0.count() == 1
        assert v0.collect()[0]["value"] == "v0"
        
        # Read version 1
        v1 = spark.read.format("delta").option("versionAsOf", 1).table("test.versioned")
        assert v1.count() == 1
        assert v1.collect()[0]["value"] == "v1"
        
        spark.stop()

    def test_describe_history(self):
        """Test DESCRIBE HISTORY command."""
        spark = MockSparkSession.builder.appName("test").getOrCreate()
        
        # Create table with multiple versions
        df0 = spark.createDataFrame([{"id": 1}])
        df0.write.format("delta").saveAsTable("test.history")
        
        df1 = spark.createDataFrame([{"id": 2}])
        df1.write.format("delta").mode("append").saveAsTable("test.history")
        
        df2 = spark.createDataFrame([{"id": 3}])
        df2.write.format("delta").mode("overwrite").saveAsTable("test.history")
        
        # Get history
        history = spark.sql("DESCRIBE HISTORY test.history")
        
        # Should have 3 versions
        assert history.count() == 3
        
        # Check version numbers
        versions = [row["version"] for row in history.collect()]
        assert set(versions) == {0, 1, 2}
        
        # Check operations
        history_list = sorted(history.collect(), key=lambda r: r["version"])
        assert history_list[0]["operation"] in ["WRITE", "CREATE"]
        assert history_list[1]["operation"] == "APPEND"
        assert history_list[2]["operation"] == "OVERWRITE"
        
        spark.stop()

    def test_multiple_versions_with_append(self):
        """Test version tracking with append operations."""
        spark = MockSparkSession.builder.appName("test").getOrCreate()
        
        # Create and append multiple times
        df1 = spark.createDataFrame([{"id": 1}])
        df1.write.format("delta").saveAsTable("test.appends")
        
        df2 = spark.createDataFrame([{"id": 2}])
        df2.write.format("delta").mode("append").saveAsTable("test.appends")
        
        df3 = spark.createDataFrame([{"id": 3}])
        df3.write.format("delta").mode("append").saveAsTable("test.appends")
        
        # Current version should have 3 rows
        current = spark.table("test.appends")
        assert current.count() == 3
        
        # Version 0 should have 1 row
        v0 = spark.read.format("delta").option("versionAsOf", 0).table("test.appends")
        assert v0.count() == 1
        assert v0.collect()[0]["id"] == 1
        
        # Version 1 should have 2 rows
        v1 = spark.read.format("delta").option("versionAsOf", 1).table("test.appends")
        assert v1.count() == 2
        ids_v1 = {row["id"] for row in v1.collect()}
        assert ids_v1 == {1, 2}
        
        # Version 2 should have 3 rows
        v2 = spark.read.format("delta").option("versionAsOf", 2).table("test.appends")
        assert v2.count() == 3
        
        spark.stop()

    def test_version_not_exists_raises_error(self):
        """Test that querying non-existent version raises error."""
        spark = MockSparkSession.builder.appName("test").getOrCreate()
        
        # Create table with only version 0
        df = spark.createDataFrame([{"id": 1}])
        df.write.format("delta").saveAsTable("test.limited")
        
        # Try to read version 99 (doesn't exist)
        with pytest.raises(AnalysisException) as exc_info:
            spark.read.format("delta").option("versionAsOf", 99).table("test.limited").collect()
        
        assert "version" in str(exc_info.value).lower() or "99" in str(exc_info.value)
        
        spark.stop()

    def test_history_includes_timestamp(self):
        """Test that history includes timestamp information."""
        spark = MockSparkSession.builder.appName("test").getOrCreate()
        
        df = spark.createDataFrame([{"id": 1}])
        df.write.format("delta").saveAsTable("test.timestamps")
        
        history = spark.sql("DESCRIBE HISTORY test.timestamps")
        row = history.collect()[0]
        
        # Should have timestamp field
        assert "timestamp" in row.asDict()
        assert row["timestamp"] is not None
        
        spark.stop()

    def test_non_delta_table_time_travel_raises_error(self):
        """Test that versionAsOf on non-Delta table raises error."""
        spark = MockSparkSession.builder.appName("test").getOrCreate()
        
        # Create non-Delta table
        df = spark.createDataFrame([{"id": 1}])
        df.write.saveAsTable("test.regular")
        
        # Try to use versionAsOf on non-Delta table
        with pytest.raises(AnalysisException) as exc_info:
            spark.read.format("delta").option("versionAsOf", 0).table("test.regular").collect()
        
        assert "delta" in str(exc_info.value).lower() or "format" in str(exc_info.value).lower()
        
        spark.stop()

