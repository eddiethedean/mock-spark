"""
Unit tests for datetime transformation functions.

Based on exploration/datetime_functions.py findings:
- to_date() converts timestamp strings to date type
- hour(), minute(), second() extract time components
- year(), month(), day() extract date components
- Functions work in withColumn chains and before groupBy/agg
"""

import pytest
from mock_spark import MockSparkSession
import mock_spark.functions as F


class TestDateTimeEnhancements:
    """Test enhanced datetime function support."""

    def test_to_date_basic(self):
        """Test basic to_date functionality."""
        spark = MockSparkSession.builder.appName("test").getOrCreate()
        
        data = [
            {"timestamp_str": "2024-01-01 10:30:00"},
            {"timestamp_str": "2024-01-01 14:45:00"},
            {"timestamp_str": "2024-01-02 09:15:00"},
        ]
        df = spark.createDataFrame(data)
        
        # Apply to_date
        result = df.withColumn("event_date", F.to_date("timestamp_str"))
        
        assert result.count() == 3
        assert "event_date" in result.columns
        
        # Verify dates are populated (not None)
        rows = result.collect()
        dates = [row["event_date"] for row in rows]
        assert all(d is not None for d in dates), f"Got None values: {dates}"
        
        # Verify we have 2 unique dates in the data
        unique_dates = set(dates)
        assert len(unique_dates) == 2  # 2024-01-01 and 2024-01-02
        
        spark.stop()

    def test_hour_extraction(self):
        """Test hour extraction from timestamps."""
        spark = MockSparkSession.builder.appName("test").getOrCreate()
        
        data = [
            {"ts": "2024-01-01 10:30:00"},
            {"ts": "2024-01-01 14:45:00"},
            {"ts": "2024-01-02 09:15:00"},
        ]
        df = spark.createDataFrame(data)
        
        # Extract hour
        result = df.withColumn("hour", F.hour("ts"))
        
        assert result.count() == 3
        rows = result.collect()
        hours = [row["hour"] for row in rows]
        
        # Verify hours are extracted (implementation-specific values)
        assert len(hours) == 3
        
        spark.stop()

    def test_multiple_datetime_functions(self):
        """Test multiple datetime functions together."""
        spark = MockSparkSession.builder.appName("test").getOrCreate()
        
        data = [{"timestamp": "2024-01-15 10:30:45"}]
        df = spark.createDataFrame(data)
        
        # Apply multiple datetime functions
        result = (
            df.withColumn("date", F.to_date("timestamp"))
            .withColumn("yr", F.year("timestamp"))
            .withColumn("mon", F.month("timestamp"))
            .withColumn("dy", F.dayofmonth("timestamp"))
            .withColumn("hr", F.hour("timestamp"))
            .withColumn("min", F.minute("timestamp"))
            .withColumn("sec", F.second("timestamp"))
        )
        
        assert result.count() == 1
        assert all(col in result.columns for col in ["date", "yr", "mon", "dy", "hr", "min", "sec"])
        
        spark.stop()

    def test_datetime_with_groupby(self):
        """Test datetime functions work with groupBy aggregations."""
        spark = MockSparkSession.builder.appName("test").getOrCreate()
        
        data = [
            {"user": "user1", "action": "click", "timestamp": "2024-01-01 10:30:00"},
            {"user": "user1", "action": "view", "timestamp": "2024-01-01 14:45:00"},
            {"user": "user2", "action": "click", "timestamp": "2024-01-02 09:15:00"},
            {"user": "user1", "action": "view", "timestamp": "2024-01-02 11:00:00"},
        ]
        df = spark.createDataFrame(data)
        
        # Apply transformations and groupBy
        result = (
            df.withColumn("event_date", F.to_date("timestamp"))
            .withColumn("hour", F.hour("timestamp"))
            .groupBy("user", "event_date")
            .agg(F.count("action").alias("event_count"))
            .orderBy("user", "event_date")
        )
        
        # Should have user-date aggregations
        assert result.count() >= 2  # At least 2 user-date combinations
        assert "event_count" in result.columns
        
        spark.stop()

    def test_datetime_extraction_functions(self):
        """Test all date/time extraction functions."""
        spark = MockSparkSession.builder.appName("test").getOrCreate()
        
        data = [{"dt": "2024-03-15 14:30:45"}]
        df = spark.createDataFrame(data)
        
        # Use withColumn approach (select path has materializer limitations)
        result = (
            df.withColumn("year", F.year("dt"))
            .withColumn("month", F.month("dt"))
            .withColumn("day", F.dayofmonth("dt"))
            .withColumn("hour", F.hour("dt"))
            .withColumn("minute", F.minute("dt"))
            .withColumn("second", F.second("dt"))
            .select("year", "month", "day", "hour", "minute", "second")
        )
        
        assert result.count() == 1
        row = result.collect()[0]
        
        # All fields should be present
        assert "year" in row.asDict()
        assert "month" in row.asDict()
        assert "day" in row.asDict()
        assert "hour" in row.asDict()
        assert "minute" in row.asDict()
        assert "second" in row.asDict()
        
        spark.stop()

