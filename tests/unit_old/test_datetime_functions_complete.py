"""
Unit tests for comprehensive datetime functions.

Tests all datetime functions including edge cases, format strings,
and date arithmetic operations.
"""

import pytest
from mock_spark import MockSparkSession, F


@pytest.mark.fast
class TestDateTimeFunctionsComplete:
    """Test comprehensive datetime functions with edge cases."""

    @pytest.fixture
    def spark(self):
        """Create a MockSparkSession for testing."""
        return MockSparkSession("test")

    @pytest.fixture
    def datetime_data(self):
        """Sample datetime data for testing."""
        return [
            {
                "timestamp": "2024-01-15 10:30:00",
                "date_str": "2024-01-15",
                "timestamp_str": "2024-01-15T10:30:00",
                "custom_format": "15/01/2024 10:30",
                "epoch_seconds": 1705312200,
                "name": "Alice",
            },
            {
                "timestamp": "2024-01-16 14:45:30",
                "date_str": "2024-01-16",
                "timestamp_str": "2024-01-16T14:45:30",
                "custom_format": "16/01/2024 14:45",
                "epoch_seconds": 1705403130,
                "name": "Bob",
            },
            {
                "timestamp": "2024-01-17 09:15:45",
                "date_str": "2024-01-17",
                "timestamp_str": "2024-01-17T09:15:45",
                "custom_format": "17/01/2024 09:15",
                "epoch_seconds": 1705491345,
                "name": "Charlie",
            },
        ]

    def test_hour_function(self, spark, datetime_data):
        """Test hour() function returns 0-23."""
        df = spark.createDataFrame(datetime_data)

        result = df.select(
            F.hour(F.col("timestamp")).alias("hour"),
            F.hour(df.timestamp).alias("hour_df"),
        )

        rows = result.collect()
        assert len(rows) == 3

        # Check that hours are in range 0-23
        for row in rows:
            hour_val = row["hour"]
            assert isinstance(hour_val, (int, float))
            assert 0 <= hour_val <= 23

    def test_dayofweek_function(self, spark, datetime_data):
        """Test dayofweek() function returns 1-7 (Sunday=1)."""
        df = spark.createDataFrame(datetime_data)

        result = df.select(
            F.dayofweek(F.col("timestamp")).alias("dayofweek"),
            F.dayofweek(df.timestamp).alias("dayofweek_df"),
        )

        rows = result.collect()
        assert len(rows) == 3

        # Check that dayofweek values are in range 1-7
        for row in rows:
            dow_val = row["dayofweek"]
            assert isinstance(dow_val, (int, float))
            assert 1 <= dow_val <= 7

    def test_to_timestamp_with_format(self, spark, datetime_data):
        """Test to_timestamp() with format strings."""
        df = spark.createDataFrame(datetime_data)

        # Test with custom format
        result = df.select(
            F.to_timestamp(F.col("custom_format"), "dd/MM/yyyy HH:mm").alias(
                "parsed_timestamp"
            )
        )

        rows = result.collect()
        assert len(rows) == 3

        # All should be valid timestamps
        for row in rows:
            timestamp_val = row["parsed_timestamp"]
            assert timestamp_val is not None

    def test_to_timestamp_auto_parse(self, spark, datetime_data):
        """Test to_timestamp() without format (auto parsing)."""
        df = spark.createDataFrame(datetime_data)

        # Test auto parsing
        result = df.select(F.to_timestamp(F.col("timestamp_str")).alias("auto_parsed"))

        rows = result.collect()
        assert len(rows) == 3

        # All should be valid timestamps
        for row in rows:
            timestamp_val = row["auto_parsed"]
            assert timestamp_val is not None

    def test_date_arithmetic_epoch_conversion(self, spark, datetime_data):
        """Test date arithmetic with epoch conversion."""
        df = spark.createDataFrame(datetime_data)

        # Test date.cast("long") for epoch conversion
        result = df.select(
            F.col("timestamp").cast("long").alias("epoch_long"),
            df.timestamp.cast("long").alias("epoch_long_df"),
        )

        rows = result.collect()
        assert len(rows) == 3

        # Check that epoch values are reasonable (around 2024)
        for row in rows:
            epoch_val = row["epoch_long"]
            assert isinstance(epoch_val, (int, float))
            # Should be around 2024 (epoch ~1.7 billion)
            assert 1_700_000_000 <= epoch_val <= 2_000_000_000

    def test_date_subtraction_arithmetic(self, spark, datetime_data):
        """Test date subtraction for day differences."""
        df = spark.createDataFrame(datetime_data)

        # Test date subtraction using epoch conversion
        result = df.select(
            (F.col("timestamp").cast("long") - F.lit(1705300000).cast("long")).alias(
                "days_diff"
            ),
            (df.timestamp.cast("long") - F.lit(1705300000).cast("long")).alias(
                "days_diff_df"
            ),
        )

        rows = result.collect()
        assert len(rows) == 3

        # All should have positive differences
        for row in rows:
            diff_val = row["days_diff"]
            assert isinstance(diff_val, (int, float))
            assert diff_val > 0

    def test_current_date_and_timestamp(self, spark, datetime_data):
        """Test current_date() and current_timestamp() functions."""
        df = spark.createDataFrame(datetime_data)

        result = df.select(
            F.current_date().alias("current_date"),
            F.current_timestamp().alias("current_timestamp"),
        )

        rows = result.collect()
        assert len(rows) == 3

        # All rows should have the same current date/timestamp
        for row in rows:
            current_date = row["current_date"]
            current_timestamp = row["current_timestamp"]
            assert current_date is not None
            assert current_timestamp is not None

    def test_date_extraction_functions(self, spark, datetime_data):
        """Test all date extraction functions."""
        df = spark.createDataFrame(datetime_data)

        result = df.select(
            F.year(F.col("timestamp")).alias("year"),
            F.month(F.col("timestamp")).alias("month"),
            F.day(F.col("timestamp")).alias("day"),
            F.hour(F.col("timestamp")).alias("hour"),
            F.minute(F.col("timestamp")).alias("minute"),
            F.second(F.col("timestamp")).alias("second"),
            F.dayofweek(F.col("timestamp")).alias("dayofweek"),
            F.dayofyear(F.col("timestamp")).alias("dayofyear"),
            F.quarter(F.col("timestamp")).alias("quarter"),
        )

        rows = result.collect()
        assert len(rows) == 3

        for row in rows:
            # Check year
            assert row["year"] == 2024

            # Check month
            assert 1 <= row["month"] <= 12

            # Check day
            assert 15 <= row["day"] <= 17

            # Check hour
            assert 0 <= row["hour"] <= 23

            # Check minute
            assert 0 <= row["minute"] <= 59

            # Check second
            assert 0 <= row["second"] <= 59

            # Check dayofweek (1-7, Sunday=1)
            assert 1 <= row["dayofweek"] <= 7

            # Check dayofyear
            assert 1 <= row["dayofyear"] <= 366

            # Check quarter
            assert 1 <= row["quarter"] <= 4

    def test_date_manipulation_functions(self, spark, datetime_data):
        """Test date manipulation functions."""
        df = spark.createDataFrame(datetime_data)

        result = df.select(
            F.date_add(F.col("timestamp"), 7).alias("date_plus_7"),
            F.date_sub(F.col("timestamp"), 7).alias("date_minus_7"),
            F.add_months(F.col("timestamp"), 1).alias("add_one_month"),
        )

        rows = result.collect()
        assert len(rows) == 3

        # All should be valid dates
        for row in rows:
            assert row["date_plus_7"] is not None
            assert row["date_minus_7"] is not None
            assert row["add_one_month"] is not None

    def test_date_formatting(self, spark, datetime_data):
        """Test date formatting functions."""
        df = spark.createDataFrame(datetime_data)

        result = df.select(
            F.date_format(F.col("timestamp"), "yyyy-MM-dd").alias("formatted_date"),
            F.date_format(F.col("timestamp"), "HH:mm:ss").alias("formatted_time"),
        )

        rows = result.collect()
        assert len(rows) == 3

        # All should be formatted strings
        for row in rows:
            assert isinstance(row["formatted_date"], str)
            assert isinstance(row["formatted_time"], str)

    def test_unix_timestamp_functions(self, spark, datetime_data):
        """Test unix timestamp functions."""
        df = spark.createDataFrame(datetime_data)

        result = df.select(
            F.unix_timestamp(F.col("timestamp")).alias("unix_ts"),
            F.from_unixtime(F.col("epoch_seconds")).alias("from_unix"),
        )

        rows = result.collect()
        assert len(rows) == 3

        # All should be valid
        for row in rows:
            assert row["unix_ts"] is not None
            assert row["from_unix"] is not None

    def test_date_truncation(self, spark, datetime_data):
        """Test date truncation functions."""
        df = spark.createDataFrame(datetime_data)

        result = df.select(
            F.date_trunc("day", F.col("timestamp")).alias("trunc_day"),
            F.date_trunc("hour", F.col("timestamp")).alias("trunc_hour"),
            F.trunc(F.col("timestamp"), "month").alias("trunc_month"),
        )

        rows = result.collect()
        assert len(rows) == 3

        # All should be valid truncated dates
        for row in rows:
            assert row["trunc_day"] is not None
            assert row["trunc_hour"] is not None
            assert row["trunc_month"] is not None

    def test_date_difference_functions(self, spark, datetime_data):
        """Test date difference functions."""
        df = spark.createDataFrame(datetime_data)

        # Create a reference date
        reference_date = "2024-01-01"

        result = df.select(
            F.datediff(F.col("timestamp"), F.lit(reference_date)).alias("days_diff"),
            F.months_between(F.col("timestamp"), F.lit(reference_date)).alias(
                "months_diff"
            ),
        )

        rows = result.collect()
        assert len(rows) == 3

        # All should have positive day differences, months_diff should be 0 (same month)
        for row in rows:
            assert row["days_diff"] > 0
            assert (
                row["months_diff"] >= 0
            )  # Should be 0 since all dates are in January 2024

    def test_complex_datetime_expressions(self, spark, datetime_data):
        """Test complex datetime expressions."""
        df = spark.createDataFrame(datetime_data)

        result = df.select(
            F.col("timestamp"),
            F.hour(F.col("timestamp")).alias("hour"),
            F.dayofweek(F.col("timestamp")).alias("dayofweek"),
            (F.col("timestamp").cast("long") - F.lit(1705300000).cast("long")).alias(
                "seconds_since_ref"
            ),
            F.when(F.hour(F.col("timestamp")) >= 12, "PM")
            .otherwise("AM")
            .alias("time_period"),
        )

        rows = result.collect()
        assert len(rows) == 3

        # Check time period logic
        for row in rows:
            hour = row["hour"]
            time_period = row["time_period"]
            if hour >= 12:
                assert time_period == "PM"
            else:
                assert time_period == "AM"

    def test_datetime_with_aggregation(self, spark, datetime_data):
        """Test datetime functions with aggregation."""
        df = spark.createDataFrame(datetime_data)

        result = df.agg(
            F.min(F.col("timestamp")).alias("min_timestamp"),
            F.max(F.col("timestamp")).alias("max_timestamp"),
            F.avg(F.hour(F.col("timestamp"))).alias("avg_hour"),
        )

        rows = result.collect()
        assert len(rows) == 1

        row = rows[0]
        assert row["min_timestamp"] is not None
        assert row["max_timestamp"] is not None
        assert row["avg_hour"] is not None
        assert 0 <= row["avg_hour"] <= 23

    def test_datetime_with_groupby(self, spark, datetime_data):
        """Test datetime functions with groupBy."""
        df = spark.createDataFrame(datetime_data)

        # Add a grouping column
        df_with_group = df.withColumn("group", F.lit("test_group"))

        result = df_with_group.groupBy("group").agg(
            F.count(F.col("timestamp")).alias("count"),
            F.min(F.hour(F.col("timestamp"))).alias("min_hour"),
            F.max(F.hour(F.col("timestamp"))).alias("max_hour"),
        )

        rows = result.collect()
        assert len(rows) == 1

        row = rows[0]
        assert row["count"] == 3
        assert 0 <= row["min_hour"] <= 23
        assert 0 <= row["max_hour"] <= 23
        assert row["min_hour"] <= row["max_hour"]

    def test_datetime_edge_cases(self, spark):
        """Test datetime functions with edge cases."""
        edge_case_data = [
            {"timestamp": "2024-02-29 00:00:00", "name": "Leap Year"},  # Leap year
            {"timestamp": "2024-12-31 23:59:59", "name": "Year End"},  # Year end
            {"timestamp": "2024-01-01 00:00:00", "name": "Year Start"},  # Year start
        ]

        df = spark.createDataFrame(edge_case_data)

        result = df.select(
            F.year(F.col("timestamp")).alias("year"),
            F.month(F.col("timestamp")).alias("month"),
            F.day(F.col("timestamp")).alias("day"),
            F.dayofyear(F.col("timestamp")).alias("dayofyear"),
            F.quarter(F.col("timestamp")).alias("quarter"),
        )

        rows = result.collect()
        assert len(rows) == 3

        # Check leap year handling
        leap_year_row = rows[0]
        assert leap_year_row["year"] == 2024
        assert leap_year_row["month"] == 2
        assert leap_year_row["day"] == 29

        # Check year end
        year_end_row = rows[1]
        assert year_end_row["month"] == 12
        assert year_end_row["day"] == 31
        assert year_end_row["quarter"] == 4

        # Check year start
        year_start_row = rows[2]
        assert year_start_row["month"] == 1
        assert year_start_row["day"] == 1
        assert year_start_row["quarter"] == 1
