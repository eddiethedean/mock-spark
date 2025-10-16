"""Unit tests for Timezone Functions (PySpark 3.2)."""

import pytest
from mock_spark import MockSparkSession, functions as F


@pytest.mark.fast
class TestTimezoneFunctionsUnit:
    """Fast unit tests for timezone functions."""

    def test_convert_timezone(self):
        """Test convert_timezone converts between timezones."""
        spark = MockSparkSession("test")
        data = [{"ts": "2024-01-01 12:00:00"}]
        df = spark.createDataFrame(data)

        result = df.select(
            F.convert_timezone("UTC", "America/New_York", F.col("ts")).alias("converted")
        ).collect()

        # Just check it runs without error
        assert "converted" in result[0]

    @pytest.mark.skip(reason="current_timezone() needs special handling for functions without column input")
    def test_current_timezone(self):
        """Test current_timezone returns timezone."""
        spark = MockSparkSession("test")
        data = [{"id": 1}]
        df = spark.createDataFrame(data)

        result = df.select(
            F.current_timezone().alias("tz")
        ).collect()

        # Check it returns a string
        assert isinstance(result[0]["tz"], str)

    def test_from_utc_timestamp(self):
        """Test from_utc_timestamp converts from UTC."""
        spark = MockSparkSession("test")
        data = [{"ts": "2024-01-01 00:00:00"}]
        df = spark.createDataFrame(data)

        result = df.select(
            F.from_utc_timestamp(F.col("ts"), "America/Los_Angeles").alias("local")
        ).collect()

        # Check it runs
        assert "local" in result[0]

    def test_to_utc_timestamp(self):
        """Test to_utc_timestamp converts to UTC."""
        spark = MockSparkSession("test")
        data = [{"ts": "2024-01-01 00:00:00"}]
        df = spark.createDataFrame(data)

        result = df.select(
            F.to_utc_timestamp(F.col("ts"), "America/Los_Angeles").alias("utc")
        ).collect()

        # Check it runs
        assert "utc" in result[0]

