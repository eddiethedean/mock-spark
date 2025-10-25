"""Unit tests for utility functions from PySpark 3.0."""

import pytest
from mock_spark import MockSparkSession, F


class TestUtilityFunctions3_0:
    """Test utility functions introduced in PySpark 3.0."""

    def setup_method(self):
        """Setup test session."""
        self.spark = MockSparkSession("test")

    def teardown_method(self):
        """Cleanup session."""
        self.spark.stop()

    @pytest.mark.skip(
        reason="overlay requires complex SQL syntax handling - implement in future release"
    )
    def test_overlay_basic(self):
        """Test overlay - basic string replacement."""
        data = [{"id": 1, "text": "Hello World"}, {"id": 2, "text": "PySpark is great"}]
        df = self.spark.createDataFrame(data)

        # Replace starting at position 7 for 5 characters
        result = df.select(
            F.col("id"),
            F.overlay(F.col("text"), F.lit("Friend"), F.lit(7), F.lit(5)).alias(
                "replaced"
            ),
        ).collect()

        assert result[0]["replaced"] == "Hello Friend"
        assert result[1]["replaced"] == "PySparkFriend great"

    @pytest.mark.skip(
        reason="overlay requires complex SQL syntax handling - implement in future release"
    )
    def test_overlay_no_length(self):
        """Test overlay without specifying length (replaces to end)."""
        data = [{"id": 1, "text": "Hello World"}]
        df = self.spark.createDataFrame(data)

        result = df.select(
            F.overlay(F.col("text"), F.lit("Friend"), F.lit(7)).alias("replaced")
        ).collect()

        assert result[0]["replaced"] == "Hello Friend"

    def test_make_date(self):
        """Test make_date - construct date from year, month, day."""
        data = [
            {"id": 1, "year": 2024, "month": 3, "day": 15},
            {"id": 2, "year": 2023, "month": 12, "day": 31},
        ]
        df = self.spark.createDataFrame(data)

        result = df.select(
            F.col("id"),
            F.make_date(F.col("year"), F.col("month"), F.col("day")).alias("date"),
        ).collect()

        assert str(result[0]["date"]) == "2024-03-15"
        assert str(result[1]["date"]) == "2023-12-31"

    def test_version(self):
        """Test version - returns Spark/mock-spark version."""
        df = self.spark.createDataFrame([{"id": 1}])

        result = df.select(F.version().alias("version")).collect()

        # Should return a version string
        assert result[0]["version"] is not None
        assert isinstance(result[0]["version"], str)
        assert len(result[0]["version"]) > 0
