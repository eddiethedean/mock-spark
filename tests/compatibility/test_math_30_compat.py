"""
Compatibility tests for PySpark 3.0 math functions.

Tests that new math functions work identically in mock-spark and real PySpark 3.0.
"""

import pytest

# Check if PySpark is available
try:
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as F_real

    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

from mock_spark import MockSparkSession
import mock_spark.functions as F_mock


@pytest.fixture(scope="module")
def real_spark():
    """Create a real PySpark session."""
    if not PYSPARK_AVAILABLE:
        pytest.skip("PySpark not available")

    spark = (
        SparkSession.builder.appName("test_math_30").master("local[1]").getOrCreate()
    )

    yield spark
    spark.stop()


@pytest.fixture(scope="module")
def mock_spark():
    """Create a mock Spark session."""
    return MockSparkSession("test_app")


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not available")
class TestAtan2Compat:
    """Test atan2 function compatibility."""

    def test_atan2_all_quadrants(self, real_spark, mock_spark):
        """Test atan2 produces identical results in all quadrants."""
        data = [
            {"y": 1.0, "x": 1.0},
            {"y": 1.0, "x": -1.0},
            {"y": -1.0, "x": -1.0},
            {"y": -1.0, "x": 1.0},
        ]

        real_df = real_spark.createDataFrame(data)
        real_result = real_df.select(
            F_real.atan2(F_real.col("y"), F_real.col("x")).alias("angle")
        ).collect()

        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.select(
            F_mock.atan2(F_mock.col("y"), F_mock.col("x")).alias("angle")
        ).collect()

        for i in range(len(data)):
            assert abs(real_result[i]["angle"] - mock_result[i]["angle"]) < 0.0001


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not available")
class TestLog10Compat:
    """Test log10 function compatibility."""

    def test_log10_powers_of_10(self, real_spark, mock_spark):
        """Test log10 with powers of 10."""
        data = [{"value": 1.0}, {"value": 10.0}, {"value": 100.0}, {"value": 1000.0}]

        real_df = real_spark.createDataFrame(data)
        real_result = real_df.select(
            F_real.log10(F_real.col("value")).alias("log10")
        ).collect()

        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.select(
            F_mock.log10(F_mock.col("value")).alias("log10")
        ).collect()

        for i in range(len(data)):
            assert abs(real_result[i]["log10"] - mock_result[i]["log10"]) < 0.0001

    def test_log10_with_nulls(self, real_spark, mock_spark):
        """Test log10 handles nulls identically."""
        data = [{"value": 100.0}, {"value": None}]

        real_df = real_spark.createDataFrame(data)
        real_result = real_df.select(
            F_real.log10(F_real.col("value")).alias("log10")
        ).collect()

        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.select(
            F_mock.log10(F_mock.col("value")).alias("log10")
        ).collect()

        assert real_result[0]["log10"] is not None
        assert mock_result[0]["log10"] is not None
        assert abs(real_result[0]["log10"] - mock_result[0]["log10"]) < 0.0001

        assert real_result[1]["log10"] is None
        assert mock_result[1]["log10"] is None


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not available")
class TestLog2Compat:
    """Test log2 function compatibility."""

    def test_log2_powers_of_2(self, real_spark, mock_spark):
        """Test log2 with powers of 2."""
        data = [
            {"value": 1.0},
            {"value": 2.0},
            {"value": 4.0},
            {"value": 8.0},
            {"value": 16.0},
        ]

        real_df = real_spark.createDataFrame(data)
        real_result = real_df.select(
            F_real.log2(F_real.col("value")).alias("log2")
        ).collect()

        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.select(
            F_mock.log2(F_mock.col("value")).alias("log2")
        ).collect()

        for i in range(len(data)):
            assert abs(real_result[i]["log2"] - mock_result[i]["log2"]) < 0.0001


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not available")
class TestLog1pCompat:
    """Test log1p function compatibility."""

    def test_log1p_small_values(self, real_spark, mock_spark):
        """Test log1p for numerical stability with small values."""
        data = [
            {"value": 0.0},
            {"value": 0.001},
            {"value": 0.01},
            {"value": 0.1},
            {"value": 1.0},
        ]

        real_df = real_spark.createDataFrame(data)
        real_result = real_df.select(
            F_real.log1p(F_real.col("value")).alias("log1p")
        ).collect()

        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.select(
            F_mock.log1p(F_mock.col("value")).alias("log1p")
        ).collect()

        for i in range(len(data)):
            # Use higher precision tolerance for very small values
            assert abs(real_result[i]["log1p"] - mock_result[i]["log1p"]) < 0.0001


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not available")
class TestExpm1Compat:
    """Test expm1 function compatibility."""

    def test_expm1_small_values(self, real_spark, mock_spark):
        """Test expm1 for numerical stability with small values."""
        data = [
            {"value": 0.0},
            {"value": 0.001},
            {"value": 0.01},
            {"value": 0.1},
            {"value": 1.0},
        ]

        real_df = real_spark.createDataFrame(data)
        real_result = real_df.select(
            F_real.expm1(F_real.col("value")).alias("expm1")
        ).collect()

        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.select(
            F_mock.expm1(F_mock.col("value")).alias("expm1")
        ).collect()

        for i in range(len(data)):
            # Use relative tolerance for exponential values
            real_val = real_result[i]["expm1"]
            mock_val = mock_result[i]["expm1"]
            if real_val == 0:
                assert abs(mock_val) < 0.0001
            else:
                assert abs((real_val - mock_val) / real_val) < 0.001
