"""Unit tests for PySpark 3.0 math functions."""

import math
from mock_spark import MockSparkSession, F


class TestTrigFunctions:
    """Test trigonometric functions from PySpark 3.0."""

    def test_atan2_quadrants(self):
        """Test atan2 in all four quadrants."""
        spark = MockSparkSession("test")
        df = spark.createDataFrame(
            [
                {"y": 1.0, "x": 1.0},  # Quadrant 1: π/4
                {"y": 1.0, "x": -1.0},  # Quadrant 2: 3π/4
                {"y": -1.0, "x": -1.0},  # Quadrant 3: -3π/4
                {"y": -1.0, "x": 1.0},  # Quadrant 4: -π/4
            ]
        )

        result = df.select(F.atan2(F.col("y"), F.col("x")).alias("angle")).collect()

        # Verify angles in correct quadrants
        assert abs(result[0]["angle"] - math.pi / 4) < 0.001
        assert abs(result[1]["angle"] - 3 * math.pi / 4) < 0.001
        assert abs(result[2]["angle"] - (-3 * math.pi / 4)) < 0.001
        assert abs(result[3]["angle"] - (-math.pi / 4)) < 0.001

    def test_atan2_special_cases(self):
        """Test atan2 with special cases."""
        spark = MockSparkSession("test")
        df = spark.createDataFrame(
            [
                {"y": 0.0, "x": 1.0},  # 0 radians
                {"y": 1.0, "x": 0.0},  # π/2 radians
                {"y": 0.0, "x": -1.0},  # π radians
                {"y": -1.0, "x": 0.0},  # -π/2 radians
            ]
        )

        result = df.select(F.atan2(F.col("y"), F.col("x")).alias("angle")).collect()

        assert abs(result[0]["angle"] - 0.0) < 0.001
        assert abs(result[1]["angle"] - math.pi / 2) < 0.001
        assert abs(result[2]["angle"] - math.pi) < 0.001
        assert abs(result[3]["angle"] - (-math.pi / 2)) < 0.001


class TestLogFunctions:
    """Test logarithmic functions from PySpark 3.0."""

    def test_log10_basic(self):
        """Test log10 with powers of 10."""
        spark = MockSparkSession("test")
        df = spark.createDataFrame(
            [{"value": 1.0}, {"value": 10.0}, {"value": 100.0}, {"value": 1000.0}]
        )

        result = df.select(F.log10(F.col("value")).alias("log10")).collect()

        # log10(1) = 0, log10(10) = 1, log10(100) = 2, log10(1000) = 3
        assert abs(result[0]["log10"] - 0.0) < 0.001
        assert abs(result[1]["log10"] - 1.0) < 0.001
        assert abs(result[2]["log10"] - 2.0) < 0.001
        assert abs(result[3]["log10"] - 3.0) < 0.001

    def test_log2_basic(self):
        """Test log2 with powers of 2."""
        spark = MockSparkSession("test")
        df = spark.createDataFrame(
            [
                {"value": 1.0},
                {"value": 2.0},
                {"value": 4.0},
                {"value": 8.0},
                {"value": 16.0},
            ]
        )

        result = df.select(F.log2(F.col("value")).alias("log2")).collect()

        # log2(1) = 0, log2(2) = 1, log2(4) = 2, log2(8) = 3, log2(16) = 4
        assert abs(result[0]["log2"] - 0.0) < 0.001
        assert abs(result[1]["log2"] - 1.0) < 0.001
        assert abs(result[2]["log2"] - 2.0) < 0.001
        assert abs(result[3]["log2"] - 3.0) < 0.001
        assert abs(result[4]["log2"] - 4.0) < 0.001

    def test_log1p_basic(self):
        """Test log1p for small values."""
        spark = MockSparkSession("test")
        df = spark.createDataFrame(
            [{"value": 0.0}, {"value": 0.01}, {"value": 0.1}, {"value": 1.0}]
        )

        result = df.select(F.log1p(F.col("value")).alias("log1p")).collect()

        # log1p(0) = ln(1) = 0
        # log1p(0.01) = ln(1.01)
        # log1p(1) = ln(2)
        assert abs(result[0]["log1p"] - 0.0) < 0.001
        assert abs(result[1]["log1p"] - math.log(1.01)) < 0.001
        assert abs(result[2]["log1p"] - math.log(1.1)) < 0.001
        assert abs(result[3]["log1p"] - math.log(2.0)) < 0.001

    def test_expm1_basic(self):
        """Test expm1 for small values."""
        spark = MockSparkSession("test")
        df = spark.createDataFrame(
            [{"value": 0.0}, {"value": 0.01}, {"value": 0.1}, {"value": 1.0}]
        )

        result = df.select(F.expm1(F.col("value")).alias("expm1")).collect()

        # expm1(0) = exp(0) - 1 = 0
        # expm1(1) = exp(1) - 1 = e - 1
        assert abs(result[0]["expm1"] - 0.0) < 0.001
        assert abs(result[1]["expm1"] - (math.exp(0.01) - 1)) < 0.001
        assert abs(result[2]["expm1"] - (math.exp(0.1) - 1)) < 0.001
        assert abs(result[3]["expm1"] - (math.e - 1)) < 0.001

    def test_log_functions_with_nulls(self):
        """Test log functions handle nulls properly."""
        spark = MockSparkSession("test")
        df = spark.createDataFrame([{"value": 10.0}, {"value": None}])

        result = df.select(
            F.log10(F.col("value")).alias("log10"), F.log2(F.col("value")).alias("log2")
        ).collect()

        # First row should have values
        assert result[0]["log10"] is not None
        assert result[0]["log2"] is not None

        # Second row should be None
        assert result[1]["log10"] is None
        assert result[1]["log2"] is None
