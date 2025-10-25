"""Unit tests for hyperbolic math functions (PySpark 3.0)."""

from mock_spark import MockSparkSession, F
import math


class TestHyperbolicFunctions:
    """Test hyperbolic math functions introduced in PySpark 3.0."""

    def setup_method(self):
        """Setup test session."""
        self.spark = MockSparkSession("test")

    def teardown_method(self):
        """Cleanup session."""
        self.spark.stop()

    def test_acosh(self):
        """Test acosh - inverse hyperbolic cosine."""
        data = [
            {"id": 1, "value": 1.5},
            {"id": 2, "value": 2.0},
            {"id": 3, "value": 3.0},
        ]
        df = self.spark.createDataFrame(data)

        result = df.select(
            F.col("id"), F.acosh(F.col("value")).alias("acosh_value")
        ).collect()

        # Verify against Python's math.acosh
        assert abs(result[0]["acosh_value"] - math.acosh(1.5)) < 0.0001
        assert abs(result[1]["acosh_value"] - math.acosh(2.0)) < 0.0001
        assert abs(result[2]["acosh_value"] - math.acosh(3.0)) < 0.0001

    def test_asinh(self):
        """Test asinh - inverse hyperbolic sine."""
        data = [
            {"id": 1, "value": 0.5},
            {"id": 2, "value": 1.0},
            {"id": 3, "value": 2.0},
        ]
        df = self.spark.createDataFrame(data)

        result = df.select(
            F.col("id"), F.asinh(F.col("value")).alias("asinh_value")
        ).collect()

        # Verify against Python's math.asinh
        assert abs(result[0]["asinh_value"] - math.asinh(0.5)) < 0.0001
        assert abs(result[1]["asinh_value"] - math.asinh(1.0)) < 0.0001
        assert abs(result[2]["asinh_value"] - math.asinh(2.0)) < 0.0001

    def test_atanh(self):
        """Test atanh - inverse hyperbolic tangent."""
        data = [
            {"id": 1, "value": 0.2},
            {"id": 2, "value": 0.5},
            {"id": 3, "value": 0.8},
        ]
        df = self.spark.createDataFrame(data)

        result = df.select(
            F.col("id"), F.atanh(F.col("value")).alias("atanh_value")
        ).collect()

        # Verify against Python's math.atanh
        assert abs(result[0]["atanh_value"] - math.atanh(0.2)) < 0.0001
        assert abs(result[1]["atanh_value"] - math.atanh(0.5)) < 0.0001
        assert abs(result[2]["atanh_value"] - math.atanh(0.8)) < 0.0001

    def test_hyperbolic_edge_cases(self):
        """Test hyperbolic functions with edge case values."""
        data = [
            {"id": 1, "acosh_val": 1.0, "asinh_val": 0.0, "atanh_val": 0.0},
            {"id": 2, "acosh_val": 10.0, "asinh_val": 5.0, "atanh_val": 0.99},
        ]
        df = self.spark.createDataFrame(data)

        result = df.select(
            F.acosh(F.col("acosh_val")).alias("acosh_result"),
            F.asinh(F.col("asinh_val")).alias("asinh_result"),
            F.atanh(F.col("atanh_val")).alias("atanh_result"),
        ).collect()

        # acosh(1.0) = 0, asinh(0.0) = 0, atanh(0.0) = 0
        assert abs(result[0]["acosh_result"] - 0.0) < 0.0001
        assert abs(result[0]["asinh_result"] - 0.0) < 0.0001
        assert abs(result[0]["atanh_result"] - 0.0) < 0.0001

        # Verify non-zero values work
        assert result[1]["acosh_result"] > 0
        assert result[1]["asinh_result"] > 0
        assert result[1]["atanh_result"] > 0
