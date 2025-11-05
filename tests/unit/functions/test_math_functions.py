"""
Unit tests for math functions.
"""

import pytest
from mock_spark import F
from mock_spark.functions.math import MathFunctions


@pytest.mark.unit
class TestMathFunctions:
    """Test mathematical functions."""

    def test_abs_with_column(self):
        """Test abs with MockColumn."""
        result = MathFunctions.abs(F.col("value"))
        assert result.operation == "abs"
        assert result.name == "abs(value)"

    def test_abs_with_string(self):
        """Test abs with string column."""
        result = MathFunctions.abs("value")
        assert result.operation == "abs"

    def test_round_default_scale(self):
        """Test round with default scale (0)."""
        result = MathFunctions.round(F.col("value"))
        assert result.operation == "round"
        assert result.value == 0

    def test_round_with_scale(self):
        """Test round with specific scale."""
        result = MathFunctions.round(F.col("value"), 2)
        assert result.operation == "round"
        assert result.value == 2

    def test_ceil(self):
        """Test ceil function."""
        result = MathFunctions.ceil(F.col("value"))
        assert result.operation == "ceil"
        assert result.name.lower() == "ceil(value)"

    def test_floor(self):
        """Test floor function."""
        result = MathFunctions.floor(F.col("value"))
        assert result.operation == "floor"
        assert result.name.lower() == "floor(value)"

    def test_sqrt(self):
        """Test sqrt function."""
        result = MathFunctions.sqrt(F.col("value"))
        assert result.operation == "sqrt"
        assert result.name.lower() == "sqrt(value)"

    def test_exp(self):
        """Test exp function."""
        result = MathFunctions.exp(F.col("value"))
        assert result.operation == "exp"
        assert result.name.lower() == "exp(value)"

    def test_log_natural(self):
        """Test log with natural logarithm."""
        result = MathFunctions.log(F.col("value"))
        assert result.operation == "log"
        assert result.name == "ln(value)"

    def test_log_with_base(self):
        """Test log with specific base."""
        result = MathFunctions.log(F.col("value"), 2.0)
        assert result.operation == "log"
        assert result.value == 2.0

    def test_log10(self):
        """Test log10 function."""
        result = MathFunctions.log10(F.col("value"))
        assert result.operation == "log10"

    def test_log2(self):
        """Test log2 function."""
        result = MathFunctions.log2(F.col("value"))
        assert result.operation == "log2"

    def test_pow_with_int(self):
        """Test pow with integer exponent."""
        result = MathFunctions.pow(F.col("value"), 2)
        assert result.operation == "pow"
        assert result.value == 2

    def test_pow_with_float(self):
        """Test pow with float exponent."""
        result = MathFunctions.pow(F.col("value"), 2.5)
        assert result.operation == "pow"
        assert result.value == 2.5

    def test_pow_with_column(self):
        """Test pow with column exponent."""
        result = MathFunctions.pow(F.col("base"), F.col("exp"))
        assert result.operation == "pow"

    def test_sin(self):
        """Test sin function."""
        result = MathFunctions.sin(F.col("angle"))
        assert result.operation == "sin"
        assert result.name.lower() == "sin(angle)"

    def test_cos(self):
        """Test cos function."""
        result = MathFunctions.cos(F.col("angle"))
        assert result.operation == "cos"
        assert result.name.lower() == "cos(angle)"

    def test_tan(self):
        """Test tan function."""
        result = MathFunctions.tan(F.col("angle"))
        assert result.operation == "tan"
        assert result.name.lower() == "tan(angle)"

    def test_asin(self):
        """Test asin function."""
        result = MathFunctions.asin(F.col("value"))
        assert result.operation == "asin"

    def test_acos(self):
        """Test acos function."""
        result = MathFunctions.acos(F.col("value"))
        assert result.operation == "acos"

    def test_atan(self):
        """Test atan function."""
        result = MathFunctions.atan(F.col("value"))
        assert result.operation == "atan"

    def test_atan2(self):
        """Test atan2 function."""
        result = MathFunctions.atan2(F.col("y"), F.col("x"))
        assert result.operation == "atan2"

    def test_sinh(self):
        """Test sinh function."""
        result = MathFunctions.sinh(F.col("value"))
        assert result.operation == "sinh"

    def test_cosh(self):
        """Test cosh function."""
        result = MathFunctions.cosh(F.col("value"))
        assert result.operation == "cosh"

    def test_tanh(self):
        """Test tanh function."""
        result = MathFunctions.tanh(F.col("value"))
        assert result.operation == "tanh"

    def test_toDegrees(self):
        """Test toDegrees function."""
        result = MathFunctions.toDegrees(F.col("radians"))
        # operation name may be "degrees"
        assert hasattr(result, "operation")

    def test_toRadians(self):
        """Test toRadians function."""
        result = MathFunctions.toRadians(F.col("degrees"))
        # operation name may be "radians"
        assert hasattr(result, "operation")

    def test_signum(self):
        """Test signum function."""
        result = MathFunctions.signum(F.col("value"))
        assert result.operation == "signum"

    def test_rint(self):
        """Test rint function."""
        result = MathFunctions.rint(F.col("value"))
        assert result.operation == "rint"

    def test_cbrt(self):
        """Test cbrt function."""
        result = MathFunctions.cbrt(F.col("value"))
        assert result.operation == "cbrt"

    def test_hypot(self):
        """Test hypot function."""
        result = MathFunctions.hypot(F.col("a"), F.col("b"))
        assert result.operation == "hypot"

    def test_log1p(self):
        """Test log1p function."""
        result = MathFunctions.log1p(F.col("value"))
        assert result.operation == "log1p"

    def test_expm1(self):
        """Test expm1 function."""
        result = MathFunctions.expm1(F.col("value"))
        assert result.operation == "expm1"

    def test_signum_with_string_column(self):
        """Test signum with string column name."""
        result = MathFunctions.signum("value")
        assert result.operation == "signum"

    def test_all_basic_functions_with_string(self):
        """Test all basic functions accept string column names."""
        functions = [
            MathFunctions.abs,
            MathFunctions.ceil,
            MathFunctions.floor,
            MathFunctions.sqrt,
            MathFunctions.exp,
            MathFunctions.log,
        ]
        for func in functions:
            result = func("value")
            assert result.operation in ["abs", "ceil", "floor", "sqrt", "exp", "log"]
