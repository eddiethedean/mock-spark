"""
Unit tests for bitwise functions.
"""

import pytest
from mock_spark import F
from mock_spark.functions.bitwise import BitwiseFunctions


@pytest.mark.unit
class TestBitwiseFunctions:
    """Test bitwise functions."""

    def test_bit_count_with_column(self):
        """Test bit_count with MockColumn."""
        col = F.col("value")
        result = BitwiseFunctions.bit_count(col)
        assert result.operation == "bit_count"
        assert result.name == "bit_count(value)"

    def test_bit_count_with_string(self):
        """Test bit_count with string column name."""
        result = BitwiseFunctions.bit_count("value")
        assert result.operation == "bit_count"
        assert result.name == "bit_count(value)"

    def test_bit_get_with_position(self):
        """Test bit_get with position."""
        result = BitwiseFunctions.bit_get(F.col("value"), 0)
        assert result.operation == "bit_get"
        assert result.value == 0

    def test_bit_get_with_negative_position(self):
        """Test bit_get with negative position."""
        result = BitwiseFunctions.bit_get(F.col("value"), -1)
        assert result.operation == "bit_get"
        assert result.value == -1

    def test_bitwise_not_with_column(self):
        """Test bitwise_not with MockColumn."""
        result = BitwiseFunctions.bitwise_not(F.col("value"))
        assert result.operation == "bitwise_not"

    def test_bitwise_not_with_string(self):
        """Test bitwise_not with string."""
        result = BitwiseFunctions.bitwise_not("value")
        assert result.operation == "bitwise_not"

    def test_bit_and_with_column(self):
        """Test bit_and aggregate function."""
        result = BitwiseFunctions.bit_and(F.col("flags"))
        assert result.function_name == "bit_and"

    def test_bit_and_with_string(self):
        """Test bit_and with string column name."""
        result = BitwiseFunctions.bit_and("flags")
        assert result.function_name == "bit_and"

    def test_bit_or_with_column(self):
        """Test bit_or aggregate function."""
        result = BitwiseFunctions.bit_or(F.col("flags"))
        assert result.function_name == "bit_or"

    def test_bit_or_with_string(self):
        """Test bit_or with string."""
        result = BitwiseFunctions.bit_or("flags")
        assert result.function_name == "bit_or"

    def test_bit_xor_with_column(self):
        """Test bit_xor aggregate function."""
        result = BitwiseFunctions.bit_xor(F.col("flags"))
        assert result.function_name == "bit_xor"

    def test_bit_xor_with_string(self):
        """Test bit_xor with string."""
        result = BitwiseFunctions.bit_xor("flags")
        assert result.function_name == "bit_xor"

    def test_bitwise_not_deprecated_alias(self):
        """Test deprecated bitwiseNOT alias."""
        with pytest.warns(FutureWarning, match="bitwiseNOT is deprecated"):
            result = BitwiseFunctions.bitwiseNOT(F.col("value"))
        assert result.operation == "bitwise_not"

    def test_bitwise_not_deprecated_returns_same_as_new(self):
        """Test deprecated alias returns same as new method."""
        with pytest.warns(FutureWarning):
            deprecated_result = BitwiseFunctions.bitwiseNOT(F.col("value"))

        new_result = BitwiseFunctions.bitwise_not(F.col("value"))
        assert deprecated_result.operation == new_result.operation
        assert deprecated_result.name == new_result.name
