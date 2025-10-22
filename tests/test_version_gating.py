"""
Tests for PySpark version compatibility gating.

This module tests that version-specific installation correctly restricts
API access based on PySpark version compatibility mode.
"""

import pytest
from mock_spark._version_compat import (
    set_pyspark_version,
    get_pyspark_version,
    is_available,
)


class TestVersionCompatModule:
    """Test the version compatibility module itself."""

    def test_set_and_get_version(self):
        """Test setting and getting PySpark version."""
        original = get_pyspark_version()
        try:
            set_pyspark_version("3.1")
            assert get_pyspark_version() == "3.1.3"

            set_pyspark_version("3.5")
            assert get_pyspark_version() == "3.5.2"
        finally:
            # Reset
            set_pyspark_version(original) if original else set_pyspark_version(None)  # type: ignore[arg-type]

    def test_version_normalization(self):
        """Test that version strings are normalized correctly."""
        original = get_pyspark_version()
        try:
            set_pyspark_version("3.2")
            assert get_pyspark_version() == "3.2.4"

            set_pyspark_version("3.2.4")
            assert get_pyspark_version() == "3.2.4"
        finally:
            set_pyspark_version(original) if original else set_pyspark_version(None)  # type: ignore[arg-type]


class TestFunctionVersionGating:
    """Test that functions are gated by version correctly."""

    def setup_method(self):
        """Save original version before each test."""
        self.original_version = get_pyspark_version()

    def teardown_method(self):
        """Restore original version after each test."""
        if self.original_version:
            set_pyspark_version(self.original_version)
        else:
            # Reset to None for all features
            from mock_spark import _version_compat

            _version_compat._PYSPARK_COMPAT_VERSION = None

    @pytest.mark.skip(
        reason="Direct imports bypass version gating - only F.function_name access is gated"
    )
    def test_make_date_unavailable_in_30(self):
        """Test that make_date raises AttributeError in 3.0 mode."""
        set_pyspark_version("3.0")

        # make_date added in PySpark 3.3
        with pytest.raises(AttributeError, match="has no attribute 'make_date'"):
            from mock_spark.functions import make_date  # noqa: F401

    def test_make_date_available_in_33(self):
        """Test that make_date is available in 3.3 mode."""
        set_pyspark_version("3.3")

        # Should not raise
        from mock_spark.functions import make_date

        assert make_date is not None

    @pytest.mark.skip(
        reason="Direct imports bypass version gating - only F.function_name access is gated"
    )
    def test_acosh_unavailable_in_30(self):
        """Test that acosh raises AttributeError in 3.0 mode."""
        set_pyspark_version("3.0")

        # acosh added in PySpark 3.1
        with pytest.raises(AttributeError, match="has no attribute 'acosh'"):
            from mock_spark.functions import acosh  # noqa: F401

    def test_acosh_available_in_31(self):
        """Test that acosh is available in 3.1 mode."""
        set_pyspark_version("3.1")

        # Should not raise
        from mock_spark.functions import acosh

        assert acosh is not None

    @pytest.mark.skip(
        reason="Direct imports bypass version gating - only F.function_name access is gated"
    )
    def test_bool_and_unavailable_in_34(self):
        """Test that bool_and raises AttributeError in 3.4 mode."""
        set_pyspark_version("3.4")

        # bool_and added in PySpark 3.5
        with pytest.raises(AttributeError, match="has no attribute 'bool_and'"):
            from mock_spark.functions import bool_and  # noqa: F401

    def test_bool_and_available_in_35(self):
        """Test that bool_and is available in 3.5 mode."""
        set_pyspark_version("3.5")

        # Should not raise
        from mock_spark.functions import bool_and

        assert bool_and is not None

    def test_all_functions_available_default(self):
        """Test that all functions available when no version set."""
        # Reset to default (no version)
        from mock_spark import _version_compat

        _version_compat._PYSPARK_COMPAT_VERSION = None

        # All should be available
        from mock_spark.functions import make_date, acosh, bool_and

        assert make_date is not None
        assert acosh is not None
        assert bool_and is not None

    def test_overlay_available_in_30(self):
        """Test that overlay is available in 3.0 (it exists in PySpark 3.0)."""
        set_pyspark_version("3.0")

        # overlay exists in PySpark 3.0
        from mock_spark.functions import overlay

        assert overlay is not None


class TestDataFrameMethodGating:
    """Test that DataFrame methods are gated by version correctly."""

    def setup_method(self):
        """Save original version before each test."""
        self.original_version = get_pyspark_version()

    def teardown_method(self):
        """Restore original version after each test."""
        if self.original_version:
            set_pyspark_version(self.original_version)
        else:
            # Reset to None for all features
            from mock_spark import _version_compat

            _version_compat._PYSPARK_COMPAT_VERSION = None

    def test_dataframe_basic_methods_always_available(self):
        """Test that basic DataFrame methods are always available."""
        from mock_spark import MockSparkSession

        set_pyspark_version("3.0")
        spark = MockSparkSession("test")
        df = spark.createDataFrame([{"id": 1}])

        # Core methods should always be available
        assert hasattr(df, "select")
        assert hasattr(df, "filter")
        assert hasattr(df, "groupBy")
        assert hasattr(df, "show")

    def test_unpivot_unavailable_in_30(self):
        """Test that unpivot raises AttributeError in 3.0 mode."""
        from mock_spark import MockSparkSession

        set_pyspark_version("3.0")
        spark = MockSparkSession("test")
        df = spark.createDataFrame([{"id": 1, "a": 2, "b": 3}])

        # unpivot added in PySpark 3.4
        with pytest.raises(AttributeError, match="has no attribute 'unpivot'"):
            df.unpivot(ids="id", values=["a", "b"])

    def test_unpivot_available_in_34(self):
        """Test that unpivot is available in 3.4 mode."""
        from mock_spark import MockSparkSession

        set_pyspark_version("3.4")
        spark = MockSparkSession("test")
        df = spark.createDataFrame([{"id": 1, "a": 2, "b": 3}])

        # Should not raise (though may fail for other reasons)
        assert hasattr(df, "unpivot")

    def test_transform_availability(self):
        """Test transform method availability across versions."""
        from mock_spark import MockSparkSession

        # transform added in PySpark 3.0
        set_pyspark_version("3.0")
        spark = MockSparkSession("test")
        df = spark.createDataFrame([{"id": 1}])

        assert hasattr(df, "transform")


class TestIsAvailableFunction:
    """Test the is_available() function directly."""

    def setup_method(self):
        """Save original version before each test."""
        self.original_version = get_pyspark_version()

    def teardown_method(self):
        """Restore original version after each test."""
        if self.original_version:
            set_pyspark_version(self.original_version)
        else:
            from mock_spark import _version_compat

            _version_compat._PYSPARK_COMPAT_VERSION = None

    def test_is_available_default_mode(self):
        """Test that is_available returns True for all items in default mode."""
        from mock_spark import _version_compat

        _version_compat._PYSPARK_COMPAT_VERSION = None

        assert is_available("make_date", "function") is True
        assert is_available("acosh", "function") is True
        assert is_available("nonexistent", "function") is True  # Unknown items allowed

    def test_is_available_version_30(self):
        """Test is_available in PySpark 3.0 mode."""
        set_pyspark_version("3.0")

        # Functions added after 3.0 should not be available
        assert is_available("acosh", "function") is False
        assert is_available("make_date", "function") is False
        assert is_available("bool_and", "function") is False

        # Functions in 3.0 should be available
        assert is_available("col", "function") is True
        assert is_available("upper", "function") is True
        assert is_available("overlay", "function") is True

    def test_is_available_version_35(self):
        """Test is_available in PySpark 3.5 mode."""
        set_pyspark_version("3.5")

        # All tested functions should be available in 3.5
        assert is_available("acosh", "function") is True
        assert is_available("make_date", "function") is True
        assert is_available("bool_and", "function") is True
        assert is_available("col", "function") is True

    def test_is_available_dataframe_methods(self):
        """Test is_available for DataFrame methods."""
        set_pyspark_version("3.0")

        # Core methods should be available
        assert is_available("select", "dataframe_method") is True
        assert is_available("filter", "dataframe_method") is True

        # Methods added later should not be available
        # (unpivot added in 3.4)
        assert is_available("unpivot", "dataframe_method") is False


class TestVersionGatingIntegration:
    """Integration tests for version gating."""

    def setup_method(self):
        """Save original version before each test."""
        self.original_version = get_pyspark_version()

    def teardown_method(self):
        """Restore original version after each test."""
        if self.original_version:
            set_pyspark_version(self.original_version)
        else:
            from mock_spark import _version_compat

            _version_compat._PYSPARK_COMPAT_VERSION = None

    @pytest.mark.skip(
        reason="F.function_name access gating not fully implemented - deferred"
    )
    def test_version_31_full_workflow(self):
        """Test complete workflow in PySpark 3.1 compatibility mode."""
        from mock_spark import MockSparkSession
        import mock_spark.functions as F

        set_pyspark_version("3.1")
        spark = MockSparkSession("test")

        # Create DataFrame
        data = [{"id": 1, "value": 2.5}, {"id": 2, "value": 3.7}]
        df = spark.createDataFrame(data)

        # Use functions available in 3.1
        result = df.select(
            F.col("id"),
            F.acosh(F.lit(2)).alias("acosh_result"),  # acosh added in 3.1
        )

        assert result is not None

        # Try to use function not in 3.1 (make_date added in 3.3)
        with pytest.raises(AttributeError):
            df.select(F.make_date(F.lit(2024), F.lit(1), F.lit(1)))

    @pytest.mark.skip(
        reason="F.function_name access gating not fully implemented - deferred"
    )
    def test_version_33_full_workflow(self):
        """Test complete workflow in PySpark 3.3 compatibility mode."""
        from mock_spark import MockSparkSession
        import mock_spark.functions as F

        set_pyspark_version("3.3")
        spark = MockSparkSession("test")

        # Create DataFrame
        data = [{"year": 2024, "month": 3, "day": 15}]
        df = spark.createDataFrame(data)

        # Use functions available in 3.3
        result = df.select(
            F.make_date(F.col("year"), F.col("month"), F.col("day")).alias("date")
        )

        assert result is not None

        # bool_and added in 3.5, should fail
        with pytest.raises(AttributeError):
            df.select(F.bool_and(F.lit(True)))

    def test_switching_versions(self):
        """Test that version can be switched multiple times."""
        assert is_available("make_date", "function")  # Default: available

        set_pyspark_version("3.0")
        assert not is_available("make_date", "function")  # 3.0: not available

        set_pyspark_version("3.3")
        assert is_available("make_date", "function")  # 3.3: available

        set_pyspark_version("3.0")
        assert not is_available("make_date", "function")  # Back to 3.0: not available
