"""
Tests for SparkContext/Session validation in function calls.

This test suite verifies that functions require an active SparkSession,
matching PySpark's behavior exactly.
"""

import pytest
from mock_spark import SparkSession, functions as F


class TestSessionValidation:
    """Test that functions require active SparkSession."""

    def test_col_requires_active_session(self):
        """Test that col() raises error without active session."""
        # Clear any existing sessions
        SparkSession._active_sessions.clear()
        SparkSession._singleton_session = None

        # No active session
        with pytest.raises(RuntimeError, match="No active SparkSession found"):
            F.col("id")

    def test_col_works_with_active_session(self):
        """Test that col() works with active session."""
        spark = SparkSession("test")
        try:
            col_expr = F.col("id")
            assert col_expr is not None
            assert col_expr.name == "id"
        finally:
            spark.stop()

    def test_col_fails_after_session_stopped(self):
        """Test that col() fails after session is stopped."""
        spark = SparkSession("test")
        spark.stop()

        with pytest.raises(RuntimeError, match="No active SparkSession found"):
            F.col("id")

    def test_lit_requires_active_session(self):
        """Test that lit() requires active session."""
        SparkSession._active_sessions.clear()
        SparkSession._singleton_session = None

        with pytest.raises(RuntimeError, match="No active SparkSession found"):
            F.lit(42)

    def test_expr_requires_active_session(self):
        """Test that expr() requires active session."""
        SparkSession._active_sessions.clear()
        SparkSession._singleton_session = None

        with pytest.raises(RuntimeError, match="No active SparkSession found"):
            F.expr("id + 1")

    def test_when_requires_active_session(self):
        """Test that when() requires active session."""
        SparkSession._active_sessions.clear()
        SparkSession._singleton_session = None

        with pytest.raises(RuntimeError, match="No active SparkSession found"):
            F.when(F.col("x") > 0, 1)

    def test_aggregate_functions_require_session(self):
        """Test that aggregate functions require active session."""
        SparkSession._active_sessions.clear()
        SparkSession._singleton_session = None

        with pytest.raises(RuntimeError, match="No active SparkSession found"):
            F.count("id")

        with pytest.raises(RuntimeError, match="No active SparkSession found"):
            F.sum("value")

        with pytest.raises(RuntimeError, match="No active SparkSession found"):
            F.avg("value")

    def test_window_functions_require_session(self):
        """Test that window functions require active session."""
        SparkSession._active_sessions.clear()
        SparkSession._singleton_session = None

        with pytest.raises(RuntimeError, match="No active SparkSession found"):
            F.row_number()

        with pytest.raises(RuntimeError, match="No active SparkSession found"):
            F.rank()

    def test_datetime_functions_require_session(self):
        """Test that datetime functions require active session."""
        SparkSession._active_sessions.clear()
        SparkSession._singleton_session = None

        with pytest.raises(RuntimeError, match="No active SparkSession found"):
            F.current_date()

        with pytest.raises(RuntimeError, match="No active SparkSession found"):
            F.current_timestamp()

    def test_multiple_sessions(self):
        """Test session tracking with multiple sessions."""
        spark1 = SparkSession("test1")
        spark2 = SparkSession("test2")

        try:
            # Should work with active sessions
            col_expr = F.col("id")
            assert col_expr is not None

            # Most recent session should be active
            assert SparkSession.get_active_session() == spark2
        finally:
            spark2.stop()
            spark1.stop()

        # Should fail after all sessions stopped
        with pytest.raises(RuntimeError, match="No active SparkSession found"):
            F.col("id")
