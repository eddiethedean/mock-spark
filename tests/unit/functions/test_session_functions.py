"""
Tests for session-aware SQL functions such as current_database() and current_user().
"""

from __future__ import annotations

import getpass

import pytest

from mock_spark.sql import SparkSession, functions as F


class TestSessionFunctions:
    """Verify session-aware helpers reflect Spark session state."""

    def setup_method(self) -> None:
        SparkSession._singleton_session = None  # type: ignore[attr-defined]
        self.spark = SparkSession.builder.appName(
            "session-functions-test"
        ).getOrCreate()
        self.spark.catalog.createDatabase("analytics", ignoreIfExists=True)
        self.spark.catalog.setCurrentDatabase("analytics")
        assert self.spark.catalog.currentDatabase() == "analytics"

    def teardown_method(self) -> None:
        try:
            self.spark.catalog.setCurrentDatabase("default")
            self.spark.catalog.dropDatabase("analytics", ignoreIfNotExists=True)
        finally:
            self.spark.stop()
            SparkSession._singleton_session = None  # type: ignore[attr-defined]

    def test_current_database_uses_active_session(self) -> None:
        df = self.spark.createDataFrame([{"value": 1}])
        result = df.select(F.current_database()).collect()[0][0]
        assert result == "analytics"

    def test_current_schema_aliases_current_database(self) -> None:
        df = self.spark.createDataFrame([{"value": 1}])
        result = df.select(F.current_schema()).collect()[0][0]
        assert result == "analytics"

    def test_current_catalog_returns_default(self) -> None:
        df = self.spark.createDataFrame([{"value": 1}])
        result = df.select(F.current_catalog()).collect()[0][0]
        assert result == "spark_catalog"

    def test_current_user_reflects_spark_context_user(self) -> None:
        df = self.spark.createDataFrame([{"value": 1}])
        result = df.select(F.current_user()).collect()[0][0]
        assert result == getpass.getuser()

    def test_error_when_no_active_session(self) -> None:
        SparkSession._singleton_session = None  # type: ignore[attr-defined]
        from mock_spark.errors import PySparkValueError

        try:
            with pytest.raises(PySparkValueError):
                F.current_database()
        finally:
            SparkSession._singleton_session = self.spark  # type: ignore[attr-defined]
