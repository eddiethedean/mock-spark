"""
Unit Tests for Simple Delta Lake Support

Tests that Delta Lake API exists and can be called.
Operations are mocks - they don't actually execute Delta logic.

Note: Uses SQL to create tables to avoid storage layer complexity.
"""

import pytest
from mock_spark import MockSparkSession, DeltaTable
from mock_spark.core.exceptions.analysis import AnalysisException


class TestDeltaTableBasicAPI:
    """Test basic Delta Table API."""

    def test_delta_api_exists(self):
        """Test that DeltaTable class exists and has required methods."""
        # Verify API exists
        assert hasattr(DeltaTable, "forName")
        assert hasattr(DeltaTable, "forPath")

        # Verify instance methods exist
        spark = MockSparkSession("test")
        dt = DeltaTable(spark, "schema.table")

        assert hasattr(dt, "toDF")
        assert hasattr(dt, "delete")
        assert hasattr(dt, "update")
        assert hasattr(dt, "merge")
        assert hasattr(dt, "vacuum")
        assert hasattr(dt, "history")
        assert hasattr(dt, "alias")

        spark.stop()

    def test_forname_nonexistent_raises(self, spark):
        """Test that forName raises for nonexistent table."""
        with pytest.raises(AnalysisException, match="not found"):
            DeltaTable.forName(spark, "nonexistent.table")

    def test_forpath(self, spark):
        """Test forPath creates wrapper."""
        dt = DeltaTable.forPath(spark, "/tmp/delta/table")
        assert dt is not None
        assert dt._table_name == "default.table"


class TestDeltaMockOperations:
    """Test that mock Delta operations can be called without error."""

    def test_mock_operations_dont_error(self, spark):
        """Test that all mock operations can be called (they're no-ops)."""
        # Create a simple wrapper (don't need actual table for mock operations)
        dt = DeltaTable(spark, "mock.table")

        # All these should be callable without error (even if no-ops)
        dt.delete("id < 10")
        dt.delete()  # Delete all
        dt.update("id = 1", {"name": "new"})
        dt.vacuum()
        dt.vacuum(retention_hours=168)

        # History should return DataFrame with mock history
        history = dt.history()
        assert history.count() >= 1  # Enhanced history returns mock data
        # Check for expected columns (enhanced version has more columns)
        assert "version" in history.columns
        assert "timestamp" in history.columns
        assert "operation" in history.columns

        # Merge builder should work
        source_data = [{"id": 1}]
        source_df = spark.createDataFrame(source_data)

        merge = (
            dt.merge(source_df, "target.id = source.id")
            .whenMatchedUpdate({"value": "new"})
            .whenNotMatchedInsert({"id": "source.id"})
            .whenMatchedDelete("target.id < 10")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
        )
        merge.execute()  # No-op


# Fixtures
@pytest.fixture
def spark():
    """Create isolated Spark session."""
    session = MockSparkSession.builder.appName("DeltaSimpleTests").getOrCreate()
    yield session
    session.stop()
