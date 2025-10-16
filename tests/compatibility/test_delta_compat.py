"""
Compatibility Tests for Delta Lake Support

Tests that basic Delta Lake API works the same in mock-spark and real Delta.
Note: Only tests API existence and basic patterns, not actual Delta features.

Requirements:
- delta-spark Python package
- Delta Lake JARs on Spark classpath (installed via spark-submit --packages)

These tests are OPTIONAL and will be skipped if Delta Lake isn't fully configured.
"""

import os
import pytest

# Skip all tests if delta-spark not available
pytest.importorskip("delta", reason="delta-spark not installed")

# Note: Delta Lake JARs are auto-downloaded via conftest.py delta_spark_session fixture
# The fixture sets PYSPARK_SUBMIT_ARGS="--packages io.delta:delta-core_2.12:2.0.2 pyspark-shell"

from pyspark.sql import SparkSession
from delta.tables import DeltaTable as PySparkDeltaTable
from mock_spark import MockSparkSession, DeltaTable as MockDeltaTable


@pytest.mark.delta
class TestDeltaAPICompatibility:
    """Test Delta API compatibility - verifies API exists in both."""

    def test_delta_api_methods_exist(self):
        """Test that DeltaTable classes have the same API methods."""
        # Both should have same class methods
        assert hasattr(PySparkDeltaTable, "forName")
        assert hasattr(MockDeltaTable, "forName")
        assert hasattr(PySparkDeltaTable, "forPath")
        assert hasattr(MockDeltaTable, "forPath")

    @pytest.mark.skip(reason="saveAsTable() requires Hive metastore - use file-based Delta in standalone tests")
    def test_delta_instance_methods_exist(self, pyspark_env):
        """Test that Delta table instances have the same API methods."""
        # Create a real Delta table for PySpark
        data = [{"id": 1, "value": "test"}]
        pyspark_df = pyspark_env["spark"].createDataFrame(data)
        pyspark_df.write.mode("overwrite").format("delta").saveAsTable("test_table")
        pyspark_dt = PySparkDeltaTable.forName(pyspark_env["spark"], "test_table")

        # Create mock Delta wrapper (doesn't need real table for API check)
        mock_spark = MockSparkSession.builder.getOrCreate()
        mock_dt = MockDeltaTable(mock_spark, "mock.test")

        # Verify both have same instance methods
        for method in ["toDF", "delete", "update", "merge", "vacuum", "history", "alias"]:
            assert hasattr(pyspark_dt, method), f"PySpark missing {method}"
            assert hasattr(mock_dt, method), f"Mock missing {method}"

        mock_spark.stop()

    @pytest.mark.skip(reason="saveAsTable() requires Hive metastore - use file-based Delta in standalone tests")
    def test_delta_todf_returns_dataframe(self, pyspark_env):
        """Test that toDF() returns DataFrame in both."""
        data = [{"id": 1}]

        # Real Delta: Create and convert to DF
        pyspark_df = pyspark_env["spark"].createDataFrame(data)
        pyspark_df.write.mode("overwrite").format("delta").saveAsTable("test_todf")
        pyspark_dt = PySparkDeltaTable.forName(pyspark_env["spark"], "test_todf")
        pyspark_result = pyspark_dt.toDF()

        assert pyspark_result is not None
        assert pyspark_result.count() == 1

    def test_delta_merge_builder(self):
        """Test that merge returns builder with correct methods."""
        # Mock Delta merge builder
        mock_spark = MockSparkSession.builder.getOrCreate()
        mock_dt = MockDeltaTable(mock_spark, "mock.target")
        source_df = mock_spark.createDataFrame([{"id": 1}])

        merge_builder = mock_dt.merge(source_df, "target.id = source.id")

        # Verify builder has all expected methods
        assert hasattr(merge_builder, "whenMatchedUpdate")
        assert hasattr(merge_builder, "whenMatchedUpdateAll")
        assert hasattr(merge_builder, "whenMatchedDelete")
        assert hasattr(merge_builder, "whenNotMatchedInsert")
        assert hasattr(merge_builder, "whenNotMatchedInsertAll")
        assert hasattr(merge_builder, "execute")

        # Execute should work (no-op)
        merge_builder.whenMatchedUpdate({"value": "new"}).execute()

        mock_spark.stop()


# Fixtures
@pytest.fixture
def mock_env():
    """Mock Spark environment."""
    spark = MockSparkSession.builder.appName("MockDeltaCompat").getOrCreate()
    yield {"spark": spark}
    spark.stop()


@pytest.fixture(scope="function")
def pyspark_env():
    """Real PySpark environment with Delta (isolated per test function)."""
    from pyspark import SparkContext
    import tempfile
    import shutil
    import uuid

    # Stop any existing Spark sessions to prevent interference
    try:
        active_session = SparkSession.getActiveSession()
        if active_session is not None:
            active_session.stop()

        active_context = SparkContext._active_spark_context
        if active_context is not None:
            active_context.stop()
    except Exception:
        pass

    # Create unique warehouse directory for isolation
    session_id = str(uuid.uuid4())[:8]
    warehouse_dir = tempfile.mkdtemp(prefix=f"delta-warehouse-{session_id}-")

    # Create Delta-enabled session with unique app name for isolation
    spark = (
        SparkSession.builder.appName(f"DeltaCompat-{session_id}")
        .master("local[1]")  # Use single thread to avoid conflicts
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.0.2")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .getOrCreate()
    )

    yield {"spark": spark}

    # Cleanup after test completes - CRITICAL for test isolation
    try:
        spark.stop()
        # Explicitly clear the active session and context
        SparkSession._instantiatedSession = None
        SparkContext._active_spark_context = None

        # Clean up warehouse directory
        if os.path.exists(warehouse_dir):
            shutil.rmtree(warehouse_dir, ignore_errors=True)
    except Exception:
        pass
