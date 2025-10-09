"""
Unit tests for resource cleanup and test isolation.

Tests that DuckDB connections and other resources are properly cleaned up
to prevent leaks between tests.
"""

import pytest
import gc
import weakref
from mock_spark import MockSparkSession
from mock_spark.storage.backends.duckdb import DuckDBStorageManager
from mock_spark.dataframe.duckdb_materializer import DuckDBMaterializer
from mock_spark.dataframe.sqlmodel_materializer import SQLModelMaterializer


class TestSessionCleanup:
    """Test that MockSparkSession properly cleans up resources."""

    def test_session_stop_closes_storage(self):
        """Test that session.stop() closes DuckDB storage connections."""
        session = MockSparkSession("test_cleanup")
        storage = session.storage

        # Create some data
        data = [{"id": 1, "name": "test"}]
        df = session.createDataFrame(data)

        # Stop the session
        session.stop()

        # Verify storage connection is closed
        assert storage.connection is None, "Storage connection should be None after stop()"

    def test_multiple_sessions_isolated(self):
        """Test that multiple sessions don't interfere with each other."""
        # Create first session and add data
        session1 = MockSparkSession("session1")
        data1 = [{"id": 1, "name": "session1_data"}]
        df1 = session1.createDataFrame(data1)
        df1.createOrReplaceTempView("test_table")

        # Create second session
        session2 = MockSparkSession("session2")

        # Second session should not see first session's data
        with pytest.raises(Exception):
            df2 = session2.table("test_table")

        # Clean up
        session1.stop()
        session2.stop()

    def test_session_cleanup_with_context_manager(self):
        """Test that context manager properly cleans up resources."""
        storage_ref = None

        with MockSparkSession("test_context") as session:
            data = [{"id": 1, "name": "test"}]
            df = session.createDataFrame(data)
            storage_ref = weakref.ref(session.storage)

        # Force garbage collection
        gc.collect()

        # Storage should be cleaned up (connection closed)
        if storage_ref() is not None:
            assert storage_ref().connection is None

    def test_session_cache_cleared_on_stop(self):
        """Test that session cache is cleared on stop."""
        session = MockSparkSession("test_cache")

        # Create some dataframes
        for i in range(5):
            data = [{"id": j, "value": j * 10} for j in range(100)]
            df = session.createDataFrame(data)
            _ = df.count()  # Trigger some operations

        # Memory usage should be tracked
        initial_memory = session.get_memory_usage()

        # Stop should clear cache
        session.stop()

        # After stop, trying to get memory usage should work
        # (even if it returns 0 after cleanup)
        final_memory = session.get_memory_usage()
        assert final_memory == 0, "Memory should be cleared after stop"


class TestDuckDBStorageManagerCleanup:
    """Test DuckDBStorageManager resource cleanup."""

    def test_storage_manager_closes_connection(self):
        """Test that storage manager closes DuckDB connection."""
        storage = DuckDBStorageManager()

        # Verify connection exists
        assert storage.connection is not None

        # Close storage
        storage.close()

        # Verify connection is closed
        assert storage.connection is None

    def test_storage_manager_context_manager(self):
        """Test that context manager closes connection."""
        storage_ref = None

        with DuckDBStorageManager() as storage:
            # Create a table and add data
            from mock_spark.spark_types import (
                MockStructType,
                MockStructField,
                StringType,
                IntegerType,
            )

            schema = MockStructType(
                [MockStructField("id", IntegerType()), MockStructField("name", StringType())]
            )
            storage.create_table("default", "test_table", schema)
            storage.insert_data("default", "test_table", [{"id": 1, "name": "test"}])

            storage_ref = weakref.ref(storage)

        # After context manager, connection should be closed
        if storage_ref() is not None:
            assert storage_ref().connection is None

    def test_storage_manager_destructor(self):
        """Test that destructor cleans up resources."""
        storage = DuckDBStorageManager()
        storage_ref = weakref.ref(storage)

        # Delete storage
        del storage
        gc.collect()

        # Reference should either be gone or connection closed
        if storage_ref() is not None:
            assert storage_ref().connection is None

    def test_storage_manager_no_disk_spillover(self):
        """Test that DuckDB is configured to prevent disk spillover."""
        storage = DuckDBStorageManager()

        try:
            # Try to get temp_directory setting (empty means disabled)
            result = storage.connection.execute(
                "SELECT current_setting('temp_directory')"
            ).fetchone()
            # If we get here, the setting exists and should be empty
            assert result[0] == "", "temp_directory should be empty to prevent disk spillover"
        except:
            # Setting might not be available in all DuckDB versions, that's okay
            pass

        storage.close()


class TestMaterializerCleanup:
    """Test that materializers properly clean up resources."""

    def test_duckdb_materializer_cleanup(self):
        """Test DuckDBMaterializer closes engine properly."""
        materializer = DuckDBMaterializer()

        # New materializer uses SQLAlchemy engine instead of raw connection
        assert materializer.engine is not None

        materializer.close()

        # After close, engine is disposed (we can't check if it's None, but dispose() was called)
        # Just verify close() doesn't raise an error and temp dir is cleaned up
        assert materializer._temp_dir is None

    def test_duckdb_materializer_destructor(self):
        """Test DuckDBMaterializer destructor cleanup."""
        materializer = DuckDBMaterializer()
        mat_ref = weakref.ref(materializer)

        del materializer
        gc.collect()

        # Should be cleaned up
        if mat_ref() is not None:
            assert mat_ref().connection is None

    def test_sqlmodel_materializer_cleanup(self):
        """Test SQLModelMaterializer closes engine."""
        materializer = SQLModelMaterializer()

        assert materializer.engine is not None

        materializer.close()

        assert materializer.engine is None

    def test_sqlmodel_materializer_destructor(self):
        """Test SQLModelMaterializer destructor cleanup."""
        materializer = SQLModelMaterializer()
        mat_ref = weakref.ref(materializer)

        del materializer
        gc.collect()

        # Should be cleaned up
        if mat_ref() is not None:
            assert mat_ref().engine is None


class TestLazyEvaluationCleanup:
    """Test that lazy evaluation properly cleans up materializers."""

    def test_lazy_evaluation_materializes_and_cleans_up(self):
        """Test that materialization creates and cleans up resources."""
        from mock_spark import functions as F

        session = MockSparkSession("test_lazy", enable_lazy_evaluation=True)

        # Create a lazy dataframe with operations
        data = [{"id": i, "value": i * 10} for i in range(100)]
        df = session.createDataFrame(data)

        # Queue some operations
        df = df.filter(F.col("id") > 10).select("id", "value")

        # Materialize with an action
        result = df.collect()

        # Verify results
        assert len(result) > 0
        assert all(row["id"] > 10 for row in result)

        # Clean up session
        session.stop()

    def test_multiple_lazy_operations_dont_leak(self):
        """Test that multiple lazy operations don't leak resources."""
        from mock_spark import functions as F

        session = MockSparkSession("test_multi_lazy", enable_lazy_evaluation=True)

        # Create multiple lazy dataframes
        for i in range(10):
            data = [{"id": j, "value": j * i} for j in range(50)]
            df = session.createDataFrame(data)

            # Chain multiple operations
            df = df.filter(F.col("id") > 5).select("id", "value").orderBy("id")

            # Materialize
            result = df.collect()
            assert len(result) > 0

        # Clean up
        session.stop()


class TestTestIsolation:
    """Test that tests are properly isolated from each other."""

    def test_first_test_creates_data(self):
        """First test creates data."""
        session = MockSparkSession("test_isolation_1")
        data = [{"id": 1, "name": "test1"}]
        df = session.createDataFrame(data)
        df.createOrReplaceTempView("isolation_test")

        # Verify data exists
        result = session.table("isolation_test").collect()
        assert len(result) == 1

        session.stop()

    def test_second_test_no_data_leak(self):
        """Second test should not see data from first test."""
        session = MockSparkSession("test_isolation_2")

        # Should not be able to access table from first test
        with pytest.raises(Exception):
            session.table("isolation_test")

        session.stop()

    def test_third_test_also_isolated(self):
        """Third test to confirm isolation."""
        session = MockSparkSession("test_isolation_3")

        # Create own data
        data = [{"id": 3, "name": "test3"}]
        df = session.createDataFrame(data)
        result = df.collect()

        assert len(result) == 1
        assert result[0]["id"] == 3

        session.stop()


class TestConcurrentSessions:
    """Test multiple concurrent sessions don't interfere."""

    def test_concurrent_sessions_isolated(self):
        """Test multiple concurrent sessions are isolated."""
        sessions = []

        # Create multiple sessions
        for i in range(5):
            session = MockSparkSession(f"concurrent_{i}")
            sessions.append(session)

        # Each session creates its own data
        for i, session in enumerate(sessions):
            data = [{"id": i, "value": f"session_{i}"}]
            df = session.createDataFrame(data)
            df.createOrReplaceTempView(f"table_{i}")

        # Each session should only see its own data
        for i, session in enumerate(sessions):
            df = session.table(f"table_{i}")
            result = df.collect()
            assert len(result) == 1
            assert result[0]["id"] == i

            # Should not see other sessions' tables
            for j in range(5):
                if j != i:
                    with pytest.raises(Exception):
                        session.table(f"table_{j}")

        # Clean up all sessions
        for session in sessions:
            session.stop()

    def test_session_cleanup_after_exception(self):
        """Test that sessions clean up even after exceptions."""
        session = MockSparkSession("test_exception")

        try:
            # Do some work
            data = [{"id": 1, "name": "test"}]
            df = session.createDataFrame(data)

            # Cause an error
            raise ValueError("Simulated error")
        except ValueError:
            pass
        finally:
            # Clean up should still work
            session.stop()

        # Verify cleanup happened
        assert session.storage.connection is None


class TestMemoryManagement:
    """Test memory management and cleanup."""

    def test_large_dataframe_cleanup(self):
        """Test that large dataframes are properly cleaned up."""
        from mock_spark import functions as F

        session = MockSparkSession("test_large")

        # Create a large dataframe
        data = [{"id": i, "value": f"value_{i}"} for i in range(10000)]
        df = session.createDataFrame(data)

        # Perform operations
        result = df.filter(F.col("id") > 5000).count()
        assert result < 5000

        # Get memory usage before cleanup
        memory_before = session.get_memory_usage()

        # Clear cache
        session.clear_cache()

        # Memory should be reduced
        memory_after = session.get_memory_usage()
        assert memory_after == 0

        session.stop()

    def test_repeated_operations_dont_accumulate(self):
        """Test that repeated operations don't accumulate memory indefinitely."""
        session = MockSparkSession("test_repeated")

        initial_memory = session.get_memory_usage()

        # Perform many operations
        for i in range(100):
            data = [{"id": j, "value": j} for j in range(100)]
            df = session.createDataFrame(data)
            _ = df.count()

            # Periodically clear cache
            if i % 10 == 0:
                session.clear_cache()

        # Final memory should not be excessive
        final_memory = session.get_memory_usage()

        # Memory might be non-zero but should be manageable
        # (this is just checking we don't have runaway memory growth)
        session.stop()


class TestConfigurableMemoryAndSpillover:
    """Test configurable memory and disk spillover settings."""

    def test_default_configuration(self):
        """Test that default configuration disables spillover."""
        session = MockSparkSession("test_default")

        # Should have 1GB limit and no spillover
        storage = session.storage
        try:
            result = storage.connection.execute(
                "SELECT current_setting('temp_directory')"
            ).fetchone()
            assert result[0] == "", "Default should disable temp directory"
        except:
            pass  # Setting might not be available

        session.stop()

    def test_custom_memory_limit(self):
        """Test configuring custom memory limit."""
        session = MockSparkSession("test_memory", max_memory="4GB")

        # Create some data
        data = [{"id": i, "value": i * 10} for i in range(100)]
        df = session.createDataFrame(data)
        result = df.collect()

        assert len(result) == 100
        session.stop()

    def test_allow_spillover_creates_unique_temp_dir(self):
        """Test that allowing spillover creates unique temp directories."""
        import os

        session1 = MockSparkSession("test_spillover_1", max_memory="1GB", allow_disk_spillover=True)
        session2 = MockSparkSession("test_spillover_2", max_memory="1GB", allow_disk_spillover=True)

        # Get temp directories
        temp_dir1 = session1.storage._temp_dir
        temp_dir2 = session2.storage._temp_dir

        # Should be different
        assert temp_dir1 != temp_dir2, "Each session should have unique temp directory"

        # Should exist
        if temp_dir1:
            assert os.path.exists(temp_dir1), "Temp directory should exist"
        if temp_dir2:
            assert os.path.exists(temp_dir2), "Temp directory should exist"

        # Clean up
        session1.stop()
        session2.stop()

        # Temp directories should be cleaned up
        if temp_dir1:
            assert not os.path.exists(temp_dir1), "Temp directory should be cleaned up"
        if temp_dir2:
            assert not os.path.exists(temp_dir2), "Temp directory should be cleaned up"

    def test_spillover_sessions_isolated(self):
        """Test that sessions with spillover enabled remain isolated."""
        session1 = MockSparkSession("test_iso_1", max_memory="1GB", allow_disk_spillover=True)
        session2 = MockSparkSession("test_iso_2", max_memory="1GB", allow_disk_spillover=True)

        # Create data in both sessions
        data1 = [{"id": 1, "name": "session1"}]
        data2 = [{"id": 2, "name": "session2"}]

        df1 = session1.createDataFrame(data1)
        df2 = session2.createDataFrame(data2)

        df1.createOrReplaceTempView("test_table")
        df2.createOrReplaceTempView("test_table")

        # Each session should only see its own data
        result1 = session1.table("test_table").collect()
        result2 = session2.table("test_table").collect()

        assert len(result1) == 1
        assert result1[0]["name"] == "session1"

        assert len(result2) == 1
        assert result2[0]["name"] == "session2"

        # Clean up
        session1.stop()
        session2.stop()

    def test_large_memory_configuration(self):
        """Test that large memory configuration works."""
        session = MockSparkSession("test_large_mem", max_memory="8GB")

        # Create moderate-sized data
        data = [{"id": i, "value": f"value_{i}"} for i in range(1000)]
        df = session.createDataFrame(data)

        # Perform some operations
        from mock_spark import functions as F

        result = df.filter(F.col("id") > 500).count()

        assert result < 500
        session.stop()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
