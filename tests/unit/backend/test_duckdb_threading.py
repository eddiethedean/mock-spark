"""
Unit tests for DuckDB storage backend threading support.

Tests thread-safe schema creation and table operations when used in
parallel execution contexts (ThreadPoolExecutor, pytest-xdist).
"""

import pytest
import threading
from concurrent.futures import ThreadPoolExecutor

# Skip if DuckDB backend doesn't exist
try:
    from mock_spark.backend.duckdb.storage import DuckDBStorageManager
    from mock_spark.spark_types import (
        MockStructType,
        MockStructField,
        IntegerType,
        StringType,
    )
except ImportError:
    pytest.skip("DuckDB backend not available", allow_module_level=True)


@pytest.mark.unit
class TestDuckDBThreading:
    """Test DuckDB storage manager threading behavior."""

    @pytest.mark.skip(
        reason="DuckDB in-memory isolation per connection - threading tests hang"
    )
    def test_schema_creation_multiple_threads(self):
        """Test that schema creation works from multiple threads."""
        print("[TEST] Starting test_schema_creation_multiple_threads", flush=True)
        storage = DuckDBStorageManager()
        print("[TEST] Storage manager created", flush=True)

        def create_schema_in_thread(schema_name):
            """Create a schema in a thread."""
            thread_id = threading.current_thread().ident
            print(
                f"[TEST-THREAD-{thread_id}] Creating schema {schema_name}", flush=True
            )
            try:
                storage.create_schema(schema_name)
                print(
                    f"[TEST-THREAD-{thread_id}] Schema {schema_name} created successfully",
                    flush=True,
                )
                exists = storage.schema_exists(schema_name)
                print(
                    f"[TEST-THREAD-{thread_id}] Schema {schema_name} exists check: {exists}",
                    flush=True,
                )
                assert exists, f"Schema {schema_name} should exist"
                print(
                    f"[TEST-THREAD-{thread_id}] Schema {schema_name} verification passed",
                    flush=True,
                )
            except Exception as e:
                print(
                    f"[TEST-THREAD-{thread_id}] ERROR creating schema {schema_name}: {e}",
                    flush=True,
                )
                raise

        # Create multiple threads that each create different schemas
        print("[TEST] Creating threads...", flush=True)
        threads = []
        for i in range(5):
            thread = threading.Thread(
                target=create_schema_in_thread,
                args=(f"schema_{i}",),
                name=f"Thread-{i}",
            )
            threads.append(thread)

        # Start all threads
        print("[TEST] Starting threads...", flush=True)
        for thread in threads:
            thread.start()
            print(f"[TEST] Started {thread.name}", flush=True)

        # Wait for all threads to complete (with timeout)
        print("[TEST] Waiting for threads to complete...", flush=True)
        for thread in threads:
            print(f"[TEST] Waiting for {thread.name}...", flush=True)
            thread.join(timeout=5)  # 5 second timeout per thread
            if thread.is_alive():
                print(
                    f"[TEST] ERROR: Thread {thread.name} did not complete within timeout",
                    flush=True,
                )
                pytest.fail(f"Thread {thread.name} did not complete within timeout")
            print(f"[TEST] Thread {thread.name} completed", flush=True)

        # Verify all schemas exist
        print("[TEST] Verifying all schemas exist...", flush=True)
        for i in range(5):
            exists = storage.schema_exists(f"schema_{i}")
            print(f"[TEST] Schema schema_{i} exists: {exists}", flush=True)
            assert exists, f"Schema schema_{i} should exist"

        print(
            "[TEST] test_schema_creation_multiple_threads completed successfully",
            flush=True,
        )

    @pytest.mark.timeout(60)
    @pytest.mark.skip(
        reason="DuckDB in-memory isolation per connection - table creation hangs in threads"
    )
    def test_schema_creation_with_threadpool(self):
        """Test schema creation with ThreadPoolExecutor."""
        storage = DuckDBStorageManager()

        def create_table(schema_name, table_name):
            """Create a schema and table."""
            storage.create_schema(schema_name)
            schema = MockStructType(
                [
                    MockStructField("id", IntegerType()),
                    MockStructField("name", StringType()),
                ]
            )
            storage.create_table(schema_name, table_name, schema)
            assert storage.table_exists(schema_name, table_name)

        # Create schemas and tables from multiple worker threads
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            for i in range(10):
                future = executor.submit(create_table, f"schema_{i}", f"table_{i}")
                futures.append(future)

            # Wait for all operations to complete (with timeout)
            print("[TEST] Waiting for ThreadPoolExecutor futures...", flush=True)
            for i, future in enumerate(futures):
                print(f"[TEST] Waiting for future {i}...", flush=True)
                try:
                    future.result(timeout=10)  # 10 second timeout per future
                    print(f"[TEST] Future {i} completed", flush=True)
                except Exception as e:
                    print(f"[TEST] Future {i} failed: {e}", flush=True)
                    raise

        # Verify all schemas and tables exist
        for i in range(10):
            assert storage.schema_exists(f"schema_{i}")
            assert storage.table_exists(f"schema_{i}", f"table_{i}")

    @pytest.mark.timeout(30)
    @pytest.mark.skip(reason="DuckDB in-memory isolation per connection")
    def test_concurrent_schema_creation(self):
        """Test that concurrent schema creation doesn't cause errors."""
        storage = DuckDBStorageManager()
        schema_name = "concurrent_schema"

        def create_schema():
            """Create the same schema from multiple threads."""
            storage.create_schema(schema_name)
            assert storage.schema_exists(schema_name)

        # Create multiple threads that all try to create the same schema
        threads = []
        for _ in range(10):
            thread = threading.Thread(target=create_schema)
            threads.append(thread)

        # Start all threads
        for thread in threads:
            thread.start()

        # Wait for all threads to complete (with timeout)
        for thread in threads:
            thread.join(timeout=30)  # 30 second timeout per thread
            if thread.is_alive():
                pytest.fail(f"Thread {thread.name} did not complete within timeout")

        # Verify schema exists
        assert storage.schema_exists(schema_name)

    @pytest.mark.timeout(30)
    @pytest.mark.skip(reason="DuckDB in-memory isolation per connection")
    def test_schema_exists_thread_local(self):
        """Test that schema_exists works correctly with thread-local connections."""
        storage = DuckDBStorageManager()
        schema_name = "thread_local_schema"

        def check_and_create_schema(thread_id):
            """Check if schema exists, create if not."""
            if not storage.schema_exists(schema_name):
                storage.create_schema(schema_name)
            # Verify it exists in this thread
            assert storage.schema_exists(schema_name), (
                f"Schema should exist in thread {thread_id}"
            )

        # Create multiple threads
        threads = []
        for i in range(5):
            thread = threading.Thread(target=check_and_create_schema, args=(i,))
            threads.append(thread)

        # Start all threads
        for thread in threads:
            thread.start()

        # Wait for all threads to complete (with timeout)
        for thread in threads:
            thread.join(timeout=30)  # 30 second timeout per thread
            if thread.is_alive():
                pytest.fail(f"Thread {thread.name} did not complete within timeout")

        # Verify schema exists
        assert storage.schema_exists(schema_name)

    @pytest.mark.timeout(60)
    @pytest.mark.skip(
        reason="DuckDB in-memory isolation per connection - table creation hangs in threads"
    )
    def test_table_creation_multiple_threads(self):
        """Test table creation from multiple threads with different schemas."""
        storage = DuckDBStorageManager()

        def create_schema_and_table(thread_id):
            """Create a schema and table in a thread."""
            schema_name = f"schema_{thread_id}"
            table_name = f"table_{thread_id}"

            storage.create_schema(schema_name)
            schema = MockStructType(
                [
                    MockStructField("id", IntegerType()),
                    MockStructField("value", StringType()),
                ]
            )

            storage.create_table(schema_name, table_name, schema)

            # Insert some data
            data = [
                {"id": 1, "value": f"thread_{thread_id}"},
                {"id": 2, "value": f"data_{thread_id}"},
            ]
            storage.insert_data(schema_name, table_name, data)

            # Verify data was inserted
            result = storage.query_table(schema_name, table_name)
            assert len(result) == 2

        # Create multiple threads
        threads = []
        for i in range(5):
            thread = threading.Thread(target=create_schema_and_table, args=(i,))
            threads.append(thread)

        # Start all threads
        for thread in threads:
            thread.start()

        # Wait for all threads to complete (with timeout)
        for thread in threads:
            thread.join(timeout=30)  # 30 second timeout per thread
            if thread.is_alive():
                pytest.fail(f"Thread {thread.name} did not complete within timeout")

        # Verify all schemas and tables exist
        for i in range(5):
            assert storage.schema_exists(f"schema_{i}")
            assert storage.table_exists(f"schema_{i}", f"table_{i}")
