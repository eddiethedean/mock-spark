"""
Unit tests for Polars storage backend threading support.

Tests thread-safe schema creation and table operations when used in
parallel execution contexts (ThreadPoolExecutor, pytest-xdist).
"""

import pytest
import threading
from concurrent.futures import ThreadPoolExecutor

from mock_spark.backend.polars.storage import PolarsStorageManager
from mock_spark.spark_types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
)


@pytest.mark.unit
class TestPolarsThreading:
    """Test Polars storage manager threading behavior."""

    def test_schema_creation_multiple_threads(self):
        """Test that schema creation works from multiple threads."""
        storage = PolarsStorageManager()

        def create_schema_in_thread(schema_name):
            """Create a schema in a thread."""
            storage.create_schema(schema_name)
            exists = storage.schema_exists(schema_name)
            assert exists, f"Schema {schema_name} should exist"

        # Create multiple threads that each create different schemas
        threads = []
        for i in range(5):
            thread = threading.Thread(
                target=create_schema_in_thread,
                args=(f"schema_{i}",),
                name=f"Thread-{i}",
            )
            threads.append(thread)

        # Start all threads
        for thread in threads:
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join(timeout=5)
            if thread.is_alive():
                pytest.fail(f"Thread {thread.name} did not complete within timeout")

        # Verify all schemas exist
        for i in range(5):
            exists = storage.schema_exists(f"schema_{i}")
            assert exists, f"Schema schema_{i} should exist"

    @pytest.mark.timeout(60)
    def test_schema_creation_with_threadpool(self):
        """Test schema creation with ThreadPoolExecutor."""
        storage = PolarsStorageManager()

        def create_table(schema_name, table_name):
            """Create a schema and table."""
            storage.create_schema(schema_name)
            schema = StructType(
                [
                    StructField("id", IntegerType()),
                    StructField("name", StringType()),
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

            # Wait for all operations to complete
            for future in futures:
                future.result(timeout=10)

        # Verify all schemas and tables exist
        for i in range(10):
            assert storage.schema_exists(f"schema_{i}")
            assert storage.table_exists(f"schema_{i}", f"table_{i}")

    @pytest.mark.timeout(30)
    def test_concurrent_schema_creation(self):
        """Test that concurrent schema creation doesn't cause errors."""
        storage = PolarsStorageManager()
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

        # Wait for all threads to complete
        for thread in threads:
            thread.join(timeout=30)
            if thread.is_alive():
                pytest.fail(f"Thread {thread.name} did not complete within timeout")

        # Verify schema exists
        assert storage.schema_exists(schema_name)

    @pytest.mark.timeout(30)
    def test_schema_exists_thread_safe(self):
        """Test that schema_exists works correctly with concurrent access."""
        storage = PolarsStorageManager()
        schema_name = "thread_safe_schema"

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

        # Wait for all threads to complete
        for thread in threads:
            thread.join(timeout=30)
            if thread.is_alive():
                pytest.fail(f"Thread {thread.name} did not complete within timeout")

        # Verify schema exists
        assert storage.schema_exists(schema_name)

    @pytest.mark.timeout(60)
    def test_table_creation_multiple_threads(self):
        """Test table creation from multiple threads with different schemas."""
        storage = PolarsStorageManager()

        def create_schema_and_table(thread_id):
            """Create a schema and table in a thread."""
            schema_name = f"schema_{thread_id}"
            table_name = f"table_{thread_id}"

            storage.create_schema(schema_name)
            schema = StructType(
                [
                    StructField("id", IntegerType()),
                    StructField("value", StringType()),
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

        # Wait for all threads to complete
        for thread in threads:
            thread.join(timeout=30)
            if thread.is_alive():
                pytest.fail(f"Thread {thread.name} did not complete within timeout")

        # Verify all schemas and tables exist
        for i in range(5):
            assert storage.schema_exists(f"schema_{i}")
            assert storage.table_exists(f"schema_{i}", f"table_{i}")
