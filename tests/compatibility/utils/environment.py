"""
Environment setup utilities for compatibility testing.
"""

import os
import tempfile
import shutil
from typing import Any, Dict, Optional
from contextlib import contextmanager


@contextmanager
def temp_spark_context():
    """Context manager for temporary Spark context."""
    # This would be used for PySpark environment setup
    # For now, we'll just yield None since we're not actually setting up Spark
    yield None


@contextmanager
def temp_directory():
    """Context manager for temporary directory."""
    temp_dir = tempfile.mkdtemp()
    try:
        yield temp_dir
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


class EnvironmentConfig:
    """Configuration for test environments."""

    def __init__(self):
        self.spark_config = {
            "spark.app.name": "mock-spark-compatibility-test",
            "spark.master": "local[2]",
            "spark.sql.adaptive.enabled": "false",
            "spark.sql.adaptive.coalescePartitions.enabled": "false",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.execution.arrow.pyspark.enabled": "false",  # Disable for consistency
        }
        self.temp_dirs = []
        self.cleanup_files = []

    def add_temp_dir(self, temp_dir: str) -> None:
        """Add a temporary directory for cleanup."""
        self.temp_dirs.append(temp_dir)

    def add_cleanup_file(self, file_path: str) -> None:
        """Add a file for cleanup."""
        self.cleanup_files.append(file_path)

    def cleanup(self) -> None:
        """Clean up temporary resources."""
        # Clean up temporary directories
        for temp_dir in self.temp_dirs:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir, ignore_errors=True)

        # Clean up temporary files
        for file_path in self.cleanup_files:
            if os.path.exists(file_path):
                os.remove(file_path)


def create_test_file(data: str, suffix: str = ".txt") -> str:
    """Create a temporary test file with the given data."""
    temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=suffix, delete=False)
    temp_file.write(data)
    temp_file.close()
    return temp_file.name


def create_test_csv(data: list, filename: str = None) -> str:
    """Create a temporary CSV file with the given data."""
    if filename is None:
        temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        filename = temp_file.name
        temp_file.close()

    # Write CSV data
    if data:
        import csv

        with open(filename, "w", newline="") as csvfile:
            if isinstance(data[0], dict):
                writer = csv.DictWriter(csvfile, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)
            else:
                writer = csv.writer(csvfile)
                writer.writerows(data)

    return filename


def create_test_json(data: list, filename: str = None) -> str:
    """Create a temporary JSON file with the given data."""
    if filename is None:
        temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
        filename = temp_file.name
        temp_file.close()

    import json

    with open(filename, "w") as jsonfile:
        json.dump(data, jsonfile, indent=2)

    return filename


def create_test_parquet(data: list, filename: str = None) -> str:
    """Create a temporary Parquet file with the given data."""
    if filename is None:
        temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".parquet", delete=False)
        filename = temp_file.name
        temp_file.close()

    # Convert to pandas DataFrame and save as Parquet
    import pandas as pd

    df = pd.DataFrame(data)
    df.to_parquet(filename, index=False)

    return filename


def get_test_data_path(data_type: str = "simple") -> str:
    """Get the path to test data file."""
    base_dir = os.path.join(os.path.dirname(__file__), "..", "..", "..", "test_data")
    os.makedirs(base_dir, exist_ok=True)

    return os.path.join(base_dir, f"{data_type}_data.csv")


def setup_test_environment() -> EnvironmentConfig:
    """Set up the test environment."""
    config = EnvironmentConfig()
    return config


def teardown_test_environment(config: EnvironmentConfig) -> None:
    """Tear down the test environment."""
    config.cleanup()


class TestEnvironment:
    """Test environment context manager."""

    def __init__(self):
        self.config = None

    def __enter__(self):
        self.config = setup_test_environment()
        return self.config

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.config:
            teardown_test_environment(self.config)


# Environment-specific configurations
MOCK_ENV_CONFIG = {
    "session_class": "MockSparkSession",
    "dataframe_class": "MockDataFrame",
    "functions_module": "mock_spark.functions",
    "types_module": "mock_spark.spark_types",
}

PYSPARK_ENV_CONFIG = {
    "session_class": "SparkSession",
    "dataframe_class": "DataFrame",
    "functions_module": "pyspark.sql.functions",
    "types_module": "pyspark.sql.types",
}


def get_environment_config(env_type: str) -> Dict[str, Any]:
    """Get configuration for the specified environment type."""
    if env_type == "mock":
        return MOCK_ENV_CONFIG
    elif env_type == "pyspark":
        return PYSPARK_ENV_CONFIG
    else:
        raise ValueError(f"Unknown environment type: {env_type}")


def import_environment_modules(env_type: str) -> Dict[str, Any]:
    """Import modules for the specified environment."""
    config = get_environment_config(env_type)
    modules = {}

    if env_type == "mock":
        import mock_spark
        from mock_spark import MockSparkSession
        from mock_spark import functions as mock_functions
        from mock_spark import spark_types as mock_types

        modules["spark_session"] = MockSparkSession
        modules["functions"] = mock_functions
        modules["types"] = mock_types
        modules["session"] = MockSparkSession("compatibility-test")

    elif env_type == "pyspark":
        import pyspark
        from pyspark import SparkContext
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as pyspark_functions
        from pyspark.sql import types as pyspark_types

        # Note: JAVA_TOOL_OPTIONS are set in conftest.py before any pyspark imports

        # Clean up any existing Spark sessions first
        try:
            active_session = SparkSession.getActiveSession()
            if active_session is not None:
                active_session.stop()

            active_context = SparkContext._active_spark_context
            if active_context is not None:
                active_context.stop()
        except Exception:
            pass

        # Create Spark session with test configuration optimized for compatibility
        # Use unique identifiers for parallel test execution isolation
        import uuid
        import tempfile

        session_id = str(uuid.uuid4())[:8]
        warehouse_dir = tempfile.mkdtemp(prefix=f"spark-warehouse-{session_id}-")

        spark = (
            SparkSession.builder.appName(f"compatibility-test-{session_id}")
            .master("local[1]")
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.execution.arrow.pyspark.enabled", "false")
            .config("spark.sql.execution.arrow.maxRecordsPerBatch", "0")
            .config("spark.driver.memory", "1g")
            .config("spark.executor.memory", "1g")
            .config("spark.driver.maxResultSize", "1g")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.warehouse.dir", warehouse_dir)
            .getOrCreate()
        )

        # Store warehouse dir for cleanup
        modules["_warehouse_dir"] = warehouse_dir

        # Set log level to reduce noise
        spark.sparkContext.setLogLevel("ERROR")

        modules["spark_session"] = SparkSession
        modules["functions"] = pyspark_functions
        modules["types"] = pyspark_types
        modules["session"] = spark

    return modules


def cleanup_environment(modules: Dict[str, Any], env_type: str) -> None:
    """Clean up environment resources."""
    if env_type == "pyspark" and "session" in modules:
        try:
            modules["session"].stop()
        except Exception:
            # Ignore errors during cleanup
            pass

        # Clean up warehouse directory if it was created
        if "_warehouse_dir" in modules:
            try:
                import shutil

                warehouse_dir = modules["_warehouse_dir"]
                if os.path.exists(warehouse_dir):
                    shutil.rmtree(warehouse_dir, ignore_errors=True)
            except Exception:
                pass

        # Also try to clean up any active Spark context
        try:
            from pyspark import SparkContext
            from pyspark.sql import SparkSession

            # Stop active session if any
            active_session = SparkSession.getActiveSession()
            if active_session is not None:
                active_session.stop()

            # Stop active context if any
            active_context = SparkContext._active_spark_context
            if active_context is not None:
                active_context.stop()
        except Exception:
            # Ignore cleanup errors
            pass
