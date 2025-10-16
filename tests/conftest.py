"""
Global pytest configuration for mock-spark tests.

This configuration ensures proper resource cleanup to prevent test leaks.
"""

import pytest
import gc


@pytest.fixture(scope="function", autouse=True)
def cleanup_after_each_test():
    """Automatically clean up resources after each test.

    This fixture runs after every test to ensure DuckDB connections
    and other resources are properly cleaned up, preventing test leaks.
    """
    yield
    # Force garbage collection to trigger __del__ methods
    gc.collect()


@pytest.fixture
def mock_spark_session():
    """Create a MockSparkSession with automatic cleanup."""
    from mock_spark import MockSparkSession

    session = MockSparkSession("test_app")
    yield session
    # Explicitly clean up
    try:
        session.stop()
    except:  # noqa: E722
        pass
    gc.collect()


@pytest.fixture
def isolated_session():
    """Create an isolated MockSparkSession for tests requiring isolation."""
    from mock_spark import MockSparkSession
    import uuid

    # Use unique name to ensure isolation
    session_name = f"test_isolated_{uuid.uuid4().hex[:8]}"
    session = MockSparkSession(session_name)
    yield session
    try:
        session.stop()
    except:  # noqa: E722
        pass
    gc.collect()


@pytest.fixture(scope="session")
def delta_spark_session():
    """Create a PySpark session with Delta Lake configured.
    
    This fixture automatically downloads and configures Delta Lake JARs.
    It's scoped to 'session' to avoid repeatedly downloading JARs.
    """
    try:
        # Import must be done after checking for delta availability
        import os
        
        # Set environment variable BEFORE importing PySpark
        # Using delta-spark 3.2.1 which is compatible with PySpark 3.5.x
        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            "--packages io.delta:delta-spark_2.12:3.2.1 pyspark-shell"
        )
        
        from pyspark.sql import SparkSession
        
        # Create Spark session with Delta Lake extensions and JAR packages
        spark = SparkSession.builder \
            .appName("delta_test_session") \
            .master("local[1]") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.1") \
            .config("spark.driver.memory", "1g") \
            .config("spark.executor.memory", "1g") \
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
            .getOrCreate()
        
        yield spark
        
        # Cleanup
        try:
            spark.stop()
        except:  # noqa: E722
            pass
            
    except Exception as e:
        pytest.skip(f"Delta Lake environment not available: {e}")


@pytest.fixture(scope="function")
def pyspark_env(delta_spark_session):
    """Provide PySpark environment with Delta Lake support.
    
    This is a wrapper around delta_spark_session that provides
    the environment in a dict format expected by compatibility tests.
    """
    import tempfile
    tmpdir = tempfile.mkdtemp()
    
    yield {
        "spark": delta_spark_session,
        "tmpdir": tmpdir
    }
    
    # Cleanup temp directory
    import shutil
    try:
        shutil.rmtree(tmpdir, ignore_errors=True)
    except:  # noqa: E722
        pass


@pytest.fixture(scope="function")
def real_spark(delta_spark_session):
    """Provide real PySpark session with Delta Lake for compatibility tests."""
    return delta_spark_session


@pytest.fixture(scope="function")
def mock_spark():
    """Provide mock spark session for compatibility tests."""
    from mock_spark import MockSparkSession
    session = MockSparkSession("test_app")
    yield session
    try:
        session.stop()
    except:  # noqa: E722
        pass
    gc.collect()


def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "delta: mark test as requiring Delta Lake (may be skipped)"
    )
    config.addinivalue_line(
        "markers", "performance: mark test as a performance benchmark"
    )
