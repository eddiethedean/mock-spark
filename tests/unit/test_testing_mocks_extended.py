"""Extended unit tests for testing mock utilities."""

import pytest
from mock_spark.testing.mocks import MockDataFrameFactory, MockSessionFactory


def test_mock_dataframe_factory_creation():
    """Test MockDataFrameFactory can be created."""
    factory = MockDataFrameFactory()
    assert factory is not None


def test_create_simple_dataframe():
    """Test creating simple DataFrame with factory."""
    factory = MockDataFrameFactory()
    df = factory.create_simple(rows=10, columns=3)
    
    assert df is not None
    assert df.count() == 10


def test_create_dataframe_with_schema():
    """Test creating DataFrame with specific schema."""
    from mock_spark.spark_types import MockStructType, MockStructField, IntegerType
    
    factory = MockDataFrameFactory()
    schema = MockStructType([MockStructField("id", IntegerType())])
    df = factory.create_with_schema(schema, rows=5)
    
    assert df is not None
    assert df.count() == 5


def test_create_empty_dataframe():
    """Test creating empty DataFrame."""
    factory = MockDataFrameFactory()
    df = factory.create_empty()
    
    assert df is not None
    assert df.count() == 0


def test_create_dataframe_with_nulls():
    """Test creating DataFrame with null values."""
    factory = MockDataFrameFactory()
    df = factory.create_with_nulls(rows=10, null_rate=0.3)
    
    assert df is not None
    assert df.count() == 10


def test_create_large_dataframe():
    """Test creating large DataFrame."""
    factory = MockDataFrameFactory()
    df = factory.create_large(rows=1000)
    
    assert df is not None
    assert df.count() == 1000


def test_mock_session_factory_creation():
    """Test MockSessionFactory can be created."""
    factory = MockSessionFactory()
    assert factory is not None


def test_create_test_session():
    """Test creating test session."""
    factory = MockSessionFactory()
    session = factory.create_session(app_name="test")
    
    assert session is not None
    session.stop()


def test_create_session_with_config():
    """Test creating session with configuration."""
    factory = MockSessionFactory()
    config = {"spark.sql.shuffle.partitions": "10"}
    session = factory.create_session_with_config(config)
    
    assert session is not None
    session.stop()


def test_create_isolated_session():
    """Test creating isolated session."""
    factory = MockSessionFactory()
    session = factory.create_isolated_session()
    
    assert session is not None
    session.stop()


def test_mock_column_creation():
    """Test mocking column operations."""
    from mock_spark.testing.mocks import MockColumnBuilder
    
    builder = MockColumnBuilder()
    col = builder.create_column("test_col")
    
    assert col is not None


def test_mock_function_creation():
    """Test mocking function operations."""
    from mock_spark.testing.mocks import MockFunctionBuilder
    
    builder = MockFunctionBuilder()
    func = builder.create_function("upper")
    
    assert func is not None


def test_create_dataframe_with_dates():
    """Test creating DataFrame with date columns."""
    factory = MockDataFrameFactory()
    df = factory.create_with_dates(rows=10, date_column="date")
    
    assert df is not None
    assert "date" in df.columns


def test_create_dataframe_with_timestamps():
    """Test creating DataFrame with timestamp columns."""
    factory = MockDataFrameFactory()
    df = factory.create_with_timestamps(rows=10)
    
    assert df is not None


def test_create_dataframe_with_arrays():
    """Test creating DataFrame with array columns."""
    factory = MockDataFrameFactory()
    df = factory.create_with_arrays(rows=5)
    
    assert df is not None


def test_create_dataframe_with_maps():
    """Test creating DataFrame with map columns."""
    factory = MockDataFrameFactory()
    df = factory.create_with_maps(rows=5)
    
    assert df is not None


def test_create_dataframe_with_structs():
    """Test creating DataFrame with struct columns."""
    factory = MockDataFrameFactory()
    df = factory.create_with_structs(rows=5)
    
    assert df is not None


def test_mock_error_dataframe():
    """Test creating DataFrame that simulates errors."""
    factory = MockDataFrameFactory()
    df = factory.create_with_errors(error_type="read")
    
    assert df is not None


def test_mock_slow_dataframe():
    """Test creating DataFrame that simulates slow operations."""
    factory = MockDataFrameFactory()
    df = factory.create_slow(delay_ms=100)
    
    assert df is not None


def test_create_partitioned_dataframe():
    """Test creating partitioned DataFrame."""
    factory = MockDataFrameFactory()
    df = factory.create_partitioned(rows=100, num_partitions=4)
    
    assert df is not None

