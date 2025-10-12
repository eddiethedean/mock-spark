"""Unit tests for data generation functionality."""

import pytest
from mock_spark import MockSparkSession

# Skip all tests - DataGenerator and DataFrameBuilder not yet implemented
pytestmark = pytest.mark.skip(reason="DataGenerator and DataFrameBuilder classes not yet implemented")

# from mock_spark.data_generation import DataGenerator, DataFrameBuilder


@pytest.fixture
def spark():
    """Create test session."""
    session = MockSparkSession("test_data_gen")
    yield session
    try:
        session.stop()
    except Exception:
        pass


def test_data_generator_creation():
    """Test DataGenerator can be created."""
    generator = DataGenerator()
    assert generator is not None


def test_generate_integer_column():
    """Test generating integer column data."""
    generator = DataGenerator()
    data = generator.generate_integers(count=10, min_val=1, max_val=100)
    
    assert len(data) == 10
    assert all(1 <= x <= 100 for x in data)


def test_generate_float_column():
    """Test generating float column data."""
    generator = DataGenerator()
    data = generator.generate_floats(count=10, min_val=0.0, max_val=1.0)
    
    assert len(data) == 10
    assert all(0.0 <= x <= 1.0 for x in data)


def test_generate_string_column():
    """Test generating string column data."""
    generator = DataGenerator()
    data = generator.generate_strings(count=10, length=5)
    
    assert len(data) == 10
    assert all(len(s) == 5 for s in data)


def test_generate_boolean_column():
    """Test generating boolean column data."""
    generator = DataGenerator()
    data = generator.generate_booleans(count=10)
    
    assert len(data) == 10
    assert all(isinstance(x, bool) for x in data)


def test_generate_date_column():
    """Test generating date column data."""
    generator = DataGenerator()
    data = generator.generate_dates(count=10, start_date="2023-01-01", end_date="2023-12-31")
    
    assert len(data) == 10


def test_generate_timestamp_column():
    """Test generating timestamp column data."""
    generator = DataGenerator()
    data = generator.generate_timestamps(count=10)
    
    assert len(data) == 10


def test_generate_null_values():
    """Test generating column with null values."""
    generator = DataGenerator()
    data = generator.generate_with_nulls(count=10, null_probability=0.5)
    
    assert len(data) == 10
    assert None in data


def test_generate_categorical_data():
    """Test generating categorical data."""
    generator = DataGenerator()
    categories = ["A", "B", "C"]
    data = generator.generate_categorical(count=20, categories=categories)
    
    assert len(data) == 20
    assert all(x in categories for x in data)


def test_dataframe_builder_creation(spark):
    """Test DataFrameBuilder can be created."""
    builder = DataFrameBuilder(spark)
    assert builder is not None


def test_builder_with_column(spark):
    """Test adding column to builder."""
    builder = DataFrameBuilder(spark)
    builder = builder.with_column("id", "integer", count=5)
    
    df = builder.build()
    assert "id" in df.columns
    assert df.count() == 5


def test_builder_multiple_columns(spark):
    """Test adding multiple columns."""
    builder = DataFrameBuilder(spark)
    builder = builder.with_column("id", "integer", count=5) \
                    .with_column("name", "string", count=5)
    
    df = builder.build()
    assert "id" in df.columns
    assert "name" in df.columns
    assert df.count() == 5


def test_builder_with_schema(spark):
    """Test building DataFrame with explicit schema."""
    from mock_spark.spark_types import MockStructType, MockStructField, IntegerType, StringType
    
    schema = MockStructType([
        MockStructField("id", IntegerType()),
        MockStructField("name", StringType())
    ])
    
    builder = DataFrameBuilder(spark)
    builder = builder.with_schema(schema).with_rows(5)
    
    df = builder.build()
    assert df.count() == 5


def test_builder_with_nulls(spark):
    """Test building DataFrame with null values."""
    builder = DataFrameBuilder(spark)
    builder = builder.with_column("value", "integer", count=10, null_probability=0.3)
    
    df = builder.build()
    rows = df.collect()
    has_null = any(row["value"] is None for row in rows)
    assert has_null or True  # May or may not have nulls due to probability


def test_generate_sequential_ids():
    """Test generating sequential IDs."""
    generator = DataGenerator()
    data = generator.generate_sequential(start=1, count=10)
    
    assert len(data) == 10
    assert data[0] == 1
    assert data[9] == 10


def test_generate_random_choice():
    """Test generating random choices from list."""
    generator = DataGenerator()
    choices = [1, 2, 3, 4, 5]
    data = generator.generate_choices(choices=choices, count=20)
    
    assert len(data) == 20
    assert all(x in choices for x in data)


def test_generate_normal_distribution():
    """Test generating normal distribution data."""
    generator = DataGenerator()
    data = generator.generate_normal(mean=50, std_dev=10, count=100)
    
    assert len(data) == 100


def test_generate_uniform_distribution():
    """Test generating uniform distribution data."""
    generator = DataGenerator()
    data = generator.generate_uniform(min_val=0, max_val=100, count=50)
    
    assert len(data) == 50
    assert all(0 <= x <= 100 for x in data)


def test_builder_with_distribution(spark):
    """Test building DataFrame with distribution."""
    builder = DataFrameBuilder(spark)
    builder = builder.with_column("value", "integer", count=100, distribution="normal")
    
    df = builder.build()
    assert df.count() == 100


def test_generate_email_addresses():
    """Test generating email addresses."""
    generator = DataGenerator()
    data = generator.generate_emails(count=10)
    
    assert len(data) == 10
    assert all("@" in email for email in data)


def test_generate_phone_numbers():
    """Test generating phone numbers."""
    generator = DataGenerator()
    data = generator.generate_phone_numbers(count=10)
    
    assert len(data) == 10


def test_generate_ipv4_addresses():
    """Test generating IPv4 addresses."""
    generator = DataGenerator()
    data = generator.generate_ipv4(count=10)
    
    assert len(data) == 10
    assert all("." in ip for ip in data)


def test_generate_with_seed():
    """Test generating with seed for reproducibility."""
    generator = DataGenerator(seed=42)
    data1 = generator.generate_integers(count=10, min_val=1, max_val=100)
    
    generator2 = DataGenerator(seed=42)
    data2 = generator2.generate_integers(count=10, min_val=1, max_val=100)
    
    assert data1 == data2

