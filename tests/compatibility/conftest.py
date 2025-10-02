"""
Shared fixtures and configuration for compatibility tests.
"""

import pytest
from typing import Dict, Any, List
from tests.compatibility.utils.environment import import_environment_modules, cleanup_environment
from tests.compatibility.utils.fixtures import get_test_dataframes_dict


@pytest.fixture(scope="session")
def mock_environment():
    """Fixture for mock_spark environment."""
    modules = import_environment_modules("mock")
    yield modules
    cleanup_environment(modules, "mock")


@pytest.fixture(scope="session")
def pyspark_environment():
    """Fixture for PySpark environment."""
    modules = import_environment_modules("pyspark")
    yield modules
    cleanup_environment(modules, "pyspark")


@pytest.fixture
def test_data():
    """Fixture providing all test datasets."""
    return get_test_dataframes_dict()


@pytest.fixture
def simple_data(test_data):
    """Fixture providing simple test data."""
    return test_data["simple"]


@pytest.fixture
def complex_data(test_data):
    """Fixture providing complex test data."""
    return test_data["complex"]


@pytest.fixture
def numerical_data(test_data):
    """Fixture providing numerical test data."""
    return test_data["numerical"]


@pytest.fixture
def empty_data(test_data):
    """Fixture providing empty test data."""
    return test_data["empty"]


@pytest.fixture
def mock_dataframe(mock_environment, simple_data):
    """Fixture providing mock_spark DataFrame."""
    session = mock_environment["session"]
    return session.createDataFrame(simple_data)


@pytest.fixture
def pyspark_dataframe(pyspark_environment, simple_data):
    """Fixture providing PySpark DataFrame."""
    session = pyspark_environment["session"]
    return session.createDataFrame(simple_data)


@pytest.fixture
def mock_empty_dataframe(mock_environment):
    """Fixture providing empty mock_spark DataFrame."""
    session = mock_environment["session"]
    types = mock_environment["types"]
    # Create empty DataFrame with the same schema as PySpark
    schema = types.MockStructType(
        [
            types.MockStructField("id", types.IntegerType(), True),
            types.MockStructField("name", types.StringType(), True),
        ]
    )
    return session.createDataFrame([], schema)


@pytest.fixture
def pyspark_empty_dataframe(pyspark_environment):
    """Fixture providing empty PySpark DataFrame."""
    session = pyspark_environment["session"]
    types = pyspark_environment["types"]
    # Create empty DataFrame with a simple schema
    schema = types.StructType(
        [
            types.StructField("id", types.IntegerType(), True),
            types.StructField("name", types.StringType(), True),
        ]
    )
    return session.createDataFrame([], schema)


@pytest.fixture
def mock_complex_dataframe(mock_environment, complex_data):
    """Fixture providing mock_spark DataFrame with complex data."""
    session = mock_environment["session"]
    return session.createDataFrame(complex_data)


@pytest.fixture
def pyspark_complex_dataframe(pyspark_environment, complex_data):
    """Fixture providing PySpark DataFrame with complex data."""
    session = pyspark_environment["session"]
    return session.createDataFrame(complex_data)


@pytest.fixture
def mock_functions(mock_environment):
    """Fixture providing mock_spark functions."""
    return mock_environment["functions"]


@pytest.fixture
def pyspark_functions(pyspark_environment):
    """Fixture providing PySpark functions."""
    return pyspark_environment["functions"]


@pytest.fixture
def mock_types(mock_environment):
    """Fixture providing mock_spark types."""
    return mock_environment["types"]


@pytest.fixture
def pyspark_types(pyspark_environment):
    """Fixture providing PySpark types."""
    return pyspark_environment["types"]


@pytest.fixture
def comparison_tolerance():
    """Fixture providing numerical comparison tolerance."""
    return 1e-6


@pytest.fixture(autouse=True)
def setup_test_environment():
    """Automatic setup/teardown for test environment."""
    # Setup
    yield
    # Teardown (handled by individual fixtures)
