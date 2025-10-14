"""
Shared fixtures and configuration for compatibility tests.
"""

import os
import subprocess

# Set Java 11+ compatibility flags BEFORE any pyspark imports
# This must be done early so the JVM launches with correct options
if "JAVA_TOOL_OPTIONS" not in os.environ or "add-opens" not in os.environ.get(
    "JAVA_TOOL_OPTIONS", ""
):
    os.environ["JAVA_TOOL_OPTIONS"] = (
        "--add-opens=java.base/java.lang=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
        "--add-opens=java.base/java.io=ALL-UNNAMED "
        "--add-opens=java.base/java.net=ALL-UNNAMED "
        "--add-opens=java.base/java.nio=ALL-UNNAMED "
        "--add-opens=java.base/java.util=ALL-UNNAMED "
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED "
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
    )


# Auto-detect and set Java 11 for PySpark 3.2.x compatibility
def _find_java11():
    """Find Java 11 installation."""
    import glob

    candidates = []

    # Priority 1: Check Homebrew openjdk@11 directory for any 11.x version
    try:
        homebrew_java11 = glob.glob(
            "/opt/homebrew/Cellar/openjdk@11/*/libexec/openjdk.jdk/Contents/Home"
        )
        if homebrew_java11:
            # Use latest version if multiple found
            candidates.append(sorted(homebrew_java11)[-1])
    except Exception:
        pass

    # Priority 2: Check /usr/libexec/java_home for Java 11
    # Note: This sometimes returns wrong version, so we validate it
    try:
        result = subprocess.run(
            ["/usr/libexec/java_home", "-v", "11"], capture_output=True, text=True, timeout=2
        )
        if result.returncode == 0 and result.stdout.strip():
            path = result.stdout.strip()
            # Validate it's actually Java 11
            if "11" in path or "1.11" in path:
                candidates.append(path)
    except Exception:
        pass

    # Return first valid candidate
    for path in candidates:
        if os.path.exists(path):
            return path
    return None


java11_home = _find_java11()
if java11_home:
    # Force Java 11 by setting JAVA_HOME and prepending to PATH
    os.environ["JAVA_HOME"] = java11_home
    # Remove any existing java from PATH and add Java 11 first
    path_parts = [p for p in os.environ.get("PATH", "").split(":") if "java" not in p.lower()]
    os.environ["PATH"] = f"{java11_home}/bin:{':'.join(path_parts)}"
else:
    # Warn that Java 11 is required
    import warnings

    warnings.warn(
        "Java 11 not found! PySpark 3.2.x requires Java 11. "
        "Install with: brew install openjdk@11 "
        "Or run: source tests/setup_spark_env.sh",
        UserWarning,
    )

import pytest  # noqa: E402
from tests.compatibility.utils.environment import (  # noqa: E402
    import_environment_modules,
    cleanup_environment,
)
from tests.compatibility.utils.fixtures import get_test_dataframes_dict  # noqa: E402


@pytest.fixture(scope="session")
def mock_environment():
    """Fixture for mock_spark environment."""
    modules = import_environment_modules("mock")
    yield modules
    cleanup_environment(modules, "mock")


@pytest.fixture(scope="function")
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


@pytest.fixture
def real_spark(pyspark_environment):
    """Fixture providing PySpark session (real_spark alias)."""
    return pyspark_environment["session"]


@pytest.fixture
def mock_spark(mock_environment):
    """Fixture providing mock_spark session (mock_spark alias)."""
    return mock_environment["session"]


@pytest.fixture(autouse=True)
def setup_test_environment():
    """Automatic setup/teardown for test environment."""
    # Setup
    yield
    # Teardown (handled by individual fixtures)
