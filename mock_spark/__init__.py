"""
Mock Spark - A lightweight mock implementation of PySpark for testing and development.

This package provides a complete mock implementation of PySpark's core functionality
without requiring a Java Virtual Machine (JVM) or actual Spark installation.

Core Features (PySpark API):
    - Complete PySpark API compatibility
    - No JVM required - pure Python implementation
    - DataFrame operations (select, filter, groupBy, join, etc.)
    - SQL query execution
    - Window functions with proper partitioning and ordering
    - 15+ data types including complex types (Array, Map, Struct)
    - Type-safe operations with automatic schema inference
    - Edge case handling (null values, unicode, large numbers)
    - 432 passing tests with 100% PySpark compatibility

Testing Utilities (Optional):
    Additional utilities to make testing easier:
    - Error simulation for testing error handling
    - Performance simulation for testing edge cases
    - Test data generation with realistic patterns

    Import explicitly when needed:
        from mock_spark.error_simulation import MockErrorSimulator
        from mock_spark.performance_simulation import MockPerformanceSimulator
        from mock_spark.data_generation import create_test_data

    See docs/testing_utilities_guide.md for details.

Quick Start:
    >>> from mock_spark import MockSparkSession, F
    >>> spark = MockSparkSession("MyApp")
    >>> data = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
    >>> df = spark.createDataFrame(data)
    >>> df.select(F.upper(F.col("name"))).show()
    MockDataFrame[2 rows, 1 columns]
    upper(name)
    ALICE
    BOB

Author: Odos Matthews
"""

import sys
from types import ModuleType

# Initialize PySpark version compatibility
from mock_spark._version_compat import check_version_from_env, check_version_from_marker

# Check for version compatibility settings
check_version_from_env()
check_version_from_marker()

from .session import MockSparkSession  # noqa: E402
from .session.context import MockSparkContext, MockJVMContext  # noqa: E402
from .dataframe import MockDataFrame, MockDataFrameWriter, MockGroupedData  # noqa: E402
from .functions import MockFunctions, MockColumn, MockColumnOperation, F  # noqa: E402
from .window import MockWindow, MockWindowSpec  # noqa: E402
from .delta import DeltaTable, DeltaMergeBuilder  # noqa: E402
from .spark_types import (  # noqa: E402
    MockDataType,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    BooleanType,
    DateType,
    TimestampType,
    DecimalType,
    ArrayType,
    MapType,
    BinaryType,
    NullType,
    FloatType,
    ShortType,
    ByteType,
    MockStructType,
    MockStructField,
)
from mock_spark.storage import MemoryStorageManager  # noqa: E402
from .errors import (  # noqa: E402
    MockException,
    AnalysisException,
    PySparkValueError,
    PySparkTypeError,
    PySparkRuntimeError,
    IllegalArgumentException,
)

# ==============================================================================
# TESTING UTILITIES - AVAILABLE VIA EXPLICIT IMPORT
# ==============================================================================
# These utilities are NOT imported here to keep the main namespace clean.
# Import them explicitly when needed:
#
#   from mock_spark.error_simulation import MockErrorSimulator
#   from mock_spark.performance_simulation import MockPerformanceSimulator
#   from mock_spark.data_generation import create_test_data
#
# Available modules:
#   - mock_spark.error_simulation - Error injection for testing
#   - mock_spark.performance_simulation - Performance testing utilities
#   - mock_spark.data_generation - Test data generation
# ==============================================================================

__version__ = "2.10.0"
__author__ = "Odos Matthews"
__email__ = "odosmatthews@gmail.com"

# ==============================================================================
# MAIN EXPORTS - CORE PYSPARK API
# ==============================================================================
# These are the primary exports that mirror PySpark's API.
# Use these for mocking PySpark in your tests.

__all__ = [
    # -------------------------------------------------------------------------
    # Session & Context (Core PySpark API)
    # -------------------------------------------------------------------------
    "MockSparkSession",  # Main entry point - like pyspark.sql.SparkSession
    "MockSparkContext",  # Spark context - like pyspark.SparkContext
    "MockJVMContext",  # JVM context compatibility
    # -------------------------------------------------------------------------
    # DataFrame & Operations (Core PySpark API)
    # -------------------------------------------------------------------------
    "MockDataFrame",  # DataFrame - like pyspark.sql.DataFrame
    "MockDataFrameWriter",  # Writer - like pyspark.sql.DataFrameWriter
    "MockGroupedData",  # Grouped data - like pyspark.sql.GroupedData
    # -------------------------------------------------------------------------
    # Functions & Columns (Core PySpark API)
    # -------------------------------------------------------------------------
    "MockFunctions",  # Functions module
    "MockColumn",  # Column - like pyspark.sql.Column
    "MockColumnOperation",  # Column operations
    "F",  # Functions shorthand - like pyspark.sql.functions
    # -------------------------------------------------------------------------
    # Window Functions (Core PySpark API)
    # -------------------------------------------------------------------------
    "MockWindow",  # Window - like pyspark.sql.Window
    "MockWindowSpec",  # Window spec - like pyspark.sql.WindowSpec
    # -------------------------------------------------------------------------
    # Delta Lake (Simple Support - Mock Operations)
    # -------------------------------------------------------------------------
    "DeltaTable",  # Basic Delta table wrapper
    "DeltaMergeBuilder",  # Delta MERGE builder (mock)
    # -------------------------------------------------------------------------
    # Data Types (Core PySpark API)
    # -------------------------------------------------------------------------
    "MockDataType",  # Base data type
    "StringType",  # String type
    "IntegerType",  # Integer type
    "LongType",  # Long type
    "DoubleType",  # Double type
    "FloatType",  # Float type
    "BooleanType",  # Boolean type
    "DateType",  # Date type
    "TimestampType",  # Timestamp type
    "DecimalType",  # Decimal type
    "ArrayType",  # Array type
    "MapType",  # Map type
    "StructType",  # Struct type (alias for MockStructType)
    "StructField",  # Struct field (alias for MockStructField)
    "MockStructType",  # Struct type
    "MockStructField",  # Struct field
    "BinaryType",  # Binary type
    "NullType",  # Null type
    "ShortType",  # Short type
    "ByteType",  # Byte type
    # -------------------------------------------------------------------------
    # Storage (Core Infrastructure)
    # -------------------------------------------------------------------------
    "MemoryStorageManager",  # Storage backend
    # -------------------------------------------------------------------------
    # Exceptions (PySpark-compatible)
    # -------------------------------------------------------------------------
    "MockException",  # Base exception
    "AnalysisException",  # Analysis exception - like pyspark.sql.utils.AnalysisException
    "PySparkValueError",  # Value error
    "PySparkTypeError",  # Type error
    "PySparkRuntimeError",  # Runtime error
    "IllegalArgumentException",  # Illegal argument exception
]


# Add type aliases for PySpark compatibility
StructType = MockStructType
StructField = MockStructField

# ==============================================================================
# DELTA MODULE ALIASING - Support "from delta.tables import DeltaTable"
# ==============================================================================
# This allows mock-spark to be used as a drop-in replacement for delta-spark
# in tests that import DeltaTable from delta.tables

# Create delta module and delta.tables submodule
delta_module = ModuleType("delta")
delta_tables_module = ModuleType("delta.tables")

# Export DeltaTable as the main class
delta_tables_module.DeltaTable = DeltaTable  # type: ignore[attr-defined]

# Set up module hierarchy
delta_module.tables = delta_tables_module  # type: ignore[attr-defined]

# Register modules in sys.modules
sys.modules["delta"] = delta_module
sys.modules["delta.tables"] = delta_tables_module
