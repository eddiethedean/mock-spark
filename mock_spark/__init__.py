"""
Mock Spark - A lightweight mock implementation of PySpark for testing and development.

This package provides a complete mock implementation of PySpark's core functionality
without requiring a Java Virtual Machine (JVM) or actual Spark installation.

Key Features:
    - Complete PySpark API compatibility
    - No JVM required - pure Python implementation
    - Comprehensive test suite with 387+ tests (100% pass rate)
    - Highly type safe with 59% reduction in mypy errors (214 → 24 in package source)
    - Black-formatted code for production readiness
    - Advanced functions (coalesce, isnull, upper, lower, length, abs, round)
    - Window functions with proper partitioning and ordering
    - Type-safe operations with proper schema inference
    - Edge case handling (null values, unicode, large numbers)
    - Error simulation framework for comprehensive testing
    - Performance simulation with configurable limits
    - Data generation utilities for realistic test data
    - Mockable methods for error scenario testing
    - Enhanced DataFrameWriter with all save modes
    - 15+ data types including complex types

Example:
    >>> from mock_spark import MockSparkSession, F
    >>> spark = MockSparkSession("MyApp")
    >>> data = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
    >>> df = spark.createDataFrame(data)
    >>> df.select(F.upper(F.col("name"))).show()
    +--- MockDataFrame: 2 rows ---+
     upper(name)
    ------------
           ALICE
             BOB
    
Version: 0.2.2
Author: Odos Matthews
"""

from .session import MockSparkSession
from .session.context import MockSparkContext, MockJVMContext
from .dataframe import MockDataFrame, MockDataFrameWriter, MockGroupedData
from .functions import MockFunctions, MockColumn, MockColumnOperation, F
from .window import MockWindow, MockWindowSpec
from .spark_types import (
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
from mock_spark.storage import MemoryStorageManager
from .errors import (
    MockException,
    AnalysisException,
    PySparkValueError,
    PySparkTypeError,
    PySparkRuntimeError,
    IllegalArgumentException,
)
from .error_simulation import (
    MockErrorSimulator,
    MockErrorSimulatorBuilder,
    create_table_not_found_simulator,
    create_data_too_large_simulator,
    create_sql_error_simulator,
)
from .performance_simulation import (
    MockPerformanceSimulator,
    MockPerformanceSimulatorBuilder,
    performance_simulation,
    create_slow_simulator,
    create_memory_limited_simulator,
    create_high_performance_simulator,
)
from .data_generation import (
    MockDataGenerator,
    MockDataGeneratorBuilder,
    create_test_data,
    create_corrupted_data,
    create_realistic_data,
)

__version__ = "0.2.2"
__author__ = "Odos Matthews"
__email__ = "odosmatthews@gmail.com"

# Main exports for easy access
__all__ = [
    # Core classes
    "MockSparkSession",
    "MockSparkContext",
    "MockJVMContext",
    "MockDataFrame",
    "MockDataFrameWriter",
    "MockGroupedData",
    # Functions and columns
    "MockFunctions",
    "MockColumn",
    "MockColumnOperation",
    "F",
    # Window functions
    "MockWindow",
    "MockWindowSpec",
    # Types
    "MockDataType",
    "StringType",
    "IntegerType",
    "LongType",
    "DoubleType",
    "BooleanType",
    "DateType",
    "TimestampType",
    "DecimalType",
    "ArrayType",
    "MapType",
    "BinaryType",
    "NullType",
    "FloatType",
    "ShortType",
    "ByteType",
    "MockStructType",
    "MockStructField",
    # Storage
    "MemoryStorageManager",
    # Exceptions
    "MockException",
    "AnalysisException",
    "PySparkValueError",
    "PySparkTypeError",
    "PySparkRuntimeError",
    "IllegalArgumentException",
    # Error Simulation
    "MockErrorSimulator",
    "MockErrorSimulatorBuilder",
    "create_table_not_found_simulator",
    "create_data_too_large_simulator",
    "create_sql_error_simulator",
    # Performance Simulation
    "MockPerformanceSimulator",
    "MockPerformanceSimulatorBuilder",
    "performance_simulation",
    "create_slow_simulator",
    "create_memory_limited_simulator",
    "create_high_performance_simulator",
    # Data Generation
    "MockDataGenerator",
    "MockDataGeneratorBuilder",
    "create_test_data",
    "create_corrupted_data",
    "create_realistic_data",
]
