"""
Mock Spark - A lightweight mock implementation of PySpark for testing and development.

This package provides a complete mock implementation of PySpark's core functionality
without requiring a Java Virtual Machine (JVM) or actual Spark installation.

Key Features:
    - Complete PySpark API compatibility
    - No JVM required - pure Python implementation
    - Comprehensive test suite with 173 tests (80% pass rate)
    - Advanced functions (coalesce, isnull, upper, lower, length, abs, round)
    - Window functions with proper partitioning and ordering
    - Type-safe operations with proper schema inference
    - Edge case handling (null values, unicode, large numbers)

Example:
    >>> from mock_spark import MockSparkSession, F
    >>> spark = MockSparkSession("MyApp")
    >>> data = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
    >>> df = spark.createDataFrame(data)
    >>> df.select(F.upper(F.col("name"))).show()
    
Version: 0.1.0
Author: Odos Matthews
"""

from .session import MockSparkSession, MockSparkContext, MockJVMContext
from .dataframe import MockDataFrame, MockDataFrameWriter, MockGroupedData
from .functions import MockFunctions, MockColumn, MockColumnOperation, F
from .window import MockWindow, MockWindowSpec
from .spark_types import (
    MockDataType,
    StringType,
    IntegerType,
    DoubleType,
    BooleanType,
    MockStructType,
    MockStructField,
)
from .storage import MockStorageManager, MockTable
from .errors import (
    MockException,
    AnalysisException,
    PySparkValueError,
    PySparkTypeError,
    PySparkRuntimeError,
    IllegalArgumentException,
)

__version__ = "0.1.0"
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
    "DoubleType",
    "BooleanType",
    "MockStructType",
    "MockStructField",
    # Storage
    "MockStorageManager",
    "MockTable",
    # Exceptions
    "MockException",
    "AnalysisException",
    "PySparkValueError",
    "PySparkTypeError",
    "PySparkRuntimeError",
    "IllegalArgumentException",
]
