"""
Mock Spark - A lightweight mock implementation of PySpark for testing and development.

This package provides a complete mock implementation of PySpark's core functionality
without requiring a Java Virtual Machine (JVM) or actual Spark installation.
"""

from .session import MockSparkSession, MockSparkContext, MockJVMContext
from .dataframe import MockDataFrame, MockDataFrameWriter, MockGroupedData
from .functions import MockFunctions, MockColumn, MockColumnOperation, F
from .spark_types import (
    MockDataType, StringType, IntegerType, DoubleType, BooleanType,
    MockStructType, MockStructField
)
from .storage import MockStorageManager, MockTable
from .errors import (
    MockException, AnalysisException, PySparkValueError,
    PySparkTypeError, PySparkRuntimeError, IllegalArgumentException
)

__version__ = "1.0.0"
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