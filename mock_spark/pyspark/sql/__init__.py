"""
PySpark SQL module - Drop-in replacement for pyspark.sql.

This module provides a complete PySpark-compatible SQL interface by re-exporting
all classes, functions, and types from mock_spark.sql.

Example:
    >>> from pyspark.sql import SparkSession, DataFrame, functions as F
    >>> from pyspark.sql.types import StringType, StructType, StructField
    >>> spark = SparkSession("MyApp")
    >>> df = spark.createDataFrame([{"name": "Alice", "age": 25}])
    >>> df.select(F.upper(F.col("name"))).show()
"""

import sys
from types import ModuleType

# Register this module in sys.modules for proper import resolution
sys.modules["pyspark.sql"] = sys.modules[__name__]

# Re-export all core classes from mock_spark.sql
from mock_spark.sql import (  # noqa: E402
    SparkSession,
    DataFrame,
    DataFrameWriter,
    GroupedData,
    Column,
    ColumnOperation,
    Row,
    Window,
    WindowSpec,
    Functions,
    F,
    functions,
    types,
    utils,
    PySparkTypeError,
    PySparkValueError,
)

__all__ = [
    # Core classes
    "SparkSession",
    "DataFrame",
    "DataFrameWriter",
    "GroupedData",
    "Column",
    "ColumnOperation",
    "Row",
    "Window",
    "WindowSpec",
    # Functions
    "Functions",
    "F",
    "functions",
    # Types submodule
    "types",
    # Utils submodule (exceptions)
    "utils",
    # Exceptions (PySpark 3.5+ compatibility)
    "PySparkTypeError",
    "PySparkValueError",
]
