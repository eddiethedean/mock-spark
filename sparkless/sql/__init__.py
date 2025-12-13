"""
Sparkless SQL module - PySpark-compatible SQL interface.

This module provides a complete mock implementation of PySpark's SQL module
that behaves identically to the real PySpark SQL interface for testing and development.

Key Features:
    - Complete PySpark SQL API compatibility
    - SparkSession, DataFrame, Column, Row, Window
    - All data types (StringType, IntegerType, etc.)
    - Functions namespace (F)
    - StructType and StructField for schema definition

Example:
    >>> from sparkless.sql import SparkSession, DataFrame, functions as F
    >>> from sparkless.sql.types import StringType, IntegerType, StructType, StructField
    >>> spark = SparkSession("MyApp")
    >>> data = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
    >>> df = spark.createDataFrame(data)
    >>> df.select(F.upper(F.col("name"))).show()
    DataFrame[2 rows, 1 columns]

    upper(name)
    ALICE
    BOB
"""

# Core classes
from ..session import SparkSession  # noqa: E402
from ..dataframe import DataFrame, DataFrameWriter, GroupedData  # noqa: E402
from ..functions import Column, ColumnOperation, F, Functions  # noqa: E402
from ..window import Window, WindowSpec  # noqa: E402
from ..spark_types import Row  # noqa: E402

# Import exceptions (PySpark 3.5+ compatibility)
from ..core.exceptions import PySparkTypeError, PySparkValueError  # noqa: E402

# Import types submodule
from . import types  # noqa: E402

# Import functions submodule
from . import functions  # noqa: E402

# Import utils submodule (PySpark-compatible exception exports)
from . import utils  # noqa: E402

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
