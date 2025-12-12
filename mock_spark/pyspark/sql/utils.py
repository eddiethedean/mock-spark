"""
PySpark SQL Utils module - Drop-in replacement for pyspark.sql.utils.

This module provides exception exports matching PySpark's pyspark.sql.utils structure,
enabling drop-in replacement compatibility for testing scenarios.

In PySpark, exceptions are imported from:
    from pyspark.sql.utils import AnalysisException

This module provides the same interface for mock-spark:
    from pyspark.sql.utils import AnalysisException

Example:
    >>> from pyspark.sql.utils import AnalysisException
    >>> raise AnalysisException("Column 'unknown' does not exist")
    AnalysisException: Column 'unknown' does not exist
"""

import sys
from types import ModuleType

# Register this module in sys.modules for proper import resolution
sys.modules["pyspark.sql.utils"] = sys.modules[__name__]

# Re-export all exceptions from mock_spark.sql.utils to match PySpark structure
from mock_spark.sql.utils import (  # noqa: E402
    MockException,
    SparkException,
    PySparkException,
    AnalysisException,
    ParseException,
    SchemaException,
    ColumnNotFoundException,
    TableNotFoundException,
    DatabaseNotFoundException,
    TypeMismatchException,
    QueryExecutionException,
    SparkUpgradeException,
    StreamingQueryException,
    TempTableAlreadyExistsException,
    UnsupportedOperationException,
    ResourceException,
    MemoryException,
    IllegalArgumentException,
    PySparkValueError,
    PySparkTypeError,
    ValidationException,
    PySparkRuntimeError,
    PySparkAttributeError,
    ConfigurationException,
)

__all__ = [
    # Base exceptions
    "MockException",
    "SparkException",
    "PySparkException",
    # Analysis exceptions (primary PySpark compatibility)
    "AnalysisException",
    "ParseException",
    "SchemaException",
    "ColumnNotFoundException",
    "TableNotFoundException",
    "DatabaseNotFoundException",
    "TypeMismatchException",
    # Execution exceptions
    "QueryExecutionException",
    "SparkUpgradeException",
    "StreamingQueryException",
    "TempTableAlreadyExistsException",
    "UnsupportedOperationException",
    "ResourceException",
    "MemoryException",
    # Validation exceptions
    "IllegalArgumentException",
    "PySparkValueError",
    "PySparkTypeError",
    "ValidationException",
    # Runtime exceptions
    "PySparkRuntimeError",
    "PySparkAttributeError",
    "ConfigurationException",
]
