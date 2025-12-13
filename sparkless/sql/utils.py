"""
Sparkless SQL Utils module - PySpark-compatible exception exports.

This module provides exception exports matching PySpark's pyspark.sql.utils structure,
enabling drop-in replacement compatibility for testing scenarios.

In PySpark, exceptions are imported from:
    from pyspark.sql.utils import AnalysisException

This module provides the same interface for sparkless:
    from sparkless.sql.utils import AnalysisException

Example:
    >>> from sparkless.sql.utils import AnalysisException
    >>> raise AnalysisException("Column 'unknown' does not exist")
    AnalysisException: Column 'unknown' does not exist
"""

# Re-export all exceptions from core.exceptions to match PySpark structure
from ..core.exceptions.base import MockException, SparkException
from ..core.exceptions.analysis import (
    AnalysisException,
    ParseException,
    SchemaException,
    ColumnNotFoundException,
    TableNotFoundException,
    DatabaseNotFoundException,
    TypeMismatchException,
)
from ..core.exceptions.execution import (
    QueryExecutionException,
    SparkUpgradeException,
    StreamingQueryException,
    TempTableAlreadyExistsException,
    UnsupportedOperationException,
    ResourceException,
    MemoryException,
)
from ..core.exceptions.validation import (
    IllegalArgumentException,
    PySparkValueError,
    PySparkTypeError,
    ValidationException,
)
from ..core.exceptions.runtime import (
    PySparkRuntimeError,
    PySparkAttributeError,
    ConfigurationException,
)

# Alias for PySparkException (same as SparkException)
PySparkException = SparkException

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
