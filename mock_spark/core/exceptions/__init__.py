"""
Exception hierarchy for Mock Spark.

This module provides a comprehensive exception hierarchy that matches
PySpark's exception structure for maximum compatibility. All exceptions
are properly typed and provide clear error messages for debugging.

Exception Categories:
    - Base exceptions: MockException, SparkException
    - Analysis exceptions: AnalysisException, ParseException
    - Execution exceptions: QueryExecutionException, SparkUpgradeException
    - Validation exceptions: IllegalArgumentException, PySparkValueError, PySparkTypeError
    - Runtime exceptions: PySparkRuntimeError, PySparkAttributeError

Benefits:
    - Complete PySpark exception compatibility
    - Clear error categorization for better debugging
    - Proper exception chaining and context
    - Type-safe exception handling

Example:
    >>> from mock_spark.core.exceptions import AnalysisException
    >>> raise AnalysisException("Column 'unknown' does not exist")
    AnalysisException: Column 'unknown' does not exist
"""

from .base import MockException, SparkException
from .analysis import AnalysisException, ParseException
from .execution import QueryExecutionException, SparkUpgradeException
from .validation import IllegalArgumentException, PySparkValueError, PySparkTypeError
from .runtime import PySparkRuntimeError, PySparkAttributeError

__all__ = [
    "MockException",
    "SparkException",
    "AnalysisException",
    "ParseException",
    "QueryExecutionException",
    "SparkUpgradeException",
    "IllegalArgumentException",
    "PySparkValueError",
    "PySparkTypeError",
    "PySparkRuntimeError",
    "PySparkAttributeError",
]
