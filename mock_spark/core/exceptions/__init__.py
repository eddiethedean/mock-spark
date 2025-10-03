"""
Exception hierarchy for Mock Spark.

This module provides a comprehensive exception hierarchy that matches
PySpark's exception structure for maximum compatibility.
"""

from .base import MockException, MockSparkException
from .analysis import AnalysisException, ParseException
from .execution import QueryExecutionException, SparkUpgradeException
from .validation import IllegalArgumentException, PySparkValueError, PySparkTypeError
from .runtime import PySparkRuntimeError, PySparkAttributeError

__all__ = [
    "MockException",
    "MockSparkException", 
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
