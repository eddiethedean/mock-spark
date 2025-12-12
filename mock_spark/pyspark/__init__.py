"""
PySpark namespace package for mock-spark.

This module provides a PySpark-compatible namespace that allows code written
for PySpark to work seamlessly with mock-spark without any import changes.

Example:
    >>> from pyspark.sql import SparkSession, functions as F
    >>> from pyspark.sql.types import StringType, StructType
    >>> from pyspark.sql.utils import AnalysisException
    
All imports work identically to PySpark, enabling drop-in replacement.
"""

import sys
from types import ModuleType

# Create pyspark module structure
pyspark_module = ModuleType("pyspark")
sys.modules["pyspark"] = pyspark_module

# Import and expose sql submodule
from . import sql  # noqa: E402

pyspark_module.sql = sql  # type: ignore[attr-defined]

__all__ = ["sql"]
