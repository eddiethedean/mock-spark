"""
PySpark SQL Functions module - Drop-in replacement for pyspark.sql.functions.

This module provides access to all PySpark functions by re-exporting from
mock_spark.sql.functions, enabling seamless compatibility with PySpark code.

In PySpark, you can import functions in two ways:
    1. from pyspark.sql import functions as F
    2. from pyspark.sql.functions import col, upper, etc.

This module supports both patterns and behaves identically to PySpark.

Example:
    >>> from pyspark.sql import functions as F
    >>> from pyspark.sql import SparkSession
    >>> spark = SparkSession("test")
    >>> df = spark.createDataFrame([{"name": "Alice", "age": 25}])
    >>> df.select(F.upper(F.col("name")), F.col("age") * 2).show()

    >>> from pyspark.sql.functions import col, upper
    >>> df.select(upper(col("name"))).show()
"""

import sys
from types import ModuleType
from typing import Any

# Register this module in sys.modules for proper import resolution
sys.modules["pyspark.sql.functions"] = sys.modules[__name__]

# Import F and Functions from mock_spark.sql.functions
from mock_spark.sql.functions import F, Functions  # noqa: E402

# Cache for dynamically accessed attributes
_cached_attrs: dict[str, object] = {}

# Build __all__ list with all public functions from F
__all__ = ["F", "Functions"]

# Pre-populate module-level attributes for all public functions
# This allows: from pyspark.sql.functions import col, upper, etc.
for attr_name in dir(F):
    if not attr_name.startswith("_"):
        attr_value = getattr(F, attr_name)
        # Only expose callable attributes (functions) and non-private attributes
        if callable(attr_value) or not attr_name.startswith("_"):
            _cached_attrs[attr_name] = attr_value
            if attr_name not in __all__:
                __all__.append(attr_name)


def __getattr__(name: str) -> object:
    """Dynamically provide access to functions from F instance.

    This makes the module behave like PySpark's functions module,
    where all functions are available at module level.
    """
    if name in _cached_attrs:
        return _cached_attrs[name]

    # Try to get from F instance
    if hasattr(F, name):
        attr_value = getattr(F, name)
        _cached_attrs[name] = attr_value
        return attr_value

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
