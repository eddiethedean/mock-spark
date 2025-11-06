"""
Mock Spark SQL Functions module - PySpark-compatible functions interface.

This module provides access to all PySpark functions, mirroring pyspark.sql.functions.

In PySpark, you can import functions in two ways:
    1. from pyspark.sql import functions as F
    2. from pyspark.sql.functions import col, upper, etc.

This module supports both patterns.

Example:
    >>> from mock_spark.sql import functions as F
    >>> from mock_spark.sql import SparkSession
    >>> spark = SparkSession("test")
    >>> df = spark.createDataFrame([{"name": "Alice", "age": 25}])
    >>> df.select(F.upper(F.col("name")), F.col("age") * 2).show()

    >>> from mock_spark.sql.functions import col, upper
    >>> df.select(upper(col("name"))).show()
"""

# Import F and Functions
from ..functions import F, Functions  # noqa: E402

# Re-export F as the default
__all__ = ["F", "Functions"]

# For compatibility with: from mock_spark.sql.functions import col, upper, etc.
# We need to make all functions available at the module level
# This is done by copying all public attributes from F to this module
import sys

# Get the current module
_current_module = sys.modules[__name__]

# Copy all public attributes from F to this module
# This allows: from mock_spark.sql.functions import col, upper, etc.
for attr_name in dir(F):
    if not attr_name.startswith("_"):
        attr_value = getattr(F, attr_name)
        # Only copy callable attributes (functions) and non-private attributes
        if callable(attr_value) or not attr_name.startswith("_"):
            setattr(_current_module, attr_name, attr_value)
            if attr_name not in __all__:
                __all__.append(attr_name)
