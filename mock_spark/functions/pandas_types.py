"""
Pandas UDF types for Mock Spark.

This module provides the PandasUDFType class for specifying different
types of Pandas UDFs in PySpark.
"""


class PandasUDFType:
    """Pandas UDF types for different execution modes (all PySpark versions).
    
    Defines constants for specifying the type of Pandas UDF:
    - SCALAR: Scalar Pandas UDF (one row at a time)
    - GROUPED_MAP: Grouped map Pandas UDF (group → DataFrame → DataFrame)
    - GROUPED_AGG: Grouped aggregate Pandas UDF (group → Series → scalar)
    - SCALAR_ITER: Scalar iterator Pandas UDF (batch processing)
    - MAP_ITER: Map iterator Pandas UDF (partition processing)
    
    Example:
        >>> from mock_spark.functions import PandasUDFType, pandas_udf
        >>> @pandas_udf("long", PandasUDFType.SCALAR)
        >>> def multiply(s):
        ...     return s * 2
    """
    
    SCALAR = 200
    GROUPED_MAP = 201
    GROUPED_AGG = 202
    SCALAR_ITER = 203
    MAP_ITER = 204

