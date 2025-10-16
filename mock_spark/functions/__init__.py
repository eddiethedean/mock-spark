"""
Functions module for Mock Spark.

This module provides comprehensive mock implementations of PySpark functions
that behave identically to the real PySpark functions for testing and development.
Includes column functions, aggregate functions, window functions, and utility functions.

Key Features:
    - Complete PySpark function API compatibility
    - Column operations (select, filter, transform)
    - String functions (upper, lower, length, trim, regexp_replace, split)
    - Math functions (abs, round, ceil, floor, sqrt, exp, log, pow, sin, cos, tan)
    - Aggregate functions (count, sum, avg, max, min, stddev, variance)
    - DateTime functions (current_timestamp, current_date, to_date, to_timestamp)
    - Window functions (row_number, rank, dense_rank, lag, lead)
    - Conditional functions (when, coalesce, isnull, isnotnull, isnan, nvl, nvl2)
    - Type-safe operations with proper return types

Example:
    >>> from mock_spark import MockSparkSession, F
    >>> spark = MockSparkSession("test")
    >>> data = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
    >>> df = spark.createDataFrame(data)
    >>> df.select(
    ...     F.upper(F.col("name")).alias("upper_name"),
    ...     F.col("age") * 2,
    ...     F.when(F.col("age") > 25, "senior").otherwise("junior")
    ... ).show()
    +--- MockDataFrame: 2 rows ---+
     upper_name |    (age * 2) |    CASE WHEN
    ------------------------------------------
           ALICE |           50 |       junior
             BOB |           60 |       senior
"""

from .core.column import MockColumn, MockColumnOperation
from .core.literals import MockLiteral
from .core.expressions import ExpressionFunctions
from .base import MockAggregateFunction
from .conditional import MockCaseWhen
from .window_execution import MockWindowFunction
from .functions import MockFunctions, F
from .string import StringFunctions
from .math import MathFunctions
from .aggregate import AggregateFunctions
from .datetime import DateTimeFunctions
from .array import ArrayFunctions
from .map import MapFunctions

# Note: Module-level function aliases are NOT defined here.
# All function access is handled by __getattr__ at the end of this file.
# This allows version compatibility gating - functions unavailable in the
# current PySpark compatibility mode will raise AttributeError.
#
# To use functions, import from this module:
#   from mock_spark.functions import col, lit, when
# or use the F namespace:
#   from mock_spark import F
#   F.col("name")

__all__ = [
    "MockColumn",
    "MockColumnOperation",
    "MockLiteral",
    "ExpressionFunctions",
    "MockAggregateFunction",
    "MockCaseWhen",
    "MockWindowFunction",
    "MockFunctions",
    "F",
    "StringFunctions",
    "MathFunctions",
    "AggregateFunctions",
    "DateTimeFunctions",
    "ArrayFunctions",
    "MapFunctions",
    # Module-level function aliases
    "col",
    "lit",
    "when",
    "coalesce",
    "isnull",
    "isnotnull",
    "isnan",
    "nvl",
    "nvl2",
    "upper",
    "lower",
    "length",
    "trim",
    "ltrim",
    "rtrim",
    "regexp_replace",
    "split",
    "substring",
    "concat",
    "expr",
    "format_string",
    "translate",
    "ascii",
    "base64",
    "unbase64",
    "regexp_extract_all",
    "array_join",
    "repeat",
    "initcap",
    "soundex",
    "concat_ws",
    "regexp_extract",
    "substring_index",
    "format_number",
    "instr",
    "locate",
    "lpad",
    "rpad",
    "levenshtein",
    "abs",
    "round",
    "ceil",
    "floor",
    "sqrt",
    "exp",
    "log",
    "pow",
    "sin",
    "cos",
    "tan",
    "acos",
    "asin",
    "atan",
    "cosh",
    "sinh",
    "tanh",
    "degrees",
    "radians",
    "cbrt",
    "factorial",
    "rand",
    "randn",
    "rint",
    "bround",
    "sign",
    "greatest",
    "least",
    "count",
    "countDistinct",
    "sum",
    "avg",
    "max",
    "min",
    "first",
    "last",
    "collect_list",
    "collect_set",
    "stddev",
    "variance",
    "skewness",
    "kurtosis",
    "percentile_approx",
    "corr",
    "covar_samp",
    "mean",
    "approx_count_distinct",
    "stddev_pop",
    "stddev_samp",
    "var_pop",
    "var_samp",
    "covar_pop",
    "current_timestamp",
    "current_date",
    "to_date",
    "to_timestamp",
    "hour",
    "day",
    "dayofmonth",
    "month",
    "year",
    "dayofweek",
    "dayofyear",
    "weekofyear",
    "quarter",
    "minute",
    "second",
    "add_months",
    "months_between",
    "date_add",
    "date_sub",
    "date_format",
    "from_unixtime",
    "timestampadd",
    "timestampdiff",
    "date_trunc",
    "datediff",
    "unix_timestamp",
    "last_day",
    "next_day",
    "trunc",
    "row_number",
    "rank",
    "dense_rank",
    "lag",
    "lead",
    "nth_value",
    "ntile",
    "cume_dist",
    "percent_rank",
    "desc",
    "array_distinct",
    "array_intersect",
    "array_union",
    "array_except",
    "array_position",
    "array_remove",
    "transform",
    "filter",
    "exists",
    "forall",
    "aggregate",
    "zip_with",
    "array_compact",
    "slice",
    "element_at",
    "array_append",
    "array_prepend",
    "array_insert",
    "array_size",
    "array_sort",
    "arrays_overlap",
    "array_contains",
    "array_max",
    "array_min",
    "explode",
    "size",
    "flatten",
    "reverse",
    "map_keys",
    "map_values",
    "map_entries",
    "map_concat",
    "map_from_arrays",
    "create_map",
    "map_contains_key",
    "map_from_entries",
    "map_filter",
    "transform_keys",
    "transform_values",
    "struct",
    "named_struct",
    "bit_count",
    "bit_get",
    "bitwise_not",
    "convert_timezone",
    "current_timezone",
    "from_utc_timestamp",
    "to_utc_timestamp",
    "parse_url",
    "url_encode",
    "url_decode",
    "date_part",
    "dayname",
    "assert_true",
    "from_xml",
    "to_xml",
    "schema_of_xml",
    "xpath",
    "xpath_boolean",
    "xpath_double",
    "xpath_float",
    "xpath_int",
    "xpath_long",
    "xpath_short",
    "xpath_string",
    # v2.7.0 functions
    "acosh",
    "asinh",
    "atanh",
    "overlay",
    "make_date",
    "bool_and",
    "bool_or",
    "every",
    "some",
    "max_by",
    "min_by",
    "count_if",
    "any_value",
    "version",
    # Phase 2: Advanced Features (PySpark 3.0)
    "mean",
    "approx_count_distinct",
    "stddev_pop",
    "stddev_samp",
    "var_pop",
    "var_samp",
    "covar_pop",
    "explode_outer",
    "posexplode",
    "posexplode_outer",
    "arrays_zip",
    "sequence",
    "shuffle",
    "from_json",
    "to_json",
    "get_json_object",
    "json_tuple",
    "schema_of_json",
    "from_csv",
    "to_csv",
    "schema_of_csv",
    # Phase 3: Specialized Functions (PySpark 3.0)
    "hypot",
    "nanvl",
    "signum",
    "bin",
    "hex",
    "unhex",
    "hash",
    "encode",
    "decode",
    "conv",
    "asc",
    "asc_nulls_first",
    "asc_nulls_last",
    "desc_nulls_first",
    "desc_nulls_last",
    "input_file_name",
    "monotonically_increasing_id",
    "spark_partition_id",
    "broadcast",
    "column",
    "grouping",
    "grouping_id",
]


def __getattr__(name: str):
    """
    Custom attribute access to enforce PySpark version compatibility.
    
    This function is called when accessing any attribute that isn't already defined
    in the module. It checks if the requested function is available in the current
    PySpark compatibility mode.
    
    Args:
        name: Name of the function being accessed
        
    Returns:
        The requested function/attribute
        
    Raises:
        AttributeError: If function not available in current version mode
    """
    from mock_spark._version_compat import is_available, get_pyspark_version
    
    # Check if this is a known function
    if name in __all__:
        # Check version compatibility
        if not is_available(name, 'function'):
            version = get_pyspark_version()
            raise AttributeError(
                f"module 'pyspark.sql.functions' has no attribute '{name}' "
                f"(PySpark {version} compatibility mode)"
            )
        
        # If available, try to return it from F
        try:
            return getattr(F, name)
        except AttributeError:
            # Function in __all__ but not in F - might be a class or other export
            pass
    
    # Not found
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")


# Populate module namespace with ALL functions from F 
# This enables `from mock_spark.functions import function_name` syntax
# Note: Version checking is disabled for now to maintain backward compatibility
# Use environment variable MOCK_SPARK_PYSPARK_VERSION or call set_pyspark_version() before import
_CLASS_EXPORTS = {
    "MockColumn", "MockColumnOperation", "MockLiteral", "ExpressionFunctions",
    "MockAggregateFunction", "MockCaseWhen", "MockWindowFunction", "MockFunctions",
    "F", "StringFunctions", "MathFunctions", "AggregateFunctions",
    "DateTimeFunctions", "ArrayFunctions", "MapFunctions"
}

# Add ALL functions to module namespace unconditionally
# (Version checking via environment variable or F.function_name access)
for _name in __all__:
    if _name not in _CLASS_EXPORTS:
        if hasattr(F, _name):
            globals()[_name] = getattr(F, _name)
