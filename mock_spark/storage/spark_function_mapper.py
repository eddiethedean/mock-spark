"""
Spark SQL Function Mapper.

Maps Spark SQL functions to SQLAlchemy func.* equivalents for SQL translation.
"""

from typing import Callable, Dict
from sqlalchemy import func


# Aggregate Functions
AGGREGATE_FUNCTIONS: Dict[str, str] = {
    "count": "count",
    "sum": "sum",
    "avg": "avg",
    "mean": "avg",  # Alias for avg
    "min": "min",
    "max": "max",
    "stddev": "stddev",
    "stddev_samp": "stddev_samp",
    "stddev_pop": "stddev_pop",
    "variance": "variance",
    "var_samp": "var_samp",
    "var_pop": "var_pop",
    "collect_list": "array_agg",
    "collect_set": "array_agg",  # Note: doesn't do distinct automatically
}

# String Functions
STRING_FUNCTIONS: Dict[str, str] = {
    "concat": "concat",
    "substring": "substr",
    "substr": "substr",
    "upper": "upper",
    "ucase": "upper",  # Alias
    "lower": "lower",
    "lcase": "lower",  # Alias
    "trim": "trim",
    "ltrim": "ltrim",
    "rtrim": "rtrim",
    "length": "length",
    "char_length": "char_length",
    "reverse": "reverse",
    "repeat": "repeat",
    "replace": "replace",
    "split": "split",
    "concat_ws": "concat_ws",
    "format_string": "format",
    "instr": "instr",
    "locate": "locate",
    "lpad": "lpad",
    "rpad": "rpad",
}

# Date/Time Functions
DATETIME_FUNCTIONS: Dict[str, str] = {
    "current_date": "current_date",
    "current_timestamp": "current_timestamp",
    "now": "now",
    "year": "year",
    "month": "month",
    "day": "day",
    "dayofmonth": "day",
    "hour": "hour",
    "minute": "minute",
    "second": "second",
    "date_add": "date_add",
    "date_sub": "date_sub",
    "datediff": "datediff",
    "add_months": "add_months",
    "last_day": "last_day",
    "next_day": "next_day",
    "trunc": "trunc",
    "date_trunc": "date_trunc",
    "from_unixtime": "from_unixtime",
    "unix_timestamp": "unix_timestamp",
    "to_date": "to_date",
    "to_timestamp": "to_timestamp",
}

# Math Functions
MATH_FUNCTIONS: Dict[str, str] = {
    "abs": "abs",
    "acos": "acos",
    "asin": "asin",
    "atan": "atan",
    "atan2": "atan2",
    "ceil": "ceil",
    "ceiling": "ceil",  # Alias
    "cos": "cos",
    "cosh": "cosh",
    "exp": "exp",
    "floor": "floor",
    "ln": "ln",
    "log": "log",
    "log10": "log10",
    "log2": "log2",
    "power": "power",
    "pow": "power",  # Alias
    "round": "round",
    "sign": "sign",
    "sin": "sin",
    "sinh": "sinh",
    "sqrt": "sqrt",
    "tan": "tan",
    "tanh": "tanh",
    "degrees": "degrees",
    "radians": "radians",
    "rand": "random",
    "random": "random",
}

# Conditional Functions
CONDITIONAL_FUNCTIONS: Dict[str, str] = {
    "coalesce": "coalesce",
    "nullif": "nullif",
    "nvl": "coalesce",  # Similar to coalesce
    "nvl2": "nvl2",
    "if": "if",
    "ifnull": "coalesce",  # Alias
}

# Window Functions
WINDOW_FUNCTIONS: Dict[str, str] = {
    "row_number": "row_number",
    "rank": "rank",
    "dense_rank": "dense_rank",
    "percent_rank": "percent_rank",
    "cume_dist": "cume_dist",
    "ntile": "ntile",
    "lag": "lag",
    "lead": "lead",
    "first_value": "first_value",
    "last_value": "last_value",
    "nth_value": "nth_value",
}

# Array Functions
ARRAY_FUNCTIONS: Dict[str, str] = {
    "array": "array",
    "array_contains": "array_contains",
    "array_distinct": "array_distinct",
    "array_except": "array_except",
    "array_intersect": "array_intersect",
    "array_join": "array_join",
    "array_max": "array_max",
    "array_min": "array_min",
    "array_position": "array_position",
    "array_remove": "array_remove",
    "array_repeat": "array_repeat",
    "array_sort": "array_sort",
    "array_union": "array_union",
    "arrays_overlap": "arrays_overlap",
    "arrays_zip": "arrays_zip",
    "flatten": "flatten",
    "size": "size",
    "sort_array": "sort_array",
}

# JSON Functions
JSON_FUNCTIONS: Dict[str, str] = {
    "get_json_object": "json_extract",
    "json_tuple": "json_tuple",
    "to_json": "to_json",
    "from_json": "from_json",
}

# Type Conversion Functions
CONVERSION_FUNCTIONS: Dict[str, str] = {
    "cast": "cast",
    "string": "cast",  # Implicit cast to string
    "int": "cast",  # Implicit cast to int
    "bigint": "cast",
    "float": "cast",
    "double": "cast",
    "decimal": "cast",
    "date": "cast",
    "timestamp": "cast",
}

# Miscellaneous Functions
MISC_FUNCTIONS: Dict[str, str] = {
    "greatest": "greatest",
    "least": "least",
    "hash": "hash",
    "md5": "md5",
    "sha1": "sha1",
    "sha2": "sha2",
    "crc32": "crc32",
    "base64": "base64",
    "unbase64": "unbase64",
    "uuid": "uuid",
    "monotonically_increasing_id": "row_number",  # Approximation
}

# Combine all function mappings
ALL_FUNCTIONS: Dict[str, str] = {
    **AGGREGATE_FUNCTIONS,
    **STRING_FUNCTIONS,
    **DATETIME_FUNCTIONS,
    **MATH_FUNCTIONS,
    **CONDITIONAL_FUNCTIONS,
    **WINDOW_FUNCTIONS,
    **ARRAY_FUNCTIONS,
    **JSON_FUNCTIONS,
    **CONVERSION_FUNCTIONS,
    **MISC_FUNCTIONS,
}


def get_sqlalchemy_function(spark_func_name: str) -> Callable:
    """
    Get SQLAlchemy function equivalent for Spark SQL function.

    Args:
        spark_func_name: Name of Spark SQL function (case-insensitive)

    Returns:
        SQLAlchemy function callable

    Raises:
        ValueError: If function is not supported
    """
    func_name_lower = spark_func_name.lower()

    if func_name_lower in ALL_FUNCTIONS:
        sqlalchemy_name = ALL_FUNCTIONS[func_name_lower]
        return getattr(func, sqlalchemy_name)

    # Try direct mapping (for functions not in our mapping)
    if hasattr(func, func_name_lower):
        return getattr(func, func_name_lower)

    raise ValueError(
        f"Unsupported Spark SQL function: {spark_func_name}. "
        f"This function may need to be added to the function mapper."
    )


def is_aggregate_function(func_name: str) -> bool:
    """Check if function is an aggregate function."""
    return func_name.lower() in AGGREGATE_FUNCTIONS


def is_window_function(func_name: str) -> bool:
    """Check if function is a window function."""
    return func_name.lower() in WINDOW_FUNCTIONS


def is_supported_function(func_name: str) -> bool:
    """Check if function is supported."""
    func_name_lower = func_name.lower()
    return func_name_lower in ALL_FUNCTIONS or hasattr(func, func_name_lower)
