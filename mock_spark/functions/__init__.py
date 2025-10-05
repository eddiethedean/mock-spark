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

# Create module-level aliases for backward compatibility
col = F.col
lit = F.lit
when = F.when
coalesce = F.coalesce
isnull = F.isnull
isnotnull = F.isnotnull
isnan = F.isnan
nvl = F.nvl
nvl2 = F.nvl2
upper = F.upper
lower = F.lower
length = F.length
trim = F.trim
ltrim = F.ltrim
rtrim = F.rtrim
regexp_replace = F.regexp_replace
split = F.split
substring = F.substring
concat = F.concat
expr = F.expr
format_string = F.format_string
translate = F.translate
ascii = F.ascii
base64 = F.base64
unbase64 = F.unbase64
abs = F.abs
round = F.round
ceil = F.ceil
floor = F.floor
sqrt = F.sqrt
exp = F.exp
log = F.log
pow = F.pow
sin = F.sin
cos = F.cos
tan = F.tan
sign = F.sign
greatest = F.greatest
least = F.least
count = F.count
countDistinct = F.countDistinct
sum = F.sum
avg = F.avg
max = F.max
min = F.min
first = F.first
last = F.last
collect_list = F.collect_list
collect_set = F.collect_set
stddev = F.stddev
variance = F.variance
skewness = F.skewness
kurtosis = F.kurtosis
percentile_approx = F.percentile_approx
corr = F.corr
covar_samp = F.covar_samp
current_timestamp = F.current_timestamp
current_date = F.current_date
to_date = F.to_date
to_timestamp = F.to_timestamp
hour = F.hour
day = F.day
month = F.month
year = F.year
dayofweek = F.dayofweek
dayofyear = F.dayofyear
weekofyear = F.weekofyear
quarter = F.quarter
minute = F.minute
second = F.second
add_months = F.add_months
months_between = F.months_between
date_add = F.date_add
date_sub = F.date_sub
row_number = F.row_number
rank = F.rank
dense_rank = F.dense_rank
lag = F.lag
lead = F.lead
nth_value = F.nth_value
ntile = F.ntile
cume_dist = F.cume_dist
percent_rank = F.percent_rank
desc = F.desc

__all__ = [
    "MockColumn",
    "MockColumnOperation",
    "MockLiteral",
    "MockAggregateFunction",
    "MockCaseWhen",
    "MockWindowFunction",
    "MockFunctions",
    "F",
    "StringFunctions",
    "MathFunctions",
    "AggregateFunctions",
    "DateTimeFunctions",
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
    "current_timestamp",
    "current_date",
    "to_date",
    "to_timestamp",
    "hour",
    "day",
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
]
