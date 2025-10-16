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
regexp_extract_all = F.regexp_extract_all
array_join = F.array_join
repeat = F.repeat
initcap = F.initcap
soundex = F.soundex
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
acosh = F.acosh
asinh = F.asinh
atanh = F.atanh
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
bool_and = F.bool_and
bool_or = F.bool_or
every = F.every
some = F.some
max_by = F.max_by
min_by = F.min_by
count_if = F.count_if
any_value = F.any_value
current_timestamp = F.current_timestamp
current_date = F.current_date
to_date = F.to_date
to_timestamp = F.to_timestamp
hour = F.hour
day = F.day
dayofmonth = F.day  # Alias for day
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
date_format = F.date_format
from_unixtime = F.from_unixtime
timestampadd = F.timestampadd
timestampdiff = F.timestampdiff
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
array_distinct = F.array_distinct
array_intersect = F.array_intersect
array_union = F.array_union
array_except = F.array_except
array_position = F.array_position
array_remove = F.array_remove
transform = F.transform
filter = F.filter
exists = F.exists
forall = F.forall
aggregate = F.aggregate
zip_with = F.zip_with
array_compact = F.array_compact
slice = F.slice
element_at = F.element_at
array_append = F.array_append
array_prepend = F.array_prepend
array_insert = F.array_insert
array_size = F.array_size
array_sort = F.array_sort
arrays_overlap = F.arrays_overlap
map_keys = F.map_keys
map_values = F.map_values
map_entries = F.map_entries
map_concat = F.map_concat
map_from_arrays = F.map_from_arrays
create_map = F.create_map
map_contains_key = F.map_contains_key
map_from_entries = F.map_from_entries
map_filter = F.map_filter
transform_keys = F.transform_keys
transform_values = F.transform_values
struct = F.struct
named_struct = F.named_struct
bit_count = F.bit_count
bit_get = F.bit_get
bitwise_not = F.bitwise_not
convert_timezone = F.convert_timezone
current_timezone = F.current_timezone
from_utc_timestamp = F.from_utc_timestamp
to_utc_timestamp = F.to_utc_timestamp
parse_url = F.parse_url
url_encode = F.url_encode
url_decode = F.url_decode
overlay = F.overlay
make_date = F.make_date
version = F.version
date_part = F.date_part
dayname = F.dayname
assert_true = F.assert_true
from_xml = F.from_xml
to_xml = F.to_xml
schema_of_xml = F.schema_of_xml
xpath = F.xpath
xpath_boolean = F.xpath_boolean
xpath_double = F.xpath_double
xpath_float = F.xpath_float
xpath_int = F.xpath_int
xpath_long = F.xpath_long
xpath_short = F.xpath_short
xpath_string = F.xpath_string

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
]
