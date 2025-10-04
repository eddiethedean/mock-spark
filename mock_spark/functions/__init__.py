"""
Functions module for Mock Spark.

This module provides function-related functionality organized into submodules.
"""

from .core import (
    MockColumn,
    MockColumnOperation,
    MockLiteral,
    MockAggregateFunction,
    MockCaseWhen,
    MockWindowFunction,
    MockFunctions,
    F,
    StringFunctions,
    MathFunctions,
    AggregateFunctions,
    DateTimeFunctions,
)

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
row_number = F.row_number
rank = F.rank
dense_rank = F.dense_rank
lag = F.lag
lead = F.lead
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
    "row_number",
    "rank",
    "dense_rank",
    "lag",
    "lead",
    "desc",
]
