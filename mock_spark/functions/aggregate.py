"""
Aggregate functions for Mock Spark.

This module provides comprehensive aggregate functions that match PySpark's
aggregate function API. Includes statistical operations, counting functions,
and data summarization operations for grouped data processing in DataFrames.

Key Features:
    - Complete PySpark aggregate function API compatibility
    - Basic aggregates (count, sum, avg, max, min)
    - Statistical functions (stddev, variance, skewness, kurtosis)
    - Collection aggregates (collect_list, collect_set, first, last)
    - Distinct counting (countDistinct)
    - Type-safe operations with proper return types
    - Support for both column references and expressions
    - Proper handling of null values and edge cases

Example:
    >>> from mock_spark import MockSparkSession, F
    >>> spark = MockSparkSession("test")
    >>> data = [{"dept": "IT", "salary": 50000}, {"dept": "IT", "salary": 60000}]
    >>> df = spark.createDataFrame(data)
    >>> grouped = df.groupBy("dept")
    >>> result = grouped.agg(
    ...     F.count("*").alias("count"),
    ...     F.avg("salary").alias("avg_salary"),
    ...     F.max("salary").alias("max_salary")
    ... )
    >>> result.show()
    +--- MockDataFrame: 1 rows ---+
            dept |        count |   avg_salary |   max_salary
    ---------------------------------------------------------
              IT |            2 |      55000.0 |        60000
"""

from typing import Union
from mock_spark.functions.base import MockAggregateFunction, MockColumn
from mock_spark.spark_types import LongType, DoubleType, BooleanType, StringType, IntegerType


class AggregateFunctions:
    """Collection of aggregate functions."""

    @staticmethod
    def count(column: Union[MockColumn, str, None] = None) -> MockAggregateFunction:
        """Count non-null values.

        Args:
            column: The column to count (None for count(*)).

        Returns:
            MockAggregateFunction representing the count function.
        """
        return MockAggregateFunction(column, "count", LongType())

    @staticmethod
    def sum(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Sum values.

        Args:
            column: The column to sum.

        Returns:
            MockAggregateFunction representing the sum function.
        """
        return MockAggregateFunction(column, "sum", DoubleType())

    @staticmethod
    def avg(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Average values.

        Args:
            column: The column to average.

        Returns:
            MockAggregateFunction representing the avg function.
        """
        return MockAggregateFunction(column, "avg", DoubleType())

    @staticmethod
    def max(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Maximum value.

        Args:
            column: The column to get max of.

        Returns:
            MockAggregateFunction representing the max function.
        """
        return MockAggregateFunction(column, "max", DoubleType())

    @staticmethod
    def min(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Minimum value.

        Args:
            column: The column to get min of.

        Returns:
            MockAggregateFunction representing the min function.
        """
        return MockAggregateFunction(column, "min", DoubleType())

    @staticmethod
    def first(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """First value.

        Args:
            column: The column to get first value of.

        Returns:
            MockAggregateFunction representing the first function.
        """
        return MockAggregateFunction(column, "first", DoubleType())

    @staticmethod
    def last(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Last value.

        Args:
            column: The column to get last value of.

        Returns:
            MockAggregateFunction representing the last function.
        """
        return MockAggregateFunction(column, "last", DoubleType())

    @staticmethod
    def collect_list(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Collect values into a list.

        Args:
            column: The column to collect.

        Returns:
            MockAggregateFunction representing the collect_list function.
        """
        return MockAggregateFunction(column, "collect_list", DoubleType())

    @staticmethod
    def collect_set(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Collect unique values into a set.

        Args:
            column: The column to collect.

        Returns:
            MockAggregateFunction representing the collect_set function.
        """
        return MockAggregateFunction(column, "collect_set", DoubleType())

    @staticmethod
    def stddev(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Standard deviation.

        Args:
            column: The column to get stddev of.

        Returns:
            MockAggregateFunction representing the stddev function.
        """
        return MockAggregateFunction(column, "stddev", DoubleType())

    @staticmethod
    def variance(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Variance.

        Args:
            column: The column to get variance of.

        Returns:
            MockAggregateFunction representing the variance function.
        """
        return MockAggregateFunction(column, "variance", DoubleType())

    @staticmethod
    def skewness(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Skewness.

        Args:
            column: The column to get skewness of.

        Returns:
            MockAggregateFunction representing the skewness function.
        """
        return MockAggregateFunction(column, "skewness", DoubleType())

    @staticmethod
    def kurtosis(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Kurtosis.

        Args:
            column: The column to get kurtosis of.

        Returns:
            MockAggregateFunction representing the kurtosis function.
        """
        return MockAggregateFunction(column, "kurtosis", DoubleType())

    @staticmethod
    def countDistinct(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Count distinct values.

        Args:
            column: The column to count distinct values of.

        Returns:
            MockAggregateFunction representing the countDistinct function.
        """
        return MockAggregateFunction(column, "countDistinct", LongType())

    @staticmethod
    def percentile_approx(
        column: Union[MockColumn, str], percentage: float, accuracy: int = 10000
    ) -> MockAggregateFunction:
        """Approximate percentile.

        Args:
            column: The column to get percentile of.
            percentage: The percentage (0.0 to 1.0).
            accuracy: The accuracy parameter.

        Returns:
            MockAggregateFunction representing the percentile_approx function.
        """
        # Store parameters in the name via MockAggregateFunction's generator (data type only is needed)
        return MockAggregateFunction(column, "percentile_approx", DoubleType())

    @staticmethod
    def corr(
        column1: Union[MockColumn, str], column2: Union[MockColumn, str]
    ) -> MockAggregateFunction:
        """Correlation between two columns.

        Args:
            column1: The first column.
            column2: The second column.

        Returns:
            MockAggregateFunction representing the corr function.
        """
        return MockAggregateFunction(column1, "corr", DoubleType())

    @staticmethod
    def covar_samp(
        column1: Union[MockColumn, str], column2: Union[MockColumn, str]
    ) -> MockAggregateFunction:
        """Sample covariance between two columns.

        Args:
            column1: The first column.
            column2: The second column.

        Returns:
            MockAggregateFunction representing the covar_samp function.
        """
        return MockAggregateFunction(column1, "covar_samp", DoubleType())

    @staticmethod
    def bool_and(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Aggregate AND - returns true if all values are true (PySpark 3.1+).

        Args:
            column: Column containing boolean values.

        Returns:
            MockAggregateFunction representing the bool_and function.
        """
        return MockAggregateFunction(column, "bool_and", BooleanType())

    @staticmethod
    def bool_or(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Aggregate OR - returns true if any value is true (PySpark 3.1+).

        Args:
            column: Column containing boolean values.

        Returns:
            MockAggregateFunction representing the bool_or function.
        """
        return MockAggregateFunction(column, "bool_or", BooleanType())

    @staticmethod
    def every(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Alias for bool_and (PySpark 3.1+).

        Args:
            column: Column containing boolean values.

        Returns:
            MockAggregateFunction representing the every function.
        """
        return MockAggregateFunction(column, "bool_and", BooleanType())

    @staticmethod
    def some(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Alias for bool_or (PySpark 3.1+).

        Args:
            column: Column containing boolean values.

        Returns:
            MockAggregateFunction representing the some function.
        """
        return MockAggregateFunction(column, "bool_or", BooleanType())

    @staticmethod
    def max_by(column: Union[MockColumn, str], ord: Union[MockColumn, str]) -> MockAggregateFunction:
        """Return value associated with the maximum of ord column (PySpark 3.1+).

        Args:
            column: Column to return value from.
            ord: Column to find maximum of.

        Returns:
            MockAggregateFunction representing the max_by function.
        """
        if isinstance(column, str):
            column = MockColumn(column)
        # Store ord column in value for handler
        col_func = MockAggregateFunction(column, "max_by", StringType())
        col_func.ord_column = ord
        return col_func

    @staticmethod
    def min_by(column: Union[MockColumn, str], ord: Union[MockColumn, str]) -> MockAggregateFunction:
        """Return value associated with the minimum of ord column (PySpark 3.1+).

        Args:
            column: Column to return value from.
            ord: Column to find minimum of.

        Returns:
            MockAggregateFunction representing the min_by function.
        """
        if isinstance(column, str):
            column = MockColumn(column)
        # Store ord column in value for handler
        col_func = MockAggregateFunction(column, "min_by", StringType())
        col_func.ord_column = ord
        return col_func

    @staticmethod
    def count_if(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Count rows where condition is true (PySpark 3.1+).

        Args:
            column: Boolean column or condition.

        Returns:
            MockAggregateFunction representing the count_if function.
        """
        return MockAggregateFunction(column, "count_if", IntegerType())

    @staticmethod
    def any_value(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Return any non-null value (non-deterministic) (PySpark 3.1+).

        Args:
            column: Column to return value from.

        Returns:
            MockAggregateFunction representing the any_value function.
        """
        return MockAggregateFunction(column, "any_value", StringType())

    @staticmethod
    def mean(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Aggregate function: returns the mean of the values (alias for avg).

        Args:
            column: Numeric column.

        Returns:
            MockAggregateFunction representing the mean function.
        """
        return MockAggregateFunction(column, "mean", DoubleType())

    @staticmethod
    def approx_count_distinct(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Returns approximate count of distinct elements (alias for approxCountDistinct).

        Args:
            column: Column to count distinct values.

        Returns:
            MockAggregateFunction representing the approx_count_distinct function.
        """
        return MockAggregateFunction(column, "approx_count_distinct", LongType())

    @staticmethod
    def stddev_pop(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Returns population standard deviation.

        Args:
            column: Numeric column.

        Returns:
            MockAggregateFunction representing the stddev_pop function.
        """
        return MockAggregateFunction(column, "stddev_pop", DoubleType())

    @staticmethod
    def stddev_samp(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Returns sample standard deviation.

        Args:
            column: Numeric column.

        Returns:
            MockAggregateFunction representing the stddev_samp function.
        """
        return MockAggregateFunction(column, "stddev_samp", DoubleType())

    @staticmethod
    def var_pop(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Returns population variance.

        Args:
            column: Numeric column.

        Returns:
            MockAggregateFunction representing the var_pop function.
        """
        return MockAggregateFunction(column, "var_pop", DoubleType())

    @staticmethod
    def var_samp(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Returns sample variance.

        Args:
            column: Numeric column.

        Returns:
            MockAggregateFunction representing the var_samp function.
        """
        return MockAggregateFunction(column, "var_samp", DoubleType())

    @staticmethod
    def covar_pop(column1: Union[MockColumn, str], column2: Union[MockColumn, str]) -> MockAggregateFunction:
        """Returns population covariance.

        Args:
            column1: First numeric column.
            column2: Second numeric column.

        Returns:
            MockAggregateFunction representing the covar_pop function.
        """
        col1 = MockColumn(column1) if isinstance(column1, str) else column1
        col2 = MockColumn(column2) if isinstance(column2, str) else column2
        agg_func = MockAggregateFunction(col1, "covar_pop", DoubleType())
        agg_func.ord_column = col2  # Store second column for covariance
        return agg_func

    # Priority 2: Statistical Aggregate Functions
    @staticmethod
    def median(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Returns the median value (PySpark 3.4+).

        Args:
            column: Numeric column.

        Returns:
            MockAggregateFunction representing the median function.
        """
        return MockAggregateFunction(column, "median", DoubleType())

    @staticmethod
    def mode(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Returns the most frequent value (mode) (PySpark 3.4+).

        Args:
            column: Column to find mode of.

        Returns:
            MockAggregateFunction representing the mode function.
        """
        return MockAggregateFunction(column, "mode", StringType())

    @staticmethod
    def percentile(column: Union[MockColumn, str], percentage: float) -> MockAggregateFunction:
        """Returns the exact percentile value (PySpark 3.5+).

        Args:
            column: Numeric column.
            percentage: Percentile to compute (between 0.0 and 1.0).

        Returns:
            MockAggregateFunction representing the percentile function.
        """
        agg_func = MockAggregateFunction(column, "percentile", DoubleType())
        agg_func.percentage = percentage  # type: ignore
        return agg_func

    # Deprecated Aliases
    @staticmethod
    def approxCountDistinct(*cols: Union[MockColumn, str]) -> MockAggregateFunction:
        """Deprecated alias for approx_count_distinct (all PySpark versions).
        
        Use approx_count_distinct instead.
        
        Args:
            cols: Columns to count distinct values for.
            
        Returns:
            MockAggregateFunction for approximate distinct count.
        """
        import warnings
        warnings.warn(
            "approxCountDistinct is deprecated. Use approx_count_distinct instead.",
            FutureWarning,
            stacklevel=2
        )
        return AggregateFunctions.approx_count_distinct(*cols)

    @staticmethod
    def sumDistinct(column: Union[MockColumn, str]) -> MockAggregateFunction:
        """Deprecated alias for sum_distinct (PySpark 3.2+).
        
        Use sum_distinct instead (or sum(distinct(col)) for earlier versions).
        
        Args:
            column: Numeric column to sum.
            
        Returns:
            MockAggregateFunction for distinct sum.
        """
        import warnings
        warnings.warn(
            "sumDistinct is deprecated. Use sum with distinct instead.",
            FutureWarning,
            stacklevel=2
        )
        # For mock implementation, create sum_distinct aggregate
        return MockAggregateFunction(column, "sum_distinct", DoubleType())
