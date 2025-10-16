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
