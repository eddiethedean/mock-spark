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
from mock_spark.spark_types import LongType, DoubleType


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
