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
    >>> from mock_spark.sql import SparkSession, functions as F
    >>> spark = SparkSession("test")
    >>> data = [{"dept": "IT", "salary": 50000}, {"dept": "IT", "salary": 60000}]
    >>> df = spark.createDataFrame(data)
    >>> grouped = df.groupBy("dept")
    >>> result = grouped.agg(
    ...     F.count("*").alias("count"),
    ...     F.avg("salary").alias("avg_salary"),
    ...     F.max("salary").alias("max_salary")
    ... )
    >>> result.show()
    DataFrame[1 rows, 4 columns]
    dept count avg_salary max_salary
    IT 2 55000.0 60000
"""

from typing import Union
from mock_spark.functions.base import AggregateFunction, Column, ColumnOperation
from mock_spark.spark_types import (
    LongType,
    DoubleType,
    BooleanType,
    StringType,
    IntegerType,
)


class AggregateFunctions:
    """Collection of aggregate functions."""

    @staticmethod
    def count(column: Union[Column, str, None] = None) -> AggregateFunction:
        """Count non-null values.

        Args:
            column: The column to count (None for count(*)).

        Returns:
            AggregateFunction representing the count function.
        """
        return AggregateFunction(column, "count", LongType(nullable=False))

    @staticmethod
    def sum(column: Union[Column, str]) -> AggregateFunction:
        """Sum values.

        Args:
            column: The column to sum.

        Returns:
            AggregateFunction representing the sum function.
        """
        return AggregateFunction(column, "sum", DoubleType())

    @staticmethod
    def avg(column: Union[Column, str]) -> AggregateFunction:
        """Average values.

        Args:
            column: The column to average.

        Returns:
            AggregateFunction representing the avg function.
        """
        return AggregateFunction(column, "avg", DoubleType())

    @staticmethod
    def max(column: Union[Column, str]) -> AggregateFunction:
        """Maximum value.

        Args:
            column: The column to get max of.

        Returns:
            AggregateFunction representing the max function.
        """
        return AggregateFunction(column, "max", DoubleType())

    @staticmethod
    def min(column: Union[Column, str]) -> AggregateFunction:
        """Minimum value.

        Args:
            column: The column to get min of.

        Returns:
            AggregateFunction representing the min function.
        """
        return AggregateFunction(column, "min", DoubleType())

    @staticmethod
    def first(column: Union[Column, str]) -> AggregateFunction:
        """First value.

        Args:
            column: The column to get first value of.

        Returns:
            AggregateFunction representing the first function.
        """
        return AggregateFunction(column, "first", DoubleType())

    @staticmethod
    def last(column: Union[Column, str]) -> AggregateFunction:
        """Last value.

        Args:
            column: The column to get last value of.

        Returns:
            AggregateFunction representing the last function.
        """
        return AggregateFunction(column, "last", DoubleType())

    @staticmethod
    def collect_list(column: Union[Column, str]) -> AggregateFunction:
        """Collect values into a list.

        Args:
            column: The column to collect.

        Returns:
            AggregateFunction representing the collect_list function.
        """
        return AggregateFunction(column, "collect_list", DoubleType())

    @staticmethod
    def collect_set(column: Union[Column, str]) -> AggregateFunction:
        """Collect unique values into a set.

        Args:
            column: The column to collect.

        Returns:
            AggregateFunction representing the collect_set function.
        """
        return AggregateFunction(column, "collect_set", DoubleType())

    @staticmethod
    def stddev(column: Union[Column, str]) -> AggregateFunction:
        """Standard deviation.

        Args:
            column: The column to get stddev of.

        Returns:
            AggregateFunction representing the stddev function.
        """
        return AggregateFunction(column, "stddev", DoubleType())

    @staticmethod
    def std(column: Union[Column, str]) -> AggregateFunction:
        """Alias for stddev - Standard deviation.

        Args:
            column: The column to get stddev of.

        Returns:
            AggregateFunction representing the std function.
        """
        return AggregateFunctions.stddev(column)

    @staticmethod
    def product(column: Union[Column, str]) -> AggregateFunction:
        """Multiply all values in column.

        Args:
            column: The column to multiply values of.

        Returns:
            AggregateFunction representing the product function.
        """
        return AggregateFunction(column, "product", DoubleType())

    @staticmethod
    def sum_distinct(column: Union[Column, str]) -> AggregateFunction:
        """Sum of distinct values.

        Args:
            column: The column to sum distinct values of.

        Returns:
            AggregateFunction representing the sum_distinct function.
        """
        return AggregateFunction(column, "sum_distinct", DoubleType())

    @staticmethod
    def variance(column: Union[Column, str]) -> AggregateFunction:
        """Variance.

        Args:
            column: The column to get variance of.

        Returns:
            AggregateFunction representing the variance function.
        """
        return AggregateFunction(column, "variance", DoubleType())

    @staticmethod
    def skewness(column: Union[Column, str]) -> AggregateFunction:
        """Skewness.

        Args:
            column: The column to get skewness of.

        Returns:
            AggregateFunction representing the skewness function.
        """
        return AggregateFunction(column, "skewness", DoubleType())

    @staticmethod
    def kurtosis(column: Union[Column, str]) -> AggregateFunction:
        """Kurtosis.

        Args:
            column: The column to get kurtosis of.

        Returns:
            AggregateFunction representing the kurtosis function.
        """
        return AggregateFunction(column, "kurtosis", DoubleType())

    @staticmethod
    def countDistinct(column: Union[Column, str]) -> AggregateFunction:
        """Count distinct values.

        Args:
            column: The column to count distinct values of.

        Returns:
            AggregateFunction representing the countDistinct function.
        """
        return AggregateFunction(column, "countDistinct", LongType(nullable=False))

    @staticmethod
    def count_distinct(column: Union[Column, str]) -> AggregateFunction:
        """Alias for countDistinct - Count distinct values.

        Args:
            column: The column to count distinct values of.

        Returns:
            AggregateFunction representing the count_distinct function.
        """
        # Call countDistinct directly (same implementation)
        return AggregateFunction(column, "countDistinct", LongType(nullable=False))

    @staticmethod
    def percentile_approx(
        column: Union[Column, str], percentage: float, accuracy: int = 10000
    ) -> AggregateFunction:
        """Approximate percentile.

        Args:
            column: The column to get percentile of.
            percentage: The percentage (0.0 to 1.0).
            accuracy: The accuracy parameter.

        Returns:
            AggregateFunction representing the percentile_approx function.
        """
        # Store parameters in the name via AggregateFunction's generator (data type only is needed)
        return AggregateFunction(column, "percentile_approx", DoubleType())

    @staticmethod
    def corr(
        column1: Union[Column, str], column2: Union[Column, str]
    ) -> AggregateFunction:
        """Correlation between two columns.

        Args:
            column1: The first column.
            column2: The second column.

        Returns:
            AggregateFunction representing the corr function.
        """
        return AggregateFunction(column1, "corr", DoubleType())

    @staticmethod
    def covar_samp(
        column1: Union[Column, str], column2: Union[Column, str]
    ) -> AggregateFunction:
        """Sample covariance between two columns.

        Args:
            column1: The first column.
            column2: The second column.

        Returns:
            AggregateFunction representing the covar_samp function.
        """
        return AggregateFunction(column1, "covar_samp", DoubleType())

    @staticmethod
    def bool_and(column: Union[Column, str]) -> AggregateFunction:
        """Aggregate AND - returns true if all values are true (PySpark 3.1+).

        Args:
            column: Column containing boolean values.

        Returns:
            AggregateFunction representing the bool_and function.
        """
        return AggregateFunction(column, "bool_and", BooleanType())

    @staticmethod
    def bool_or(column: Union[Column, str]) -> AggregateFunction:
        """Aggregate OR - returns true if any value is true (PySpark 3.1+).

        Args:
            column: Column containing boolean values.

        Returns:
            AggregateFunction representing the bool_or function.
        """
        return AggregateFunction(column, "bool_or", BooleanType())

    @staticmethod
    def every(column: Union[Column, str]) -> AggregateFunction:
        """Alias for bool_and (PySpark 3.1+).

        Args:
            column: Column containing boolean values.

        Returns:
            AggregateFunction representing the every function.
        """
        return AggregateFunction(column, "bool_and", BooleanType())

    @staticmethod
    def some(column: Union[Column, str]) -> AggregateFunction:
        """Alias for bool_or (PySpark 3.1+).

        Args:
            column: Column containing boolean values.

        Returns:
            AggregateFunction representing the some function.
        """
        return AggregateFunction(column, "bool_or", BooleanType())

    @staticmethod
    def max_by(
        column: Union[Column, str], ord: Union[Column, str]
    ) -> AggregateFunction:
        """Return value associated with the maximum of ord column (PySpark 3.1+).

        Args:
            column: Column to return value from.
            ord: Column to find maximum of.

        Returns:
            AggregateFunction representing the max_by function.
        """
        if isinstance(column, str):
            column = Column(column)
        # Store ord column in value for handler
        col_func = AggregateFunction(column, "max_by", StringType())
        col_func.ord_column = ord
        return col_func

    @staticmethod
    def min_by(
        column: Union[Column, str], ord: Union[Column, str]
    ) -> AggregateFunction:
        """Return value associated with the minimum of ord column (PySpark 3.1+).

        Args:
            column: Column to return value from.
            ord: Column to find minimum of.

        Returns:
            AggregateFunction representing the min_by function.
        """
        if isinstance(column, str):
            column = Column(column)
        # Store ord column in value for handler
        col_func = AggregateFunction(column, "min_by", StringType())
        col_func.ord_column = ord
        return col_func

    @staticmethod
    def count_if(column: Union[Column, str]) -> AggregateFunction:
        """Count rows where condition is true (PySpark 3.1+).

        Args:
            column: Boolean column or condition.

        Returns:
            AggregateFunction representing the count_if function.
        """
        return AggregateFunction(column, "count_if", IntegerType())

    @staticmethod
    def any_value(column: Union[Column, str]) -> AggregateFunction:
        """Return any non-null value (non-deterministic) (PySpark 3.1+).

        Args:
            column: Column to return value from.

        Returns:
            AggregateFunction representing the any_value function.
        """
        return AggregateFunction(column, "any_value", StringType())

    @staticmethod
    def mean(column: Union[Column, str]) -> AggregateFunction:
        """Aggregate function: returns the mean of the values (alias for avg).

        Args:
            column: Numeric column.

        Returns:
            AggregateFunction representing the mean function.
        """
        return AggregateFunction(column, "mean", DoubleType())

    @staticmethod
    def approx_count_distinct(column: Union[Column, str]) -> AggregateFunction:
        """Returns approximate count of distinct elements (alias for approxCountDistinct).

        Args:
            column: Column to count distinct values.

        Returns:
            AggregateFunction representing the approx_count_distinct function.
        """
        return AggregateFunction(column, "approx_count_distinct", LongType())

    @staticmethod
    def stddev_pop(column: Union[Column, str]) -> AggregateFunction:
        """Returns population standard deviation.

        Args:
            column: Numeric column.

        Returns:
            AggregateFunction representing the stddev_pop function.
        """
        return AggregateFunction(column, "stddev_pop", DoubleType())

    @staticmethod
    def stddev_samp(column: Union[Column, str]) -> AggregateFunction:
        """Returns sample standard deviation.

        Args:
            column: Numeric column.

        Returns:
            AggregateFunction representing the stddev_samp function.
        """
        return AggregateFunction(column, "stddev_samp", DoubleType())

    @staticmethod
    def var_pop(column: Union[Column, str]) -> AggregateFunction:
        """Returns population variance.

        Args:
            column: Numeric column.

        Returns:
            AggregateFunction representing the var_pop function.
        """
        return AggregateFunction(column, "var_pop", DoubleType())

    @staticmethod
    def var_samp(column: Union[Column, str]) -> AggregateFunction:
        """Returns sample variance.

        Args:
            column: Numeric column.

        Returns:
            AggregateFunction representing the var_samp function.
        """
        return AggregateFunction(column, "var_samp", DoubleType())

    @staticmethod
    def covar_pop(
        column1: Union[Column, str], column2: Union[Column, str]
    ) -> AggregateFunction:
        """Returns population covariance.

        Args:
            column1: First numeric column.
            column2: Second numeric column.

        Returns:
            AggregateFunction representing the covar_pop function.
        """
        col1 = Column(column1) if isinstance(column1, str) else column1
        col2 = Column(column2) if isinstance(column2, str) else column2
        agg_func = AggregateFunction(col1, "covar_pop", DoubleType())
        agg_func.ord_column = col2  # Store second column for covariance
        return agg_func

    # Priority 2: Statistical Aggregate Functions
    @staticmethod
    def median(column: Union[Column, str]) -> AggregateFunction:
        """Returns the median value (PySpark 3.4+).

        Args:
            column: Numeric column.

        Returns:
            AggregateFunction representing the median function.
        """
        return AggregateFunction(column, "median", DoubleType())

    @staticmethod
    def mode(column: Union[Column, str]) -> AggregateFunction:
        """Returns the most frequent value (mode) (PySpark 3.4+).

        Args:
            column: Column to find mode of.

        Returns:
            AggregateFunction representing the mode function.
        """
        return AggregateFunction(column, "mode", StringType())

    @staticmethod
    def percentile(column: Union[Column, str], percentage: float) -> AggregateFunction:
        """Returns the exact percentile value (PySpark 3.5+).

        Args:
            column: Numeric column.
            percentage: Percentile to compute (between 0.0 and 1.0).

        Returns:
            AggregateFunction representing the percentile function.
        """
        agg_func = AggregateFunction(column, "percentile", DoubleType())
        agg_func.percentage = percentage  # type: ignore
        return agg_func

    # Deprecated Aliases
    @staticmethod
    def approxCountDistinct(*cols: Union[Column, str]) -> AggregateFunction:
        """Deprecated alias for approx_count_distinct (all PySpark versions).

        Use approx_count_distinct instead.

        Args:
            cols: Columns to count distinct values for.

        Returns:
            AggregateFunction for approximate distinct count.
        """
        import warnings

        warnings.warn(
            "approxCountDistinct is deprecated. Use approx_count_distinct instead.",
            FutureWarning,
            stacklevel=2,
        )
        return AggregateFunctions.approx_count_distinct(*cols)

    @staticmethod
    def sumDistinct(column: Union[Column, str]) -> AggregateFunction:
        """Deprecated alias for sum_distinct (PySpark 3.2+).

        Use sum_distinct instead (or sum(distinct(col)) for earlier versions).

        Args:
            column: Numeric column to sum.

        Returns:
            AggregateFunction for distinct sum.
        """
        import warnings

        warnings.warn(
            "sumDistinct is deprecated. Use sum with distinct instead.",
            FutureWarning,
            stacklevel=2,
        )
        # For mock implementation, create sum_distinct aggregate
        return AggregateFunction(column, "sum_distinct", DoubleType())

    @staticmethod
    def regr_avgx(y: Union[Column, str], x: Union[Column, str]) -> AggregateFunction:
        """Linear regression average of x.

        Args:
            y: The y column.
            x: The x column.

        Returns:
            AggregateFunction representing the regr_avgx function.
        """
        from mock_spark.functions.base import Column

        y_col = Column(y) if isinstance(y, str) else y
        x_col = Column(x) if isinstance(x, str) else x

        # Store both columns in the operation
        operation = ColumnOperation(
            y_col, "regr_avgx", x_col, name=f"regr_avgx({y_col.name}, {x_col.name})"
        )
        return AggregateFunction(operation, "regr_avgx", DoubleType())

    @staticmethod
    def regr_avgy(y: Union[Column, str], x: Union[Column, str]) -> AggregateFunction:
        """Linear regression average of y.

        Args:
            y: The y column.
            x: The x column.

        Returns:
            AggregateFunction representing the regr_avgy function.
        """
        from mock_spark.functions.base import Column

        y_col = Column(y) if isinstance(y, str) else y
        x_col = Column(x) if isinstance(x, str) else x

        operation = ColumnOperation(
            y_col, "regr_avgy", x_col, name=f"regr_avgy({y_col.name}, {x_col.name})"
        )
        return AggregateFunction(operation, "regr_avgy", DoubleType())

    @staticmethod
    def regr_count(y: Union[Column, str], x: Union[Column, str]) -> AggregateFunction:
        """Linear regression count.

        Args:
            y: The y column.
            x: The x column.

        Returns:
            AggregateFunction representing the regr_count function.
        """
        from mock_spark.functions.base import Column

        y_col = Column(y) if isinstance(y, str) else y
        x_col = Column(x) if isinstance(x, str) else x

        operation = ColumnOperation(
            y_col, "regr_count", x_col, name=f"regr_count({y_col.name}, {x_col.name})"
        )
        return AggregateFunction(operation, "regr_count", LongType())

    @staticmethod
    def regr_intercept(
        y: Union[Column, str], x: Union[Column, str]
    ) -> AggregateFunction:
        """Linear regression intercept.

        Args:
            y: The y column.
            x: The x column.

        Returns:
            AggregateFunction representing the regr_intercept function.
        """
        from mock_spark.functions.base import Column

        y_col = Column(y) if isinstance(y, str) else y
        x_col = Column(x) if isinstance(x, str) else x

        operation = ColumnOperation(
            y_col,
            "regr_intercept",
            x_col,
            name=f"regr_intercept({y_col.name}, {x_col.name})",
        )
        return AggregateFunction(operation, "regr_intercept", DoubleType())

    @staticmethod
    def regr_r2(y: Union[Column, str], x: Union[Column, str]) -> AggregateFunction:
        """Linear regression R-squared.

        Args:
            y: The y column.
            x: The x column.

        Returns:
            AggregateFunction representing the regr_r2 function.
        """
        from mock_spark.functions.base import Column

        y_col = Column(y) if isinstance(y, str) else y
        x_col = Column(x) if isinstance(x, str) else x

        operation = ColumnOperation(
            y_col, "regr_r2", x_col, name=f"regr_r2({y_col.name}, {x_col.name})"
        )
        return AggregateFunction(operation, "regr_r2", DoubleType())

    @staticmethod
    def regr_slope(y: Union[Column, str], x: Union[Column, str]) -> AggregateFunction:
        """Linear regression slope.

        Args:
            y: The y column.
            x: The x column.

        Returns:
            AggregateFunction representing the regr_slope function.
        """
        from mock_spark.functions.base import Column

        y_col = Column(y) if isinstance(y, str) else y
        x_col = Column(x) if isinstance(x, str) else x

        operation = ColumnOperation(
            y_col, "regr_slope", x_col, name=f"regr_slope({y_col.name}, {x_col.name})"
        )
        return AggregateFunction(operation, "regr_slope", DoubleType())

    @staticmethod
    def regr_sxx(y: Union[Column, str], x: Union[Column, str]) -> AggregateFunction:
        """Linear regression sum of squares of x.

        Args:
            y: The y column.
            x: The x column.

        Returns:
            AggregateFunction representing the regr_sxx function.
        """
        from mock_spark.functions.base import Column

        y_col = Column(y) if isinstance(y, str) else y
        x_col = Column(x) if isinstance(x, str) else x

        operation = ColumnOperation(
            y_col, "regr_sxx", x_col, name=f"regr_sxx({y_col.name}, {x_col.name})"
        )
        return AggregateFunction(operation, "regr_sxx", DoubleType())

    @staticmethod
    def regr_sxy(y: Union[Column, str], x: Union[Column, str]) -> AggregateFunction:
        """Linear regression sum of products.

        Args:
            y: The y column.
            x: The x column.

        Returns:
            AggregateFunction representing the regr_sxy function.
        """
        from mock_spark.functions.base import Column

        y_col = Column(y) if isinstance(y, str) else y
        x_col = Column(x) if isinstance(x, str) else x

        operation = ColumnOperation(
            y_col, "regr_sxy", x_col, name=f"regr_sxy({y_col.name}, {x_col.name})"
        )
        return AggregateFunction(operation, "regr_sxy", DoubleType())

    @staticmethod
    def regr_syy(y: Union[Column, str], x: Union[Column, str]) -> AggregateFunction:
        """Linear regression sum of squares of y.

        Args:
            y: The y column.
            x: The x column.

        Returns:
            AggregateFunction representing the regr_syy function.
        """
        from mock_spark.functions.base import Column

        y_col = Column(y) if isinstance(y, str) else y
        x_col = Column(x) if isinstance(x, str) else x

        operation = ColumnOperation(
            y_col, "regr_syy", x_col, name=f"regr_syy({y_col.name}, {x_col.name})"
        )
        return AggregateFunction(operation, "regr_syy", DoubleType())

    @staticmethod
    def approx_percentile(
        column: Union[Column, str],
        percentage: Union[float, Column, str],
        accuracy: Union[int, Column, str] = 10000,
    ) -> AggregateFunction:
        """Compute approximate percentile (PySpark 3.5+).

        Args:
            column: The column to compute percentile for.
            percentage: The percentage (0.0 to 1.0) or array of percentages.
            accuracy: The accuracy parameter (default: 10000).

        Returns:
            AggregateFunction representing the approx_percentile function.

        Example:
            >>> df.groupBy("dept").agg(F.approx_percentile(F.col("salary"), 0.5))
        """
        from mock_spark.functions.base import Column

        col = Column(column) if isinstance(column, str) else column

        if isinstance(percentage, (int, float)):
            # Single percentage value
            if isinstance(accuracy, (int, float)):
                operation = ColumnOperation(
                    col,
                    "approx_percentile",
                    value=(percentage, accuracy),
                    name=f"approx_percentile({col.name}, {percentage}, {accuracy})",
                )
            else:
                acc_col = Column(accuracy) if isinstance(accuracy, str) else accuracy
                operation = ColumnOperation(
                    col,
                    "approx_percentile",
                    value=(percentage, acc_col),
                    name=f"approx_percentile({col.name}, {percentage}, {acc_col.name})",
                )
        else:
            # Percentage as column
            perc_col = Column(percentage) if isinstance(percentage, str) else percentage
            if isinstance(accuracy, (int, float)):
                operation = ColumnOperation(
                    col,
                    "approx_percentile",
                    value=(perc_col, accuracy),
                    name=f"approx_percentile({col.name}, {perc_col.name}, {accuracy})",
                )
            else:
                acc_col = Column(accuracy) if isinstance(accuracy, str) else accuracy
                operation = ColumnOperation(
                    col,
                    "approx_percentile",
                    value=(perc_col, acc_col),
                    name=f"approx_percentile({col.name}, {perc_col.name}, {acc_col.name})",
                )

        return AggregateFunction(operation, "approx_percentile", DoubleType())
