"""
DataFrame Statistical Operations

This module provides statistical summary operations for MockDataFrames,
extracted from dataframe.py to follow the Single Responsibility Principle.

The DataFrameStatistics class provides:
- describe(): Basic statistics (count, mean, stddev, min, max)
- summary(): Extended statistics with percentiles
"""

import statistics
from typing import TYPE_CHECKING, Any, Dict, List, cast

if TYPE_CHECKING:
    from .dataframe import MockDataFrame


class DataFrameStatistics:
    """Provides statistical summary operations for DataFrames.

    This class contains static methods for computing statistical summaries
    on DataFrame data, separated from the main DataFrame class to improve
    modularity and maintainability.
    """

    @staticmethod
    def describe(df: "MockDataFrame", *cols: str) -> "MockDataFrame":
        """Compute basic statistics for numeric columns.

        Computes count, mean, standard deviation, min, and max for each
        numeric column in the DataFrame.

        Args:
            df: MockDataFrame to compute statistics for
            *cols: Column names to describe. If empty, describes all numeric columns.

        Returns:
            MockDataFrame with statistics (count, mean, stddev, min, max).

        Example:
            >>> df.describe()
            +-------+-----+---+
            |summary|age  |...|
            +-------+-----+---+
            |count  |100  |...|
            |mean   |25.5 |...|
            |stddev |5.2  |...|
            |min    |18   |...|
            |max    |65   |...|
            +-------+-----+---+
        """
        from ..spark_types import MockStructType, MockStructField, StringType
        from ..core.exceptions.analysis import ColumnNotFoundException
        from .dataframe import MockDataFrame

        # Determine which columns to describe
        if not cols:
            # Describe all numeric columns
            numeric_cols = []
            for field in df.schema.fields:
                field_type = field.dataType.typeName()
                if field_type in [
                    "long",
                    "int",
                    "integer",
                    "bigint",
                    "double",
                    "float",
                ]:
                    numeric_cols.append(field.name)
        else:
            numeric_cols = list(cols)
            # Validate that columns exist
            available_cols = [field.name for field in df.schema.fields]
            for col in numeric_cols:
                if col not in available_cols:
                    raise ColumnNotFoundException(col)

        if not numeric_cols:
            # No numeric columns found
            return MockDataFrame([], df.schema, cast(Any, df.storage))  # type: ignore[has-type]

        # Calculate statistics for each column
        result_data = []

        for col in numeric_cols:
            # Extract values for this column
            values = []
            for row in df.data:
                value = row.get(col)
                if value is not None and isinstance(value, (int, float)):
                    values.append(value)

            if not values:
                # No valid numeric values
                stats_row = {
                    "summary": col,
                    "count": "0",
                    "mean": "NaN",
                    "stddev": "NaN",
                    "min": "NaN",
                    "max": "NaN",
                }
            else:
                stats_row = {
                    "summary": col,
                    "count": str(len(values)),
                    "mean": str(round(statistics.mean(values), 4)),
                    "stddev": str(round(statistics.stdev(values) if len(values) > 1 else 0.0, 4)),
                    "min": str(min(values)),
                    "max": str(max(values)),
                }

            result_data.append(stats_row)

        # Create result schema
        result_schema = MockStructType(
            [
                MockStructField("summary", StringType()),
                MockStructField("count", StringType()),
                MockStructField("mean", StringType()),
                MockStructField("stddev", StringType()),
                MockStructField("min", StringType()),
                MockStructField("max", StringType()),
            ]
        )

        return MockDataFrame(result_data, result_schema, cast(Any, df.storage))  # type: ignore[has-type]

    @staticmethod
    def summary(df: "MockDataFrame", *stats: str) -> "MockDataFrame":
        """Compute extended statistics for numeric columns.

        Computes specified statistics including percentiles (25%, 50%, 75%)
        for numeric columns in the DataFrame.

        Args:
            df: MockDataFrame to compute statistics for
            *stats: Statistics to compute. Default: ["count", "mean", "stddev",
                   "min", "25%", "50%", "75%", "max"].

        Returns:
            MockDataFrame with extended statistics.

        Example:
            >>> df.summary("count", "mean", "50%")
            +-------+-----+
            |summary|count|mean|50% |
            +-------+-----+----+----+
            |age    |100  |25.5|24.0|
            +-------+-----+----+----+
        """
        from ..spark_types import MockStructType, MockStructField, StringType
        from .dataframe import MockDataFrame

        # Default statistics if none provided
        if not stats:
            stats = ("count", "mean", "stddev", "min", "25%", "50%", "75%", "max")

        # Find numeric columns
        numeric_cols = []
        for field in df.schema.fields:
            field_type = field.dataType.typeName()
            if field_type in ["long", "int", "integer", "bigint", "double", "float"]:
                numeric_cols.append(field.name)

        if not numeric_cols:
            # No numeric columns found
            return MockDataFrame([], df.schema, cast(Any, df.storage))  # type: ignore[has-type]

        # Calculate statistics for each column
        result_data = []

        for col in numeric_cols:
            # Extract values for this column
            values = []
            for row in df.data:
                value = row.get(col)
                if value is not None and isinstance(value, (int, float)):
                    values.append(value)

            if not values:
                # No valid numeric values
                stats_row = {"summary": col}
                for stat in stats:
                    stats_row[stat] = "NaN"
            else:
                stats_row = {"summary": col}
                values_sorted = sorted(values)
                n = len(values)

                for stat in stats:
                    if stat == "count":
                        stats_row[stat] = str(n)
                    elif stat == "mean":
                        stats_row[stat] = str(round(statistics.mean(values), 4))
                    elif stat == "stddev":
                        stats_row[stat] = str(round(statistics.stdev(values) if n > 1 else 0.0, 4))
                    elif stat == "min":
                        stats_row[stat] = str(values_sorted[0])
                    elif stat == "max":
                        stats_row[stat] = str(values_sorted[-1])
                    elif stat == "25%":
                        q1_idx = int(0.25 * (n - 1))
                        stats_row[stat] = str(values_sorted[q1_idx])
                    elif stat == "50%":
                        q2_idx = int(0.5 * (n - 1))
                        stats_row[stat] = str(values_sorted[q2_idx])
                    elif stat == "75%":
                        q3_idx = int(0.75 * (n - 1))
                        stats_row[stat] = str(values_sorted[q3_idx])
                    else:
                        stats_row[stat] = "NaN"

            result_data.append(stats_row)

        # Create result schema
        result_fields = [MockStructField("summary", StringType())]
        for stat in stats:
            result_fields.append(MockStructField(stat, StringType()))

        result_schema = MockStructType(result_fields)
        return MockDataFrame(result_data, result_schema, cast(Any, df.storage))  # type: ignore[has-type]
