"""
DataFrame statistical operations for Mock Spark.

This module provides statistical operations like count, describe, summary, etc.
"""

from typing import List, Iterator, TYPE_CHECKING

if TYPE_CHECKING:
    from .dataframe_refactored import MockDataFrame
    import pandas
import statistics
from ..spark_types import MockStructType, MockStructField, StringType, MockRow
from ..core.exceptions.analysis import ColumnNotFoundException


class DataFrameStatistics:
    """Handles DataFrame statistical operations (count, describe, summary, etc.)."""

    def __init__(self, dataframe: "MockDataFrame"):
        """Initialize statistics handler.

        Args:
            dataframe: The DataFrame instance to analyze
        """
        self.dataframe = dataframe

    def count(self) -> int:
        """Count number of rows.

        Returns:
            Number of rows in the DataFrame
        """
        # Materialize lazy operations if needed
        if self.dataframe._operations_queue:
            materialized = self.dataframe._materialize_if_lazy()
            # Don't call count() recursively - just return the length of materialized data
            return len(materialized.data)

        if self.dataframe._cached_count is None:
            self.dataframe._cached_count = len(self.dataframe.data)
        return self.dataframe._cached_count

    def describe(self, *cols: str) -> "MockDataFrame":
        """Compute basic statistics for numeric columns.

        Args:
            *cols: Column names to describe. If empty, describes all numeric columns.

        Returns:
            MockDataFrame with statistics (count, mean, stddev, min, max).
        """
        # Determine which columns to describe
        if not cols:
            # Describe all numeric columns
            numeric_cols = []
            for field in self.dataframe.schema.fields:
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
            available_cols = [field.name for field in self.dataframe.schema.fields]
            for col in numeric_cols:
                if col not in available_cols:
                    raise ColumnNotFoundException(col)

        if not numeric_cols:
            # No numeric columns found
            return self.dataframe.__class__(
                [], self.dataframe.schema, self.dataframe.storage
            )

        # Calculate statistics for each column
        result_data = []

        for col in numeric_cols:
            # Extract values for this column
            values = []
            for row in self.dataframe.data:
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
                    "stddev": str(
                        round(statistics.stdev(values) if len(values) > 1 else 0.0, 4)
                    ),
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

        return self.dataframe.__class__(
            result_data, result_schema, self.dataframe.storage
        )

    def summary(self, *stats: str) -> "MockDataFrame":
        """Compute extended statistics for numeric columns.

        Args:
            *stats: Statistics to compute. Default: ["count", "mean", "stddev", "min", "25%", "50%", "75%", "max"].

        Returns:
            MockDataFrame with extended statistics.
        """
        # Default statistics if none provided
        if not stats:
            stats = ("count", "mean", "stddev", "min", "25%", "50%", "75%", "max")

        # Find numeric columns
        numeric_cols = []
        for field in self.dataframe.schema.fields:
            field_type = field.dataType.typeName()
            if field_type in ["long", "int", "integer", "bigint", "double", "float"]:
                numeric_cols.append(field.name)

        if not numeric_cols:
            # No numeric columns found
            return self.dataframe.__class__(
                [], self.dataframe.schema, self.dataframe.storage
            )

        # Calculate statistics for each column
        result_data = []

        for col in numeric_cols:
            # Extract values for this column
            values = []
            for row in self.dataframe.data:
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
                        stats_row[stat] = str(
                            round(statistics.stdev(values) if n > 1 else 0.0, 4)
                        )
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
        return self.dataframe.__class__(
            result_data, result_schema, self.dataframe.storage
        )

    def collect(self) -> List["MockRow"]:
        """Collect all rows into a list.

        Returns:
            List of MockRow objects
        """
        # Materialize if lazy
        materialized = self.dataframe._materialize_if_lazy()
        return [MockRow(row) for row in materialized.data]

    def toPandas(self) -> "pandas.DataFrame":
        """Convert to pandas DataFrame.

        Returns:
            pandas DataFrame
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("pandas is required for toPandas() method")

        # Materialize if lazy
        materialized = self.dataframe._materialize_if_lazy()
        return pd.DataFrame(materialized.data)

    def toLocalIterator(self) -> "Iterator[MockRow]":
        """Convert to local iterator.

        Returns:
            Iterator of MockRow objects
        """
        # Materialize if lazy
        materialized = self.dataframe._materialize_if_lazy()
        return iter([MockRow(row) for row in materialized.data])

    def isEmpty(self) -> bool:
        """Check if DataFrame is empty.

        Returns:
            True if DataFrame is empty, False otherwise
        """
        return self.count() == 0

    def distinct(self) -> "MockDataFrame":
        """Return distinct rows.

        Returns:
            New MockDataFrame with distinct rows
        """
        # Materialize if lazy
        materialized = self.dataframe._materialize_if_lazy()

        # Get distinct rows by converting to tuples for hashing
        seen = set()
        distinct_data = []

        for row in materialized.data:
            # Convert row to tuple for hashing
            row_tuple = tuple(sorted(row.items()))
            if row_tuple not in seen:
                seen.add(row_tuple)
                distinct_data.append(row)

        return self.dataframe.__class__(
            distinct_data, materialized.schema, materialized.storage
        )
