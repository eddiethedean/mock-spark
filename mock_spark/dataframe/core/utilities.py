"""
DataFrame utilities module for Mock Spark.

This module contains DataFrame utility operations including ordering,
limiting, sampling, statistics, and other utility methods.
"""

from typing import Any, Dict, List, Optional, Union, Tuple
import random
import json

from ...functions import MockColumn, MockColumnOperation
from ...spark_types import MockRow, MockStructType, MockStructField, StringType, LongType, DoubleType
from .dataframe import MockDataFrame


class DataFrameUtilities:
    """Mixin class providing DataFrame utility functionality."""

    def orderBy(self, *columns: Union[str, MockColumn]) -> "MockDataFrame":
        """Order by columns.
        
        Args:
            *columns: Column names or MockColumn objects to order by.
            
        Returns:
            New MockDataFrame with ordered data.
        """
        col_names: List[str] = []
        sort_orders: List[bool] = []

        for col in columns:
            if isinstance(col, MockColumn):
                col_names.append(col.name)
                sort_orders.append(True)  # Default ascending
            elif hasattr(col, "operation") and hasattr(col, "column"):
                # Handle MockColumnOperation (e.g., col.desc())
                if col.operation == "desc":
                    col_names.append(col.column.name)
                    sort_orders.append(False)  # Descending
                elif col.operation == "asc":
                    col_names.append(col.column.name)
                    sort_orders.append(True)  # Ascending
                else:
                    col_names.append(col.column.name)
                    sort_orders.append(True)  # Default ascending
            else:
                col_names.append(col)
                sort_orders.append(True)  # Default ascending

        # Sort data by columns with proper ordering
        def sort_key(row: Dict[str, Any]) -> Tuple[Any, ...]:
            key_values = []
            for i, col in enumerate(col_names):
                value = row.get(col, None)
                # Handle None values for sorting
                if value is None:
                    value = float("inf") if sort_orders[i] else float("-inf")
                key_values.append(value)
            return tuple(key_values)

        sorted_data = sorted(
            self.data, key=sort_key, reverse=any(not order for order in sort_orders)
        )

        return MockDataFrame(sorted_data, self.schema, self.storage)

    def limit(self, n: int) -> "MockDataFrame":
        """Limit number of rows.
        
        Args:
            n: Maximum number of rows to return.
            
        Returns:
            New MockDataFrame with limited rows.
        """
        limited_data = self.data[:n]
        return MockDataFrame(limited_data, self.schema, self.storage)

    def take(self, n: int) -> List[MockRow]:
        """Take first n rows as list of Row objects.
        
        Args:
            n: Number of rows to take.
            
        Returns:
            List of MockRow objects.
        """
        return [MockRow(row) for row in self.data[:n]]

    def head(self, n: int = 1) -> List[MockRow]:
        """Get first n rows as list of Row objects.
        
        Args:
            n: Number of rows to return (default: 1).
            
        Returns:
            List of MockRow objects.
        """
        return [MockRow(row) for row in self.data[:n]]

    def tail(self, n: int = 1) -> List[MockRow]:
        """Get last n rows as list of Row objects.
        
        Args:
            n: Number of rows to return (default: 1).
            
        Returns:
            List of MockRow objects.
        """
        return [MockRow(row) for row in self.data[-n:]]

    def toJSON(self) -> "MockDataFrame":
        """Convert DataFrame to JSON format.
        
        Returns:
            New MockDataFrame with JSON string column.
        """
        json_data = []
        for row in self.data:
            json_str = json.dumps(row)
            json_data.append({"value": json_str})

        new_schema = MockStructType([MockStructField("value", StringType())])
        return MockDataFrame(json_data, new_schema, self.storage)

    def repartition(self, numPartitions: int, *cols) -> "MockDataFrame":
        """Repartition DataFrame (no-op in mock).
        
        Args:
            numPartitions: Number of partitions (ignored in mock).
            *cols: Columns to partition by (ignored in mock).
            
        Returns:
            Same DataFrame (no-op).
        """
        return self

    def coalesce(self, numPartitions: int) -> "MockDataFrame":
        """Coalesce partitions (no-op in mock).
        
        Args:
            numPartitions: Number of partitions (ignored in mock).
            
        Returns:
            Same DataFrame (no-op).
        """
        return self

    def checkpoint(self, eager: bool = False) -> "MockDataFrame":
        """Checkpoint DataFrame (no-op in mock).
        
        Args:
            eager: Whether to checkpoint eagerly (ignored in mock).
            
        Returns:
            Same DataFrame (no-op).
        """
        return self

    def cache(self) -> "MockDataFrame":
        """Cache DataFrame (no-op in mock).
        
        Returns:
            Same DataFrame (no-op).
        """
        return self

    def persist(self) -> "MockDataFrame":
        """Persist DataFrame (no-op in mock).
        
        Returns:
            Same DataFrame (no-op).
        """
        return self

    def unpersist(self) -> "MockDataFrame":
        """Unpersist DataFrame (no-op in mock).
        
        Returns:
            Same DataFrame (no-op).
        """
        return self

    def sample(
        self,
        withReplacement: bool = False,
        fraction: float = 0.1,
        seed: Optional[int] = None,
    ) -> "MockDataFrame":
        """Sample rows from DataFrame.
        
        Args:
            withReplacement: Whether to sample with replacement.
            fraction: Fraction of rows to sample (0.0 to 1.0).
            seed: Random seed for reproducibility.
            
        Returns:
            New MockDataFrame with sampled rows.
        """
        if seed is not None:
            random.seed(seed)

        if withReplacement:
            # Sample with replacement
            sample_size = int(len(self.data) * fraction)
            sampled_data = random.choices(self.data, k=sample_size)
        else:
            # Sample without replacement
            sample_size = int(len(self.data) * fraction)
            sampled_data = random.sample(self.data, min(sample_size, len(self.data)))

        return MockDataFrame(sampled_data, self.schema, self.storage)

    def randomSplit(
        self, weights: List[float], seed: Optional[int] = None
    ) -> List["MockDataFrame"]:
        """Randomly split DataFrame into multiple DataFrames.
        
        Args:
            weights: List of weights for each split.
            seed: Random seed for reproducibility.
            
        Returns:
            List of MockDataFrames.
        """
        if seed is not None:
            random.seed(seed)

        # Normalize weights
        total_weight = sum(weights)
        normalized_weights = [w / total_weight for w in weights]

        # Calculate split points
        split_points = []
        cumulative_weight = 0.0
        for weight in normalized_weights:
            cumulative_weight += weight
            split_points.append(int(len(self.data) * cumulative_weight))

        # Shuffle data
        shuffled_data = self.data.copy()
        random.shuffle(shuffled_data)

        # Split data
        result_dataframes = []
        start_idx = 0
        for end_idx in split_points:
            split_data = shuffled_data[start_idx:end_idx]
            result_dataframes.append(MockDataFrame(split_data, self.schema, self.storage))
            start_idx = end_idx

        return result_dataframes

    def describe(self, *cols: str) -> "MockDataFrame":
        """Describe statistical summary of columns.
        
        Args:
            *cols: Column names to describe (if empty, describes all numeric columns).
            
        Returns:
            New MockDataFrame with statistical summary.
        """
        if not cols:
            # Describe all numeric columns
            numeric_cols = []
            for field in self.schema.fields:
                if field.dataType.__class__.__name__ in ["LongType", "DoubleType", "IntegerType"]:
                    numeric_cols.append(field.name)
            cols = numeric_cols

        if not cols:
            return MockDataFrame([], self.schema, self.storage)

        # Calculate statistics for each column
        summary_data = []
        for col in cols:
            values = [row.get(col) for row in self.data if row.get(col) is not None]
            if not values:
                summary_data.append({
                    "summary": col,
                    "count": 0,
                    "mean": None,
                    "stddev": None,
                    "min": None,
                    "max": None
                })
            else:
                try:
                    numeric_values = [float(v) for v in values]
                    summary_data.append({
                        "summary": col,
                        "count": len(numeric_values),
                        "mean": sum(numeric_values) / len(numeric_values),
                        "stddev": self._calculate_stddev(numeric_values),
                        "min": min(numeric_values),
                        "max": max(numeric_values)
                    })
                except (ValueError, TypeError):
                    # Non-numeric column
                    summary_data.append({
                        "summary": col,
                        "count": len(values),
                        "mean": None,
                        "stddev": None,
                        "min": None,
                        "max": None
                    })

        # Create schema for summary
        summary_schema = MockStructType([
            MockStructField("summary", StringType()),
            MockStructField("count", LongType()),
            MockStructField("mean", DoubleType()),
            MockStructField("stddev", DoubleType()),
            MockStructField("min", DoubleType()),
            MockStructField("max", DoubleType())
        ])

        return MockDataFrame(summary_data, summary_schema, self.storage)

    def summary(self, *stats: str) -> "MockDataFrame":
        """Generate summary statistics.
        
        Args:
            *stats: Statistics to calculate (default: count, mean, stddev, min, max).
            
        Returns:
            New MockDataFrame with summary statistics.
        """
        if not stats:
            stats = ("count", "mean", "stddev", "min", "max")

        # Get all numeric columns
        numeric_cols = []
        for field in self.schema.fields:
            if field.dataType.__class__.__name__ in ["LongType", "DoubleType", "IntegerType"]:
                numeric_cols.append(field.name)

        if not numeric_cols:
            return MockDataFrame([], self.schema, self.storage)

        # Calculate statistics for each column
        summary_data = []
        for col in numeric_cols:
            values = [row.get(col) for row in self.data if row.get(col) is not None]
            if not values:
                continue

            try:
                numeric_values = [float(v) for v in values]
                col_stats = {"summary": col}
                
                for stat in stats:
                    if stat == "count":
                        col_stats[stat] = len(numeric_values)
                    elif stat == "mean":
                        col_stats[stat] = sum(numeric_values) / len(numeric_values)
                    elif stat == "stddev":
                        col_stats[stat] = self._calculate_stddev(numeric_values)
                    elif stat == "min":
                        col_stats[stat] = min(numeric_values)
                    elif stat == "max":
                        col_stats[stat] = max(numeric_values)
                    else:
                        col_stats[stat] = None

                summary_data.append(col_stats)
            except (ValueError, TypeError):
                continue

        # Create schema for summary
        fields = [MockStructField("summary", StringType())]
        for stat in stats:
            if stat == "count":
                fields.append(MockStructField(stat, LongType()))
            else:
                fields.append(MockStructField(stat, DoubleType()))

        summary_schema = MockStructType(fields)
        return MockDataFrame(summary_data, summary_schema, self.storage)

    def _calculate_stddev(self, values: List[float]) -> float:
        """Calculate standard deviation."""
        if len(values) <= 1:
            return 0.0
        
        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / (len(values) - 1)
        return variance ** 0.5
