"""
Window functions for Mock Spark.

This module contains window function implementations including row_number, rank, etc.
"""

from typing import Any, List, Union, Optional
from mock_spark.spark_types import MockDataType, StringType


class MockWindowFunction:
    """Represents a window function.

    This class handles window functions like row_number(), rank(), etc.
    that operate over a window specification.
    """

    def __init__(self, function: Any, window_spec: "MockWindowSpec"):
        """Initialize MockWindowFunction.

        Args:
            function: The window function (e.g., row_number(), rank()).
            window_spec: The window specification.
        """
        self.function = function
        self.window_spec = window_spec
        self.function_name = getattr(function, 'function_name', 'window_function')
        self.name = self._generate_name()

    def _generate_name(self) -> str:
        """Generate a name for this window function."""
        return f"{self.function_name}() OVER ({self.window_spec})"

    def alias(self, name: str) -> "MockWindowFunction":
        """Create an alias for this window function.

        Args:
            name: The alias name.

        Returns:
            Self for method chaining.
        """
        self.name = name
        return self

    def evaluate(self, data: List[dict]) -> List[Any]:
        """Evaluate the window function over the data.

        Args:
            data: List of data rows.

        Returns:
            List of window function results.
        """
        if self.function_name == "row_number":
            return self._evaluate_row_number(data)
        elif self.function_name == "rank":
            return self._evaluate_rank(data)
        elif self.function_name == "dense_rank":
            return self._evaluate_dense_rank(data)
        elif self.function_name == "lag":
            return self._evaluate_lag(data)
        elif self.function_name == "lead":
            return self._evaluate_lead(data)
        else:
            return [None] * len(data)

    def _evaluate_row_number(self, data: List[dict]) -> List[int]:
        """Evaluate row_number() window function."""
        return list(range(1, len(data) + 1))

    def _evaluate_rank(self, data: List[dict]) -> List[int]:
        """Evaluate rank() window function."""
        if not data:
            return []
        
        # Get the ordering columns from window spec
        order_columns = getattr(self.window_spec, '_order_by', [])
        if not order_columns:
            # If no ordering, return row numbers
            return list(range(1, len(data) + 1))
        
        # Extract the first ordering column (for simplicity)
        order_col = order_columns[0]
        if hasattr(order_col, 'name'):
            col_name = order_col.name
        else:
            col_name = str(order_col)
        
        # Get values for ranking
        values = []
        for i, row in enumerate(data):
            value = row.get(col_name)
            values.append((value, i))  # (value, original_index)
        
        # Sort by value (ascending by default)
        values.sort(key=lambda x: x[0] if x[0] is not None else float('inf'))
        
        # Assign ranks (PySpark rank behavior: same rank for ties, skip ranks after ties)
        ranks = [0] * len(data)
        current_rank = 1
        
        for i, (value, original_idx) in enumerate(values):
            if i == 0:
                ranks[original_idx] = current_rank
            else:
                prev_value = values[i-1][0]
                if value != prev_value:
                    # Different value, assign new rank
                    current_rank = i + 1
                # Same value gets the same rank (no increment)
                ranks[original_idx] = current_rank
        
        return ranks

    def _evaluate_dense_rank(self, data: List[dict]) -> List[int]:
        """Evaluate dense_rank() window function."""
        # Simple dense rank implementation - returns row numbers for now
        return list(range(1, len(data) + 1))

    def _evaluate_lag(self, data: List[dict]) -> List[Any]:
        """Evaluate lag() window function."""
        results = [None]  # First row has no previous value
        for i in range(1, len(data)):
            results.append(data[i - 1])
        return results

    def _evaluate_lead(self, data: List[dict]) -> List[Any]:
        """Evaluate lead() window function."""
        results = []
        for i in range(len(data) - 1):
            results.append(data[i + 1])
        results.append(None)  # Last row has no next value
        return results
