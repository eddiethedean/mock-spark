"""
Window functions for Mock Spark.

This module contains window function implementations including row_number, rank, etc.
"""

from typing import Any, List
from mock_spark.window import MockWindowSpec


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
        self.function_name = getattr(function, "function_name", "window_function")
        self.column_name = getattr(function, "column", None)
        if self.column_name and hasattr(self.column_name, "name"):
            self.column_name = self.column_name.name
        elif self.column_name and isinstance(self.column_name, str):
            self.column_name = self.column_name
        else:
            self.column_name = None
        self.name = self._generate_name()

        # Add column property for compatibility with query executor
        self.column = getattr(function, "column", None)

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
        elif self.function_name == "nth_value":
            return self._evaluate_nth_value(data)
        elif self.function_name == "ntile":
            return self._evaluate_ntile(data)
        elif self.function_name == "cume_dist":
            return self._evaluate_cume_dist(data)
        elif self.function_name == "percent_rank":
            return self._evaluate_percent_rank(data)
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
        order_columns = getattr(self.window_spec, "_order_by", [])
        if not order_columns:
            # If no ordering, return row numbers
            return list(range(1, len(data) + 1))

        # Extract the first ordering column (for simplicity)
        order_col = order_columns[0]
        if hasattr(order_col, "name"):
            col_name = order_col.name
        else:
            col_name = str(order_col)

        # Get values for ranking
        values = []
        for i, row in enumerate(data):
            value = row.get(col_name)
            values.append((value, i))  # (value, original_index)

        # Check if ordering is descending
        is_desc = False
        if hasattr(order_col, "operation") and order_col.operation == "desc":
            is_desc = True

        # Sort by value (check for DESC)
        if is_desc:
            values.sort(
                key=lambda x: x[0] if x[0] is not None else float("-inf"), reverse=True
            )
        else:
            values.sort(key=lambda x: x[0] if x[0] is not None else float("inf"))

        # Assign ranks (PySpark rank behavior: same rank for ties, skip ranks after ties)
        ranks = [0] * len(data)
        current_rank = 1

        for i, (value, original_idx) in enumerate(values):
            if i == 0:
                ranks[original_idx] = current_rank
            else:
                prev_value = values[i - 1][0]
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
        results: List[Any] = [None]  # First row has no previous value
        for i in range(1, len(data)):
            results.append(data[i - 1])
        return results

    def _evaluate_lead(self, data: List[dict]) -> List[Any]:
        """Evaluate lead() window function."""
        results: List[Any] = []
        for i in range(len(data) - 1):
            results.append(data[i + 1])
        results.append(None)  # Last row has no next value
        return results

    def _evaluate_nth_value(self, data: List[dict]) -> List[Any]:
        """Evaluate nth_value() window function."""
        # For simplicity, return the first value for all rows
        if not data:
            return []
        return [data[0]] * len(data)

    def _evaluate_ntile(self, data: List[dict]) -> List[int]:
        """Evaluate ntile() window function."""
        if not data:
            return []
        # For simplicity, return equal distribution
        n = len(data)
        return list(range(1, n + 1))

    def _evaluate_cume_dist(self, data: List[dict]) -> List[float]:
        """Evaluate cume_dist() window function."""
        if not data:
            return []
        n = len(data)
        return [i / n for i in range(1, n + 1)]

    def _evaluate_percent_rank(self, data: List[dict]) -> List[float]:
        """Evaluate percent_rank() window function."""
        if not data:
            return []
        n = len(data)
        return [i / (n - 1) if n > 1 else 0.0 for i in range(n)]
