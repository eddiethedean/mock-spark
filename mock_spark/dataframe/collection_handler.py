"""
Collection handler for MockDataFrame.

This module handles data collection and materialization operations
following the Single Responsibility Principle.
"""

from typing import List, Dict, Any, Iterator, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from ..spark_types import MockRow, MockStructType


class CollectionHandler:
    """Handles data collection and materialization operations."""

    def collect(
        self, data: List[Dict[str, Any]], schema: "MockStructType"
    ) -> List["MockRow"]:
        """Convert data to MockRow objects."""
        from ..spark_types import MockRow

        return [MockRow(row, schema) for row in data]

    def take(
        self, data: List[Dict[str, Any]], schema: "MockStructType", n: int
    ) -> List["MockRow"]:
        """Take first n rows."""
        from ..spark_types import MockRow

        return [MockRow(row, schema) for row in data[:n]]

    def head(
        self, data: List[Dict[str, Any]], schema: "MockStructType", n: int = 1
    ) -> Union["MockRow", List["MockRow"], None]:
        """Get first row(s)."""
        if not data:
            return None
        if n == 1:
            from ..spark_types import MockRow

            return MockRow(data[0], schema)
        return self.take(data, schema, n)

    def tail(
        self, data: List[Dict[str, Any]], schema: "MockStructType", n: int = 1
    ) -> Union["MockRow", List["MockRow"], None]:
        """Get last n rows."""
        if not data:
            return None
        if n == 1:
            from ..spark_types import MockRow

            return MockRow(data[-1], schema)
        return self.take(data[-n:], schema, n)

    def to_local_iterator(
        self,
        data: List[Dict[str, Any]],
        schema: "MockStructType",
        prefetch: bool = False,
    ) -> Iterator["MockRow"]:
        """Return iterator over rows."""
        return iter(self.collect(data, schema))
