"""
Collection handler for DataFrame.

This module handles data collection and materialization operations
following the Single Responsibility Principle.
"""

from typing import List, Dict, Any, Iterator, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from ..spark_types import Row, StructType


class CollectionHandler:
    """Handles data collection and materialization operations."""

    def collect(self, data: List[Dict[str, Any]], schema: "StructType") -> List["Row"]:
        """Convert data to Row objects."""
        from ..spark_types import Row

        return [Row(row, schema) for row in data]

    def take(
        self, data: List[Dict[str, Any]], schema: "StructType", n: int
    ) -> List["Row"]:
        """Take first n rows."""
        from ..spark_types import Row

        return [Row(row, schema) for row in data[:n]]

    def head(
        self, data: List[Dict[str, Any]], schema: "StructType", n: int = 1
    ) -> Union["Row", List["Row"], None]:
        """Get first row(s)."""
        if not data:
            return None
        if n == 1:
            from ..spark_types import Row

            return Row(data[0], schema)
        return self.take(data, schema, n)

    def tail(
        self, data: List[Dict[str, Any]], schema: "StructType", n: int = 1
    ) -> Union["Row", List["Row"], None]:
        """Get last n rows."""
        if not data:
            return None
        if n == 1:
            from ..spark_types import Row

            return Row(data[-1], schema)
        return self.take(data[-n:], schema, n)

    def to_local_iterator(
        self,
        data: List[Dict[str, Any]],
        schema: "StructType",
        prefetch: bool = False,
    ) -> Iterator["Row"]:
        """Return iterator over rows."""
        return iter(self.collect(data, schema))
