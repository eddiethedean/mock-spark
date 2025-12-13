"""
Collection handler for DataFrame.

This module handles data collection and materialization operations
following the Single Responsibility Principle.
"""

from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Union

if TYPE_CHECKING:
    from ..spark_types import Row, StructType


class CollectionHandler:
    """Handles data collection and materialization operations."""

    def collect(self, data: list[dict[str, Any]], schema: "StructType") -> list["Row"]:
        """Convert data to Row objects."""
        from ..spark_types import Row

        return [Row(row, schema) for row in data]

    def take(
        self, data: list[dict[str, Any]], schema: "StructType", n: int
    ) -> list["Row"]:
        """Take first n rows."""
        from ..spark_types import Row

        return [Row(row, schema) for row in data[:n]]

    def head(
        self, data: list[dict[str, Any]], schema: "StructType", n: int = 1
    ) -> Union["Row", list["Row"], None]:
        """Get first row(s)."""
        if not data:
            return None
        # PySpark always returns a list, even when n=1
        from ..spark_types import Row
        rows = [Row(data[i], schema) for i in range(min(n, len(data)))]
        return rows

    def tail(
        self, data: list[dict[str, Any]], schema: "StructType", n: int = 1
    ) -> Union["Row", list["Row"], None]:
        """Get last n rows."""
        if not data:
            return None
        # PySpark always returns a list, even when n=1
        from ..spark_types import Row
        rows = [Row(data[i], schema) for i in range(max(0, len(data) - n), len(data))]
        return rows

    def to_local_iterator(
        self,
        data: list[dict[str, Any]],
        schema: "StructType",
        prefetch: bool = False,
    ) -> Iterator["Row"]:
        """Return iterator over rows."""
        return iter(self.collect(data, schema))
