"""
Mock Window functions implementation for PySpark compatibility.

This module provides comprehensive mock implementations of PySpark window
functions that behave identically to the real PySpark window functions.
Includes window specifications, partitioning, ordering, and boundary
definitions for advanced analytics operations.

Key Features:
    - Complete PySpark Window API compatibility
    - Window specification with partitionBy and orderBy
    - Row-based and range-based window boundaries
    - Window functions (row_number, rank, etc.)
    - Proper partitioning and ordering logic

Example:
    >>> from mock_spark.window import MockWindow
    >>> from mock_spark import F
    >>> window = MockWindow.partitionBy("department").orderBy("salary")
    >>> df.select(F.row_number().over(window).alias("rank"))
"""

from typing import List, Optional, Union, Tuple
from .functions import MockColumn


class MockWindowSpec:
    """Mock WindowSpec for window function specifications.

    Provides a PySpark-compatible interface for defining window specifications
    including partitioning, ordering, and boundary conditions for window functions.

    Attributes:
        _partition_by: List of columns to partition by.
        _order_by: List of columns to order by.
        _rows_between: Row-based window boundaries.
        _range_between: Range-based window boundaries.

    Example:
        >>> window = MockWindowSpec()
        >>> window.partitionBy("department").orderBy("salary")
        >>> window.rowsBetween(-1, 1)
    """

    def __init__(self) -> None:
        self._partition_by: List[Union[str, MockColumn]] = []
        self._order_by: List[Union[str, MockColumn]] = []
        self._rows_between: Optional[Tuple[int, int]] = None
        self._range_between: Optional[Tuple[int, int]] = None

    def partitionBy(self, *cols: Union[str, MockColumn]) -> "MockWindowSpec":
        """Add partition by columns."""
        self._partition_by = list(cols)
        return self

    def orderBy(self, *cols: Union[str, MockColumn]) -> "MockWindowSpec":
        """Add order by columns."""
        self._order_by = list(cols)
        return self

    def rowsBetween(self, start: int, end: int) -> "MockWindowSpec":
        """Set rows between boundaries."""
        self._rows_between = (start, end)
        return self

    def rangeBetween(self, start: int, end: int) -> "MockWindowSpec":
        """Set range between boundaries."""
        self._range_between = (start, end)
        return self

    def __repr__(self) -> str:
        """String representation."""
        parts = []
        if self._partition_by:
            parts.append(
                f"partitionBy({', '.join(str(col) for col in self._partition_by)})"
            )
        if self._order_by:
            parts.append(f"orderBy({', '.join(str(col) for col in self._order_by)})")
        if self._rows_between:
            parts.append(
                f"rowsBetween({self._rows_between[0]}, {self._rows_between[1]})"
            )
        if self._range_between:
            parts.append(
                f"rangeBetween({self._range_between[0]}, {self._range_between[1]})"
            )
        return f"MockWindowSpec({', '.join(parts)})"


class MockWindow:
    """Mock Window class for creating window specifications.

    Provides static methods for creating window specifications with partitioning,
    ordering, and boundary conditions. Equivalent to PySpark's Window class.

    Example:
        >>> MockWindow.partitionBy("department")
        >>> MockWindow.orderBy("salary")
        >>> MockWindow.partitionBy("department").orderBy("salary")
    """

    @staticmethod
    def partitionBy(*cols: Union[str, MockColumn]) -> MockWindowSpec:
        """Create a window spec with partition by columns."""
        return MockWindowSpec().partitionBy(*cols)

    @staticmethod
    def orderBy(*cols: Union[str, MockColumn]) -> MockWindowSpec:
        """Create a window spec with order by columns."""
        return MockWindowSpec().orderBy(*cols)

    @staticmethod
    def rowsBetween(start: int, end: int) -> MockWindowSpec:
        """Create a window spec with rows between boundaries."""
        return MockWindowSpec().rowsBetween(start, end)

    @staticmethod
    def rangeBetween(start: int, end: int) -> MockWindowSpec:
        """Create a window spec with range between boundaries."""
        return MockWindowSpec().rangeBetween(start, end)
