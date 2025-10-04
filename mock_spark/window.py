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
    >>> from mock_spark import F, MockSparkSession
    >>> spark = MockSparkSession("test")
    >>> data = [{"department": "IT", "salary": 50000}, {"department": "IT", "salary": 60000}]
    >>> df = spark.createDataFrame(data)
    >>> window = MockWindow.partitionBy("department").orderBy("salary")
    >>> result = df.select(F.row_number().over(window).alias("rank"))
    >>> result.show()
    +--- MockDataFrame: 2 rows ---+
            rank
    ------------
               1
               2
"""

import sys
from typing import List, Optional, Union, Tuple, TYPE_CHECKING, Type

if TYPE_CHECKING:
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
        self._partition_by: List[Union[str, "MockColumn"]] = []
        self._order_by: List[Union[str, "MockColumn"]] = []
        self._rows_between: Optional[Tuple[int, int]] = None
        self._range_between: Optional[Tuple[int, int]] = None

    def partitionBy(self, *cols: Union[str, "MockColumn"]) -> "MockWindowSpec":
        """Add partition by columns.

        Args:
            *cols: Column names or "MockColumn" objects to partition by.

        Returns:
            Self for method chaining.

        Raises:
            ValueError: If no columns provided or invalid column types.
        """
        if not cols:
            raise ValueError("At least one column must be specified for partitionBy")

        for col in cols:
            # Check if it's a string or has the name attribute (MockColumn-like)
            if not isinstance(col, str) and not hasattr(col, "name"):
                raise ValueError(f"Invalid column type: {type(col)}. Must be str or MockColumn")

        self._partition_by = list(cols)
        return self

    def orderBy(self, *cols: Union[str, "MockColumn"]) -> "MockWindowSpec":
        """Add order by columns.

        Args:
            *cols: Column names or "MockColumn" objects to order by.

        Returns:
            Self for method chaining.

        Raises:
            ValueError: If no columns provided or invalid column types.
        """
        if not cols:
            raise ValueError("At least one column must be specified for orderBy")

        for col in cols:
            # Check if it's a string or has the name attribute (MockColumn-like)
            if not isinstance(col, str) and not hasattr(col, "name"):
                raise ValueError(f"Invalid column type: {type(col)}. Must be str or MockColumn")

        self._order_by = list(cols)
        return self

    def rowsBetween(self, start: int, end: int) -> "MockWindowSpec":
        """Set rows between boundaries.

        Args:
            start: Starting row offset (negative for preceding rows).
            end: Ending row offset (positive for following rows).

        Returns:
            Self for method chaining.

        Raises:
            ValueError: If start > end or invalid values.
        """
        if start > end:
            raise ValueError(f"start ({start}) cannot be greater than end ({end})")

        self._rows_between = (start, end)
        return self

    def rangeBetween(self, start: int, end: int) -> "MockWindowSpec":
        """Set range between boundaries.

        Args:
            start: Starting range offset (negative for preceding range).
            end: Ending range offset (positive for following range).

        Returns:
            Self for method chaining.

        Raises:
            ValueError: If start > end or invalid values.
        """
        if start > end:
            raise ValueError(f"start ({start}) cannot be greater than end ({end})")

        self._range_between = (start, end)
        return self

    def __repr__(self) -> str:
        """String representation."""
        parts = []
        if self._partition_by:
            parts.append(f"partitionBy({', '.join(str(col) for col in self._partition_by)})")
        if self._order_by:
            parts.append(f"orderBy({', '.join(str(col) for col in self._order_by)})")
        if self._rows_between:
            parts.append(f"rowsBetween({self._rows_between[0]}, {self._rows_between[1]})")
        if self._range_between:
            parts.append(f"rangeBetween({self._range_between[0]}, {self._range_between[1]})")
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

    # Window boundary constants
    currentRow = 0
    unboundedPreceding = -sys.maxsize - 1
    unboundedFollowing = sys.maxsize

    @staticmethod
    def partitionBy(*cols: Union[str, "MockColumn"]) -> MockWindowSpec:
        """Create a window spec with partition by columns."""
        return MockWindowSpec().partitionBy(*cols)

    @staticmethod
    def orderBy(*cols: Union[str, "MockColumn"]) -> MockWindowSpec:
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
