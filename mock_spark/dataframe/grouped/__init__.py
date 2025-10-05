"""
Grouped DataFrame operations module for Mock Spark.

This module provides grouped data functionality for DataFrame aggregation
operations, maintaining compatibility with PySpark's GroupedData interface.
"""

from .base import MockGroupedData
from .rollup import MockRollupGroupedData
from .cube import MockCubeGroupedData
from .pivot import MockPivotGroupedData

__all__ = [
    "MockGroupedData",
    "MockRollupGroupedData",
    "MockCubeGroupedData",
    "MockPivotGroupedData",
]
