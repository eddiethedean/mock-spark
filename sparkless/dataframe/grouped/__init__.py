"""
Grouped DataFrame operations module for Sparkless.

This module provides grouped data functionality for DataFrame aggregation
operations, maintaining compatibility with PySpark's GroupedData interface.
"""

from .base import GroupedData
from .rollup import RollupGroupedData
from .cube import CubeGroupedData
from .pivot import PivotGroupedData

__all__ = [
    "GroupedData",
    "RollupGroupedData",
    "CubeGroupedData",
    "PivotGroupedData",
]
