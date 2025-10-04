"""
DataFrame operations module.

This module contains all DataFrame operation implementations organized by category.
"""

from .selection import SelectionOperations
from .filtering import FilteringOperations
from .grouping import GroupingOperations
from .ordering import OrderingOperations
from .joining import JoiningOperations
from .aggregation import AggregationOperations

__all__ = [
    "SelectionOperations",
    "FilteringOperations", 
    "GroupingOperations",
    "OrderingOperations",
    "JoiningOperations",
    "AggregationOperations",
]
