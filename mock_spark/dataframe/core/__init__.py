"""
Core DataFrame functionality for Mock Spark.

This module contains the core DataFrame implementation and related operations
that have been refactored from the monolithic dataframe.py file for better
maintainability and organization.
"""

from .dataframe import MockDataFrame
from .operations import DataFrameOperations
from .joins import DataFrameJoins
from .aggregations import DataFrameAggregations
from .utilities import DataFrameUtilities

__all__ = [
    'MockDataFrame',
    'DataFrameOperations', 
    'DataFrameJoins',
    'DataFrameAggregations',
    'DataFrameUtilities'
]
