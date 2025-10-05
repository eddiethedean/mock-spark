"""
Mock Spark Analytics Module for 1.0.0

Provides advanced analytical operations using DuckDB's analytical engine.
"""

from .analytics_engine import AnalyticsEngine
from .statistical_functions import StatisticalFunctions
from .time_series import TimeSeriesAnalysis
from .ml_preprocessing import MLPreprocessing

__all__ = [
    "AnalyticsEngine",
    "StatisticalFunctions", 
    "TimeSeriesAnalysis",
    "MLPreprocessing"
]
