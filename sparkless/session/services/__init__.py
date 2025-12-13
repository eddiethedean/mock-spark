"""
Session services for SparkSession.

This module provides service classes that handle specific responsibilities
for the SparkSession, following the Single Responsibility Principle.
"""

from .protocols import (
    IDataFrameFactory,
    ISQLParameterBinder,
    ISessionLifecycleManager,
    IMockingCoordinator,
)

from .dataframe_factory import DataFrameFactory
from .sql_parameter_binder import SQLParameterBinder
from .lifecycle_manager import SessionLifecycleManager
from .mocking_coordinator import MockingCoordinator

__all__ = [
    # Protocols
    "IDataFrameFactory",
    "ISQLParameterBinder",
    "ISessionLifecycleManager",
    "IMockingCoordinator",
    # Implementations
    "DataFrameFactory",
    "SQLParameterBinder",
    "SessionLifecycleManager",
    "MockingCoordinator",
]
