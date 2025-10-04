"""
Core abstractions and interfaces for Mock Spark.

This module provides the foundational interfaces and abstractions that define
the contract for all Mock Spark components, ensuring consistency and enabling
dependency injection throughout the system.
"""

from .interfaces.dataframe import IDataFrame, IDataFrameWriter, IDataFrameReader
from .interfaces.session import ISession, ISparkContext, ICatalog
from .interfaces.storage import IStorageManager, ITable, ISchema
from .interfaces.functions import IFunction, IColumnFunction, IAggregateFunction

__all__ = [
    "IDataFrame",
    "IDataFrameWriter",
    "IDataFrameReader",
    "ISession",
    "ISparkContext",
    "ICatalog",
    "IStorageManager",
    "ITable",
    "ISchema",
    "IFunction",
    "IColumnFunction",
    "IAggregateFunction",
]
