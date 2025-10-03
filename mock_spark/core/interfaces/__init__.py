"""
Core interfaces for Mock Spark components.

This module defines the abstract interfaces that all Mock Spark components
must implement, ensuring consistency and enabling dependency injection.
"""

from .dataframe import IDataFrame, IDataFrameWriter, IDataFrameReader
from .session import ISession, ISparkContext, ICatalog
from .storage import IStorageManager, ITable, ISchema
from .functions import IFunction, IColumnFunction, IAggregateFunction

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
