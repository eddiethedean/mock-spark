"""
Core interfaces for Sparkless components.

This module defines the abstract interfaces that all Sparkless components
must implement, ensuring consistency, type safety, and enabling dependency injection.
These interfaces provide the contract for all major Sparkless functionality.

Key Interfaces:
    - IDataFrame, IDataFrameWriter, IDataFrameReader: DataFrame operations
    - ISession, ISparkContext, ICatalog: Session and context management
    - IStorageManager, ITable, ISchema: Storage and schema operations
    - IFunction, IColumnFunction, IAggregateFunction: Function system

Benefits:
    - Ensures consistent API across all implementations
    - Enables easy testing with mock implementations
    - Supports dependency injection and modular architecture
    - Provides clear contracts for extension and customization

Example:
    >>> from sparkless.core.interfaces import IDataFrame, ISession
    >>> # These interfaces define the contract for DataFrame and Session implementations
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
