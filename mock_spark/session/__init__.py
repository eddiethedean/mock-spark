"""
Session management module for Mock Spark.

This module provides session management, SQL processing, and configuration
management for Mock Spark, following the Single Responsibility Principle
and enabling better testability and modularity.

Components:
    - MockSparkSession: Main session class
    - MockSparkContext: Spark context management
    - MockCatalog: Database and table catalog operations
    - Configuration management
    - SQL processing pipeline
"""

from .core import (
    MockSparkSession,
    MockSparkSessionBuilder,
    MockSparkContext,
    MockJVMContext,
)
from .catalog import MockCatalog, MockDatabase, MockTable
from .config import MockConfiguration

__all__ = [
    "MockSparkSession",
    "MockSparkSessionBuilder",
    "MockSparkContext",
    "MockJVMContext",
    "MockCatalog",
    "MockDatabase",
    "MockTable",
    "MockConfiguration",
]
