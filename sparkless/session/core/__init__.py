"""
Core session components for Sparkless.

This module provides the core session components including the main session class,
builder pattern implementation, and context management.
"""

from .session import SparkSession
from .builder import SparkSessionBuilder
from ..context import SparkContext, JVMContext, MockJVMFunctions

__all__ = [
    "SparkSession",
    "SparkSessionBuilder",
    "SparkContext",
    "JVMContext",
    "MockJVMFunctions",
]
