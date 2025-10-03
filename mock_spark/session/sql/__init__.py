"""
SQL processing module for Mock Spark.

This module provides SQL parsing, execution, and optimization
for Mock Spark, separated from session management for better
modularity and testability.

Components:
    - SQL Parser: Parse SQL queries into AST
    - SQL Executor: Execute parsed SQL queries
    - Query Optimizer: Optimize query execution plans
    - SQL Validator: Validate SQL syntax and semantics
"""

from .parser import MockSQLParser
from .executor import MockSQLExecutor
from .optimizer import MockQueryOptimizer
from .validation import MockSQLValidator

__all__ = [
    "MockSQLParser",
    "MockSQLExecutor", 
    "MockQueryOptimizer",
    "MockSQLValidator",
]
