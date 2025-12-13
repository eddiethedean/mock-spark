"""
SQL processing module for Sparkless.

This module provides SQL parsing, execution, and optimization
for Sparkless, separated from session management for better
modularity and testability.

Components:
    - SQL Parser: Parse SQL queries into AST
    - SQL Executor: Execute parsed SQL queries
    - Query Optimizer: Optimize query execution plans
    - SQL Validator: Validate SQL syntax and semantics
"""

from .parser import SQLParser, SQLAST
from .executor import SQLExecutor
from .optimizer import SQLQueryOptimizer, QueryPlan
from .validation import SQLValidator

__all__ = [
    "SQLParser",
    "SQLAST",
    "SQLExecutor",
    "SQLQueryOptimizer",
    "QueryPlan",
    "SQLValidator",
]
