"""
Query optimization module for Mock-Spark.

Provides query optimization capabilities including filter pushdown,
column pruning, join optimization, and memory management.
"""

from .query_optimizer import QueryOptimizer
from .optimization_rules import (
    FilterPushdownRule,
    ColumnPruningRule,
    JoinOptimizationRule,
    PredicatePushdownRule,
    ProjectionPushdownRule,
)

__all__ = [
    "QueryOptimizer",
    "FilterPushdownRule",
    "ColumnPruningRule", 
    "JoinOptimizationRule",
    "PredicatePushdownRule",
    "ProjectionPushdownRule",
]
