"""
Query Execution Engines

This module contains execution engines for materializing lazy DataFrame operations
and executing SQL queries. Separated from storage backends to follow the Single
Responsibility Principle.
"""

from .materializer import DuckDBMaterializer
from .query_executor import SQLAlchemyMaterializer

__all__ = ["DuckDBMaterializer", "SQLAlchemyMaterializer"]
