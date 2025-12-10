"""
Type aliases and type utilities for DataFrame operations.

This module provides type aliases to help mypy understand the relationship
between DataFrame and SupportsDataFrameOps protocol.
"""

from typing import TYPE_CHECKING

try:
    from typing import TypeAlias  # type: ignore[attr-defined]
except ImportError:
    from typing_extensions import TypeAlias

from .protocols import SupportsDataFrameOps

if TYPE_CHECKING:
    # TypeAlias to help mypy understand DataFrame satisfies SupportsDataFrameOps
    # This creates an explicit type relationship for static type checking
    DataFrameType: TypeAlias = SupportsDataFrameOps
else:
    # At runtime, this is just an alias
    DataFrameType = SupportsDataFrameOps

__all__ = ["DataFrameType"]
