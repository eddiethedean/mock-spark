"""
Column ordering functions for Sparkless.

This module provides functions for specifying sort order with null handling.
"""

from typing import Union
from sparkless.functions.base import Column, ColumnOperation


class OrderingFunctions:
    """Collection of column ordering functions."""

    @staticmethod
    def asc(column: Union[Column, str]) -> ColumnOperation:
        """Sort in ascending order.

        Args:
            column: Column to sort

        Returns:
            ColumnOperation representing ascending order
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(column, "asc", name=f"{column.name} ASC")

    @staticmethod
    def asc_nulls_first(column: Union[Column, str]) -> ColumnOperation:
        """Sort ascending with nulls first.

        Args:
            column: Column to sort

        Returns:
            ColumnOperation representing ascending nulls first
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(
            column, "asc_nulls_first", name=f"{column.name} ASC NULLS FIRST"
        )

    @staticmethod
    def asc_nulls_last(column: Union[Column, str]) -> ColumnOperation:
        """Sort ascending with nulls last.

        Args:
            column: Column to sort

        Returns:
            ColumnOperation representing ascending nulls last
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(
            column, "asc_nulls_last", name=f"{column.name} ASC NULLS LAST"
        )

    @staticmethod
    def desc_nulls_first(column: Union[Column, str]) -> ColumnOperation:
        """Sort descending with nulls first.

        Args:
            column: Column to sort

        Returns:
            ColumnOperation representing descending nulls first
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(
            column, "desc_nulls_first", name=f"{column.name} DESC NULLS FIRST"
        )

    @staticmethod
    def desc_nulls_last(column: Union[Column, str]) -> ColumnOperation:
        """Sort descending with nulls last.

        Args:
            column: Column to sort

        Returns:
            ColumnOperation representing descending nulls last
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(
            column, "desc_nulls_last", name=f"{column.name} DESC NULLS LAST"
        )
