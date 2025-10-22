"""
Column ordering functions for Mock Spark.

This module provides functions for specifying sort order with null handling.
"""

from typing import Union
from mock_spark.functions.base import MockColumn, MockColumnOperation


class OrderingFunctions:
    """Collection of column ordering functions."""

    @staticmethod
    def asc(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Sort in ascending order.

        Args:
            column: Column to sort

        Returns:
            MockColumnOperation representing ascending order
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(column, "asc", name=f"{column.name} ASC")

    @staticmethod
    def asc_nulls_first(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Sort ascending with nulls first.

        Args:
            column: Column to sort

        Returns:
            MockColumnOperation representing ascending nulls first
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(
            column, "asc_nulls_first", name=f"{column.name} ASC NULLS FIRST"
        )

    @staticmethod
    def asc_nulls_last(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Sort ascending with nulls last.

        Args:
            column: Column to sort

        Returns:
            MockColumnOperation representing ascending nulls last
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(
            column, "asc_nulls_last", name=f"{column.name} ASC NULLS LAST"
        )

    @staticmethod
    def desc_nulls_first(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Sort descending with nulls first.

        Args:
            column: Column to sort

        Returns:
            MockColumnOperation representing descending nulls first
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(
            column, "desc_nulls_first", name=f"{column.name} DESC NULLS FIRST"
        )

    @staticmethod
    def desc_nulls_last(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Sort descending with nulls last.

        Args:
            column: Column to sort

        Returns:
            MockColumnOperation representing descending nulls last
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(
            column, "desc_nulls_last", name=f"{column.name} DESC NULLS LAST"
        )
