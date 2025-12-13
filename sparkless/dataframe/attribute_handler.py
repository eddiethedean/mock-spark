"""
Attribute handler for DataFrame.

This module provides attribute access handling for DataFrame, including
column access via dot notation.
"""

from typing import Any
from ..functions import Column


class DataFrameAttributeHandler:
    """Handles attribute access for DataFrame."""

    @staticmethod
    def handle_getattribute(obj: Any, name: str, super_getattribute: Any) -> Any:
        """
        Handle __getattribute__ for DataFrame.

        This intercepts all attribute access for DataFrame objects.

        Args:
            obj: The DataFrame instance
            name: Name of the attribute/method being accessed
            super_getattribute: The super().__getattribute__ method

        Returns:
            The requested attribute/method

        Raises:
            AttributeError: If attribute doesn't exist
        """
        # Always allow access to private/protected attributes and core attributes
        if name.startswith("_") or name in ["data", "schema", "storage"]:
            return super_getattribute(name)

        # For public methods, just return the attribute
        return super_getattribute(name)

    @staticmethod
    def handle_getattr(obj: Any, name: str) -> Column:
        """
        Handle __getattr__ for column access via dot notation.

        Enables df.column_name syntax for column access (PySpark compatibility).

        Args:
            obj: The DataFrame instance
            name: Name of the attribute being accessed

        Returns:
            Column instance for the column

        Raises:
            SparkColumnNotFoundError: If column doesn't exist
        """
        # Avoid infinite recursion - access object.__getattribute__ directly
        try:
            columns = object.__getattribute__(obj, "columns")
            if name in columns:
                # Use F.col to create Column
                from ..functions import F

                return F.col(name)
        except AttributeError:
            pass

        # If not a column, raise SparkColumnNotFoundError for better error messages
        # Use object.__getattribute__ to avoid recursion when accessing columns property
        try:
            available_cols = object.__getattribute__(obj, "columns")
        except AttributeError:
            available_cols = []
        from ..core.exceptions.operation import SparkColumnNotFoundError

        raise SparkColumnNotFoundError(name, available_cols)
