"""
Schema service for DataFrame operations.

This service provides schema-related operations using composition instead of mixin inheritance.
Note: SchemaOperations only contains properties which remain on DataFrame directly.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..dataframe import DataFrame


class SchemaService:
    """Service providing schema-related operations for DataFrame.

    Note: SchemaOperations mixin only contained properties (columns, schema, dtypes)
    which remain on DataFrame directly. This service is a placeholder for future
    schema-related methods.
    """

    def __init__(self, df: "DataFrame"):
        """Initialize schema service with DataFrame instance."""
        self._df = df
