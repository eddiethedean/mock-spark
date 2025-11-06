"""
Assertion operations mixin for DataFrame.

This mixin provides assertion methods that can be mixed into
the DataFrame class for testing purposes.
"""

from typing import Any, Dict, List, TYPE_CHECKING

if TYPE_CHECKING:
    from ..dataframe import DataFrame
    from ...spark_types import StructType


class AssertionOperations:
    """Mixin providing assertion operations for DataFrame testing."""

    def assert_has_columns(self: "DataFrame", expected_columns: List[str]) -> None:
        """Assert that DataFrame has the expected columns."""
        from .assertions import DataFrameAssertions

        return DataFrameAssertions.assert_has_columns(self, expected_columns)

    def assert_row_count(self: "DataFrame", expected_count: int) -> None:
        """Assert that DataFrame has the expected row count."""
        from .assertions import DataFrameAssertions

        return DataFrameAssertions.assert_row_count(self, expected_count)

    def assert_schema_matches(self: "DataFrame", expected_schema: "StructType") -> None:
        """Assert that DataFrame schema matches the expected schema."""
        from .assertions import DataFrameAssertions

        return DataFrameAssertions.assert_schema_matches(self, expected_schema)

    def assert_data_equals(
        self: "DataFrame", expected_data: List[Dict[str, Any]]
    ) -> None:
        """Assert that DataFrame data equals the expected data."""
        from .assertions import DataFrameAssertions

        return DataFrameAssertions.assert_data_equals(self, expected_data)
