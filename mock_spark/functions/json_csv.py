"""
JSON and CSV functions for Mock Spark.

This module provides JSON and CSV processing functions that match PySpark's API.
Includes parsing, generation, and schema inference for JSON and CSV data.
"""

from typing import Union, Optional, Any
from mock_spark.functions.base import MockColumn, MockColumnOperation


class JSONCSVFunctions:
    """Collection of JSON and CSV manipulation functions."""

    @staticmethod
    def from_json(
        column: Union[MockColumn, str], schema: Any, options: Optional[dict] = None
    ) -> MockColumnOperation:
        """Parse JSON string column into struct/array column.

        Args:
            column: JSON string column
            schema: Target schema
            options: Optional parsing options

        Returns:
            MockColumnOperation representing from_json
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(
            column,
            "from_json",
            value=(schema, options),
            name=f"from_json({column.name})",
        )

    @staticmethod
    def to_json(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Convert struct/array column to JSON string.

        Args:
            column: Struct or array column

        Returns:
            MockColumnOperation representing to_json
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(column, "to_json", name=f"to_json({column.name})")

    @staticmethod
    def get_json_object(
        column: Union[MockColumn, str], path: str
    ) -> MockColumnOperation:
        """Extract JSON object at specified path.

        Args:
            column: JSON string column
            path: JSON path (e.g., '$.field')

        Returns:
            MockColumnOperation representing get_json_object
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(
            column,
            "get_json_object",
            value=path,
            name=f"get_json_object({column.name}, {path})",
        )

    @staticmethod
    def json_tuple(column: Union[MockColumn, str], *fields: str) -> MockColumnOperation:
        """Extract multiple fields from JSON string.

        Args:
            column: JSON string column
            *fields: Field names to extract

        Returns:
            MockColumnOperation representing json_tuple
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(
            column, "json_tuple", value=fields, name=f"json_tuple({column.name}, ...)"
        )

    @staticmethod
    def schema_of_json(json_string: str) -> MockColumnOperation:
        """Infer schema from JSON string.

        Args:
            json_string: Sample JSON string

        Returns:
            MockColumnOperation representing schema_of_json
        """
        from mock_spark.functions.core.literals import MockLiteral

        return MockColumnOperation(
            MockLiteral(json_string), "schema_of_json", name="schema_of_json(...)"
        )

    @staticmethod
    def from_csv(
        column: Union[MockColumn, str], schema: Any, options: Optional[dict] = None
    ) -> MockColumnOperation:
        """Parse CSV string column into struct column.

        Args:
            column: CSV string column
            schema: Target schema
            options: Optional parsing options

        Returns:
            MockColumnOperation representing from_csv
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(
            column, "from_csv", value=(schema, options), name=f"from_csv({column.name})"
        )

    @staticmethod
    def to_csv(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Convert struct column to CSV string.

        Args:
            column: Struct column

        Returns:
            MockColumnOperation representing to_csv
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(column, "to_csv", name=f"to_csv({column.name})")

    @staticmethod
    def schema_of_csv(csv_string: str) -> MockColumnOperation:
        """Infer schema from CSV string.

        Args:
            csv_string: Sample CSV string

        Returns:
            MockColumnOperation representing schema_of_csv
        """
        from mock_spark.functions.core.literals import MockLiteral

        return MockColumnOperation(
            MockLiteral(csv_string), "schema_of_csv", name="schema_of_csv(...)"
        )
