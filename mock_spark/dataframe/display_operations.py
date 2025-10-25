"""
DataFrame display operations for Mock Spark.

This module provides display operations like show, printSchema, to_markdown, etc.
"""

from typing import Any, List, Optional, Iterator, TYPE_CHECKING

if TYPE_CHECKING:
    from .dataframe_refactored import MockDataFrame
from ..spark_types import MockRow


class DataFrameDisplay:
    """Handles DataFrame display operations (show, printSchema, to_markdown, etc.)."""

    def __init__(self, dataframe: "MockDataFrame"):
        """Initialize display handler.

        Args:
            dataframe: The DataFrame instance to display
        """
        self.dataframe = dataframe

    def show(self, n: int = 20, truncate: bool = True) -> None:
        """Display DataFrame content in a clean table format.

        Args:
            n: Number of rows to display (default: 20).
            truncate: Whether to truncate long values (default: True).

        Example:
            >>> df.show(5)
            MockDataFrame[3 rows, 3 columns]
            name    age  salary
            Alice   25   50000
            Bob     30   60000
            Charlie 35   70000
        """
        print(
            f"MockDataFrame[{len(self.dataframe.data)} rows, {len(self.dataframe.schema.fields)} columns]"
        )
        if not self.dataframe.data:
            print("(empty)")
            return

        # Show first n rows
        display_data = self.dataframe.data[:n]

        # Get column names
        columns = (
            list(display_data[0].keys())
            if display_data
            else self.dataframe.schema.fieldNames()
        )

        # Calculate column widths
        col_widths = {}
        for col in columns:
            # Start with column name width
            col_widths[col] = len(col)
            # Check data widths
            for row in display_data:
                value = str(row.get(col, "null"))
                if truncate and len(value) > 20:
                    value = value[:17] + "..."
                col_widths[col] = max(col_widths[col], len(value))

        # Print header (no extra padding) - add blank line for separation
        print()  # Add blank line between metadata and headers
        header_parts = []
        for col in columns:
            header_parts.append(col.ljust(col_widths[col]))
        print(" ".join(header_parts))

        # Print data rows (with padding for alignment)
        for row in display_data:
            row_parts = []
            for col in columns:
                value = str(row.get(col, "null"))
                if truncate and len(value) > 20:
                    value = value[:17] + "..."
                # Add padding to data but not headers
                padded_width = col_widths[col] + 2
                row_parts.append(value.ljust(padded_width))
            print(" ".join(row_parts))

        if len(self.dataframe.data) > n:
            print(f"\n... ({len(self.dataframe.data) - n} more rows)")

    def to_markdown(
        self, n: int = 20, truncate: bool = True, underline_headers: bool = True
    ) -> str:
        """
        Return DataFrame as a markdown table string.

        Args:
            n: Number of rows to show
            truncate: Whether to truncate long strings
            underline_headers: Whether to underline headers with = symbols

        Returns:
            String representation of DataFrame as markdown table
        """
        if not self.dataframe.data:
            return f"MockDataFrame[{len(self.dataframe.data)} rows, {len(self.dataframe.schema.fields)} columns]\n\n(empty)"

        # Show first n rows
        display_data = self.dataframe.data[:n]

        # Get column names
        columns = (
            list(display_data[0].keys())
            if display_data
            else self.dataframe.schema.fieldNames()
        )

        # Build markdown table
        lines = []
        lines.append(
            f"MockDataFrame[{len(self.dataframe.data)} rows, {len(self.dataframe.schema.fields)} columns]"
        )
        lines.append("")  # Blank line

        # Header row
        header_row = "| " + " | ".join(columns) + " |"
        lines.append(header_row)

        # Separator row - use underlines for better visual distinction
        if underline_headers:
            separator_row = (
                "| " + " | ".join(["=" * len(col) for col in columns]) + " |"
            )
        else:
            separator_row = "| " + " | ".join(["---" for _ in columns]) + " |"
        lines.append(separator_row)

        # Data rows
        for row in display_data:
            row_values = []
            for col in columns:
                value = str(row.get(col, "null"))
                if truncate and len(value) > 20:
                    value = value[:17] + "..."
                row_values.append(value)
            data_row = "| " + " | ".join(row_values) + " |"
            lines.append(data_row)

        if len(self.dataframe.data) > n:
            lines.append(f"\n... ({len(self.dataframe.data) - n} more rows)")

        return "\n".join(lines)

    def printSchema(self) -> None:
        """Print DataFrame schema."""
        print("MockDataFrame Schema:")
        for field in self.dataframe.schema.fields:
            nullable = "nullable" if field.nullable else "not nullable"
            print(
                f" |-- {field.name}: {field.dataType.__class__.__name__} ({nullable})"
            )

    def toPandas(self) -> Any:
        """Convert to pandas DataFrame (requires pandas as optional dependency)."""
        from .export import DataFrameExporter

        return DataFrameExporter.to_pandas(self.dataframe)

    def toDuckDB(self, connection: Any = None, table_name: Optional[str] = None) -> str:
        """Convert to DuckDB table for analytical operations.

        Args:
            connection: DuckDB connection or SQLAlchemy Engine (creates temporary if None)
            table_name: Name for the table (auto-generated if None)

        Returns:
            Table name in DuckDB
        """
        from .export import DataFrameExporter

        return DataFrameExporter.to_duckdb(self.dataframe, connection, table_name)

    def _get_duckdb_type(self, data_type: Any) -> str:
        """Map MockSpark data type to DuckDB type (backwards compatibility).

        This method is kept for backwards compatibility with existing tests.
        Implementation delegated to DataFrameExporter.
        """
        from .export import DataFrameExporter

        return DataFrameExporter._get_duckdb_type(data_type)

    def collect(self) -> List[MockRow]:
        """Collect all data as list of Row objects."""
        if self.dataframe._operations_queue:
            materialized = self.dataframe._materialize_if_lazy()
            # Don't call collect() recursively - just return the materialized data
            return [MockRow(row, materialized.schema) for row in materialized.data]
        return [MockRow(row, self.dataframe.schema) for row in self.dataframe.data]

    def toLocalIterator(self) -> "Iterator[MockRow]":
        """Convert to local iterator.

        Returns:
            Iterator of MockRow objects
        """
        # Materialize if lazy
        materialized = self.dataframe._materialize_if_lazy()
        return iter([MockRow(row) for row in materialized.data])

    def take(self, n: int) -> List[MockRow]:
        """Take the first n rows.

        Args:
            n: Number of rows to take

        Returns:
            List of MockRow objects
        """
        # Materialize if lazy
        materialized = self.dataframe._materialize_if_lazy()
        return [MockRow(row) for row in materialized.data[:n]]

    def first(self) -> Optional[MockRow]:
        """Get the first row.

        Returns:
            First MockRow or None if empty
        """
        rows = self.take(1)
        return rows[0] if rows else None

    def head(self, n: int = 5) -> List[MockRow]:
        """Get the first n rows.

        Args:
            n: Number of rows to get (default: 5)

        Returns:
            List of MockRow objects
        """
        return self.take(n)
