"""
Core MockDataFrame implementation for Mock Spark.

This module contains the core DataFrame class with basic properties,
initialization, and fundamental methods that are essential to the
DataFrame functionality.
"""

from typing import Any, Dict, List, Optional, Union, Tuple
import pandas as pd

from ...spark_types import (
    MockStructType,
    MockStructField,
    MockRow,
)
from ...functions import MockColumn, MockColumnOperation, F, MockLiteral
from ...storage import MemoryStorageManager
from ...core.exceptions import (
    AnalysisException,
    IllegalArgumentException,
    PySparkValueError,
)
from ...core.exceptions.analysis import ColumnNotFoundException
from ..writer import MockDataFrameWriter


class MockDataFrame:
    """Core Mock DataFrame implementation with essential PySpark API compatibility.

    Provides the core DataFrame functionality including initialization,
    basic properties, and fundamental operations. This is the base class
    that other DataFrame modules will extend.

    Attributes:
        data: List of dictionaries representing DataFrame rows.
        schema: MockStructType defining the DataFrame schema.
        storage: Optional storage manager for persistence operations.

    Example:
        >>> from mock_spark import MockSparkSession, F
        >>> spark = MockSparkSession("test")
        >>> data = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
        >>> df = spark.createDataFrame(data)
        >>> df.show()
        MockDataFrame[2 rows, 2 columns]
        name  age
        Alice  25
        Bob    30
    """

    def __init__(
        self,
        data: List[Dict[str, Any]],
        schema: MockStructType,
        storage: Optional[MemoryStorageManager] = None,
    ):
        """Initialize MockDataFrame.

        Args:
            data: List of dictionaries representing DataFrame rows.
            schema: MockStructType defining the DataFrame schema.
            storage: Optional storage manager for persistence operations.
                    Defaults to a new MemoryStorageManager instance.
        """
        self.data = data
        self.schema = schema
        self.storage = storage or MemoryStorageManager()
        self._cached_count: Optional[int] = None

    def __repr__(self) -> str:
        """Return string representation of the DataFrame."""
        return f"MockDataFrame[{len(self.data)} rows, {len(self.schema.fields)} columns]"

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
        print(f"MockDataFrame[{len(self.data)} rows, {len(self.schema.fields)} columns]")
        if not self.data:
            print("(empty)")
            return

        # Show first n rows
        display_data = self.data[:n]

        # Get column names
        columns = list(display_data[0].keys()) if display_data else self.schema.fieldNames()

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

        if len(self.data) > n:
            print(f"\n... ({len(self.data) - n} more rows)")

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
        if not self.data:
            return f"MockDataFrame[{len(self.data)} rows, {len(self.schema.fields)} columns]\n\n(empty)"

        # Show first n rows
        display_data = self.data[:n]

        # Get column names
        columns = list(display_data[0].keys()) if display_data else self.schema.fieldNames()

        # Build markdown table
        lines = []
        lines.append(f"MockDataFrame[{len(self.data)} rows, {len(self.schema.fields)} columns]")
        lines.append("")  # Blank line

        # Header row
        header_row = "| " + " | ".join(columns) + " |"
        lines.append(header_row)

        # Separator row - use underlines for better visual distinction
        if underline_headers:
            separator_row = "| " + " | ".join(["=" * len(col) for col in columns]) + " |"
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

        if len(self.data) > n:
            lines.append(f"\n... ({len(self.data) - n} more rows)")

        return "\n".join(lines)

    def collect(self) -> List[MockRow]:
        """Collect all data as list of Row objects."""
        return [MockRow(row) for row in self.data]

    def toPandas(self) -> Any:
        """Convert to pandas DataFrame (requires pandas as optional dependency)."""
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "pandas is required for toPandas() method. "
                "Install with: pip install mock-spark[pandas] or pip install pandas"
            )
        
        if not self.data:
            # Create empty DataFrame with correct column structure
            return pd.DataFrame(columns=[field.name for field in self.schema.fields])
        return pd.DataFrame(self.data)

    def toDuckDB(self, connection=None, table_name: str = None) -> str:
        """Convert to DuckDB table for analytical operations.
        
        Args:
            connection: DuckDB connection (creates temporary if None)
            table_name: Name for the table (auto-generated if None)
            
        Returns:
            Table name in DuckDB
        """
        try:
            import duckdb
        except ImportError:
            raise ImportError(
                "duckdb is required for toDuckDB() method. "
                "Install with: pip install duckdb"
            )
        
        if connection is None:
            connection = duckdb.connect(":memory:")
        
        if table_name is None:
            table_name = f"temp_df_{id(self)}"
        
        # Create table from schema
        self._create_duckdb_table(connection, table_name)
        
        # Insert data
        if self.data:
            for row in self.data:
                values = [row.get(field.name) for field in self.schema.fields]
                placeholders = ", ".join(["?" for _ in values])
                connection.execute(
                    f"INSERT INTO {table_name} VALUES ({placeholders})", values
                )
        
        return table_name

    def _create_duckdb_table(self, connection, table_name: str) -> None:
        """Create DuckDB table from MockSpark schema."""
        try:
            import duckdb
        except ImportError:
            raise ImportError("duckdb is required")
        
        columns = []
        for field in self.schema.fields:
            duckdb_type = self._get_duckdb_type(field.dataType)
            columns.append(f"{field.name} {duckdb_type}")
        
        create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)})"
        connection.execute(create_sql)

    def _get_duckdb_type(self, data_type) -> str:
        """Convert MockSpark data type to DuckDB type."""
        type_name = type(data_type).__name__
        if "String" in type_name:
            return "VARCHAR"
        elif "Integer" in type_name or "Long" in type_name:
            return "INTEGER"
        elif "Double" in type_name or "Float" in type_name:
            return "DOUBLE"
        elif "Boolean" in type_name:
            return "BOOLEAN"
        else:
            return "VARCHAR"

    def count(self) -> int:
        """Count number of rows."""
        if self._cached_count is None:
            self._cached_count = len(self.data)
        return self._cached_count

    @property
    def columns(self) -> List[str]:
        """Get column names."""
        return [field.name for field in self.schema.fields]

    def printSchema(self) -> None:
        """Print DataFrame schema."""
        print("MockDataFrame Schema:")
        for field in self.schema.fields:
            nullable = "nullable" if field.nullable else "not nullable"
            print(f" |-- {field.name}: {field.dataType.__class__.__name__} ({nullable})")

    def dtypes(self) -> List[Tuple[str, str]]:
        """Get column names and their data types as tuples."""
        return [(field.name, field.dataType.__class__.__name__) for field in self.schema.fields]

    def rdd(self) -> "MockRDD":
        """Convert to MockRDD."""
        from ..rdd import MockRDD

        return MockRDD(self.data)

    def explain(self) -> None:
        """Print the logical plan of the DataFrame."""
        print("MockDataFrame Logical Plan:")
        print(
            "  MockDataFrame[{} rows, {} columns]".format(len(self.data), len(self.schema.fields))
        )

    def isStreaming(self) -> bool:
        """Check if DataFrame is streaming (always False for mock)."""
        return False

    @property
    def write(self) -> "MockDataFrameWriter":
        """Get DataFrameWriter for saving the DataFrame."""
        return MockDataFrameWriter(self)

    @property
    def schema(self) -> MockStructType:
        """Get the schema of the DataFrame."""
        return self._schema

    @schema.setter
    def schema(self, value: MockStructType) -> None:
        """Set the schema of the DataFrame."""
        self._schema = value

    def _get_column_value(self, row: Dict[str, Any], column: Any) -> Any:
        """Get value from a row for a given column."""
        if isinstance(column, str):
            return row.get(column)
        elif isinstance(column, MockColumn):
            return self._evaluate_mock_column(row, column)
        elif isinstance(column, MockLiteral):
            return column.value
        else:
            return column

    def _evaluate_mock_column(self, row: Dict[str, Any], column: MockColumn) -> Any:
        """Evaluate a MockColumn expression against a row."""
        if column.name in row:
            return row[column.name]
        else:
            # Handle function calls and expressions
            return self._evaluate_column_expression(row, column)

    def _evaluate_column_expression(self, row: Dict[str, Any], column_expression: Any) -> Any:
        """Evaluate a column expression against a row."""
        # This is a simplified version - the full implementation will be in operations.py
        if isinstance(column_expression, str):
            return row.get(column_expression)
        elif isinstance(column_expression, MockColumn):
            return self._evaluate_mock_column(row, column_expression)
        elif isinstance(column_expression, MockLiteral):
            return column_expression.value
        else:
            return column_expression
