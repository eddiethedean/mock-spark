"""
Mock DataFrameWriter implementation for DataFrame write operations.

This module provides DataFrame writing functionality, maintaining compatibility
with PySpark's DataFrameWriter interface.
"""

from typing import Any, Dict, Optional, TYPE_CHECKING
from ..storage import MemoryStorageManager

if TYPE_CHECKING:
    from .dataframe import MockDataFrame


class MockDataFrameWriter:
    """Mock DataFrame writer for saveAsTable operations.

    Provides a PySpark-compatible interface for writing DataFrames to storage
    formats. Supports various formats and save modes for testing and development.

    Attributes:
        df: The DataFrame to be written.
        storage: Storage manager for persisting data.
        format_name: Output format (e.g., 'parquet', 'json').
        save_mode: Save mode ('append', 'overwrite', 'error', 'ignore').
        options: Additional options for the writer.

    Example:
        >>> df.write.format("parquet").mode("overwrite").saveAsTable("my_table")
    """

    def __init__(self, df: "MockDataFrame", storage: MemoryStorageManager):
        """Initialize MockDataFrameWriter.
        
        Args:
            df: The DataFrame to be written.
            storage: Storage manager for persisting data.
        """
        self.df = df
        self.storage = storage
        self.format_name = "parquet"
        self.save_mode = "append"
        self._options: Dict[str, Any] = {}

    def format(self, source: str) -> "MockDataFrameWriter":
        """Set the output format for the DataFrame writer.

        Args:
            source: The output format (e.g., 'parquet', 'json', 'csv').

        Returns:
            Self for method chaining.

        Example:
            >>> df.write.format("parquet")
        """
        self.format_name = source
        return self

    def mode(self, mode: str) -> "MockDataFrameWriter":
        """Set the save mode for the DataFrame writer.

        Args:
            mode: Save mode ('append', 'overwrite', 'error', 'ignore').

        Returns:
            Self for method chaining.

        Raises:
            IllegalArgumentException: If mode is not valid.

        Example:
            >>> df.write.mode("overwrite")
        """
        valid_modes = ["append", "overwrite", "error", "ignore"]
        if mode not in valid_modes:
            from ..errors import IllegalArgumentException
            raise IllegalArgumentException(
                f"Unknown save mode: {mode}. Must be one of {valid_modes}"
            )

        self.save_mode = mode
        return self

    @property
    def saveMode(self) -> str:
        """Get the current save mode (PySpark compatibility).

        Returns:
            Current save mode string.
        """
        return self.save_mode

    def option(self, key: str, value: Any) -> "MockDataFrameWriter":
        """Set an option for the DataFrame writer.

        Args:
            key: Option key.
            value: Option value.

        Returns:
            Self for method chaining.

        Example:
            >>> df.write.option("compression", "snappy")
        """
        self._options[key] = value
        return self

    def options(self, **kwargs: Any) -> "MockDataFrameWriter":
        """Set multiple options for the DataFrame writer.

        Args:
            **kwargs: Option key-value pairs.

        Returns:
            Self for method chaining.

        Example:
            >>> df.write.options(compression="snappy", format="parquet")
        """
        self._options.update(kwargs)
        return self

    def saveAsTable(self, table_name: str) -> None:
        """Save DataFrame as a table in storage.

        Args:
            table_name: Name of the table (can include schema, e.g., 'schema.table').

        Raises:
            AnalysisException: If table operations fail.
            IllegalArgumentException: If table name is invalid.

        Example:
            >>> df.write.saveAsTable("my_table")
            >>> df.write.saveAsTable("schema.my_table")
        """
        if not table_name:
            from ..errors import IllegalArgumentException
            raise IllegalArgumentException("Table name cannot be empty")

        schema, table = (
            table_name.split(".", 1) if "." in table_name else ("default", table_name)
        )

        # Ensure schema exists
        if not self.storage.schema_exists(schema):
            self.storage.create_schema(schema)

        # Handle different save modes
        if self.save_mode == "error":
            if self.storage.table_exists(schema, table):
                from ..errors import AnalysisException
                raise AnalysisException(f"Table '{schema}.{table}' already exists")
            self.storage.create_table(schema, table, self.df.schema.fields)

        elif self.save_mode == "ignore":
            if not self.storage.table_exists(schema, table):
                self.storage.create_table(schema, table, self.df.schema.fields)
            else:
                return  # Do nothing if table exists

        elif self.save_mode == "overwrite":
            if self.storage.table_exists(schema, table):
                self.storage.drop_table(schema, table)
            self.storage.create_table(schema, table, self.df.schema.fields)

        elif self.save_mode == "append":
            if not self.storage.table_exists(schema, table):
                self.storage.create_table(schema, table, self.df.schema.fields)

        # Insert data
        data = self.df.collect()
        # Convert MockRow objects to dictionaries
        dict_data = [row.asDict() for row in data]
        self.storage.insert_data(schema, table, dict_data, mode=self.save_mode)

    def save(self, path: Optional[str] = None) -> None:
        """Save DataFrame to a file path.

        Args:
            path: Optional file path to save to. If None, uses a default path.

        Raises:
            IllegalArgumentException: If path is invalid.

        Example:
            >>> df.write.format("parquet").mode("overwrite").save("/path/to/file")
        """
        if path is None:
            from ..errors import IllegalArgumentException
            raise IllegalArgumentException("Path cannot be None")

        if not path:
            from ..errors import IllegalArgumentException
            raise IllegalArgumentException("Path cannot be empty")

        # For mock implementation, we'll just log the operation
        # In a real implementation, this would save to the specified path
        print(
            f"Mock save: DataFrame saved to {path} in {self.format_name} format with mode {self.save_mode}"
        )

        # Store options for reference
        if self._options:
            print(f"Options: {self._options}")

    def parquet(self, path: str, **options: Any) -> None:
        """Save DataFrame in Parquet format.

        Args:
            path: Path to save the Parquet file.
            **options: Additional options for Parquet format.

        Example:
            >>> df.write.parquet("/path/to/file.parquet")
        """
        self.format("parquet").options(**options).save(path)

    def json(self, path: str, **options: Any) -> None:
        """Save DataFrame in JSON format.

        Args:
            path: Path to save the JSON file.
            **options: Additional options for JSON format.

        Example:
            >>> df.write.json("/path/to/file.json")
        """
        self.format("json").options(**options).save(path)

    def csv(self, path: str, **options: Any) -> None:
        """Save DataFrame in CSV format.

        Args:
            path: Path to save the CSV file.
            **options: Additional options for CSV format.

        Example:
            >>> df.write.csv("/path/to/file.csv")
        """
        self.format("csv").options(**options).save(path)

    def orc(self, path: str, **options: Any) -> None:
        """Save DataFrame in ORC format.

        Args:
            path: Path to save the ORC file.
            **options: Additional options for ORC format.

        Example:
            >>> df.write.orc("/path/to/file.orc")
        """
        self.format("orc").options(**options).save(path)

    def text(self, path: str, **options: Any) -> None:
        """Save DataFrame in text format.

        Args:
            path: Path to save the text file.
            **options: Additional options for text format.

        Example:
            >>> df.write.text("/path/to/file.txt")
        """
        self.format("text").options(**options).save(path)
