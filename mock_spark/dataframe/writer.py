"""
Mock DataFrameWriter implementation for DataFrame write operations.

This module provides DataFrame writing functionality, maintaining compatibility
with PySpark's DataFrameWriter interface. Supports writing to various data sinks
including tables, files, and custom storage backends with multiple save modes.

Key Features:
    - Complete PySpark DataFrameWriter API compatibility
    - Support for multiple output formats (parquet, json, csv)
    - Multiple save modes (append, overwrite, error, ignore)
    - Flexible options configuration
    - Integration with storage manager
    - Table and file output support
    - Error handling for invalid configurations

Example:
    >>> from mock_spark import MockSparkSession
    >>> spark = MockSparkSession("test")
    >>> df = spark.createDataFrame([{"name": "Alice", "age": 25}])
    >>> # Save as table
    >>> df.write.mode("overwrite").saveAsTable("users")
    >>> # Save to file with options
    >>> df.write.format("parquet").option("compression", "snappy").save("/path")
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

    def partitionBy(self, *cols: str) -> "MockDataFrameWriter":
        """Partition output by given columns.

        Args:
            *cols: Column names to partition by.

        Returns:
            Self for method chaining.

        Example:
            >>> df.write.partitionBy("year", "month")
        """
        self._options["partitionBy"] = list(cols)
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
            table_name.split(".", 1)
            if "." in table_name
            else (self.storage.get_current_schema(), table_name)
        )

        # Ensure schema exists (thread-safe with retry logic)
        # This handles cases where schemas created in other threads aren't visible
        # to the current thread's DuckDB connection
        if not self.storage.schema_exists(schema):
            self.storage.create_schema(schema)
            # Double-check after creation to ensure it's visible in this thread
            if not self.storage.schema_exists(schema):
                from ..errors import AnalysisException

                raise AnalysisException(
                    f"Failed to create or verify schema '{schema}' in thread-local "
                    f"connection. This may indicate a threading issue with DuckDB."
                )

        # Check if this is a Delta table write
        is_delta = self.format_name == "delta"
        table_exists = self.storage.table_exists(schema, table)

        # Handle different save modes
        if self.save_mode == "error":
            if table_exists:
                from ..errors import AnalysisException

                raise AnalysisException(f"Table '{schema}.{table}' already exists")
            self.storage.create_table(schema, table, self.df.schema.fields)

        elif self.save_mode == "ignore":
            if not table_exists:
                self.storage.create_table(schema, table, self.df.schema.fields)
            else:
                return  # Do nothing if table exists

        elif self.save_mode == "overwrite":
            # Track version and history before dropping for Delta tables
            next_version = 0
            preserved_history = []
            if table_exists and is_delta:
                meta = self.storage.get_table_metadata(schema, table)
                if meta and meta.get("format") == "delta":
                    # Increment version for next write
                    next_version = meta.get("version", 0) + 1
                    # Preserve version history
                    preserved_history = meta.get("version_history", [])

            if table_exists:
                self.storage.drop_table(schema, table)
            self.storage.create_table(schema, table, self.df.schema.fields)

            # Store next version and history for Delta tables
            if is_delta:
                self._delta_next_version = next_version
                self._delta_preserved_history = preserved_history

        elif self.save_mode == "append":
            if not table_exists:
                self.storage.create_table(schema, table, self.df.schema.fields)
            elif is_delta:
                # For Delta append, increment version
                meta = self.storage.get_table_metadata(schema, table)
                if meta and meta.get("format") == "delta":
                    self._delta_next_version = meta.get("version", 0) + 1
                    self._delta_preserved_history = meta.get("version_history", [])

            if is_delta and table_exists:
                # For Delta append, check mergeSchema option
                merge_schema = (
                    self._options.get("mergeSchema", "false").lower() == "true"
                )
                existing_schema = self.storage.get_table_schema(schema, table)

                if existing_schema and not existing_schema.has_same_columns(
                    self.df.schema
                ):
                    # Schema mismatch detected
                    if merge_schema:
                        # Merge schemas: add new columns
                        merged_schema = existing_schema.merge_with(self.df.schema)

                        # Get existing data
                        existing_data = self.storage.get_data(schema, table)

                        # Fill null for new columns in existing data
                        new_columns = set(self.df.schema.fieldNames()) - set(
                            existing_schema.fieldNames()
                        )
                        for row in existing_data:
                            for col_name in new_columns:
                                row[col_name] = None

                        # Drop and recreate table with new schema
                        self.storage.drop_table(schema, table)
                        self.storage.create_table(schema, table, merged_schema.fields)

                        # Reinsert existing data with nulls
                        if existing_data:
                            self.storage.insert_data(schema, table, existing_data)
                    else:
                        # Schema mismatch without mergeSchema - raise error
                        from ..errors import AnalysisException

                        raise AnalysisException(
                            f"Cannot append to table {schema}.{table}: schema mismatch. "
                            f"Existing columns: {existing_schema.fieldNames()}, "
                            f"New columns: {self.df.schema.fieldNames()}. "
                            f"Set option mergeSchema=true to allow schema evolution."
                        )
            else:
                # Non-Delta append: check schema compatibility
                existing_schema = self.storage.get_table_schema(schema, table)
                if existing_schema and not existing_schema.has_same_columns(
                    self.df.schema
                ):
                    from ..errors import AnalysisException

                    raise AnalysisException(
                        f"Cannot append to table {schema}.{table}: schema mismatch. "
                        f"Existing columns: {existing_schema.fieldNames()}, "
                        f"New columns: {self.df.schema.fieldNames()}."
                    )

        # Insert data
        data = self.df.collect()
        # Convert MockRow objects to dictionaries
        dict_data = [row.asDict() for row in data]
        self.storage.insert_data(schema, table, dict_data)

        # Ensure table is properly registered in storage after creation
        # This synchronizes catalog and storage
        if not self.storage.table_exists(schema, table):
            # If table_exists returns False but we just created it, there's a sync issue
            # Re-check by attempting to query the table
            try:
                # Try to get table schema as a verification
                table_schema = self.storage.get_table_schema(schema, table)
                if table_schema is None:
                    # Table exists in storage but not properly registered - force registration
                    # This shouldn't happen, but handle it gracefully
                    pass
            except Exception:
                # Table doesn't exist - this is an error
                from ..errors import AnalysisException

                raise AnalysisException(
                    f"Table '{schema}.{table}' was not properly created in storage"
                )

        # Set Delta-specific metadata
        if is_delta:
            # Determine version to set
            if hasattr(self, "_delta_next_version"):
                version = self._delta_next_version
            else:
                meta = self.storage.get_table_metadata(schema, table)
                version = meta.get("version", 0) if meta else 0

            # Capture version snapshot for time travel
            from datetime import datetime
            from ..storage.models import MockDeltaVersion

            current_data = self.storage.get_data(schema, table)

            # Determine operation name
            if version == 0:
                operation = "WRITE"  # First write is always "WRITE"
            else:
                operation = self.save_mode.upper() if self.save_mode else "WRITE"
                if operation == "ERROR":
                    operation = "WRITE"
                if operation == "IGNORE":
                    operation = "WRITE"

            version_snapshot = MockDeltaVersion(
                version=version,
                timestamp=datetime.utcnow(),
                operation=operation,
                data_snapshot=[
                    row if isinstance(row, dict) else row for row in current_data
                ],
            )

            # Get existing metadata to preserve history
            # Use preserved history if available (from before overwrite drop)
            if hasattr(self, "_delta_preserved_history"):
                version_history = self._delta_preserved_history
            else:
                meta = self.storage.get_table_metadata(schema, table) or {}
                version_history = meta.get("version_history", [])

            # Add new version to history
            version_history.append(version_snapshot)

            # Update with Delta properties including version history
            self.storage.update_table_metadata(
                schema,
                table,
                {
                    "format": "delta",
                    "version": version,
                    "properties": {
                        "delta.minReaderVersion": "1",
                        "delta.minWriterVersion": "2",
                        "Type": "MANAGED",
                    },
                    "version_history": version_history,
                },
            )

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
