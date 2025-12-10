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
    >>> from mock_spark.sql import SparkSession
    >>> spark = SparkSession("test")
    >>> df = spark.createDataFrame([{"name": "Alice", "age": 25}])
    >>> # Save as table
    >>> df.write.mode("overwrite").saveAsTable("users")
    >>> # Save to file with options
    >>> df.write.format("parquet").option("compression", "snappy").save("/path")
"""

from __future__ import annotations

import os
import shutil
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import polars as pl

from mock_spark.backend.polars.schema_utils import align_frame_to_schema
from mock_spark.errors import AnalysisException, IllegalArgumentException

if TYPE_CHECKING:
    from mock_spark.backend.protocols import StorageBackend
    from .dataframe import DataFrame

from ..spark_types import StructType


class DataFrameWriter:
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

    def __init__(self, df: DataFrame, storage: StorageBackend):
        """Initialize DataFrameWriter.

        Args:
            df: The DataFrame to be written.
            storage: Storage manager for persisting data.
        """
        self.df = df
        self.storage = storage
        self.format_name = "parquet"
        self.save_mode = "append"
        self._options: dict[str, Any] = {}
        self._table_name: str | None = None

    def format(self, source: str) -> DataFrameWriter:
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

    def mode(self, mode: str) -> DataFrameWriter:
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

    def option(self, key: str, value: Any) -> DataFrameWriter:
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

    def options(self, **kwargs: Any) -> DataFrameWriter:
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

    def partitionBy(self, *cols: str) -> DataFrameWriter:
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

        # Ensure schema exists (thread-safe)
        # Polars backend is thread-safe by design, no special handling needed
        if not self.storage.schema_exists(schema):
            self.storage.create_schema(schema)
            # Double-check after creation to ensure it's visible in this thread
            if not self.storage.schema_exists(schema):
                from ..errors import AnalysisException

                raise AnalysisException(
                    f"Failed to create or verify schema '{schema}' in thread-local "
                    f"connection. This may indicate a threading issue."
                )

        # Check if this is a Delta table write
        is_delta = self.format_name == "delta"
        table_exists = self.storage.table_exists(schema, table)

        # Initialize DataFrame to write (may be modified by schema merging)
        df_to_write = self.df

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
                if isinstance(meta, dict) and meta.get("format") == "delta":
                    # Increment version for next write
                    next_version = meta.get("version", 0) + 1
                    # Preserve version history
                    preserved_history = meta.get("version_history", [])

            # Handle overwriteSchema option (Issue #2 fix)
            overwrite_schema = self._options.get("overwriteSchema", "false")
            if isinstance(overwrite_schema, bool):
                overwrite_schema_enabled = overwrite_schema
            else:
                overwrite_schema_enabled = str(overwrite_schema).lower() == "true"

            schema_to_use = self.df.schema

            if table_exists and overwrite_schema_enabled:
                # Read existing schema and merge with current schema (Issue #4 fix)
                existing_schema = self.storage.get_table_schema(schema, table)
                if existing_schema and isinstance(existing_schema, StructType):
                    df_to_write, schema_to_use = self._merge_schemas_for_overwrite(
                        existing_schema, self.df
                    )

            if table_exists:
                self.storage.drop_table(schema, table)
            self.storage.create_table(schema, table, schema_to_use.fields)

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
                if isinstance(meta, dict) and meta.get("format") == "delta":
                    self._delta_next_version = meta.get("version", 0) + 1
                    self._delta_preserved_history = meta.get("version_history", [])

            if is_delta and table_exists:
                # For Delta append, check mergeSchema option
                merge_schema = (
                    self._options.get("mergeSchema", "false").lower() == "true"
                )
                existing_schema = self.storage.get_table_schema(schema, table)

                if existing_schema:
                    existing_struct = cast("StructType", existing_schema)

                    if not existing_struct.has_same_columns(self.df.schema):
                        if merge_schema:
                            # Merge schemas: add new columns
                            merged_schema = existing_struct.merge_with(self.df.schema)

                            # Get existing data
                            existing_data = self.storage.get_data(schema, table)

                            # Fill null for new columns in existing data
                            new_columns = set(self.df.schema.fieldNames()) - set(
                                existing_struct.fieldNames()
                            )
                            for row in existing_data:
                                for col_name in new_columns:
                                    row[col_name] = None

                            # Drop and recreate table with new schema
                            self.storage.drop_table(schema, table)
                            self.storage.create_table(
                                schema, table, merged_schema.fields
                            )

                            # Reinsert existing data with nulls
                            if existing_data:
                                self.storage.insert_data(schema, table, existing_data)
                        else:
                            # Schema mismatch without mergeSchema - raise error
                            from ..errors import AnalysisException

                            raise AnalysisException(
                                f"Cannot append to table {schema}.{table}: schema mismatch. "
                                f"Existing columns: {existing_struct.fieldNames()}, "
                                f"New columns: {self.df.schema.fieldNames()}. "
                                f"Set option mergeSchema=true to allow schema evolution."
                            )
            else:
                # Non-Delta append: check schema compatibility
                existing_schema = self.storage.get_table_schema(schema, table)
                if existing_schema:
                    existing_struct = cast("StructType", existing_schema)
                    if not existing_struct.has_same_columns(self.df.schema):
                        from ..errors import AnalysisException

                        raise AnalysisException(
                            f"Cannot append to table {schema}.{table}: schema mismatch. "
                            f"Existing columns: {existing_struct.fieldNames()}, "
                            f"New columns: {self.df.schema.fieldNames()}."
                        )

        # Insert data (use merged DataFrame if schema merging occurred)
        data = df_to_write.collect()
        # Convert Row objects to dictionaries
        dict_data = [row.asDict() for row in data]
        self.storage.insert_data(schema, table, dict_data)

        # Ensure table is immediately accessible in catalog (Issue #3 fix)
        # This synchronizes catalog and storage - table must be available right after saveAsTable
        if not self.storage.table_exists(schema, table):
            # If table_exists returns False but we just created/inserted data, there's a sync issue
            # Try to rehydrate from disk if using persistent storage
            if hasattr(self.storage, "_rehydrate_from_disk"):
                self.storage._rehydrate_from_disk()

            # Verify table is now accessible
            if not self.storage.table_exists(schema, table):
                # Last resort: try to get table schema directly
                try:
                    table_schema = self.storage.get_table_schema(schema, table)
                    if table_schema is None:
                        # Table doesn't exist - this is an error
                        from ..errors import AnalysisException  # type: ignore[unreachable]

                        raise AnalysisException(
                            f"Table '{schema}.{table}' was not properly created in storage. "
                            f"Table registration failed after saveAsTable()."
                        )
                except Exception as e:
                    # Table doesn't exist - this is an error
                    from ..errors import AnalysisException

                    raise AnalysisException(
                        f"Table '{schema}.{table}' was not properly created in storage: {e}"
                    ) from e

        # Set Delta-specific metadata
        if is_delta:
            # Determine version to set
            if hasattr(self, "_delta_next_version"):
                version = self._delta_next_version
            else:
                meta = self.storage.get_table_metadata(schema, table)
                version = meta.get("version", 0) if isinstance(meta, dict) else 0

            # Capture version snapshot for time travel
            from datetime import datetime, timezone
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
                timestamp=datetime.now(timezone.utc),
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
                meta = self.storage.get_table_metadata(schema, table)
                meta_dict = meta if isinstance(meta, dict) else {}
                version_history = meta_dict.get("version_history", [])

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

    def save(self, path: str | None = None) -> None:
        """Save DataFrame to a file path.

        Args:
            path: Optional file path to save to. If None, uses a default path.

        Raises:
            IllegalArgumentException: If path is invalid.

        Example:
            >>> df.write.format("parquet").mode("overwrite").save("/path/to/file")
        """
        if path is None:
            raise IllegalArgumentException("Path cannot be None")

        if not path:
            raise IllegalArgumentException("Path cannot be empty")

        resolved_format = (self.format_name or "parquet").lower()
        target_path = Path(path)

        if self._should_skip_write(target_path):
            return

        data_frame = self._materialize_dataframe()
        polars_frame = self._to_polars_frame(data_frame.data, data_frame.schema)

        if resolved_format == "parquet":
            self._write_parquet(polars_frame, target_path)
        elif resolved_format == "json":
            self._write_json(polars_frame, target_path)
        elif resolved_format == "csv":
            self._write_csv(polars_frame, target_path)
        elif resolved_format == "text":
            self._write_text(data_frame.data, data_frame.schema, target_path)
        else:
            raise AnalysisException(
                f"File format '{self.format_name}' is not supported."
            )

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

    def delta(self, path: str, **options: Any) -> None:
        """Save DataFrame in Delta Lake format.

        This is a convenience method equivalent to:
        df.write.format("delta").save(path)

        Args:
            path: Path to save the Delta Lake table.
            **options: Additional options for Delta Lake format.

        Example:
            >>> df.write.delta("/path/to/delta_table")
            >>> df.write.delta("/path/to/delta_table", mergeSchema=True)
        """
        self.format("delta").options(**options).save(path)

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

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _materialize_dataframe(self) -> DataFrame:
        """Materialize the underlying DataFrame (handling lazy evaluation)."""
        from .dataframe import DataFrame

        materialized = cast("DataFrame", self.df._materialize_if_lazy())
        # _materialize_if_lazy returns SupportsDataFrameOps which is a DataFrame in practice
        if not isinstance(materialized, DataFrame):
            raise TypeError("Expected DataFrame after materialization")
        return materialized

    def _should_skip_write(self, path: Path) -> bool:
        """Handle save modes before writing; return True if no write is needed."""
        if path.exists():
            if self.save_mode == "error":
                raise AnalysisException(f"Path '{path}' already exists")
            if self.save_mode == "ignore":
                return True
            if self.save_mode == "overwrite":
                self._delete_path(path)
            # append mode keeps existing files
        else:
            if self.save_mode == "append":
                # Append to new location behaves like regular write
                pass

        if not path.parent.exists():
            path.parent.mkdir(parents=True, exist_ok=True)

        return False

    def _delete_path(self, path: Path) -> None:
        """Remove existing output path for overwrite mode."""
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()

    def _to_polars_frame(
        self, data: list[dict[str, Any]], schema: StructType
    ) -> pl.DataFrame:
        """Convert row dictionaries and schema into a Polars DataFrame."""
        if not schema.fields:
            return pl.DataFrame(data)

        # Create DataFrame from dictionaries (handles empty data gracefully)
        frame = (
            pl.DataFrame(data)
            if data
            else pl.DataFrame({f.name: [] for f in schema.fields})
        )
        return align_frame_to_schema(frame, schema)

    def _next_part_file(self, path: Path, extension: str) -> Path:
        """Generate a unique part file name within the target directory."""
        path.mkdir(parents=True, exist_ok=True)
        existing = sorted(path.glob(f"part-*{extension}"))
        index = len(existing)
        unique = uuid.uuid4().hex[:8]
        filename = f"part-{index:05d}-{unique}{extension}"
        return path / filename

    def _write_parquet(self, frame: pl.DataFrame, path: Path) -> None:
        target = (
            path
            if path.suffix == ".parquet"
            else self._next_part_file(path, ".parquet")
        )
        compression = self._options.get("compression", "snappy")
        target.parent.mkdir(parents=True, exist_ok=True)
        frame.write_parquet(str(target), compression=compression)

    def _write_json(self, frame: pl.DataFrame, path: Path) -> None:
        target = path if path.suffix == ".json" else self._next_part_file(path, ".json")
        target.parent.mkdir(parents=True, exist_ok=True)
        # Spark writes newline-delimited JSON; Polars handles this via write_ndjson
        frame.write_ndjson(str(target))

    def _write_csv(self, frame: pl.DataFrame, path: Path) -> None:
        target = path if path.suffix == ".csv" else self._next_part_file(path, ".csv")
        target.parent.mkdir(parents=True, exist_ok=True)

        include_header = self._get_bool_option("header", default=True)
        delimiter = self._options.get("sep", self._options.get("delimiter", ","))
        null_value = self._options.get("nullValue")
        compression = self._options.get("compression")

        kwargs: dict[str, Any] = {"include_header": include_header}
        if delimiter:
            kwargs["separator"] = delimiter
        if null_value is not None:
            kwargs["null_value"] = null_value
        if compression is not None:
            kwargs["compression"] = compression

        frame.write_csv(str(target), **kwargs)

    def _write_text(
        self, data: list[dict[str, Any]], schema: StructType, path: Path
    ) -> None:
        column_name = "value"
        if schema.fields:
            column_name = schema.fields[0].name
        target = path if path.suffix == ".txt" else self._next_part_file(path, ".txt")
        target.parent.mkdir(parents=True, exist_ok=True)

        mode = (
            "a"
            if path.exists() and self.save_mode == "append" and target == path
            else "w"
        )
        with open(target, mode, encoding="utf-8") as handle:
            for row in data:
                value = row.get(column_name)
                handle.write("" if value is None else str(value))
                handle.write(os.linesep)

    def _get_bool_option(self, key: str, default: bool = False) -> bool:
        """Resolve boolean option values with Spark-compatible parsing."""
        if key not in self._options:
            return default
        value = self._options[key]
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "yes", "y"}

    def _merge_schemas_for_overwrite(
        self, existing_schema: StructType, current_df: DataFrame
    ) -> tuple[DataFrame, StructType]:
        """
        Merge existing table schema with current DataFrame schema for overwrite mode.

        This implements Delta Lake's schema evolution:
        - Preserves all columns from existing schema
        - Adds new columns from current DataFrame
        - Fills missing columns with null values of the correct type

        Args:
            existing_schema: Schema of existing table
            current_df: Current DataFrame to write

        Returns:
            Tuple of (merged_dataframe, merged_schema)
        """
        # Merge schemas: preserve existing columns, add new ones
        merged_schema = existing_schema.merge_with(current_df.schema)

        # Identify missing columns in current DataFrame
        existing_columns = set(existing_schema.fieldNames())
        current_columns = set(current_df.schema.fieldNames())
        missing_columns = existing_columns - current_columns

        # Add missing columns with null values to DataFrame
        merged_df = current_df
        if missing_columns:
            from ..functions import Functions

            for col_name in sorted(missing_columns):
                # Get the field from existing schema
                field = existing_schema.get_field_by_name(col_name)
                if field is not None:
                    # Add column with null values of the correct type
                    field_data_type = field.dataType
                    from .protocols import SupportsDataFrameOps

                    merged_df = cast("SupportsDataFrameOps", merged_df).withColumn(
                        col_name, Functions.lit(None).cast(field_data_type)
                    )

            # Reorder columns: existing first, then new (sorted)
            all_columns = list(existing_schema.fieldNames()) + sorted(
                current_columns - existing_columns
            )
            from .protocols import SupportsDataFrameOps  # noqa: TC001

            merged_df = cast("SupportsDataFrameOps", merged_df).select(*all_columns)

        return merged_df, merged_schema
