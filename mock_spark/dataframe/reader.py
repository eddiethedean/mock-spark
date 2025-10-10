"""
Mock DataFrameReader implementation for DataFrame read operations.

This module provides DataFrame reading functionality, maintaining compatibility
with PySpark's DataFrameReader interface. Supports reading from various data sources
including tables, files, and custom storage backends.

Key Features:
    - Complete PySpark DataFrameReader API compatibility
    - Support for multiple data formats (parquet, json, csv, table)
    - Flexible options configuration
    - Integration with storage manager
    - Schema inference and validation
    - Error handling for missing data sources

Example:
    >>> from mock_spark import MockSparkSession
    >>> spark = MockSparkSession("test")
    >>> # Read from table
    >>> df = spark.read.table("my_table")
    >>> # Read with format and options
    >>> df = spark.read.format("parquet").option("header", "true").load("/path")
"""

from typing import Any, Dict, Optional, Union
from ..core.interfaces.dataframe import IDataFrame
from ..core.interfaces.session import ISession
from ..spark_types import MockStructType


class MockDataFrameReader:
    """Mock DataFrameReader for reading data from various sources.

    Provides a PySpark-compatible interface for reading DataFrames from storage
    formats and tables. Supports various formats and options for testing and development.

    Attributes:
        session: Mock Spark session instance.
        _format: Input format (e.g., 'parquet', 'json').
        _options: Additional options for the reader.

    Example:
        >>> spark.read.format("parquet").load("/path/to/file")
        >>> spark.read.table("my_table")
    """

    def __init__(self, session: ISession):
        """Initialize MockDataFrameReader.

        Args:
            session: Mock Spark session instance.
        """
        self.session = session
        self._format = "parquet"
        self._options: Dict[str, str] = {}

    def format(self, source: str) -> "MockDataFrameReader":
        """Set input format.

        Args:
            source: Data source format.

        Returns:
            Self for method chaining.

        Example:
            >>> spark.read.format("parquet")
        """
        self._format = source
        return self

    def option(self, key: str, value: Any) -> "MockDataFrameReader":
        """Set option.

        Args:
            key: Option key.
            value: Option value.

        Returns:
            Self for method chaining.

        Example:
            >>> spark.read.option("header", "true")
        """
        self._options[key] = value
        return self

    def options(self, **options: Any) -> "MockDataFrameReader":
        """Set multiple options.

        Args:
            **options: Option key-value pairs.

        Returns:
            Self for method chaining.

        Example:
            >>> spark.read.options(header="true", inferSchema="true")
        """
        self._options.update(options)
        return self

    def schema(self, schema: Union[MockStructType, str]) -> "MockDataFrameReader":
        """Set schema.

        Args:
            schema: Schema definition.

        Returns:
            Self for method chaining.

        Example:
            >>> spark.read.schema("name STRING, age INT")
        """
        # Mock implementation - store schema for reference
        self._schema = schema
        return self

    def load(
        self, path: Optional[str] = None, format: Optional[str] = None, **options: Any
    ) -> IDataFrame:
        """Load data.

        Args:
            path: Path to data.
            format: Data format.
            **options: Additional options.

        Returns:
            DataFrame with loaded data.

        Example:
            >>> spark.read.load("/path/to/file")
            >>> spark.read.format("parquet").load("/path/to/file")
        """
        # Mock implementation - return empty DataFrame
        from .dataframe import MockDataFrame
        from typing import cast
        from ..core.interfaces.dataframe import IDataFrame

        return cast(IDataFrame, MockDataFrame([], MockStructType([])))

    def table(self, table_name: str) -> IDataFrame:
        """Load table.

        Args:
            table_name: Table name.

        Returns:
            DataFrame with table data.

        Example:
            >>> spark.read.table("my_table")
            >>> spark.read.format("delta").option("versionAsOf", 0).table("my_table")
        """
        # Check for versionAsOf option (Delta time travel)
        if "versionAsOf" in self._options and self._format == "delta":
            version_number = int(self._options["versionAsOf"])

            # Parse schema and table name
            if "." in table_name:
                schema_name, table_only = table_name.split(".", 1)
            else:
                schema_name, table_only = "default", table_name

            # Get table metadata to access version history
            meta = self.session.storage.get_table_metadata(schema_name, table_only)  # type: ignore[attr-defined]

            if not meta or meta.get("format") != "delta":
                from ..errors import AnalysisException

                raise AnalysisException(
                    f"Table {table_name} is not a Delta table. "
                    "versionAsOf can only be used with Delta format tables."
                )

            version_history = meta.get("version_history", [])

            # Find the requested version
            target_version = None
            for v in version_history:
                # Handle both MockDeltaVersion objects and dicts
                v_num = v.version if hasattr(v, "version") else v.get("version")
                if v_num == version_number:
                    target_version = v
                    break

            if target_version is None:
                from ..errors import AnalysisException

                raise AnalysisException(
                    f"Version {version_number} does not exist for table {table_name}. "
                    f"Available versions: {[v.version if hasattr(v, 'version') else v.get('version') for v in version_history]}"
                )

            # Get the data snapshot for this version
            data_snapshot = (
                target_version.data_snapshot
                if hasattr(target_version, "data_snapshot")
                else target_version.get("data_snapshot", [])
            )

            # Create DataFrame with the historical data using session's createDataFrame
            return self.session.createDataFrame(data_snapshot)

        return self.session.table(table_name)

    def json(self, path: str, **options: Any) -> IDataFrame:
        """Load JSON data.

        Args:
            path: Path to JSON file.
            **options: Additional options.

        Returns:
            DataFrame with JSON data.

        Example:
            >>> spark.read.json("/path/to/file.json")
        """
        # Mock implementation
        from .dataframe import MockDataFrame
        from typing import cast
        from ..core.interfaces.dataframe import IDataFrame

        return cast(IDataFrame, MockDataFrame([], MockStructType([])))

    def csv(self, path: str, **options: Any) -> IDataFrame:
        """Load CSV data.

        Args:
            path: Path to CSV file.
            **options: Additional options.

        Returns:
            DataFrame with CSV data.

        Example:
            >>> spark.read.csv("/path/to/file.csv")
        """
        # Mock implementation
        from .dataframe import MockDataFrame
        from typing import cast
        from ..core.interfaces.dataframe import IDataFrame

        return cast(IDataFrame, MockDataFrame([], MockStructType([])))

    def parquet(self, path: str, **options: Any) -> IDataFrame:
        """Load Parquet data.

        Args:
            path: Path to Parquet file.
            **options: Additional options.

        Returns:
            DataFrame with Parquet data.

        Example:
            >>> spark.read.parquet("/path/to/file.parquet")
        """
        # Mock implementation
        from .dataframe import MockDataFrame
        from typing import cast
        from ..core.interfaces.dataframe import IDataFrame

        return cast(IDataFrame, MockDataFrame([], MockStructType([])))

    def orc(self, path: str, **options: Any) -> IDataFrame:
        """Load ORC data.

        Args:
            path: Path to ORC file.
            **options: Additional options.

        Returns:
            DataFrame with ORC data.

        Example:
            >>> spark.read.orc("/path/to/file.orc")
        """
        # Mock implementation
        from .dataframe import MockDataFrame
        from typing import cast
        from ..core.interfaces.dataframe import IDataFrame

        return cast(IDataFrame, MockDataFrame([], MockStructType([])))

    def text(self, path: str, **options: Any) -> IDataFrame:
        """Load text data.

        Args:
            path: Path to text file.
            **options: Additional options.

        Returns:
            DataFrame with text data.

        Example:
            >>> spark.read.text("/path/to/file.txt")
        """
        # Mock implementation
        from .dataframe import MockDataFrame
        from typing import cast
        from ..core.interfaces.dataframe import IDataFrame

        return cast(IDataFrame, MockDataFrame([], MockStructType([])))

    def jdbc(self, url: str, table: str, **options: Any) -> IDataFrame:
        """Load data from JDBC source.

        Args:
            url: JDBC URL.
            table: Table name.
            **options: Additional options.

        Returns:
            DataFrame with JDBC data.

        Example:
            >>> spark.read.jdbc("jdbc:postgresql://localhost:5432/db", "table")
        """
        # Mock implementation
        from .dataframe import MockDataFrame
        from typing import cast
        from ..core.interfaces.dataframe import IDataFrame

        return cast(IDataFrame, MockDataFrame([], MockStructType([])))
