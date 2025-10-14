"""
Simple Delta Lake support for Mock Spark.

Provides minimal Delta Lake API compatibility by wrapping regular tables.
Good enough for basic Delta tests without requiring the delta-spark library.

Usage:
    # Create table normally
    df.write.saveAsTable("schema.table")

    # Access as Delta
    dt = DeltaTable.forName(spark, "schema.table")
    df = dt.toDF()

    # Mock operations (don't actually execute)
    dt.delete("id < 10")  # No-op
    dt.merge(source, "condition").execute()  # No-op

For real Delta operations (MERGE, time travel, etc.), use real PySpark + delta-spark.
"""

from __future__ import annotations
from typing import Any, Optional, Dict, TYPE_CHECKING

if TYPE_CHECKING:
    from .dataframe import MockDataFrame


class DeltaTable:
    """
    Simple DeltaTable wrapper for basic Delta Lake compatibility.

    Just wraps existing tables - doesn't implement real Delta features.
    Sufficient for tests that check Delta API exists and can be called.
    """

    def __init__(self, spark_session: Any, table_name: str):
        """Initialize DeltaTable wrapper."""
        self._spark = spark_session
        self._table_name = table_name

    @classmethod
    def forName(cls, spark_session: Any, table_name: str) -> DeltaTable:
        """
        Get DeltaTable for existing table.

        Usage:
            df.write.saveAsTable("schema.table")
            dt = DeltaTable.forName(spark, "schema.table")
        """
        # Import here to avoid circular imports
        from .core.exceptions.analysis import AnalysisException

        # Parse table name
        if "." in table_name:
            schema, table = table_name.split(".", 1)
        else:
            schema, table = "default", table_name

        # Check table exists (only for MockSparkSession)
        if hasattr(spark_session, "storage"):
            if not spark_session.storage.table_exists(schema, table):
                raise AnalysisException(f"Table or view not found: {table_name}")
        # For real SparkSession, we'll just assume the table exists
        # and let it fail naturally if it doesn't

        return cls(spark_session, table_name)

    @classmethod
    def forPath(cls, spark_session: Any, path: str) -> DeltaTable:
        """Get DeltaTable by path (mock - treats path as table name)."""
        table_name = path.split("/")[-1] if "/" in path else path
        return cls(spark_session, f"default.{table_name}")

    def toDF(self) -> MockDataFrame:
        """Get DataFrame from Delta table."""
        return self._spark.table(self._table_name)

    def alias(self, alias: str) -> DeltaTable:
        """Alias table (returns self for chaining)."""
        return self

    # Mock operations - don't actually execute
    def delete(self, condition: Optional[str] = None) -> None:
        """Mock delete (no-op)."""
        pass

    def update(self, condition: str, set_values: Dict[str, Any]) -> None:
        """Mock update (no-op)."""
        pass

    def merge(self, source: Any, condition: str) -> DeltaMergeBuilder:
        """Mock merge (returns builder for chaining)."""
        return DeltaMergeBuilder(self)

    def vacuum(self, retention_hours: Optional[float] = None) -> None:
        """Mock vacuum (no-op)."""
        pass

    def optimize(self) -> DeltaTable:
        """
        Mock OPTIMIZE operation.

        In real Delta Lake, this compacts small files.
        For testing, this is a no-op that returns self.

        Returns:
            self for method chaining
        """
        return self

    def detail(self) -> MockDataFrame:
        """
        Mock table detail information.

        Returns a DataFrame with table metadata.

        Returns:
            MockDataFrame with table details
        """
        from .spark_types import (
            MockStructType,
            MockStructField,
            StringType,
            LongType,
            ArrayType,
            MapType,
        )
        from .dataframe import MockDataFrame

        # Create mock table details
        details: list[Dict[str, Any]] = [
            {
                "format": "delta",
                "id": f"mock-table-{hash(self._table_name)}",
                "name": self._table_name,
                "description": None,
                "location": f"/mock/delta/{self._table_name.replace('.', '/')}",
                "createdAt": "2024-01-01T00:00:00.000+0000",
                "lastModified": "2024-01-01T00:00:00.000+0000",
                "partitionColumns": [],
                "numFiles": 1,
                "sizeInBytes": 1024,
                "properties": {},
                "minReaderVersion": 1,
                "minWriterVersion": 2,
            }
        ]

        schema = MockStructType(
            [
                MockStructField("format", StringType()),
                MockStructField("id", StringType()),
                MockStructField("name", StringType()),
                MockStructField("description", StringType()),
                MockStructField("location", StringType()),
                MockStructField("createdAt", StringType()),
                MockStructField("lastModified", StringType()),
                MockStructField("partitionColumns", ArrayType(StringType())),
                MockStructField("numFiles", LongType()),
                MockStructField("sizeInBytes", LongType()),
                MockStructField("properties", MapType(StringType(), StringType())),
                MockStructField("minReaderVersion", LongType()),
                MockStructField("minWriterVersion", LongType()),
            ]
        )

        return MockDataFrame(details, schema, self._spark.storage)

    def history(self, limit: Optional[int] = None) -> MockDataFrame:
        """
        Mock table history.

        Returns a DataFrame with table version history.

        Args:
            limit: Optional limit on number of versions to return

        Returns:
            MockDataFrame with version history
        """
        from .spark_types import (
            MockStructType,
            MockStructField,
            StringType,
            LongType,
            MapType,
        )
        from .dataframe import MockDataFrame

        # Create mock history
        history = [
            {
                "version": 0,
                "timestamp": "2024-01-01T00:00:00.000+0000",
                "userId": "mock_user",
                "userName": "mock_user",
                "operation": "CREATE TABLE",
                "operationParameters": {},
                "readVersion": None,
                "isolationLevel": "Serializable",
                "isBlindAppend": True,
            }
        ]

        if limit and limit < len(history):
            history = history[:limit]

        schema = MockStructType(
            [
                MockStructField("version", LongType()),
                MockStructField("timestamp", StringType()),
                MockStructField("userId", StringType()),
                MockStructField("userName", StringType()),
                MockStructField("operation", StringType()),
                MockStructField("operationParameters", MapType(StringType(), StringType())),
                MockStructField("readVersion", LongType()),
                MockStructField("isolationLevel", StringType()),
                MockStructField("isBlindAppend", LongType()),
            ]
        )

        return MockDataFrame(history, schema, self._spark.storage)


class DeltaMergeBuilder:
    """Mock merge builder for method chaining."""

    def __init__(self, delta_table: DeltaTable):
        self._table = delta_table

    def whenMatchedUpdate(self, set_values: Dict[str, Any]) -> DeltaMergeBuilder:
        return self

    def whenMatchedUpdateAll(self) -> DeltaMergeBuilder:
        return self

    def whenMatchedDelete(self, condition: Optional[str] = None) -> DeltaMergeBuilder:
        return self

    def whenNotMatchedInsert(self, values: Dict[str, Any]) -> DeltaMergeBuilder:
        return self

    def whenNotMatchedInsertAll(self) -> DeltaMergeBuilder:
        return self

    def execute(self) -> None:
        """Execute merge (no-op)."""
        pass
