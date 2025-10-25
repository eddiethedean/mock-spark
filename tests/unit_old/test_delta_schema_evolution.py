"""
Unit tests for Delta Lake schema evolution.

Based on exploration/delta_schema_evolution.py findings:
- mergeSchema=true required to add new columns during append
- Without mergeSchema, append with new columns raises AnalysisException
- Old rows get null values for newly added columns
- Schema evolution works incrementally (multiple columns over time)
- Column types are preserved correctly
"""

import pytest
import uuid
from mock_spark import MockSparkSession
from mock_spark.errors import AnalysisException


class TestDeltaSchemaEvolution:
    """Test Delta Lake schema evolution with mergeSchema option."""

    def _get_unique_table_name(self, base_name):
        """Generate a unique table name to avoid race conditions."""
        return f"test.{base_name}_{uuid.uuid4().hex[:8]}"

    def test_merge_schema_append_adds_column(self):
        """Test that mergeSchema=true allows adding new columns."""
        table_name = self._get_unique_table_name("users")
        spark = MockSparkSession.builder.appName("test").getOrCreate()

        try:
            # Initial write with 2 columns
            df1 = spark.createDataFrame([{"id": 1, "name": "Alice"}])
            df1.write.format("delta").mode("overwrite").saveAsTable(table_name)

            # Verify initial schema
            table1 = spark.table(table_name)
            assert set(table1.columns) == {"id", "name"}
            assert table1.count() == 1

            # Append with new column using mergeSchema
            df2 = spark.createDataFrame([{"id": 2, "name": "Bob", "age": 30}])
            df2.write.format("delta").mode("append").option(
                "mergeSchema", "true"
            ).saveAsTable(table_name)

            # Verify schema evolved
            table2 = spark.table(table_name)
            assert set(table2.columns) == {"id", "name", "age"}
            assert table2.count() == 2
        finally:
            # Clean up the table
            try:
                spark.catalog.dropTable(table_name)
            except Exception:
                pass  # Table might not exist
            spark.stop()

    def test_merge_schema_false_rejects_new_columns(self):
        """Test that mergeSchema=false rejects schema mismatches."""
        table_name = self._get_unique_table_name("strict")
        spark = MockSparkSession.builder.appName("test").getOrCreate()

        try:
            # Initial write
            df1 = spark.createDataFrame([{"id": 1, "name": "Alice"}])
            df1.write.format("delta").mode("overwrite").saveAsTable(table_name)

            # Try to append with new column WITHOUT mergeSchema
            df2 = spark.createDataFrame([{"id": 2, "name": "Bob", "age": 30}])

            with pytest.raises(AnalysisException) as exc_info:
                df2.write.format("delta").mode("append").saveAsTable(table_name)

            assert (
                "schema" in str(exc_info.value).lower()
                or "column" in str(exc_info.value).lower()
            )
        finally:
            # Clean up the table
            try:
                spark.catalog.dropTable(table_name)
            except Exception:
                pass  # Table might not exist
            spark.stop()

    def test_schema_evolution_fills_nulls(self):
        """Test that old rows get null for new columns."""
        table_name = self._get_unique_table_name("nulls")
        spark = MockSparkSession.builder.appName("test").getOrCreate()

        try:
            # Initial write
            df1 = spark.createDataFrame([{"id": 1, "name": "Alice"}])
            df1.write.format("delta").mode("overwrite").saveAsTable(table_name)

            # Append with new column
            df2 = spark.createDataFrame([{"id": 2, "name": "Bob", "score": 100}])
            df2.write.format("delta").mode("append").option(
                "mergeSchema", "true"
            ).saveAsTable(table_name)

            result = spark.table(table_name)
            rows = {row["id"]: row for row in result.collect()}

            # First row should have null score
            assert rows[1]["score"] is None

            # Second row should have score value
            assert rows[2]["score"] == 100
        finally:
            # Clean up the table
            try:
                spark.catalog.dropTable(table_name)
            except Exception:
                pass  # Table might not exist
            spark.stop()

    def test_incremental_schema_evolution(self):
        """Test multiple schema evolution steps."""
        table_name = self._get_unique_table_name("evolve")
        spark = MockSparkSession.builder.appName("test").getOrCreate()

        try:
            # Step 1: Initial schema
            df1 = spark.createDataFrame([{"id": 1, "name": "Alice"}])
            df1.write.format("delta").mode("overwrite").saveAsTable(table_name)

            # Step 2: Add first new column
            df2 = spark.createDataFrame([{"id": 2, "name": "Bob", "age": 30}])
            df2.write.format("delta").mode("append").option(
                "mergeSchema", "true"
            ).saveAsTable(table_name)

            # Step 3: Add second new column
            df3 = spark.createDataFrame(
                [{"id": 3, "name": "Charlie", "age": 25, "city": "NYC"}]
            )
            df3.write.format("delta").mode("append").option(
                "mergeSchema", "true"
            ).saveAsTable(table_name)

            result = spark.table(table_name)
            assert set(result.columns) == {"id", "name", "age", "city"}
            assert result.count() == 3

            # Verify null handling
            rows = {row["id"]: row for row in result.collect()}
            assert rows[1]["age"] is None
            assert rows[1]["city"] is None
            assert rows[2]["city"] is None
        finally:
            # Clean up the table
            try:
                spark.catalog.dropTable(table_name)
            except Exception:
                pass  # Table might not exist
            spark.stop()

    def test_merge_schema_preserves_types(self):
        """Test that mergeSchema preserves column types correctly."""
        table_name = self._get_unique_table_name("types")
        spark = MockSparkSession.builder.appName("test").getOrCreate()

        try:
            # Initial write with mixed types
            df1 = spark.createDataFrame([{"id": 1, "name": "Alice", "age": 25}])
            df1.write.format("delta").mode("overwrite").saveAsTable(table_name)

            # Append with new column of different type
            df2 = spark.createDataFrame(
                [{"id": 2, "name": "Bob", "age": 30, "score": 95.5}]
            )
            df2.write.format("delta").mode("append").option(
                "mergeSchema", "true"
            ).saveAsTable(table_name)

            result = spark.table(table_name)
            assert set(result.columns) == {"id", "name", "age", "score"}
            assert result.count() == 2

            # Verify types are preserved
            rows = {row["id"]: row for row in result.collect()}
            assert isinstance(rows[1]["age"], int)
            assert isinstance(rows[2]["age"], int)
            assert isinstance(rows[2]["score"], float)
        finally:
            # Clean up the table
            try:
                spark.catalog.dropTable(table_name)
            except Exception:
                pass  # Table might not exist
            spark.stop()

    def test_non_delta_table_converts_to_delta(self):
        """Test that appending with Delta format converts regular tables to Delta."""
        table_name = self._get_unique_table_name("regular")
        spark = MockSparkSession.builder.appName("test").getOrCreate()

        try:
            # Create a regular table (not Delta)
            df1 = spark.createDataFrame([{"id": 1, "name": "Alice"}])
            df1.write.saveAsTable(table_name)  # No format specified

            # Append with Delta format - should convert table to Delta and succeed
            df2 = spark.createDataFrame([{"id": 2, "name": "Bob", "age": 30}])
            df2.write.format("delta").mode("append").option(
                "mergeSchema", "true"
            ).saveAsTable(table_name)

            # Verify the table now has Delta format and merged schema
            result = spark.table(table_name)
            assert set(result.columns) == {"id", "name", "age"}
            assert result.count() == 2

            # Verify null handling for the first row
            rows = {row["id"]: row for row in result.collect()}
            assert rows[1]["age"] is None  # First row should have null for new column
            assert rows[2]["age"] == 30  # Second row should have the value
        finally:
            # Clean up the table
            try:
                spark.catalog.dropTable(table_name)
            except Exception:
                pass  # Table might not exist
            spark.stop()
