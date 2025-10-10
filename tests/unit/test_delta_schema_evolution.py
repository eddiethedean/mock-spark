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
from mock_spark import MockSparkSession
from mock_spark.errors import AnalysisException


class TestDeltaSchemaEvolution:
    """Test Delta Lake schema evolution with mergeSchema option."""

    def test_merge_schema_append_adds_column(self):
        """Test that mergeSchema=true allows adding new columns."""
        spark = MockSparkSession.builder.appName("test").getOrCreate()

        # Initial write with 2 columns
        df1 = spark.createDataFrame([{"id": 1, "name": "Alice"}])
        df1.write.format("delta").mode("overwrite").saveAsTable("test.users")

        # Verify initial schema
        table1 = spark.table("test.users")
        assert set(table1.columns) == {"id", "name"}
        assert table1.count() == 1

        # Append with new column using mergeSchema
        df2 = spark.createDataFrame([{"id": 2, "name": "Bob", "age": 30}])
        df2.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
            "test.users"
        )

        # Verify schema evolved
        table2 = spark.table("test.users")
        assert set(table2.columns) == {"id", "name", "age"}
        assert table2.count() == 2

        spark.stop()

    def test_merge_schema_false_rejects_new_columns(self):
        """Test that mergeSchema=false rejects schema mismatches."""
        spark = MockSparkSession.builder.appName("test").getOrCreate()

        # Initial write
        df1 = spark.createDataFrame([{"id": 1, "name": "Alice"}])
        df1.write.format("delta").mode("overwrite").saveAsTable("test.strict")

        # Try to append with new column WITHOUT mergeSchema
        df2 = spark.createDataFrame([{"id": 2, "name": "Bob", "age": 30}])

        with pytest.raises(AnalysisException) as exc_info:
            df2.write.format("delta").mode("append").saveAsTable("test.strict")

        assert "schema" in str(exc_info.value).lower() or "column" in str(exc_info.value).lower()

        spark.stop()

    def test_schema_evolution_fills_nulls(self):
        """Test that old rows get null for new columns."""
        spark = MockSparkSession.builder.appName("test").getOrCreate()

        # Initial write
        df1 = spark.createDataFrame([{"id": 1, "name": "Alice"}])
        df1.write.format("delta").mode("overwrite").saveAsTable("test.nulls")

        # Append with new column
        df2 = spark.createDataFrame([{"id": 2, "name": "Bob", "score": 100}])
        df2.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
            "test.nulls"
        )

        result = spark.table("test.nulls")
        rows = {row["id"]: row for row in result.collect()}

        # First row should have null score
        assert rows[1]["score"] is None

        # Second row should have score value
        assert rows[2]["score"] == 100

        spark.stop()

    def test_incremental_schema_evolution(self):
        """Test multiple schema evolution steps."""
        spark = MockSparkSession.builder.appName("test").getOrCreate()

        # Version 1: column 'a'
        df1 = spark.createDataFrame([{"a": 1}])
        df1.write.format("delta").mode("overwrite").saveAsTable("test.evolve")
        assert set(spark.table("test.evolve").columns) == {"a"}

        # Version 2: add column 'b'
        df2 = spark.createDataFrame([{"a": 2, "b": "two"}])
        df2.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
            "test.evolve"
        )
        assert set(spark.table("test.evolve").columns) == {"a", "b"}

        # Version 3: add column 'c'
        df3 = spark.createDataFrame([{"a": 3, "b": "three", "c": 3.0}])
        df3.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
            "test.evolve"
        )

        result = spark.table("test.evolve")
        assert set(result.columns) == {"a", "b", "c"}
        assert result.count() == 3

        # Verify nulls propagated
        rows = sorted(result.collect(), key=lambda r: r["a"])
        assert rows[0]["b"] is None  # First row: only 'a'
        assert rows[0]["c"] is None
        assert rows[1]["c"] is None  # Second row: 'a', 'b'
        assert rows[2]["c"] == 3.0  # Third row: all columns

        spark.stop()

    def test_merge_schema_preserves_types(self):
        """Test that column types are preserved during evolution."""
        spark = MockSparkSession.builder.appName("test").getOrCreate()

        # Initial: integer and string
        df1 = spark.createDataFrame([{"id": 1, "value": 100}])
        df1.write.format("delta").mode("overwrite").saveAsTable("test.types")

        # Add float column
        df2 = spark.createDataFrame([{"id": 2, "value": 200, "score": 99.5}])
        df2.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
            "test.types"
        )

        result = spark.table("test.types")
        assert "score" in result.columns

        # Get row with score
        row2 = [r for r in result.collect() if r["id"] == 2][0]
        assert row2["score"] == 99.5

        spark.stop()

    def test_non_delta_table_ignores_merge_schema(self):
        """Test that mergeSchema option only applies to Delta tables."""
        spark = MockSparkSession.builder.appName("test").getOrCreate()

        # Create non-Delta table
        df1 = spark.createDataFrame([{"id": 1}])
        df1.write.saveAsTable("test.regular")  # No format specified

        # Try to append with mergeSchema to non-Delta table
        df2 = spark.createDataFrame([{"id": 2, "new_col": "value"}])

        # Should raise error regardless of mergeSchema (non-Delta tables don't support it)
        with pytest.raises(AnalysisException):
            df2.write.mode("append").option("mergeSchema", "true").saveAsTable("test.regular")

        spark.stop()
