"""
Unit tests for Delta Lake basic write support.

Based on exploration/delta_basic_write.py findings:
- format('delta').saveAsTable() works with qualified table names
- Modes: overwrite, append, error (raises exception), ignore (silent)
- DESCRIBE EXTENDED shows Provider = delta
- save() to path also works with format('delta')
- Tables are tracked in catalog/metastore
"""

import pytest
from mock_spark import MockSparkSession
from mock_spark.errors import AnalysisException


class TestDeltaBasicWrite:
    """Test basic Delta format write operations."""

    def test_basic_delta_write_save_as_table(self):
        """Test basic Delta write with saveAsTable()."""
        spark = MockSparkSession.builder.appName("test").getOrCreate()
        
        # Create DataFrame
        data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        df = spark.createDataFrame(data)
        
        # Write as Delta table
        df.write.format("delta").mode("overwrite").saveAsTable("test_schema.users")
        
        # Read back
        result = spark.table("test_schema.users")
        assert result.count() == 2
        rows = result.collect()
        assert rows[0]["name"] in ["Alice", "Bob"]
        
        # Verify it's marked as Delta format
        table_meta = spark.storage.get_table_metadata("test_schema", "users")
        assert table_meta is not None
        assert table_meta.get("format") == "delta"
        assert table_meta.get("version") == 0
        
        spark.stop()

    def test_delta_write_mode_overwrite(self):
        """Test Delta write with overwrite mode."""
        spark = MockSparkSession.builder.appName("test").getOrCreate()
        
        # First write
        df1 = spark.createDataFrame([{"id": 1, "value": "first"}])
        df1.write.format("delta").mode("overwrite").saveAsTable("test.modes_test")
        
        assert spark.table("test.modes_test").count() == 1
        
        # Second write - overwrite
        df2 = spark.createDataFrame([{"id": 2, "value": "second"}])
        df2.write.format("delta").mode("overwrite").saveAsTable("test.modes_test")
        
        result = spark.table("test.modes_test")
        assert result.count() == 1  # Should only have new data
        assert result.collect()[0]["id"] == 2
        
        # Version should increment
        meta = spark.storage.get_table_metadata("test", "modes_test")
        assert meta.get("version") == 1  # Version incremented
        
        spark.stop()

    def test_delta_write_mode_append(self):
        """Test Delta write with append mode."""
        spark = MockSparkSession.builder.appName("test").getOrCreate()
        
        # Initial write
        df1 = spark.createDataFrame([{"id": 1, "value": "first"}])
        df1.write.format("delta").mode("overwrite").saveAsTable("test.append_test")
        
        # Append
        df2 = spark.createDataFrame([{"id": 2, "value": "second"}])
        df2.write.format("delta").mode("append").saveAsTable("test.append_test")
        
        result = spark.table("test.append_test")
        assert result.count() == 2  # Both records present
        
        spark.stop()

    def test_delta_write_mode_error_raises_on_existing(self):
        """Test that error mode raises exception when table exists."""
        spark = MockSparkSession.builder.appName("test").getOrCreate()
        
        # Create table
        df1 = spark.createDataFrame([{"id": 1}])
        df1.write.format("delta").mode("overwrite").saveAsTable("test.error_test")
        
        # Try to write again with error mode
        df2 = spark.createDataFrame([{"id": 2}])
        
        with pytest.raises(AnalysisException) as exc_info:
            df2.write.format("delta").mode("error").saveAsTable("test.error_test")
        
        assert "already exists" in str(exc_info.value)
        
        spark.stop()

    def test_delta_write_mode_ignore(self):
        """Test that ignore mode silently does nothing when table exists."""
        spark = MockSparkSession.builder.appName("test").getOrCreate()
        
        # Create table
        df1 = spark.createDataFrame([{"id": 1, "value": "original"}])
        df1.write.format("delta").mode("overwrite").saveAsTable("test.ignore_test")
        
        initial_count = spark.table("test.ignore_test").count()
        
        # Try to write with ignore mode
        df2 = spark.createDataFrame([{"id": 2, "value": "should be ignored"}])
        df2.write.format("delta").mode("ignore").saveAsTable("test.ignore_test")
        
        # Table should be unchanged
        final_count = spark.table("test.ignore_test").count()
        assert final_count == initial_count == 1
        assert spark.table("test.ignore_test").collect()[0]["value"] == "original"
        
        spark.stop()

    def test_delta_write_to_path(self):
        """Test Delta write to file path with save()."""
        spark = MockSparkSession.builder.appName("test").getOrCreate()
        
        df = spark.createDataFrame([{"id": 1, "name": "Path Test"}])
        
        # Write to path (mock implementation logs the operation)
        delta_path = "/tmp/delta_path_test"
        df.write.format("delta").mode("overwrite").save(delta_path)
        
        # In mock implementation, this is logged but doesn't create actual files
        # Just verify no exceptions were raised
        
        spark.stop()

    def test_delta_table_properties(self):
        """Test that Delta tables have appropriate properties."""
        spark = MockSparkSession.builder.appName("test").getOrCreate()
        
        df = spark.createDataFrame([{"id": 1}])
        df.write.format("delta").mode("overwrite").saveAsTable("test.props_test")
        
        # Get table metadata
        meta = spark.storage.get_table_metadata("test", "props_test")
        
        assert meta is not None
        assert meta.get("format") == "delta"
        assert meta.get("version") is not None
        assert isinstance(meta.get("version"), int)
        
        # Check for Delta-specific properties
        props = meta.get("properties", {})
        assert "delta.minReaderVersion" in props
        assert "delta.minWriterVersion" in props
        
        spark.stop()

    def test_non_delta_table_has_no_delta_properties(self):
        """Test that non-Delta tables don't have Delta properties."""
        spark = MockSparkSession.builder.appName("test").getOrCreate()
        
        df = spark.createDataFrame([{"id": 1}])
        df.write.saveAsTable("test.regular_table")  # No format specified
        
        meta = spark.storage.get_table_metadata("test", "regular_table")
        assert meta.get("format") != "delta"
        
        spark.stop()


class TestDeltaWriteWithOptions:
    """Test Delta write with various options."""

    def test_delta_write_with_partition_by(self):
        """Test Delta write with partitionBy option."""
        spark = MockSparkSession.builder.appName("test").getOrCreate()
        
        df = spark.createDataFrame([
            {"year": 2024, "month": 1, "value": 100},
            {"year": 2024, "month": 2, "value": 200},
        ])
        
        # Write with partitioning
        df.write.format("delta").partitionBy("year", "month").mode("overwrite").saveAsTable(
            "test.partitioned"
        )
        
        # Verify table exists and data is correct
        result = spark.table("test.partitioned")
        assert result.count() == 2
        
        spark.stop()

    def test_delta_write_creates_schema_if_not_exists(self):
        """Test that Delta write creates schema if it doesn't exist."""
        spark = MockSparkSession.builder.appName("test").getOrCreate()
        
        df = spark.createDataFrame([{"id": 1}])
        
        # Write to non-existent schema
        df.write.format("delta").mode("overwrite").saveAsTable("new_schema.new_table")
        
        # Verify schema was created
        assert spark.storage.schema_exists("new_schema")
        assert spark.storage.table_exists("new_schema", "new_table")
        
        spark.stop()

