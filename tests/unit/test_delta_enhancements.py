"""
Unit tests for Delta Lake enhancements in mock-spark.

Tests for:
- DeltaTable.optimize()
- DeltaTable.detail()
- DeltaTable.history()
- date_format() function
- from_unixtime() function
- delta.tables module aliasing
"""

import pytest
from mock_spark import MockSparkSession, F


class TestDeltaTableEnhancements:
    """Test enhanced DeltaTable methods."""

    def test_delta_table_optimize(self):
        """Test optimize() returns self for method chaining."""
        from delta.tables import DeltaTable

        spark = MockSparkSession("test")
        spark.storage.create_schema("test")
        df = spark.createDataFrame([(1, "a")], ["id", "value"])
        df.write.format("delta").saveAsTable("test.my_table")

        delta_table = DeltaTable.forName(spark, "test.my_table")
        result = delta_table.optimize()

        assert result is delta_table  # Method chaining

    def test_delta_table_detail(self):
        """Test detail() returns DataFrame with table metadata."""
        from delta.tables import DeltaTable

        spark = MockSparkSession("test")
        spark.storage.create_schema("test")
        df = spark.createDataFrame([(1, "a")], ["id", "value"])
        df.write.format("delta").saveAsTable("test.my_table")

        delta_table = DeltaTable.forName(spark, "test.my_table")
        details = delta_table.detail()

        assert details.count() > 0
        assert "format" in details.columns
        assert "location" in details.columns
        assert "name" in details.columns
        assert "numFiles" in details.columns
        assert "sizeInBytes" in details.columns

        # Check the detail row
        detail_row = details.collect()[0]
        assert detail_row["format"] == "delta"
        assert detail_row["name"] == "test.my_table"
        assert detail_row["numFiles"] == 1
        assert detail_row["sizeInBytes"] == 1024

    def test_delta_table_history(self):
        """Test history() returns DataFrame with version history."""
        from delta.tables import DeltaTable

        spark = MockSparkSession("test")
        spark.storage.create_schema("test")
        df = spark.createDataFrame([(1, "a")], ["id", "value"])
        df.write.format("delta").saveAsTable("test.my_table")

        delta_table = DeltaTable.forName(spark, "test.my_table")
        history = delta_table.history()

        assert history.count() > 0
        assert "version" in history.columns
        assert "timestamp" in history.columns
        assert "operation" in history.columns
        assert "userId" in history.columns
        assert "userName" in history.columns

        # Check the history row
        history_row = history.collect()[0]
        assert history_row["version"] == 0
        assert history_row["operation"] == "CREATE TABLE"
        assert history_row["userId"] == "mock_user"

    def test_delta_table_history_with_limit(self):
        """Test history() respects limit parameter."""
        from delta.tables import DeltaTable

        spark = MockSparkSession("test")
        spark.storage.create_schema("test")
        df = spark.createDataFrame([(1, "a")], ["id", "value"])
        df.write.format("delta").saveAsTable("test.my_table")

        delta_table = DeltaTable.forName(spark, "test.my_table")
        history = delta_table.history(limit=1)

        assert history.count() <= 1

    def test_delta_table_not_found(self):
        """Test DeltaTable.forName raises for non-existent table."""
        from mock_spark.errors import AnalysisException
        from delta.tables import DeltaTable

        spark = MockSparkSession("test")

        with pytest.raises(AnalysisException):
            DeltaTable.forName(spark, "nonexistent.table")


class TestDateTimeFunctions:
    """Test new datetime functions."""

    def test_date_format(self):
        """Test date_format() function."""
        spark = MockSparkSession("test")
        data = [{"timestamp": "2024-01-15 10:30:00"}]
        df = spark.createDataFrame(data)

        # Test date_format function exists and can be called
        result = df.select(F.date_format(F.col("timestamp"), "yyyy-MM-dd"))

        assert result is not None
        assert len(result.columns) == 1
        assert "date_format(timestamp, 'yyyy-MM-dd')" in result.columns

    def test_from_unixtime(self):
        """Test from_unixtime() function."""
        spark = MockSparkSession("test")
        data = [{"unix_timestamp": 1705312200}]  # 2024-01-15 10:30:00
        df = spark.createDataFrame(data)

        # Test from_unixtime function exists and can be called
        result = df.select(F.from_unixtime(F.col("unix_timestamp")))

        assert result is not None
        assert len(result.columns) == 1
        assert "from_unixtime(unix_timestamp, 'yyyy-MM-dd HH:mm:ss')" in result.columns

    def test_from_unixtime_with_format(self):
        """Test from_unixtime() with custom format."""
        spark = MockSparkSession("test")
        data = [{"unix_timestamp": 1705312200}]
        df = spark.createDataFrame(data)

        # Test from_unixtime with custom format
        result = df.select(F.from_unixtime(F.col("unix_timestamp"), "yyyy-MM-dd"))

        assert result is not None
        assert len(result.columns) == 1
        assert "from_unixtime(unix_timestamp, 'yyyy-MM-dd')" in result.columns


class TestDeltaModuleAliasing:
    """Test delta.tables module aliasing."""

    def test_delta_tables_import(self):
        """Test that 'from delta.tables import DeltaTable' works."""
        # This should work without errors
        from delta.tables import DeltaTable

        assert DeltaTable is not None

    def test_delta_tables_import_works_with_spark(self):
        """Test that imported DeltaTable works with MockSparkSession."""
        from delta.tables import DeltaTable

        spark = MockSparkSession("test")
        spark.storage.create_schema("test")
        df = spark.createDataFrame([(1, "a")], ["id", "value"])
        df.write.format("delta").saveAsTable("test.my_table")

        # Should work with the imported DeltaTable
        delta_table = DeltaTable.forName(spark, "test.my_table")
        assert delta_table is not None
        assert delta_table._table_name == "test.my_table"

    def test_delta_tables_module_attributes(self):
        """Test that delta.tables module has expected attributes."""
        import delta.tables

        assert hasattr(delta.tables, "DeltaTable")
        assert delta.tables.DeltaTable is not None

    def test_delta_module_structure(self):
        """Test that delta module structure is correct."""
        import delta

        assert hasattr(delta, "tables")
        assert delta.tables is not None
        assert hasattr(delta.tables, "DeltaTable")


class TestDeltaTableIntegration:
    """Integration tests for DeltaTable with all new features."""

    def test_complete_delta_workflow(self):
        """Test complete workflow with all DeltaTable features."""
        spark = MockSparkSession("test")
        spark.storage.create_schema("test")

        # Create table
        df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
        df.write.format("delta").saveAsTable("test.workflow_table")

        # Import from delta.tables
        from delta.tables import DeltaTable

        # Get DeltaTable
        delta_table = DeltaTable.forName(spark, "test.workflow_table")

        # Test all methods
        assert delta_table is not None

        # Test optimize
        result = delta_table.optimize()
        assert result is delta_table

        # Test detail
        details = delta_table.detail()
        assert details.count() > 0
        assert details.collect()[0]["name"] == "test.workflow_table"

        # Test history
        history = delta_table.history()
        assert history.count() > 0
        assert history.collect()[0]["version"] == 0

        # Test vacuum
        delta_table.vacuum(24.0)  # Should not raise

        # Test toDF
        df_result = delta_table.toDF()
        assert df_result.count() == 2

    def test_delta_table_with_datetime_functions(self):
        """Test DeltaTable with new datetime functions."""
        spark = MockSparkSession("test")
        spark.storage.create_schema("test")

        # Create table with timestamp data
        data = [{"id": 1, "timestamp": "2024-01-15 10:30:00", "unix_time": 1705312200}]
        df = spark.createDataFrame(data)
        df.write.format("delta").saveAsTable("test.timestamp_table")

        # Get DeltaTable
        from delta.tables import DeltaTable

        delta_table = DeltaTable.forName(spark, "test.timestamp_table")

        # Read data and apply datetime functions
        df_result = delta_table.toDF()
        result = df_result.select(
            F.date_format(F.col("timestamp"), "yyyy-MM-dd"),
            F.from_unixtime(F.col("unix_time"), "yyyy-MM-dd"),
        )

        assert result is not None
        assert len(result.columns) == 2

