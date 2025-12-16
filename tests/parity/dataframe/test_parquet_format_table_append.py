"""Test parquet format table append persistence - ensures data is visible after append writes.

Tests fix for issue #114: Table data not visible after append write via parquet format tables.
"""

import os
import pytest
from tests.fixtures.parity_base import ParityTestBase
from sparkless.spark_types import StructType, StructField, IntegerType, StringType


class TestParquetFormatTableAppend(ParityTestBase):
    """Test that table data is immediately visible after append writes with parquet format."""

    @pytest.fixture(autouse=True)
    def setup_method(self, spark):
        """Set up test environment and clean up after test."""
        self.spark = spark
        self.schema_name = "test_schema"
        self.table_name = "test_table"
        self.table_fqn = f"{self.schema_name}.{self.table_name}"

        # Ensure schema exists
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.schema_name}")

        yield

        # Clean up table and schema after test
        self.spark.sql(f"DROP TABLE IF EXISTS {self.table_fqn}")
        self.spark.sql(f"DROP SCHEMA IF EXISTS {self.schema_name} CASCADE")

        # Clean up any created parquet files manually if not in-memory
        if self.spark._storage.db_path != ":memory:":
            table_path = os.path.join(
                self.spark._storage.db_path,
                self.schema_name,
                f"{self.table_name}.parquet",
            )
            if os.path.exists(table_path):
                os.remove(table_path)
            schema_dir = os.path.join(self.spark._storage.db_path, self.schema_name)
            if os.path.exists(schema_dir) and not os.listdir(schema_dir):
                os.rmdir(schema_dir)

    def test_parquet_format_append_to_existing_table(self, spark):
        """Test that data appended to an existing parquet format table is immediately visible."""
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
            ]
        )

        # Create empty table with parquet format (like LogWriter does)
        empty_df = spark.createDataFrame([], schema)
        empty_df.write.format("parquet").mode("overwrite").saveAsTable(self.table_fqn)

        # Verify initial state
        result1 = spark.table(self.table_fqn)
        assert result1.count() == 0, "Initial table should be empty"

        # Append data with parquet format
        data1 = [{"id": 1, "name": "test1"}]
        df1 = spark.createDataFrame(data1, schema)
        df1.write.format("parquet").mode("append").saveAsTable(self.table_fqn)

        # Verify appended data is immediately visible
        result2 = spark.table(self.table_fqn)
        count = result2.count()
        assert count == 1, (
            f"Table should have 1 row after append, got {count}. "
            "This verifies fix for issue #114."
        )

        # Append more data
        data2 = [{"id": 2, "name": "test2"}]
        df2 = spark.createDataFrame(data2, schema)
        df2.write.format("parquet").mode("append").saveAsTable(self.table_fqn)

        # Verify all data is visible
        result3 = spark.table(self.table_fqn)
        count3 = result3.count()
        assert count3 == 2, (
            f"Table should have 2 rows after second append, got {count3}"
        )
        rows = result3.collect()
        assert {row["id"] for row in rows} == {1, 2}

    def test_parquet_format_append_to_new_table(self, spark):
        """Test that appending to a non-existent parquet format table creates it correctly."""
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
            ]
        )

        # Append to non-existent table with parquet format (should create it)
        data1 = [{"id": 1, "name": "initial"}]
        df1 = spark.createDataFrame(data1, schema)
        df1.write.format("parquet").mode("append").saveAsTable(self.table_fqn)

        # Verify data is immediately visible
        result = spark.table(self.table_fqn)
        count = result.count()
        assert count == 1, f"New table created by append should have 1 row, got {count}"
        assert result.collect()[0]["id"] == 1

        # Append more data
        data2 = [{"id": 2, "name": "appended"}]
        df2 = spark.createDataFrame(data2, schema)
        df2.write.format("parquet").mode("append").saveAsTable(self.table_fqn)

        # Verify all data is visible
        result_after_append = spark.table(self.table_fqn)
        count_after_append = result_after_append.count()
        assert count_after_append == 2, (
            f"Table should have 2 rows after append, got {count_after_append}"
        )
        rows = result_after_append.collect()
        assert {row["id"] for row in rows} == {1, 2}

    def test_parquet_format_multiple_append_operations(self, spark):
        """Test that multiple parquet format append operations preserve all data."""
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("value", StringType(), True),
            ]
        )

        # Create initial table with parquet format
        data1 = [{"id": 1, "value": "a"}]
        df1 = spark.createDataFrame(data1, schema)
        df1.write.format("parquet").mode("overwrite").saveAsTable(self.table_fqn)

        # Perform multiple append operations with parquet format
        for i in range(2, 6):
            data = [{"id": i, "value": chr(ord("a") + i - 1)}]
            df = spark.createDataFrame(data, schema)
            df.write.format("parquet").mode("append").saveAsTable(self.table_fqn)

            # Verify data is visible after each append
            result = spark.table(self.table_fqn)
            assert result.count() == i, (
                f"After {i - 1} appends, table should have {i} rows, got {result.count()}"
            )

        # Final verification
        result = spark.table(self.table_fqn)
        assert result.count() == 5, "Final table should have 5 rows"
        rows = result.collect()
        ids = {row["id"] for row in rows}
        assert ids == {1, 2, 3, 4, 5}, "All rows should be present"
