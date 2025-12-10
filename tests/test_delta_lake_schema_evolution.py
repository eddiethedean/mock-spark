"""
Comprehensive tests for Delta Lake schema evolution in mock-spark.

These tests validate that mock-spark behaves like PySpark for schema evolution,
specifically for Delta Lake features like overwriteSchema and schema merging.
"""

from mock_spark import functions as F
from mock_spark import StringType, IntegerType, DoubleType


class TestDeltaLakeSchemaEvolution:
    """Test Delta Lake schema evolution features."""

    def test_lit_none_works(self, spark):
        """Test that F.lit(None) works without JVM error."""
        df = spark.createDataFrame([(1, "test")], ["id", "name"])

        # This should work without errors - no JVM dependency
        result = df.withColumn("null_col", F.lit(None))
        assert "null_col" in result.columns

        # Should also work with casting
        result2 = df.withColumn("null_str", F.lit(None).cast(StringType()))
        assert "null_str" in result2.columns

        # Verify the column exists and can be selected
        rows = result2.select("null_str").collect()
        assert len(rows) == 1
        assert rows[0]["null_str"] is None

    def test_type_casting_works(self, spark):
        """Test that type casting works without JVM."""
        df = spark.createDataFrame([(1, "test")], ["id", "name"])

        # Test casting None to different types
        df1 = df.withColumn("null_str", F.lit(None).cast(StringType()))
        df2 = df.withColumn("null_int", F.lit(None).cast(IntegerType()))
        df3 = df.withColumn("null_double", F.lit(None).cast(DoubleType()))

        assert "null_str" in df1.columns
        assert "null_int" in df2.columns
        assert "null_double" in df3.columns

        # Test casting actual values
        df4 = df.withColumn("id_str", F.col("id").cast(StringType()))
        assert df4.select("id_str").collect()[0]["id_str"] == "1"

    def test_immediate_table_access(self, spark):
        """Test that table is immediately accessible after saveAsTable."""
        spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")

        df = spark.createDataFrame([(1, "test")], ["id", "name"])

        # Write table
        df.write.mode("overwrite").saveAsTable("test_schema.test_table")

        # Should be immediately accessible (no delay, no retry needed)
        table = spark.table("test_schema.test_table")
        assert table is not None
        assert table.count() == 1
        assert "id" in table.columns
        assert "name" in table.columns

    def test_basic_schema_evolution(self, spark):
        """Test basic schema evolution: add new columns."""
        spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")

        # Initial table
        df1 = spark.createDataFrame([(1, "Alice")], ["id", "name"])
        df1.write.mode("overwrite").saveAsTable("test_schema.users")

        # Add new column with overwriteSchema
        df2 = spark.createDataFrame([(1, "Alice", 25)], ["id", "name", "age"])
        df2.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
            "test_schema.users"
        )

        result = spark.table("test_schema.users")
        assert set(result.columns) == {"id", "name", "age"}

    def test_overwrite_schema_option(self, spark):
        """Test that overwriteSchema option preserves existing columns."""
        spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")

        # Create initial table
        df1 = spark.createDataFrame([(1, "Alice")], ["id", "name"])
        df1.write.mode("overwrite").saveAsTable("test_schema.users")

        # Verify initial schema
        table1 = spark.table("test_schema.users")
        assert set(table1.columns) == {"id", "name"}

        # Add new column with overwriteSchema
        df2 = spark.createDataFrame([(1, "Alice", 25)], ["id", "name", "age"])
        df2.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
            "test_schema.users"
        )

        # Verify schema evolution: existing columns preserved, new column added
        table2 = spark.table("test_schema.users")
        assert "id" in table2.columns
        assert "name" in table2.columns
        assert "age" in table2.columns
        assert len(table2.columns) == 3

    def test_preserve_existing_columns(self, spark):
        """Test that existing columns are preserved when adding new ones."""
        spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")

        # Initial: [id, name, value]
        df1 = spark.createDataFrame([(1, "Alice", 100)], ["id", "name", "value"])
        df1.write.mode("overwrite").saveAsTable("test_schema.data")

        # Overwrite with: [id, age] (missing name and value)
        df2 = spark.createDataFrame([(1, 25)], ["id", "age"])
        df2.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
            "test_schema.data"
        )

        result = spark.table("test_schema.data")
        assert "id" in result.columns
        assert "name" in result.columns  # Should be preserved
        assert "value" in result.columns  # Should be preserved
        assert "age" in result.columns  # New column

    def test_schema_merge_on_overwrite(self, spark):
        """Test that overwrite preserves existing columns when overwriteSchema=true."""
        spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")

        # Initial table with columns: [id, name, value]
        df1 = spark.createDataFrame(
            [(1, "Alice", 100), (2, "Bob", 200)], ["id", "name", "value"]
        )
        df1.write.mode("overwrite").saveAsTable("test_schema.users")

        # Overwrite with new columns but missing some existing
        # New DataFrame has: [id, age, city] (missing name and value)
        df2 = spark.createDataFrame(
            [(1, 25, "NYC"), (2, 30, "LA")], ["id", "age", "city"]
        )

        # With overwriteSchema=true, should preserve name and value (with nulls)
        df2.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
            "test_schema.users"
        )

        # Verify all columns exist
        result = spark.table("test_schema.users")
        assert "id" in result.columns
        assert "name" in result.columns  # Preserved from original
        assert "value" in result.columns  # Preserved from original
        assert "age" in result.columns  # New column
        assert "city" in result.columns  # New column

        # Verify null values for missing columns
        rows = result.collect()
        assert rows[0]["name"] is None  # Should be null (wasn't in new DataFrame)
        assert rows[0]["value"] is None  # Should be null
        assert rows[0]["age"] == 25  # Should have value from new DataFrame
        assert rows[0]["city"] == "NYC"  # Should have value from new DataFrame
