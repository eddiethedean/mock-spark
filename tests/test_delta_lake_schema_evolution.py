"""
Comprehensive tests for Delta Lake schema evolution in mock-spark.

These tests validate that mock-spark behaves like PySpark for schema evolution,
specifically for Delta Lake features like overwriteSchema and schema merging.

Note: These tests are primarily for mock-spark's schema evolution features.
When running with PySpark, some tests may be skipped or need special handling
due to differences in table management and schema evolution behavior.
"""

from tests.fixtures.spark_backend import get_backend_type, BackendType

# Import appropriate types and functions based on backend
_backend = get_backend_type()
if _backend == BackendType.PYSPARK:
    try:
        from pyspark.sql import functions as F
        from pyspark.sql.types import StringType, IntegerType, DoubleType
    except ImportError:
        from mock_spark import functions as F
        from mock_spark import StringType, IntegerType, DoubleType
else:
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
        import uuid

        # Use unique table name to avoid conflicts when running with PySpark
        table_suffix = str(uuid.uuid4()).replace("-", "_")[:8]
        schema_name = f"test_schema_{table_suffix}"
        table_name = f"{schema_name}.test_table"

        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

        df = spark.createDataFrame([(1, "test")], ["id", "name"])

        # Write table
        df.write.mode("overwrite").saveAsTable(table_name)

        # Should be immediately accessible (no delay, no retry needed)
        table = spark.table(table_name)
        assert table is not None
        assert table.count() == 1
        assert "id" in table.columns
        assert "name" in table.columns

        # Cleanup
        try:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            spark.sql(f"DROP SCHEMA IF EXISTS {schema_name}")
        except Exception:
            pass  # Ignore cleanup errors

    def test_basic_schema_evolution(self, spark):
        """Test basic schema evolution: add new columns."""
        import uuid

        # Use unique table name to avoid conflicts when running with PySpark
        table_suffix = str(uuid.uuid4()).replace("-", "_")[:8]
        schema_name = f"test_schema_{table_suffix}"
        table_name = f"{schema_name}.users"

        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

        # Initial table
        df1 = spark.createDataFrame([(1, "Alice")], ["id", "name"])
        df1.write.mode("overwrite").saveAsTable(table_name)

        # Add new column with overwriteSchema
        df2 = spark.createDataFrame([(1, "Alice", 25)], ["id", "name", "age"])
        df2.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
            table_name
        )

        result = spark.table(table_name)
        assert set(result.columns) == {"id", "name", "age"}

        # Cleanup
        try:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            spark.sql(f"DROP SCHEMA IF EXISTS {schema_name}")
        except Exception:
            pass  # Ignore cleanup errors

    def test_overwrite_schema_option(self, spark):
        """Test that overwriteSchema option preserves existing columns."""
        import uuid

        # Use unique table name to avoid conflicts when running with PySpark
        table_suffix = str(uuid.uuid4()).replace("-", "_")[:8]
        schema_name = f"test_schema_{table_suffix}"
        table_name = f"{schema_name}.users"

        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

        # Create initial table
        df1 = spark.createDataFrame([(1, "Alice")], ["id", "name"])
        df1.write.mode("overwrite").saveAsTable(table_name)

        # Verify initial schema
        table1 = spark.table(table_name)
        assert set(table1.columns) == {"id", "name"}

        # Add new column with overwriteSchema
        df2 = spark.createDataFrame([(1, "Alice", 25)], ["id", "name", "age"])
        df2.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
            table_name
        )

        # Verify schema evolution: existing columns preserved, new column added
        table2 = spark.table(table_name)
        assert "id" in table2.columns
        assert "name" in table2.columns
        assert "age" in table2.columns
        assert len(table2.columns) == 3

        # Cleanup
        try:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            spark.sql(f"DROP SCHEMA IF EXISTS {schema_name}")
        except Exception:
            pass  # Ignore cleanup errors

    def test_preserve_existing_columns(self, spark):
        """Test that overwriteSchema completely overwrites the schema (PySpark behavior).

        Note: In PySpark, overwriteSchema=true means completely overwrite the schema,
        NOT merge/preserve existing columns. This test verifies that behavior.
        """
        import uuid

        # Use unique table name to avoid conflicts when running with PySpark
        table_suffix = str(uuid.uuid4()).replace("-", "_")[:8]
        schema_name = f"test_schema_{table_suffix}"
        table_name = f"{schema_name}.data"

        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

        # Initial: [id, name, value]
        df1 = spark.createDataFrame([(1, "Alice", 100)], ["id", "name", "value"])
        df1.write.mode("overwrite").saveAsTable(table_name)

        # Overwrite with: [id, age] (missing name and value)
        df2 = spark.createDataFrame([(1, 25)], ["id", "age"])
        df2.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
            table_name
        )

        result = spark.table(table_name)
        assert "id" in result.columns
        assert "age" in result.columns  # New column
        # PySpark behavior: overwriteSchema completely overwrites, doesn't preserve
        assert "name" not in result.columns  # Should NOT be preserved
        assert "value" not in result.columns  # Should NOT be preserved

        # Cleanup
        try:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            spark.sql(f"DROP SCHEMA IF EXISTS {schema_name}")
        except Exception:
            pass  # Ignore cleanup errors

    def test_schema_merge_on_overwrite(self, spark):
        """Test that overwriteSchema completely overwrites the schema (PySpark behavior).

        Note: In PySpark, overwriteSchema=true means completely overwrite the schema,
        NOT merge/preserve existing columns. This test verifies that behavior.
        """
        import uuid

        # Use unique table name to avoid conflicts when running with PySpark
        table_suffix = str(uuid.uuid4()).replace("-", "_")[:8]
        schema_name = f"test_schema_{table_suffix}"
        table_name = f"{schema_name}.users"

        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

        # Initial table with columns: [id, name, value]
        df1 = spark.createDataFrame(
            [(1, "Alice", 100), (2, "Bob", 200)], ["id", "name", "value"]
        )
        df1.write.mode("overwrite").saveAsTable(table_name)

        # Overwrite with new columns but missing some existing
        # New DataFrame has: [id, age, city] (missing name and value)
        df2 = spark.createDataFrame(
            [(1, 25, "NYC"), (2, 30, "LA")], ["id", "age", "city"]
        )

        # With overwriteSchema=true, PySpark completely overwrites the schema
        df2.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
            table_name
        )

        # Verify: PySpark behavior is to completely overwrite, not merge
        result = spark.table(table_name)
        assert "id" in result.columns
        assert "age" in result.columns  # New column
        assert "city" in result.columns  # New column
        # PySpark behavior: overwriteSchema completely overwrites, doesn't preserve
        assert "name" not in result.columns  # Should NOT be preserved
        assert "value" not in result.columns  # Should NOT be preserved

        # Cleanup
        try:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            spark.sql(f"DROP SCHEMA IF EXISTS {schema_name}")
        except Exception:
            pass  # Ignore cleanup errors
