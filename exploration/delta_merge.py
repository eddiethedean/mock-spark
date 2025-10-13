"""
Explore how real PySpark handles Delta Lake MERGE INTO operations.

Run with: python exploration/delta_merge.py > exploration/outputs/delta_merge_output.txt 2>&1
"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import tempfile
import shutil

# Create temp directory for Delta tables
temp_dir = tempfile.mkdtemp(prefix="delta_merge_")
print(f"Using temp directory: {temp_dir}\n")

try:
    # Create Spark session with Delta Lake support
    spark = (
        SparkSession.builder.appName("DeltaMergeExploration")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.0.2")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", f"{temp_dir}/spark-warehouse")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sql("CREATE SCHEMA IF NOT EXISTS test")

    print("=" * 80)
    print("TEST 1: Basic MERGE - UPDATE matched, INSERT not matched")
    print("=" * 80)

    # Create target table
    target_data = [{"id": 1, "name": "Alice", "score": 100}, {"id": 2, "name": "Bob", "score": 200}]
    target_df = spark.createDataFrame(target_data)
    target_df.write.format("delta").mode("overwrite").saveAsTable("test.target")

    print("\nTarget table (before MERGE):")
    spark.table("test.target").orderBy("id").show()

    # Create source table
    source_data = [
        {"id": 1, "name": "Alice Updated", "score": 150},  # Match - will update
        {"id": 3, "name": "Charlie", "score": 300},  # No match - will insert
    ]
    source_df = spark.createDataFrame(source_data)
    source_df.createOrReplaceTempView("source")

    print("\nSource table:")
    spark.sql("SELECT * FROM source").show()

    # Execute MERGE
    print("\nExecuting MERGE INTO...")
    merge_sql = """
        MERGE INTO test.target AS t
        USING source AS s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET t.name = s.name, t.score = s.score
        WHEN NOT MATCHED THEN INSERT (id, name, score) VALUES (s.id, s.name, s.score)
    """
    spark.sql(merge_sql)

    print("\nTarget table (after MERGE):")
    result = spark.table("test.target").orderBy("id")
    result.show()
    print(f"Count: {result.count()}")

    # Verify specific rows
    alice = result.filter("id = 1").collect()[0]
    print(f"\nAlice updated: name='{alice['name']}', score={alice['score']}")

    charlie = result.filter("id = 3").collect()
    print(f"Charlie inserted: {len(charlie) > 0}")
    if charlie:
        print(f"  name='{charlie[0]['name']}', score={charlie[0]['score']}")

    print("\n" + "=" * 80)
    print("TEST 2: MERGE with MATCHED only (no inserts)")
    print("=" * 80)

    target2 = spark.createDataFrame(
        [{"id": 1, "value": 10}, {"id": 2, "value": 20}, {"id": 3, "value": 30}]
    )
    target2.write.format("delta").mode("overwrite").saveAsTable("test.target2")

    print("\nBefore MERGE:")
    spark.table("test.target2").orderBy("id").show()

    source2 = spark.createDataFrame(
        [
            {"id": 1, "value": 100},
            {"id": 3, "value": 300},
            {"id": 99, "value": 9900},  # Won't be inserted (no WHEN NOT MATCHED)
        ]
    )
    source2.createOrReplaceTempView("source2")

    merge_matched_only = """
        MERGE INTO test.target2 AS t
        USING source2 AS s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET t.value = s.value
    """
    spark.sql(merge_matched_only)

    print("\nAfter MERGE (matched only):")
    result2 = spark.table("test.target2").orderBy("id")
    result2.show()
    print(f"Count (should still be 3): {result2.count()}")

    print("\n" + "=" * 80)
    print("TEST 3: MERGE with NOT MATCHED only (inserts only)")
    print("=" * 80)

    target3 = spark.createDataFrame([{"id": 1, "value": 10}, {"id": 2, "value": 20}])
    target3.write.format("delta").mode("overwrite").saveAsTable("test.target3")

    print("\nBefore MERGE:")
    spark.table("test.target3").orderBy("id").show()

    source3 = spark.createDataFrame(
        [
            {"id": 1, "value": 999},  # Matches but won't update (no WHEN MATCHED)
            {"id": 3, "value": 30},  # Will insert
            {"id": 4, "value": 40},  # Will insert
        ]
    )
    source3.createOrReplaceTempView("source3")

    merge_not_matched_only = """
        MERGE INTO test.target3 AS t
        USING source3 AS s
        ON t.id = s.id
        WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value)
    """
    spark.sql(merge_not_matched_only)

    print("\nAfter MERGE (not matched only):")
    result3 = spark.table("test.target3").orderBy("id")
    result3.show()
    print(f"Count (should be 4): {result3.count()}")

    # Verify id=1 unchanged
    row1 = result3.filter("id = 1").collect()[0]
    print(f"\nid=1 value unchanged: {row1['value']} (should be 10, not 999)")

    print("\n" + "=" * 80)
    print("TEST 4: MERGE with complex conditions")
    print("=" * 80)

    target4 = spark.createDataFrame(
        [
            {"id": 1, "category": "A", "value": 10},
            {"id": 2, "category": "B", "value": 20},
            {"id": 3, "category": "A", "value": 30},
        ]
    )
    target4.write.format("delta").mode("overwrite").saveAsTable("test.target4")

    source4 = spark.createDataFrame(
        [
            {"id": 1, "category": "A", "value": 100},
            {"id": 2, "category": "B", "value": 200},
            {"id": 4, "category": "C", "value": 400},
        ]
    )
    source4.createOrReplaceTempView("source4")

    # MERGE only on category A
    merge_complex = """
        MERGE INTO test.target4 AS t
        USING source4 AS s
        ON t.id = s.id AND t.category = 'A'
        WHEN MATCHED THEN UPDATE SET t.value = s.value
        WHEN NOT MATCHED THEN INSERT (id, category, value) VALUES (s.id, s.category, s.value)
    """
    spark.sql(merge_complex)

    print("\nAfter MERGE (complex condition - only category A matched):")
    spark.table("test.target4").orderBy("id").show()

    print("\n" + "=" * 80)
    print("TEST 5: MERGE with DELETE when matched")
    print("=" * 80)

    target5 = spark.createDataFrame(
        [{"id": 1, "active": True}, {"id": 2, "active": True}, {"id": 3, "active": True}]
    )
    target5.write.format("delta").mode("overwrite").saveAsTable("test.target5")

    print("\nBefore MERGE:")
    spark.table("test.target5").orderBy("id").show()

    # Soft delete: mark inactive
    source5 = spark.createDataFrame([{"id": 2}])
    source5.createOrReplaceTempView("source5")

    try:
        # Try DELETE syntax
        merge_delete = """
            MERGE INTO test.target5 AS t
            USING source5 AS s
            ON t.id = s.id
            WHEN MATCHED THEN DELETE
        """
        spark.sql(merge_delete)

        print("\nAfter MERGE with DELETE:")
        result5 = spark.table("test.target5").orderBy("id")
        result5.show()
        print(f"Count (should be 2): {result5.count()}")
    except Exception as e:
        print(f"DELETE syntax error: {type(e).__name__}: {str(e)[:200]}")

    print("\n" + "=" * 80)
    print("SUMMARY: Key Findings")
    print("=" * 80)
    print("1. MERGE requires target to be Delta format")
    print("2. WHEN MATCHED THEN UPDATE updates matching rows")
    print("3. WHEN NOT MATCHED THEN INSERT inserts new rows")
    print("4. Can have either or both WHEN clauses")
    print("5. Condition can be complex (multiple columns, AND/OR)")
    print("6. WHEN MATCHED THEN DELETE removes matched rows")
    print("7. Order matters: MERGE is atomic operation")

finally:
    spark.stop()
    shutil.rmtree(temp_dir, ignore_errors=True)
    print(f"\nCleaned up temp directory: {temp_dir}")
