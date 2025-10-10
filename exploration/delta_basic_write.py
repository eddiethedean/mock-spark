"""
Explore how real PySpark handles basic Delta format writes.

Run with: python exploration/delta_basic_write.py > exploration/outputs/delta_basic_write_output.txt 2>&1
"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import tempfile
import shutil

# Create temp directory for Delta tables
temp_dir = tempfile.mkdtemp(prefix="delta_exploration_")
print(f"Using temp directory: {temp_dir}\n")

try:
    # Create Spark session with Delta Lake support
    # Delta Lake 2.0.2 is compatible with PySpark 3.2.4
    spark = (
        SparkSession.builder.appName("DeltaBasicWriteExploration")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.0.2")
        .config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", f"{temp_dir}/spark-warehouse")
        .master("local[*]")
        .getOrCreate()
    )

    # Create test schema first
    spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")
    print("Created test_schema\n")
    
    print("=" * 80)
    print("TEST 1: Basic Delta Write with saveAsTable")
    print("=" * 80)
    
    data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    df = spark.createDataFrame(data)
    
    print("\nOriginal DataFrame:")
    df.show()
    df.printSchema()
    
    # Write as Delta table
    print("\nWriting as Delta table...")
    df.write.format("delta").mode("overwrite").saveAsTable("test_schema.users")
    
    # Read back
    print("\nReading back from Delta table:")
    result = spark.table("test_schema.users")
    result.show()
    print(f"Count: {result.count()}")
    print(f"Schema: {result.schema}")
    
    # DESCRIBE EXTENDED
    print("\nDESCRIBE EXTENDED test_schema.users:")
    desc = spark.sql("DESCRIBE EXTENDED test_schema.users")
    desc.show(100, truncate=False)
    
    # Check if format is mentioned
    format_info = desc.filter("col_name == 'Provider'").collect()
    if format_info:
        print(f"\nProvider: {format_info[0]['data_type']}")
    
    print("\n" + "=" * 80)
    print("TEST 2: Delta Write Modes - OVERWRITE")
    print("=" * 80)
    
    # First write
    df1 = spark.createDataFrame([{"id": 1, "value": "first"}])
    df1.write.format("delta").mode("overwrite").saveAsTable("test_schema.modes_test")
    print("\nAfter first write (overwrite):")
    spark.table("test_schema.modes_test").show()
    
    # Second write - overwrite
    df2 = spark.createDataFrame([{"id": 2, "value": "second"}])
    df2.write.format("delta").mode("overwrite").saveAsTable("test_schema.modes_test")
    print("\nAfter second write (overwrite):")
    result = spark.table("test_schema.modes_test")
    result.show()
    print(f"Count after overwrite: {result.count()}")
    
    print("\n" + "=" * 80)
    print("TEST 3: Delta Write Modes - APPEND")
    print("=" * 80)
    
    df3 = spark.createDataFrame([{"id": 1, "value": "first"}])
    df3.write.format("delta").mode("overwrite").saveAsTable("test_schema.append_test")
    print("\nInitial data:")
    spark.table("test_schema.append_test").show()
    
    df4 = spark.createDataFrame([{"id": 2, "value": "second"}])
    df4.write.format("delta").mode("append").saveAsTable("test_schema.append_test")
    print("\nAfter append:")
    result = spark.table("test_schema.append_test")
    result.show()
    print(f"Count after append: {result.count()}")
    
    print("\n" + "=" * 80)
    print("TEST 4: Delta Write with save() to path")
    print("=" * 80)
    
    delta_path = f"{temp_dir}/delta_path_test"
    df5 = spark.createDataFrame([{"id": 1, "name": "Path Test"}])
    df5.write.format("delta").mode("overwrite").save(delta_path)
    print(f"\nWrote to path: {delta_path}")
    
    # Read from path
    df_read = spark.read.format("delta").load(delta_path)
    print("\nRead from path:")
    df_read.show()
    
    print("\n" + "=" * 80)
    print("TEST 5: Delta Write Modes - ERROR (should fail on existing table)")
    print("=" * 80)
    
    try:
        df6 = spark.createDataFrame([{"id": 3, "value": "third"}])
        df6.write.format("delta").mode("error").saveAsTable("test_schema.append_test")
        print("ERROR: Should have raised an exception!")
    except Exception as e:
        print(f"✓ Expected exception: {type(e).__name__}")
        print(f"  Message: {str(e)[:200]}")
    
    print("\n" + "=" * 80)
    print("TEST 6: Delta Write Modes - IGNORE")
    print("=" * 80)
    
    initial_count = spark.table("test_schema.append_test").count()
    print(f"\nInitial count: {initial_count}")
    
    df7 = spark.createDataFrame([{"id": 99, "value": "should be ignored"}])
    df7.write.format("delta").mode("ignore").saveAsTable("test_schema.append_test")
    
    final_count = spark.table("test_schema.append_test").count()
    print(f"Final count: {final_count}")
    print(f"Table unchanged: {initial_count == final_count}")
    
    print("\n" + "=" * 80)
    print("TEST 7: Table Metadata and Properties")
    print("=" * 80)
    
    # Show tables
    print("\nAll tables in test_schema:")
    spark.sql("SHOW TABLES IN test_schema").show()
    
    # Get table properties
    print("\nTable properties:")
    spark.sql("SHOW TBLPROPERTIES test_schema.users").show(truncate=False)
    
    print("\n" + "=" * 80)
    print("SUMMARY: Key Findings")
    print("=" * 80)
    print("1. format('delta').saveAsTable() works with qualified table names")
    print("2. Modes: overwrite, append, error (raises exception), ignore (silent)")
    print("3. DESCRIBE EXTENDED shows Provider = delta")
    print("4. save() to path also works with format('delta')")
    print("5. Tables are tracked in catalog/metastore")

finally:
    # Cleanup
    spark.stop()
    shutil.rmtree(temp_dir, ignore_errors=True)
    print(f"\nCleaned up temp directory: {temp_dir}")

