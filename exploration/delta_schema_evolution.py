"""
Explore how real PySpark handles Delta Lake schema evolution.

Run with: python exploration/delta_schema_evolution.py > exploration/outputs/delta_schema_evolution_output.txt 2>&1
"""

from pyspark.sql import SparkSession
import tempfile
import shutil

# Create temp directory for Delta tables
temp_dir = tempfile.mkdtemp(prefix="delta_schema_ev_")
print(f"Using temp directory: {temp_dir}\n")

try:
    # Create Spark session with Delta Lake support
    spark = (
        SparkSession.builder.appName("DeltaSchemaEvolutionExploration")
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
    print("TEST 1: Schema Evolution with mergeSchema=true")
    print("=" * 80)

    # Initial write with 2 columns
    df1 = spark.createDataFrame([{"id": 1, "name": "Alice"}])
    print("\nInitial data (2 columns):")
    df1.show()
    df1.printSchema()

    df1.write.format("delta").mode("overwrite").saveAsTable("test.users")

    # Read to verify
    print("\nAfter initial write:")
    result1 = spark.table("test.users")
    result1.show()
    print(f"Columns: {result1.columns}")
    print(f"Schema: {result1.schema}")

    # Append with new column WITHOUT mergeSchema (should fail)
    print("\n" + "-" * 80)
    print("Attempting append with new column WITHOUT mergeSchema...")
    print("-" * 80)
    df2 = spark.createDataFrame([{"id": 2, "name": "Bob", "age": 30}])
    print("\nNew data (3 columns):")
    df2.show()
    df2.printSchema()

    try:
        df2.write.format("delta").mode("append").saveAsTable("test.users")
        print("ERROR: Should have raised exception!")
    except Exception as e:
        print(f"✓ Expected exception: {type(e).__name__}")
        print(f"  Message: {str(e)[:300]}")

    # Append with new column WITH mergeSchema=true
    print("\n" + "-" * 80)
    print("Attempting append with new column WITH mergeSchema=true...")
    print("-" * 80)

    df2.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("test.users")

    print("\nAfter schema evolution:")
    result2 = spark.table("test.users")
    result2.show()
    print(f"Columns: {result2.columns}")
    print(f"Schema: {result2.schema}")
    print(f"Count: {result2.count()}")

    # Check that old rows have null for new column
    print("\nFirst row (should have null age):")
    first_row = result2.filter("id = 1").collect()[0]
    print(f"  id: {first_row['id']}")
    print(f"  name: {first_row['name']}")
    print(f"  age: {first_row['age']}")
    print(f"  age is None: {first_row['age'] is None}")

    print("\nSecond row (should have age=30):")
    second_row = result2.filter("id = 2").collect()[0]
    print(f"  id: {second_row['id']}")
    print(f"  name: {second_row['name']}")
    print(f"  age: {second_row['age']}")

    print("\n" + "=" * 80)
    print("TEST 2: Multiple Schema Evolution Steps")
    print("=" * 80)

    # Start with 1 column
    df_v1 = spark.createDataFrame([{"a": 1}])
    df_v1.write.format("delta").mode("overwrite").saveAsTable("test.evolve")
    print("\nVersion 1 (column: a):")
    spark.table("test.evolve").show()
    print(f"Columns: {spark.table('test.evolve').columns}")

    # Add column b
    df_v2 = spark.createDataFrame([{"a": 2, "b": "two"}])
    df_v2.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
        "test.evolve"
    )
    print("\nVersion 2 (columns: a, b):")
    spark.table("test.evolve").show()
    print(f"Columns: {spark.table('test.evolve').columns}")

    # Add column c
    df_v3 = spark.createDataFrame([{"a": 3, "b": "three", "c": 3.0}])
    df_v3.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
        "test.evolve"
    )
    print("\nVersion 3 (columns: a, b, c):")
    result = spark.table("test.evolve")
    result.show()
    print(f"Columns: {result.columns}")
    print(f"Count: {result.count()}")

    # Verify nulls propagated correctly
    print("\nAll rows with null checking:")
    result.select("a", "b", "c").orderBy("a").show()

    print("\n" + "=" * 80)
    print("TEST 3: mergeSchema with Different Column Types")
    print("=" * 80)

    # Integer column
    df_int = spark.createDataFrame([{"id": 1, "value": 100}])
    df_int.write.format("delta").mode("overwrite").saveAsTable("test.type_test")
    print("\nInitial schema:")
    spark.table("test.type_test").printSchema()

    # Add string column
    df_str = spark.createDataFrame([{"id": 2, "value": 200, "description": "two hundred"}])
    df_str.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
        "test.type_test"
    )
    print("\nAfter adding string column:")
    spark.table("test.type_test").printSchema()
    spark.table("test.type_test").show()

    print("\n" + "=" * 80)
    print("SUMMARY: Key Findings")
    print("=" * 80)
    print("1. mergeSchema=true required to add new columns during append")
    print("2. Without mergeSchema, append with new columns raises AnalysisException")
    print("3. Old rows get null values for newly added columns")
    print("4. Schema evolution works incrementally (can add multiple columns over time)")
    print("5. Column types are preserved correctly")

finally:
    spark.stop()
    shutil.rmtree(temp_dir, ignore_errors=True)
    print(f"\nCleaned up temp directory: {temp_dir}")
