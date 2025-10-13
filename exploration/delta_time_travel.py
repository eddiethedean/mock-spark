"""
Explore how real PySpark handles Delta Lake time travel and versioning.

Run with: python exploration/delta_time_travel.py > exploration/outputs/delta_time_travel_output.txt 2>&1
"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import tempfile
import shutil
import time

# Create temp directory for Delta tables
temp_dir = tempfile.mkdtemp(prefix="delta_time_travel_")
print(f"Using temp directory: {temp_dir}\n")

try:
    # Create Spark session with Delta Lake support
    spark = (
        SparkSession.builder.appName("DeltaTimeTravelExploration")
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
    print("TEST 1: Basic Version Tracking")
    print("=" * 80)

    # Version 0 - initial write
    df_v0 = spark.createDataFrame([{"id": 1, "value": "v0"}])
    df_v0.write.format("delta").mode("overwrite").saveAsTable("test.versioned")
    print("\nVersion 0 written")
    spark.table("test.versioned").show()

    time.sleep(0.1)  # Small delay between versions

    # Version 1 - overwrite
    df_v1 = spark.createDataFrame([{"id": 1, "value": "v1"}])
    df_v1.write.format("delta").mode("overwrite").saveAsTable("test.versioned")
    print("\nVersion 1 written")
    spark.table("test.versioned").show()

    time.sleep(0.1)

    # Version 2 - append
    df_v2 = spark.createDataFrame([{"id": 2, "value": "v2"}])
    df_v2.write.format("delta").mode("append").saveAsTable("test.versioned")
    print("\nVersion 2 written")
    spark.table("test.versioned").orderBy("id").show()

    # Check current version (latest)
    print("\n" + "-" * 80)
    print("Current version (latest):")
    current = spark.table("test.versioned").orderBy("id")
    current.show()
    print(f"Count: {current.count()}")

    print("\n" + "=" * 80)
    print("TEST 2: DESCRIBE HISTORY")
    print("=" * 80)

    history = spark.sql("DESCRIBE HISTORY test.versioned")
    print("\nHistory:")
    history.show(truncate=False)

    print("\nHistory schema:")
    history.printSchema()

    print(f"\nNumber of versions: {history.count()}")

    # Get version details
    versions = history.select("version", "operation", "operationParameters").collect()
    for v in versions:
        print(f"  Version {v['version']}: {v['operation']}")

    print("\n" + "=" * 80)
    print("TEST 3: Time Travel - versionAsOf")
    print("=" * 80)

    # Read version 0
    print("\nReading version 0:")
    v0 = spark.read.format("delta").option("versionAsOf", "0").table("test.versioned")
    v0.show()
    print(f"Count: {v0.count()}")
    row_v0 = v0.collect()[0]
    print(f"Value at v0: {row_v0['value']}")

    # Read version 1
    print("\nReading version 1:")
    v1 = spark.read.format("delta").option("versionAsOf", "1").table("test.versioned")
    v1.show()
    print(f"Count: {v1.count()}")

    # Read version 2 (current)
    print("\nReading version 2 (current):")
    v2 = spark.read.format("delta").option("versionAsOf", "2").table("test.versioned")
    v2.orderBy("id").show()
    print(f"Count: {v2.count()}")

    print("\n" + "=" * 80)
    print("TEST 4: Time Travel - Using @ syntax")
    print("=" * 80)

    try:
        # Try @ syntax if supported
        print("\nTrying @ syntax for version 0:")
        v0_at = spark.table("test.versioned@v0")
        v0_at.show()
    except Exception as e:
        print(f"@ syntax not supported: {type(e).__name__}")

    print("\n" + "=" * 80)
    print("TEST 5: Invalid Version Access")
    print("=" * 80)

    try:
        print("\nAttempting to read version 999 (doesn't exist):")
        bad_version = (
            spark.read.format("delta").option("versionAsOf", "999").table("test.versioned")
        )
        bad_version.show()
    except Exception as e:
        print(f"✓ Expected exception: {type(e).__name__}")
        print(f"  Message: {str(e)[:200]}")

    print("\n" + "=" * 80)
    print("SUMMARY: Key Findings")
    print("=" * 80)
    print("1. Each write operation creates a new version (0, 1, 2, ...)")
    print("2. DESCRIBE HISTORY shows all versions with metadata")
    print("3. versionAsOf option reads specific version")
    print("4. History includes: version, timestamp, operation, operationParameters")
    print("5. Can time travel to any previous version")
    print("6. Invalid version raises exception")

finally:
    spark.stop()
    shutil.rmtree(temp_dir, ignore_errors=True)
    print(f"\nCleaned up temp directory: {temp_dir}")
