"""
Explore how real PySpark handles datetime transformation functions.

Run with: python exploration/datetime_functions.py > exploration/outputs/datetime_functions_output.txt 2>&1
"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import tempfile
import shutil

# Create temp directory
temp_dir = tempfile.mkdtemp(prefix="datetime_explore_")
print(f"Using temp directory: {temp_dir}\n")

try:
    # Create Spark session
    spark = (
        SparkSession.builder.appName("DateTimeFunctionsExploration")
        .config("spark.sql.warehouse.dir", f"{temp_dir}/spark-warehouse")
        .master("local[*]")
        .getOrCreate()
    )

    print("=" * 80)
    print("TEST 1: to_date() function")
    print("=" * 80)

    # Create data with timestamp strings
    data = [
        ("2024-01-01 10:30:00",),
        ("2024-01-01 14:45:00",),
        ("2024-01-02 09:15:00",),
    ]
    df = spark.createDataFrame(data, ["timestamp_str"])

    print("\nOriginal data:")
    df.show()

    # Apply to_date
    df_with_date = df.withColumn("event_date", F.to_date("timestamp_str"))
    print("\nAfter to_date():")
    df_with_date.show()
    df_with_date.printSchema()

    # Check distinct dates
    print("\nDistinct dates:")
    df_with_date.select("event_date").distinct().orderBy("event_date").show()
    print(f"Distinct count: {df_with_date.select('event_date').distinct().count()}")

    print("\n" + "=" * 80)
    print("TEST 2: hour() function")
    print("=" * 80)

    df_with_hour = df.withColumn("hour", F.hour("timestamp_str"))
    print("\nAfter hour():")
    df_with_hour.show()
    df_with_hour.printSchema()

    # Get hour values
    hours = [row["hour"] for row in df_with_hour.collect()]
    print(f"\nExtracted hours: {hours}")

    print("\n" + "=" * 80)
    print("TEST 3: Multiple datetime functions together")
    print("=" * 80)

    df_full = (
        df.withColumn("event_date", F.to_date("timestamp_str"))
        .withColumn("hour", F.hour("timestamp_str"))
        .withColumn("minute", F.minute("timestamp_str"))
        .withColumn("second", F.second("timestamp_str"))
        .withColumn("year", F.year("timestamp_str"))
        .withColumn("month", F.month("timestamp_str"))
        .withColumn("day", F.dayofmonth("timestamp_str"))
    )

    print("\nAll datetime extractions:")
    df_full.show()
    df_full.printSchema()

    print("\n" + "=" * 80)
    print("TEST 4: DateTime functions with groupBy")
    print("=" * 80)

    # Extended data
    data2 = [
        ("user1", "click", "2024-01-01 10:30:00"),
        ("user1", "view", "2024-01-01 14:45:00"),
        ("user2", "click", "2024-01-02 09:15:00"),
        ("user1", "view", "2024-01-02 11:00:00"),
    ]
    df2 = spark.createDataFrame(data2, ["user_id", "action", "timestamp"])

    print("\nOriginal data:")
    df2.show()

    # Apply transformations and groupBy
    result = (
        df2.withColumn("event_date", F.to_date("timestamp"))
        .withColumn("hour", F.hour("timestamp"))
        .groupBy("user_id", "event_date")
        .agg(F.count("action").alias("event_count"))
        .orderBy("user_id", "event_date")
    )

    print("\nAfter groupBy with datetime functions:")
    result.show()
    result.printSchema()

    rows = result.collect()
    print(f"\nNumber of groups: {len(rows)}")
    for row in rows:
        print(f"  {row['user_id']} on {row['event_date']}: {row['event_count']} events")

    print("\n" + "=" * 80)
    print("TEST 5: to_date with different formats")
    print("=" * 80)

    data3 = [
        ("2024-01-15",),  # Date only
        ("2024-01-15 10:30:45",),  # Full timestamp
        ("2024/01/16",),  # Different separator
    ]
    df3 = spark.createDataFrame(data3, ["date_str"])

    print("\nInput strings:")
    df3.show()

    # Default to_date
    df3_default = df3.withColumn("parsed_date", F.to_date("date_str"))
    print("\nWith default to_date():")
    df3_default.show()

    # With format specification
    df3_format = df3.withColumn("parsed_date", F.to_date("date_str", "yyyy/MM/dd"))
    print("\nWith format 'yyyy/MM/dd':")
    df3_format.show()

    print("\n" + "=" * 80)
    print("TEST 6: hour() on timestamp column (not string)")
    print("=" * 80)

    from pyspark.sql.types import TimestampType
    import datetime

    data4 = [
        (datetime.datetime(2024, 1, 1, 10, 30, 0),),
        (datetime.datetime(2024, 1, 1, 14, 45, 30),),
    ]
    df4 = spark.createDataFrame(data4, ["timestamp"])

    print("\nTimestamp column (actual TimestampType):")
    df4.show()
    df4.printSchema()

    df4_hour = df4.withColumn("hour", F.hour("timestamp"))
    print("\nAfter hour() on TimestampType:")
    df4_hour.show()

    print("\n" + "=" * 80)
    print("SUMMARY: Key Findings")
    print("=" * 80)
    print("1. to_date() converts timestamp strings to date type")
    print("2. hour(), minute(), second() extract time components")
    print("3. year(), month(), day() extract date components")
    print("4. Datetime functions work in withColumn chains")
    print("5. Can be used before groupBy/agg")
    print("6. to_date() accepts optional format parameter")
    print("7. Functions work on both string and timestamp columns")

finally:
    spark.stop()
    shutil.rmtree(temp_dir, ignore_errors=True)
    print(f"\nCleaned up temp directory: {temp_dir}")
