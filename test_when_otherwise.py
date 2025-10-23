#!/usr/bin/env python3
"""Test script to verify when/otherwise boolean literal behavior."""

from mock_spark import MockSparkSession, functions as F

print("Testing Mock-Spark when/otherwise with boolean literals...")

try:
    spark = MockSparkSession.builder.appName("test").getOrCreate()
    df = spark.createDataFrame([(1,), (2,), (3,)], ["id"])

    result = df.select(
        F.when(F.col("id") > 2, F.lit(True)).otherwise(F.lit(False)).alias("result")
    )

    print("Schema:", result.schema)
    rows = result.collect()
    print("Rows:", rows)
    for row in rows:
        val = row.result
        print(f"  Value: {val}, Type: {type(val)}, Repr: {repr(val)}")

    print("SUCCESS: No crash occurred!")

except Exception as e:
    print(f"ERROR: {e}")
    import traceback

    traceback.print_exc()
