#!/usr/bin/env python3
"""Debug schema inference for when/otherwise."""

from mock_spark import MockSparkSession, functions as F

print("Testing schema inference...")

try:
    spark = MockSparkSession.builder.appName('test').getOrCreate()
    df = spark.createDataFrame([(1,), (2,), (3,)], ['id'])
    
    print("Original DataFrame schema:", df.schema)
    print("Original DataFrame data:", df.collect())
    
    # Test the condition separately
    condition = F.col('id') > 2
    print("Condition type:", type(condition))
    print("Condition operation:", getattr(condition, 'operation', 'N/A'))
    print("Condition column:", getattr(condition, 'column', 'N/A'))
    print("Condition value:", getattr(condition, 'value', 'N/A'))
    
    # Test the when/otherwise
    when_expr = F.when(F.col('id') > 2, F.lit(True)).otherwise(F.lit(False))
    print("When expression type:", type(when_expr))
    print("When expression conditions:", getattr(when_expr, 'conditions', 'N/A'))
    print("When expression default_value:", getattr(when_expr, 'default_value', 'N/A'))
    
except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()
