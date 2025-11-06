# PySpark → Mock-Spark Migration Guide

## Quick Swap

```python
# from pyspark.sql import SparkSession
from mock_spark.sql import SparkSession as SparkSession, functions as F, Window
```

## Common Patterns

- Keep DataFrame APIs unchanged (select, filter, withColumn, groupBy)
- Replace `Window` with `mock_spark.Window` if imported directly
- Exceptions: use `AnalysisException` semantics for missing columns

## Tips

- Use `validation_mode="strict"` to catch schema mismatches
- Use session `benchmark_operation` for performance checks
- Use `clear_cache()` between tests to control memory
