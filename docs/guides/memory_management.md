# Memory Management

Track and clear session memory for large test suites.

```python
from mock_spark import MockSparkSession

spark = MockSparkSession()

# after creating multiple DataFrames
print(spark.get_memory_usage())

spark.clear_cache()  # frees memory tracked by the session
print(spark.get_memory_usage())  # 0
```
