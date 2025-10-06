# Benchmarking

Use the session benchmarking API to time operations and inspect results.

```python
from mock_spark import MockSparkSession, functions as F

spark = MockSparkSession()

df = spark.createDataFrame([{ "x": i } for i in range(100)])

result_df = spark.benchmark_operation(
    "filter_gt_50",
    lambda: df.filter(F.col("x") > 50)
)

stats = spark.get_benchmark_results()["filter_gt_50"]
print(stats["duration_s"], stats["memory_used_bytes"], stats["result_size"])  # e.g. 0.001 0 49
```
