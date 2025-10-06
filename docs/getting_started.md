# Getting Started

Install:

```bash
pip install mock-spark
```

Quickstart:

```python
from mock_spark import MockSparkSession, functions as F, Window

spark = MockSparkSession()

df = spark.createDataFrame([
    {"id": 1, "name": "Alice"},
    {"id": 2, "name": "Bob"},
])

result = df.filter(F.col("id") > 1).select("name").collect()
print([r["name"] for r in result])  # ['Bob']
```
