# Configuration

Mock-Spark configuration is managed via MockSparkSession constructor options.

```python
from mock_spark import MockSparkSession

spark = MockSparkSession(
    validation_mode="relaxed",           # strict | relaxed | minimal
    enable_type_coercion=True,
)
```

Key settings:
- validation_mode: controls strictness of schema/data checks
- enable_type_coercion: attempts to coerce types during DataFrame creation
