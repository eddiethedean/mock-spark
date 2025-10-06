# Pytest Integration

Built-in patterns for using Mock-Spark in pytest suites.

```python
# conftest.py
import pytest
from mock_spark import MockSparkSession

@pytest.fixture
def spark():
    return MockSparkSession(validation_mode="relaxed")
```

```python
# example test
from mock_spark import functions as F

def test_basic(spark):
    df = spark.createDataFrame([{"x": 1}, {"x": 2}])
    assert df.filter(F.col("x") > 1).count() == 1
```

