# Testing Plan for PySpark 3.2 Features

**Created:** 2025-01-14  
**Target Version:** mock-spark 2.5.0  
**PySpark Version:** 3.2.0  

---

## Executive Summary

This document outlines the comprehensive testing strategy for implementing PySpark 3.2 features in mock-spark. The testing plan covers unit tests, integration tests, compatibility tests, and performance tests to ensure high-quality implementation.

### Testing Objectives

1. **Functional Correctness:** All features work as expected
2. **PySpark Compatibility:** Behavior matches PySpark 3.2+
3. **Performance:** No significant performance degradation
4. **Reliability:** Robust error handling and edge cases
5. **Maintainability:** Well-documented, maintainable test suite

---

## Testing Strategy Overview

### Test Pyramid

```
                    /\
                   /  \  E2E Tests (5%)
                  /____\
                 /      \  Integration Tests (15%)
                /________\
               /          \  Unit Tests (80%)
              /____________\
```

### Test Types

1. **Unit Tests (80%)** - Individual function/class testing
2. **Integration Tests (15%)** - Feature interactions and workflows
3. **E2E Tests (5%)** - Complete user scenarios
4. **Compatibility Tests** - PySpark 3.2+ compatibility verification
5. **Performance Tests** - Performance benchmarks and regression detection

---

## Phase 1: Quick Wins Testing

### 1.1 TIMESTAMPADD Function

#### Unit Tests

**File:** `tests/unit/test_datetime_functions.py`

```python
import pytest
from mock_spark import MockSparkSession, F

class TestTimestampAdd:
    """Test TIMESTAMPADD function implementation."""
    
    def test_timestampadd_days(self):
        """Test adding days to timestamp."""
        spark = MockSparkSession("test")
        data = [{"ts": "2024-01-01 12:00:00"}]
        df = spark.createDataFrame(data)
        
        result = df.select(F.timestampadd("DAY", 7, F.col("ts"))).collect()
        assert result[0][0] == "2024-01-08 12:00:00"
    
    def test_timestampadd_months(self):
        """Test adding months to timestamp."""
        spark = MockSparkSession("test")
        data = [{"ts": "2024-01-15 12:00:00"}]
        df = spark.createDataFrame(data)
        
        result = df.select(F.timestampadd("MONTH", 1, F.col("ts"))).collect()
        assert result[0][0] == "2024-02-15 12:00:00"
    
    def test_timestampadd_years(self):
        """Test adding years to timestamp."""
        spark = MockSparkSession("test")
        data = [{"ts": "2024-01-01 12:00:00"}]
        df = spark.createDataFrame(data)
        
        result = df.select(F.timestampadd("YEAR", 1, F.col("ts"))).collect()
        assert result[0][0] == "2025-01-01 12:00:00"
    
    def test_timestampadd_hours(self):
        """Test adding hours to timestamp."""
        spark = MockSparkSession("test")
        data = [{"ts": "2024-01-01 12:00:00"}]
        df = spark.createDataFrame(data)
        
        result = df.select(F.timestampadd("HOUR", 5, F.col("ts"))).collect()
        assert result[0][0] == "2024-01-01 17:00:00"
    
    def test_timestampadd_negative(self):
        """Test subtracting time units."""
        spark = MockSparkSession("test")
        data = [{"ts": "2024-01-15 12:00:00"}]
        df = spark.createDataFrame(data)
        
        result = df.select(F.timestampadd("DAY", -7, F.col("ts"))).collect()
        assert result[0][0] == "2024-01-08 12:00:00"
    
    def test_timestampadd_leap_year(self):
        """Test leap year handling."""
        spark = MockSparkSession("test")
        data = [{"ts": "2024-02-29 12:00:00"}]
        df = spark.createDataFrame(data)
        
        result = df.select(F.timestampadd("YEAR", 1, F.col("ts"))).collect()
        assert result[0][0] == "2025-02-28 12:00:00"
    
    def test_timestampadd_month_boundary(self):
        """Test month boundary handling."""
        spark = MockSparkSession("test")
        data = [{"ts": "2024-01-31 12:00:00"}]
        df = spark.createDataFrame(data)
        
        result = df.select(F.timestampadd("MONTH", 1, F.col("ts"))).collect()
        assert result[0][0] == "2024-02-29 12:00:00"
    
    def test_timestampadd_with_column(self):
        """Test using column for quantity."""
        spark = MockSparkSession("test")
        data = [{"ts": "2024-01-01 12:00:00", "days": 7}]
        df = spark.createDataFrame(data)
        
        result = df.select(F.timestampadd("DAY", F.col("days"), F.col("ts"))).collect()
        assert result[0][0] == "2024-01-08 12:00:00"
    
    def test_timestampadd_null(self):
        """Test null handling."""
        spark = MockSparkSession("test")
        data = [{"ts": None, "days": 7}]
        df = spark.createDataFrame(data)
        
        result = df.select(F.timestampadd("DAY", 7, F.col("ts"))).collect()
        assert result[0][0] is None
    
    def test_timestampadd_invalid_unit(self):
        """Test invalid unit raises error."""
        spark = MockSparkSession("test")
        data = [{"ts": "2024-01-01 12:00:00"}]
        df = spark.createDataFrame(data)
        
        with pytest.raises(IllegalArgumentException):
            df.select(F.timestampadd("INVALID", 7, F.col("ts"))).collect()
    
    def test_timestampadd_all_units(self):
        """Test all supported time units."""
        spark = MockSparkSession("test")
        data = [{"ts": "2024-01-01 12:00:00"}]
        df = spark.createDataFrame(data)
        
        units = ["YEAR", "QUARTER", "MONTH", "WEEK", "DAY", "HOUR", "MINUTE", "SECOND"]
        for unit in units:
            result = df.select(F.timestampadd(unit, 1, F.col("ts"))).collect()
            assert result[0][0] is not None
```

#### Integration Tests

**File:** `tests/integration/test_timestampadd_integration.py`

```python
import pytest
from mock_spark import MockSparkSession, F

class TestTimestampAddIntegration:
    """Integration tests for TIMESTAMPADD function."""
    
    def test_timestampadd_with_filter(self):
        """Test TIMESTAMPADD in WHERE clause."""
        spark = MockSparkSession("test")
        data = [
            {"id": 1, "created_at": "2024-01-01 12:00:00"},
            {"id": 2, "created_at": "2024-01-15 12:00:00"},
        ]
        df = spark.createDataFrame(data)
        
        result = df.filter(
            F.col("created_at") > F.timestampadd("DAY", -10, F.current_timestamp())
        ).collect()
        
        assert len(result) >= 0
    
    def test_timestampadd_with_groupby(self):
        """Test TIMESTAMPADD with GROUP BY."""
        spark = MockSparkSession("test")
        data = [
            {"date": "2024-01-01", "value": 10},
            {"date": "2024-01-02", "value": 20},
        ]
        df = spark.createDataFrame(data)
        
        result = df.groupBy(
            F.timestampadd("DAY", 1, F.col("date")).alias("next_day")
        ).agg(F.sum("value")).collect()
        
        assert len(result) > 0
    
    def test_timestampadd_with_window(self):
        """Test TIMESTAMPADD with window functions."""
        spark = MockSparkSession("test")
        data = [
            {"id": 1, "date": "2024-01-01"},
            {"id": 2, "date": "2024-01-02"},
        ]
        df = spark.createDataFrame(data)
        
        from mock_spark.window import Window
        result = df.withColumn(
            "next_date",
            F.timestampadd("DAY", 1, F.col("date"))
        ).collect()
        
        assert len(result) == 2
```

#### Compatibility Tests

**File:** `tests/compatibility/test_timestampadd_compatibility.py`

```python
import pytest
from mock_spark import MockSparkSession, F

class TestTimestampAddCompatibility:
    """Compatibility tests comparing mock-spark with PySpark 3.2+."""
    
    @pytest.mark.requires_pyspark
    def test_timestampadd_compatibility(self):
        """Test TIMESTAMPADD matches PySpark behavior."""
        from pyspark.sql import SparkSession
        
        # Mock-spark test
        mock_spark = MockSparkSession("test")
        mock_data = [{"ts": "2024-01-01 12:00:00"}]
        mock_df = mock_spark.createDataFrame(mock_data)
        mock_result = mock_df.select(F.timestampadd("DAY", 7, F.col("ts"))).collect()
        
        # PySpark test
        real_spark = SparkSession.builder.appName("test").getOrCreate()
        real_df = real_spark.createDataFrame(mock_data)
        real_result = real_df.select(F.timestampadd("DAY", 7, F.col("ts"))).collect()
        
        # Compare results
        assert mock_result[0][0] == real_result[0][0]
```

#### Performance Tests

**File:** `tests/performance/test_timestampadd_performance.py`

```python
import pytest
import time
from mock_spark import MockSparkSession, F

class TestTimestampAddPerformance:
    """Performance tests for TIMESTAMPADD function."""
    
    def test_timestampadd_performance_large_dataset(self):
        """Test TIMESTAMPADD performance with large dataset."""
        spark = MockSparkSession("test")
        data = [{"ts": "2024-01-01 12:00:00"} for _ in range(100000)]
        df = spark.createDataFrame(data)
        
        start = time.time()
        result = df.select(F.timestampadd("DAY", 7, F.col("ts"))).collect()
        elapsed = time.time() - start
        
        assert len(result) == 100000
        assert elapsed < 5.0  # Should complete in under 5 seconds
    
    def test_timestampadd_performance_comparison(self):
        """Compare TIMESTAMPADD performance with manual calculation."""
        spark = MockSparkSession("test")
        data = [{"ts": "2024-01-01 12:00:00"} for _ in range(10000)]
        df = spark.createDataFrame(data)
        
        # TIMESTAMPADD approach
        start1 = time.time()
        result1 = df.select(F.timestampadd("DAY", 7, F.col("ts"))).collect()
        elapsed1 = time.time() - start1
        
        # Manual calculation approach (if applicable)
        # This test ensures TIMESTAMPADD is not significantly slower
        
        assert elapsed1 < 2.0  # Should be reasonably fast
```

---

### 1.2 TIMESTAMPDIFF Function

#### Unit Tests

**File:** `tests/unit/test_datetime_functions.py`

```python
class TestTimestampDiff:
    """Test TIMESTAMPDIFF function implementation."""
    
    def test_timestampdiff_days(self):
        """Test difference in days between timestamps."""
        spark = MockSparkSession("test")
        data = [
            {"start": "2024-01-01 12:00:00", "end": "2024-01-08 12:00:00"}
        ]
        df = spark.createDataFrame(data)
        
        result = df.select(
            F.timestampdiff("DAY", F.col("start"), F.col("end"))
        ).collect()
        
        assert result[0][0] == 7
    
    def test_timestampdiff_months(self):
        """Test difference in months between timestamps."""
        spark = MockSparkSession("test")
        data = [
            {"start": "2024-01-15 12:00:00", "end": "2024-02-15 12:00:00"}
        ]
        df = spark.createDataFrame(data)
        
        result = df.select(
            F.timestampdiff("MONTH", F.col("start"), F.col("end"))
        ).collect()
        
        assert result[0][0] == 1
    
    def test_timestampdiff_years(self):
        """Test difference in years between timestamps."""
        spark = MockSparkSession("test")
        data = [
            {"start": "2024-01-01 12:00:00", "end": "2025-01-01 12:00:00"}
        ]
        df = spark.createDataFrame(data)
        
        result = df.select(
            F.timestampdiff("YEAR", F.col("start"), F.col("end"))
        ).collect()
        
        assert result[0][0] == 1
    
    def test_timestampdiff_hours(self):
        """Test difference in hours between timestamps."""
        spark = MockSparkSession("test")
        data = [
            {"start": "2024-01-01 12:00:00", "end": "2024-01-01 17:00:00"}
        ]
        df = spark.createDataFrame(data)
        
        result = df.select(
            F.timestampdiff("HOUR", F.col("start"), F.col("end"))
        ).collect()
        
        assert result[0][0] == 5
    
    def test_timestampdiff_negative(self):
        """Test negative difference (end before start)."""
        spark = MockSparkSession("test")
        data = [
            {"start": "2024-01-15 12:00:00", "end": "2024-01-08 12:00:00"}
        ]
        df = spark.createDataFrame(data)
        
        result = df.select(
            F.timestampdiff("DAY", F.col("start"), F.col("end"))
        ).collect()
        
        assert result[0][0] == -7
    
    def test_timestampdiff_same_timestamps(self):
        """Test difference of zero for same timestamps."""
        spark = MockSparkSession("test")
        data = [
            {"start": "2024-01-01 12:00:00", "end": "2024-01-01 12:00:00"}
        ]
        df = spark.createDataFrame(data)
        
        result = df.select(
            F.timestampdiff("DAY", F.col("start"), F.col("end"))
        ).collect()
        
        assert result[0][0] == 0
    
    def test_timestampdiff_leap_year(self):
        """Test leap year handling in difference calculation."""
        spark = MockSparkSession("test")
        data = [
            {"start": "2024-02-29 12:00:00", "end": "2025-02-28 12:00:00"}
        ]
        df = spark.createDataFrame(data)
        
        result = df.select(
            F.timestampdiff("YEAR", F.col("start"), F.col("end"))
        ).collect()
        
        assert result[0][0] == 1
    
    def test_timestampdiff_null(self):
        """Test null handling."""
        spark = MockSparkSession("test")
        data = [{"start": None, "end": "2024-01-08 12:00:00"}]
        df = spark.createDataFrame(data)
        
        result = df.select(
            F.timestampdiff("DAY", F.col("start"), F.col("end"))
        ).collect()
        
        assert result[0][0] is None
    
    def test_timestampdiff_invalid_unit(self):
        """Test invalid unit raises error."""
        spark = MockSparkSession("test")
        data = [
            {"start": "2024-01-01 12:00:00", "end": "2024-01-08 12:00:00"}
        ]
        df = spark.createDataFrame(data)
        
        with pytest.raises(IllegalArgumentException):
            df.select(
                F.timestampdiff("INVALID", F.col("start"), F.col("end"))
            ).collect()
    
    def test_timestampdiff_all_units(self):
        """Test all supported time units."""
        spark = MockSparkSession("test")
        data = [
            {"start": "2024-01-01 12:00:00", "end": "2024-01-02 12:00:00"}
        ]
        df = spark.createDataFrame(data)
        
        units = ["YEAR", "QUARTER", "MONTH", "WEEK", "DAY", "HOUR", "MINUTE", "SECOND"]
        for unit in units:
            result = df.select(
                F.timestampdiff(unit, F.col("start"), F.col("end"))
            ).collect()
            assert result[0][0] is not None
```

---

### 1.3 Enhanced Error Messages

#### Unit Tests

**File:** `tests/unit/test_exceptions.py`

```python
import pytest
from mock_spark import MockSparkSession
from mock_spark.core.exceptions import AnalysisException

class TestEnhancedErrorMessages:
    """Test enhanced error messages."""
    
    def test_column_not_found_with_suggestions(self):
        """Test column not found error with suggestions."""
        spark = MockSparkSession("test")
        data = [{"user_id": 1, "name": "Alice"}]
        df = spark.createDataFrame(data)
        
        with pytest.raises(AnalysisException) as exc_info:
            df.select("user_name").collect()
        
        error_msg = str(exc_info.value)
        assert "user_name" in error_msg
        assert "user_id" in error_msg or "name" in error_msg
        assert "Did you mean" in error_msg
    
    def test_table_not_found_with_context(self):
        """Test table not found error with context."""
        spark = MockSparkSession("test")
        
        with pytest.raises(AnalysisException) as exc_info:
            spark.table("nonexistent_table").collect()
        
        error_msg = str(exc_info.value)
        assert "nonexistent_table" in error_msg
        assert "not found" in error_msg
    
    def test_type_mismatch_with_details(self):
        """Test type mismatch error with details."""
        spark = MockSparkSession("test")
        data = [{"value": "123"}]
        df = spark.createDataFrame(data)
        
        with pytest.raises(AnalysisException) as exc_info:
            df.select(F.col("value") + 5).collect()
        
        error_msg = str(exc_info.value)
        assert "type" in error_msg.lower()
        assert "mismatch" in error_msg.lower()
    
    def test_error_code_present(self):
        """Test error messages include error codes."""
        spark = MockSparkSession("test")
        
        with pytest.raises(AnalysisException) as exc_info:
            spark.table("nonexistent").collect()
        
        error_msg = str(exc_info.value)
        # Check for error code format
        assert "[" in error_msg and "]" in error_msg
    
    def test_error_context_information(self):
        """Test error messages include context information."""
        spark = MockSparkSession("test")
        data = [{"id": 1, "name": "Alice"}]
        df = spark.createDataFrame(data)
        
        with pytest.raises(AnalysisException) as exc_info:
            df.select("unknown_column").collect()
        
        error_msg = str(exc_info.value)
        assert "unknown_column" in error_msg
        assert "id" in error_msg or "name" in error_msg
```

---

### 1.4 Additional String Functions

#### Unit Tests

**File:** `tests/unit/test_string_functions.py`

```python
class TestAdditionalStringFunctions:
    """Test additional string functions."""
    
    def test_regexp_extract_all(self):
        """Test regexp_extract_all function."""
        spark = MockSparkSession("test")
        data = [{"text": "hello123world456"}]
        df = spark.createDataFrame(data)
        
        result = df.select(F.regexp_extract_all("text", r"\d+", 0)).collect()
        assert len(result[0][0]) == 2
        assert "123" in result[0][0]
        assert "456" in result[0][0]
    
    def test_array_join(self):
        """Test array_join function."""
        spark = MockSparkSession("test")
        data = [{"arr": ["a", "b", "c"]}]
        df = spark.createDataFrame(data)
        
        result = df.select(F.array_join("arr", ",")).collect()
        assert result[0][0] == "a,b,c"
    
    def test_repeat(self):
        """Test repeat function."""
        spark = MockSparkSession("test")
        data = [{"text": "hello"}]
        df = spark.createDataFrame(data)
        
        result = df.select(F.repeat("text", 3)).collect()
        assert result[0][0] == "hellohellohello"
    
    def test_initcap(self):
        """Test initcap function."""
        spark = MockSparkSession("test")
        data = [{"text": "hello world"}]
        df = spark.createDataFrame(data)
        
        result = df.select(F.initcap("text")).collect()
        assert result[0][0] == "Hello World"
    
    def test_soundex(self):
        """Test soundex function."""
        spark = MockSparkSession("test")
        data = [{"text": "smith"}]
        df = spark.createDataFrame(data)
        
        result = df.select(F.soundex("text")).collect()
        assert result[0][0] == "S530"
```

---

## Phase 2: Core Features Testing

### 2.1 Pandas API on Spark - mapInPandas

#### Unit Tests

**File:** `tests/unit/test_pandas_api.py`

```python
import pandas as pd
from mock_spark import MockSparkSession, F

class TestMapInPandas:
    """Test mapInPandas function."""
    
    def test_mapinpandas_basic(self):
        """Test basic mapInPandas operation."""
        spark = MockSparkSession("test")
        data = [{"value": 1}, {"value": 2}, {"value": 3}]
        df = spark.createDataFrame(data)
        
        def multiply_by_two(iterator):
            for pdf in iterator:
                yield pdf * 2
        
        result = df.mapInPandas(multiply_by_two, schema="value int").collect()
        assert len(result) == 3
        assert result[0][0] == 2
        assert result[1][0] == 4
        assert result[2][0] == 6
    
    def test_mapinpandas_with_schema(self):
        """Test mapInPandas with explicit schema."""
        spark = MockSparkSession("test")
        data = [{"value": 1}, {"value": 2}]
        df = spark.createDataFrame(data)
        
        def add_column(iterator):
            for pdf in iterator:
                pdf['doubled'] = pdf['value'] * 2
                yield pdf
        
        schema = "value int, doubled int"
        result = df.mapInPandas(add_column, schema=schema).collect()
        assert len(result) == 2
        assert result[0][1] == 2
    
    def test_mapinpandas_empty_result(self):
        """Test mapInPandas with empty result."""
        spark = MockSparkSession("test")
        data = [{"value": 1}]
        df = spark.createDataFrame(data)
        
        def empty_iterator(iterator):
            return iter([])
        
        result = df.mapInPandas(empty_iterator, schema="value int").collect()
        assert len(result) == 0
    
    def test_mapinpandas_error_handling(self):
        """Test mapInPandas error handling."""
        spark = MockSparkSession("test")
        data = [{"value": 1}]
        df = spark.createDataFrame(data)
        
        def raise_error(iterator):
            raise ValueError("Test error")
        
        with pytest.raises(ValueError):
            df.mapInPandas(raise_error, schema="value int").collect()
    
    def test_mapinpandas_schema_validation(self):
        """Test mapInPandas schema validation."""
        spark = MockSparkSession("test")
        data = [{"value": 1}]
        df = spark.createDataFrame(data)
        
        def wrong_schema(iterator):
            for pdf in iterator:
                yield pd.DataFrame({"wrong": [1]})
        
        with pytest.raises(AnalysisException):
            df.mapInPandas(wrong_schema, schema="value int").collect()
```

---

### 2.2 Pandas API on Spark - applyInPandas

#### Unit Tests

**File:** `tests/unit/test_pandas_api.py`

```python
class TestApplyInPandas:
    """Test applyInPandas function."""
    
    def test_applyinpandas_basic(self):
        """Test basic applyInPandas operation."""
        spark = MockSparkSession("test")
        data = [
            {"category": "A", "value": 1},
            {"category": "A", "value": 2},
            {"category": "B", "value": 3},
        ]
        df = spark.createDataFrame(data)
        
        def sum_group(pdf):
            return pd.DataFrame({"category": [pdf['category'].iloc[0]], "sum": [pdf['value'].sum()]})
        
        result = df.groupBy("category").applyInPandas(sum_group, schema="category string, sum int").collect()
        assert len(result) == 2
    
    def test_applyinpandas_with_aggregation(self):
        """Test applyInPandas with aggregation."""
        spark = MockSparkSession("test")
        data = [
            {"group": "A", "value": 10},
            {"group": "A", "value": 20},
        ]
        df = spark.createDataFrame(data)
        
        def mean_group(pdf):
            return pd.DataFrame({"group": [pdf['group'].iloc[0]], "mean": [pdf['value'].mean()]})
        
        result = df.groupBy("group").applyInPandas(mean_group, schema="group string, mean double").collect()
        assert len(result) == 1
        assert result[0][1] == 15.0
```

---

## Phase 3: Advanced Features Testing

### 3.1 Parameterized SQL Queries

#### Unit Tests

**File:** `tests/unit/test_sql_queries.py`

```python
class TestParameterizedSQL:
    """Test parameterized SQL queries."""
    
    def test_positional_parameters(self):
        """Test positional parameters in SQL."""
        spark = MockSparkSession("test")
        data = [{"id": 1, "age": 25}, {"id": 2, "age": 30}]
        df = spark.createDataFrame(data)
        df.createOrReplaceTempView("users")
        
        result = spark.sql("SELECT * FROM users WHERE age > ?", 25).collect()
        assert len(result) == 1
        assert result[0][1] == 30
    
    def test_named_parameters(self):
        """Test named parameters in SQL."""
        spark = MockSparkSession("test")
        data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        df = spark.createDataFrame(data)
        df.createOrReplaceTempView("users")
        
        result = spark.sql("SELECT * FROM users WHERE name = :name", name="Alice").collect()
        assert len(result) == 1
        assert result[0][1] == "Alice"
    
    def test_multiple_parameters(self):
        """Test multiple parameters in SQL."""
        spark = MockSparkSession("test")
        data = [{"id": 1, "age": 25}, {"id": 2, "age": 30}, {"id": 3, "age": 35}]
        df = spark.createDataFrame(data)
        df.createOrReplaceTempView("users")
        
        result = spark.sql("SELECT * FROM users WHERE age > ? AND age < ?", 20, 35).collect()
        assert len(result) == 2
    
    def test_sql_injection_prevention(self):
        """Test SQL injection prevention."""
        spark = MockSparkSession("test")
        data = [{"id": 1, "name": "Alice"}]
        df = spark.createDataFrame(data)
        df.createOrReplaceTempView("users")
        
        # Attempt SQL injection
        malicious_input = "'; DROP TABLE users; --"
        result = spark.sql("SELECT * FROM users WHERE name = ?", malicious_input).collect()
        # Should not drop table, just return no results
        assert len(result) == 0
```

---

## Test Execution Strategy

### Continuous Integration

```yaml
# .github/workflows/test.yml
name: Test Suite

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: [3.9, 3.10, 3.11, 3.12]
    
    steps:
    - uses: actions/checkout@v3
    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: ${{ matrix.python-version }}
    
    - name: Install dependencies
      run: |
        pip install -e ".[dev]"
        pip install pytest pytest-cov pytest-xdist
    
    - name: Run unit tests
      run: pytest tests/unit/ -v --cov=mock_spark --cov-report=xml
    
    - name: Run integration tests
      run: pytest tests/integration/ -v
    
    - name: Run compatibility tests
      run: pytest tests/compatibility/ -v -m "not requires_pyspark"
    
    - name: Upload coverage
      uses: codecov/codecov-action@v3
```

### Test Commands

```bash
# Run all tests
pytest tests/ -v

# Run specific test category
pytest tests/unit/ -v
pytest tests/integration/ -v
pytest tests/compatibility/ -v

# Run with coverage
pytest tests/ --cov=mock_spark --cov-report=html

# Run specific test file
pytest tests/unit/test_datetime_functions.py -v

# Run specific test class
pytest tests/unit/test_datetime_functions.py::TestTimestampAdd -v

# Run specific test method
pytest tests/unit/test_datetime_functions.py::TestTimestampAdd::test_timestampadd_days -v

# Run with parallel execution
pytest tests/ -n auto

# Run with verbose output
pytest tests/ -v -s

# Run only failed tests
pytest tests/ --lf

# Run tests matching pattern
pytest tests/ -k "timestamp"
```

---

## Test Coverage Goals

### Coverage Targets

- **Overall Coverage:** 90%+
- **New Code Coverage:** 95%+
- **Critical Path Coverage:** 100%
- **Function Coverage:** 90%+
- **Branch Coverage:** 85%+

### Coverage Reports

```bash
# Generate HTML coverage report
pytest tests/ --cov=mock_spark --cov-report=html
open htmlcov/index.html

# Generate terminal coverage report
pytest tests/ --cov=mock_spark --cov-report=term-missing

# Generate XML coverage report (for CI)
pytest tests/ --cov=mock_spark --cov-report=xml
```

---

## Performance Testing

### Performance Benchmarks

**File:** `tests/performance/test_performance_benchmarks.py`

```python
import pytest
import time
from mock_spark import MockSparkSession, F

class TestPerformanceBenchmarks:
    """Performance benchmarks for new features."""
    
    def benchmark_timestampadd(self, benchmark):
        """Benchmark TIMESTAMPADD performance."""
        spark = MockSparkSession("test")
        data = [{"ts": "2024-01-01 12:00:00"} for _ in range(10000)]
        df = spark.createDataFrame(data)
        
        def run():
            return df.select(F.timestampadd("DAY", 7, F.col("ts"))).collect()
        
        result = benchmark(run)
        assert len(result) == 10000
    
    def benchmark_mapinpandas(self, benchmark):
        """Benchmark mapInPandas performance."""
        spark = MockSparkSession("test")
        data = [{"value": i} for i in range(10000)]
        df = spark.createDataFrame(data)
        
        def multiply(pdf):
            pdf['doubled'] = pdf['value'] * 2
            return pdf
        
        def run():
            return df.mapInPandas(multiply, schema="value int, doubled int").collect()
        
        result = benchmark(run)
        assert len(result) == 10000
```

### Performance Regression Detection

```bash
# Run performance benchmarks
pytest tests/performance/ --benchmark-only

# Compare with previous run
pytest tests/performance/ --benchmark-compare

# Generate performance report
pytest tests/performance/ --benchmark-json=benchmark.json
```

---

## Test Data Management

### Test Fixtures

**File:** `tests/conftest.py`

```python
import pytest
from mock_spark import MockSparkSession

@pytest.fixture
def spark_session():
    """Create a test Spark session."""
    spark = MockSparkSession("test")
    yield spark
    spark.stop()

@pytest.fixture
def sample_dataframe(spark_session):
    """Create a sample DataFrame for testing."""
    data = [
        {"id": 1, "name": "Alice", "age": 25},
        {"id": 2, "name": "Bob", "age": 30},
        {"id": 3, "name": "Charlie", "age": 35},
    ]
    return spark_session.createDataFrame(data)

@pytest.fixture
def large_dataframe(spark_session):
    """Create a large DataFrame for performance testing."""
    data = [{"value": i} for i in range(100000)]
    return spark_session.createDataFrame(data)
```

---

## Test Maintenance

### Test Documentation

- Document test purpose in docstrings
- Add comments for complex test logic
- Include expected behavior in test names
- Document test data requirements

### Test Organization

```
tests/
├── unit/                    # Unit tests (80%)
│   ├── test_datetime_functions.py
│   ├── test_string_functions.py
│   ├── test_pandas_api.py
│   └── ...
├── integration/             # Integration tests (15%)
│   ├── test_timestampadd_integration.py
│   └── ...
├── compatibility/           # Compatibility tests
│   ├── test_timestampadd_compatibility.py
│   └── ...
├── performance/             # Performance tests (5%)
│   ├── test_performance_benchmarks.py
│   └── ...
└── conftest.py             # Shared fixtures
```

### Test Review Checklist

- [ ] Test name clearly describes what is being tested
- [ ] Test covers happy path
- [ ] Test covers error cases
- [ ] Test covers edge cases
- [ ] Test is independent (no dependencies on other tests)
- [ ] Test is deterministic (same result every run)
- [ ] Test is fast (< 1 second for unit tests)
- [ ] Test has proper assertions
- [ ] Test has proper cleanup
- [ ] Test is well documented

---

## Conclusion

This comprehensive testing plan ensures that all PySpark 3.2 features are thoroughly tested before release. The plan covers:

1. **Unit Tests** - Individual function testing
2. **Integration Tests** - Feature interaction testing
3. **Compatibility Tests** - PySpark compatibility verification
4. **Performance Tests** - Performance regression detection
5. **E2E Tests** - Complete user scenario testing

With this testing strategy, mock-spark 2.5.0 will have high-quality, reliable implementations of PySpark 3.2 features.

---

**Plan Version:** 1.0  
**Created:** 2025-01-14  
**Target Release:** mock-spark 2.5.0 (Q2 2025)

