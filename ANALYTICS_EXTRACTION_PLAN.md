# Analytics Module Extraction Plan

**Date**: October 7, 2025  
**Purpose**: Extract analytics module into standalone `duckdb-analytics` package

---

## 📦 Why Extract?

### Problems with Current State
1. **Scope Creep**: Analytics features are NOT part of PySpark API
2. **Mission Drift**: mock-spark should mock PySpark, not extend it
3. **Maintenance Burden**: 1,630 lines of non-essential code
4. **Confusion**: Users expect PySpark compatibility, not extras

### Benefits of Standalone Package
1. **Clear Purpose**: DuckDB-powered analytics library
2. **Reusable**: Can work with any data source (Pandas, Polars, etc.)
3. **Focused Development**: Not tied to PySpark API
4. **Better Naming**: "duckdb-analytics" describes what it does

---

## 📊 Current Analytics Module

### Files to Extract (1,630 lines total)

```
mock_spark/analytics/
├── __init__.py (13 lines)
├── analytics_engine.py (356 lines)
├── statistical_functions.py (388 lines)
├── time_series.py (399 lines)
└── ml_preprocessing.py (468 lines)
```

### Test File (1 file)
```
tests/unit/test_analytics_module.py
```

### Features to Extract

1. **AnalyticsEngine** (356 lines)
   - DataFrame registration
   - SQL query execution
   - Grouped aggregations
   - Window functions
   - Correlation analysis
   - Statistical summaries
   - View creation
   - Parquet/CSV export

2. **StatisticalFunctions** (388 lines)
   - Descriptive statistics
   - Hypothesis testing
   - Distribution analysis
   - Outlier detection
   - Sample statistics

3. **TimeSeriesAnalysis** (399 lines)
   - Lag/lead operations
   - Moving averages
   - Seasonal decomposition
   - Trend analysis
   - Anomaly detection

4. **MLPreprocessing** (468 lines)
   - Train/test split
   - Feature scaling
   - Encoding
   - Missing value handling
   - Feature engineering

---

## 🚀 New Package: `duckdb-analytics`

### Package Structure

```
duckdb-analytics/
├── pyproject.toml
├── README.md
├── LICENSE
├── duckdb_analytics/
│   ├── __init__.py
│   ├── engine.py (from analytics_engine.py)
│   ├── statistics.py (from statistical_functions.py)
│   ├── timeseries.py (from time_series.py)
│   └── preprocessing.py (from ml_preprocessing.py)
├── tests/
│   └── test_analytics.py
└── examples/
    └── basic_usage.py
```

### Package Metadata

```toml
[project]
name = "duckdb-analytics"
version = "0.1.0"
description = "High-performance analytics toolkit powered by DuckDB"
authors = [{name = "Your Name", email = "email@example.com"}]
license = {text = "MIT"}
requires-python = ">=3.8"
dependencies = [
    "duckdb>=0.9.0",
    "sqlmodel>=0.0.14",
    "pandas>=1.5.0",
    "numpy>=1.23.0",
]
```

### Key Features

**Standalone DuckDB Analytics Library**:
- ✅ Works with any data source (Pandas, Polars, Arrow, CSV, Parquet)
- ✅ High-performance SQL analytics
- ✅ Statistical functions
- ✅ Time series analysis
- ✅ ML preprocessing utilities
- ✅ Export to Parquet/CSV
- ✅ No PySpark dependency

### Example Usage

```python
from duckdb_analytics import AnalyticsEngine
import pandas as pd

# Create engine
engine = AnalyticsEngine()

# Register a Pandas DataFrame
df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
table = engine.register_dataframe(df, 'my_table')

# Run analytics
stats = engine.statistical_summary(table, ['a', 'b'])
correlations = engine.correlation_analysis(table, ['a', 'b'])

# Time series
ts_results = engine.time_series_analysis(table, 'date_col', 'value_col')

# Export
engine.export_to_parquet(table, 'output.parquet')
```

---

## 🔧 Removal from mock-spark

### Files to Delete
```bash
rm -rf mock_spark/analytics/
rm tests/unit/test_analytics_module.py
```

### References to Update

1. **README.md**: Remove "Analytics Engine" section
2. **pyproject.toml**: No dependencies to remove (DuckDB already used elsewhere)
3. **mock_spark/__init__.py**: Remove analytics imports (if any)
4. **docs/**: Update any documentation mentioning analytics

### Impact
- **Lines removed**: ~1,630
- **Files removed**: 5 module files + 1 test file
- **Dependencies**: None (DuckDB used elsewhere)
- **Breaking change**: Yes, but minimal (feature not in compatibility tests)

---

## 📝 Migration Guide for Users

### Before (mock-spark with analytics)
```python
from mock_spark.analytics import AnalyticsEngine

analytics = AnalyticsEngine()
# ... analytics code
```

### After (separate packages)
```python
# Install both packages
# pip install mock-spark duckdb-analytics

from mock_spark import MockSparkSession
from duckdb_analytics import AnalyticsEngine

spark = MockSparkSession()
df = spark.createDataFrame(data)

# Convert to DuckDB for analytics
analytics = AnalyticsEngine()
table = analytics.register_dataframe(df.toPandas(), 'my_table')
stats = analytics.statistical_summary(table, ['col1', 'col2'])
```

---

## ✅ Action Plan

### Phase 1: Extract and Document (Now)
- [x] Analyze current analytics module
- [x] Document features and structure
- [x] Create extraction plan

### Phase 2: Remove from mock-spark (Next)
- [ ] Delete `mock_spark/analytics/` directory
- [ ] Delete `tests/unit/test_analytics_module.py`
- [ ] Update README.md (remove analytics section)
- [ ] Update references
- [ ] Run tests to ensure no breakage
- [ ] Commit removal

### Phase 3: Create New Package (Future - Optional)
- [ ] Create `duckdb-analytics` repository
- [ ] Copy and adapt analytics code
- [ ] Generalize to work with Pandas/Polars/Arrow
- [ ] Write comprehensive tests
- [ ] Create documentation
- [ ] Publish to PyPI

---

## 🎯 Benefits Summary

### For mock-spark
- ✅ **Focused mission**: Pure PySpark API compatibility
- ✅ **Less code**: -1,630 lines to maintain
- ✅ **Clearer purpose**: No feature creep
- ✅ **Better testing**: Focus on PySpark compatibility
- ✅ **Easier contribution**: Clear scope

### For duckdb-analytics (new package)
- ✅ **Clear identity**: Analytics toolkit, not PySpark mock
- ✅ **Broader appeal**: Works with any data tool
- ✅ **Focused development**: Not constrained by PySpark API
- ✅ **Better naming**: Describes actual functionality
- ✅ **Room to grow**: Can add features without scope concerns

---

## 📦 Package Comparison

| Feature | mock-spark (current) | mock-spark (after) | duckdb-analytics (new) |
|---------|---------------------|-------------------|------------------------|
| **Purpose** | Mock PySpark + Analytics | Mock PySpark only | DuckDB Analytics |
| **Lines of code** | ~14,000 | ~12,400 | ~1,600 |
| **Dependencies** | PySpark (dev), DuckDB | PySpark (dev), DuckDB | DuckDB only |
| **Scope** | Confused | Clear | Clear |
| **PySpark API** | Yes | Yes | No (independent) |
| **Analytics** | Mixed in | None | Primary feature |

---

## 🚀 Next Steps

1. **Approve this plan** ✅
2. **Remove analytics from mock-spark** (10 minutes)
3. **Update documentation** (5 minutes)
4. **Run tests** (5 minutes)
5. **Commit and push** (2 minutes)
6. **[Future] Create duckdb-analytics package** (separate project)

---

**Ready to proceed with removal from mock-spark?**

