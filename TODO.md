# TODO & Future Enhancements

## Recently Completed (2024)

### PySpark Compatibility
- [x] Convert `mock_spark/sql/functions.py` to export a proper module instead of Functions class, using `__getattr__` to expose all functions. (Ensures `isinstance(functions, ModuleType)` returns True for PySpark compatibility.)
- [x] Create `mock_spark/sql/utils.py` to export exceptions matching PySpark structure (AnalysisException, ParseException, IllegalArgumentException, etc.).
- [x] Update module structure to match PySpark's `pyspark.sql.types`, `pyspark.sql.functions`, and `pyspark.sql.utils` organization.
- [x] Fix PySpark environment configuration in API parity tests to resolve `java.net.BindException` issues. (Configured `spark.driver.bindAddress`, `spark.driver.host`, `spark.master`, and `spark.ui.enabled`.)

### Window Functions
- [x] Implement support for `rowsBetween` and `rangeBetween` window frames in Polars backend.
- [x] Fix window function complex ordering to handle multiple columns with different directions (asc/desc). (Updated `PolarsWindowHandler` to correctly extract base column names and apply sort directions.)
- [x] Implement reverse cumulative sum for `rowsBetween(currentRow, unboundedFollowing)` with Python evaluation fallback.
- [x] Implement all 11 window functions with proper partitioning and ordering support: `row_number()`, `rank()`, `dense_rank()`, `cume_dist()`, `percent_rank()`, `lag()`, `lead()`, `first_value()`, `last_value()`, `nth_value()`, and `ntile()`.
- [x] Add Python fallback mechanism for window functions not supported in Polars backend (cume_dist, percent_rank, nth_value, ntile).
- [x] Fix `nth_value()` to return NULL for rows before the nth position (PySpark-compatible behavior).
- [x] Fix `cume_dist()` and `percent_rank()` calculations to handle ties correctly using rank-based calculations.
- [x] All 11 window function tests passing (previously 7 passing, 4 skipped).

### Function Implementations
- [x] Fix trim/ltrim/rtrim functions to only remove ASCII space characters (`" "`) to match PySpark's behavior (not all whitespace).
- [x] Fix `concat` function to correctly handle string literals by wrapping them in `Literal` objects.
- [x] Add `rlike` method to `ColumnOperations` to support `df.col.rlike()` syntax.
- [x] Fix `isin` function to correctly handle column expressions and literals.
- [x] Fix `when/otherwise` expressions to be handled by Polars backend instead of forcing Python evaluation.
- [x] Fix `date_format`, `datediff`, `day` extraction, and timestamp casting functions.
- [x] Fix string-to-numeric casting for decimal strings like '10.5'.
- [x] Fix array and map casting to string types.
- [x] Fix date and timestamp casting to/from string types.

### Code Quality
- [x] Fix all mypy type checking errors across 161 source files. (Added proper type annotations, fixed Optional types, resolved Union syntax for Python 3.9 compatibility.)
- [x] Fix all ruff linting errors. (Removed unused imports, simplified nested if statements, combined if branches, replaced if-else blocks with ternary operators.)
- [x] Ensure all code passes `ruff format`, `ruff check`, and `mypy` validation.
- [x] All 1088 tests passing with 47 expected skips.
- [x] Fix mypy error in `MiscellaneousOperations` by accessing columns via schema instead of direct property access.

## Performance & Optimisation
- [x] Profile Polars execution hot paths (`backend/polars/operation_executor.py`, `dataframe/evaluation/expression_evaluator.py`) and introduce vectorised shortcuts or caching for common operators. (Feature-flagged profiling utilities added in `mock_spark/utils/profiling.py`; hot paths instrumented with caching and documented in `docs/performance/profiling.md`.)
- [x] Evaluate adaptive execution simulation hook in `mock_spark/optimizer/query_optimizer.py` to better mirror Spark's AQE plans under skew. (Adaptive simulation toggle implemented with regression tests under `tests/unit/optimizer/test_query_optimizer_adaptive.py` and documented in `docs/backend_architecture.md`.)
- [x] Benchmark stubbed `pandas` fallback and explore lightweight real dependency opt-in for consumers that want parity with `toPandas`. (Optional native pandas backend with benchmark script in `scripts/benchmark_pandas_fallback.py`; guidance captured in `docs/performance/pandas_fallback.md`.)

## Testing & Reliability
- [x] Extend regression suite for session-aware helpers (`F.current_*`) to cover multi-session scenarios and catalog drop/recreate workflows. (New cases in `tests/unit/functions/test_session_functions.py` verify isolation and catalog lifecycle resilience.)
- [x] Add integration smoke tests for `scripts/discover_pyspark_api.py` to ensure generated matrices stay in sync with new function coverage. (`tests/integration/scripts/test_discover_pyspark_api.py` stubs discovery to validate artifact generation.)
- [x] Harden documentation example harness to fail fast when dependencies (e.g. pandas stub) are missing or stale. (`tests/documentation/test_examples.py` now enforces optional dependency versions and skips with guidance when absent.)

## Documentation & Community
- [ ] Document new session-aware literals and schema tracking in guides (`docs/sql_operations_guide.md`, `docs/getting_started.md` advanced section).
- [ ] Publish troubleshooting guide for native dependency crashes, referencing the pure-Python percentile/covariance fallbacks.
- [ ] Draft migration notes for upcoming performance knobs to help users tune mock behaviour per pipeline.
- [ ] Document PySpark compatibility improvements and module structure changes in migration guide.

