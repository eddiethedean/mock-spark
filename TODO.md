# TODO & Future Enhancements

## Performance & Optimisation
- [x] Profile Polars execution hot paths (`backend/polars/operation_executor.py`, `dataframe/evaluation/expression_evaluator.py`) and introduce vectorised shortcuts or caching for common operators. (Feature-flagged profiling utilities added in `mock_spark/utils/profiling.py`; hot paths instrumented with caching and documented in `docs/performance/profiling.md`.)
- [x] Evaluate adaptive execution simulation hook in `mock_spark/optimizer/query_optimizer.py` to better mirror Spark's AQE plans under skew. (Adaptive simulation toggle implemented with regression tests under `tests/unit/optimizer/test_query_optimizer_adaptive.py` and documented in `docs/backend_architecture.md`.)
- [x] Benchmark stubbed `pandas` fallback and explore lightweight real dependency opt-in for consumers that want parity with `toPandas`. (Optional native pandas backend with benchmark script in `scripts/benchmark_pandas_fallback.py`; guidance captured in `docs/performance/pandas_fallback.md`.)

## Testing & Reliability
- [ ] Extend regression suite for session-aware helpers (`F.current_*`) to cover multi-session scenarios and catalog drop/recreate workflows.
- [ ] Add integration smoke tests for `scripts/discover_pyspark_api.py` to ensure generated matrices stay in sync with new function coverage.
- [ ] Harden documentation example harness to fail fast when dependencies (e.g. pandas stub) are missing or stale.

## Tooling & Release Flow
- [ ] Automate CHANGELOG and README “Recent Updates” entries during release tagging.
- [ ] Wire `scripts/discover_pyspark_api.py` into CI (nightly) so compatibility matrices stay current without manual invocation.
- [ ] Add packaging check that prevents publishing if stub modules (like `pandas` shim) drift from production behaviour expectations.

## Documentation & Community
- [ ] Document new session-aware literals and schema tracking in guides (`docs/sql_operations_guide.md`, `docs/getting_started.md` advanced section).
- [ ] Publish troubleshooting guide for native dependency crashes, referencing the pure-Python percentile/covariance fallbacks.
- [ ] Draft migration notes for upcoming performance knobs to help users tune mock behaviour per pipeline.

