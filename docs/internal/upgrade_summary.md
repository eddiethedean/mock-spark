# Mock-Spark 3.0.0 Upgrade Summary

## Highlights
- Restored Delta `mergeSchema` compatibility for the Polars backend via schema
  reconciliation helpers.
- Added datetime normalisation utilities to ease downstream migrations where
  string slicing is still required.
- Exposed backend selection through constructor overrides, builder config, and
  the `MOCK_SPARK_BACKEND` environment variable. Documented the workflow in the
  new backend selection guide.

## Test Coverage
- New unit tests cover schema evolution, datetime compatibility helpers, backend
  selection, and collected datetime normalisation.
- Full suite executed with `bash tests/run_all_tests.sh`; documentation example
  tests still fail when a conflicting global `mock_spark` installation shadows
  the workspace. Resolution requires aligning the interpreter used for doc
  scripts.

## Follow-Ups
- Confirm upstream guidance on Polars merge-schema behaviour and timestamp
  string conversions (see `docs/upstream.md`).
- Decide on a long-term story for documentation example execution (adjust test
  harness vs. shipping a helper CLI).

## 2025-11-13 Baseline Observations
- Full `bash tests/run_all_tests.sh` invocation terminates with a native
  segmentation fault before any unit, compatibility, or performance tests
  complete. The failure occurs during pytest collection while Hypothesis falls
  back to an in-memory database (`returncode=-11`).
- Documentation example tests (`tests/documentation/test_examples.py`) surface
  the same crash when launching `examples/basic_usage.py` and
  `examples/comprehensive_usage.py`. Run duration until failure: 6.54s.
- Coverage instrumentation reports `no-data-collected` because the session
  aborts prior to executing tracked code.
- `python scripts/benchmark_backends.py` also exits with code 139 without
  emitting metrics, indicating a broader stability issue likely tied to native
  bindings (Polars/Arrow/Rust) under Python 3.11.13 on macOS 14.6.
- Recommended next steps: reproduce under a debug build, bisect recent Polars
  upgrades, and consider pinning to a known-good toolchain to restore baseline
  visibility.

## 2025-11-13 Stabilisation
- Added session-aware helpers (`current_catalog`, `current_database`,
  `current_schema`, `current_user`) plus schema tracking inside the Polars
  storage backend so `setCurrentDatabase` updates take effect.
- With those changes in place `bash tests/run_all_tests.sh` now passes end-to-end
  and the documentation example harness executes without the earlier native
  crash.

