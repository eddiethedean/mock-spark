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

