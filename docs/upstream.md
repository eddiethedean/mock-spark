# Upstream Coordination Notes

## Delta Schema Evolution
- **Issue:** Polars backend raises `ValueError: type String is incompatible with expected type Null`
  when appending with `mergeSchema=true`.
- **Local Mitigation:** `mock_spark/backend/polars/schema_utils.py` now coerces write batches to the
  registered schema and fills new columns with correctly typed nulls.
- **Upstream Ask:** Confirm whether Polars intends to support concatenating `Null` and
  concrete dtypes automatically. If yes, track the fix; if no, document the requirement
  to normalise data before insertion.
- **Artifacts:** Regression test `tests/unit/delta/test_delta_schema_evolution.py` reproduces
  the scenario.

## Datetime Conversions
- **Issue:** `to_date`/`to_timestamp` now return Python `date`/`datetime` objects under
  Polars, breaking substring-based assertions in downstream code.
- **Local Mitigation:** New helpers in `mock_spark.compat.datetime` offer column- and
  value-level normalisation.
- **Upstream Ask:** Clarify whether future Polars releases will expose string-returning
  variants or document recommended migration paths.

## Documentation Examples
- **Issue:** Documentation tests execute `python3 examples/...` which resolves to the global
  interpreter and imports the pip-installed `mock_spark`. This diverges from the workspace
  version.
- **Proposed Action:** Decide whether to ship a CLI wrapper that sets `PYTHONPATH` before
  running examples or update tests to invoke `sys.executable` instead of `python3`.

