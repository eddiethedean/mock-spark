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
- **Local Mitigation:** `mock_spark.compat.datetime.to_date_typed` round-trips ISO strings
  through `to_date` so that both the real and Mock Spark engines emit native date objects
  without manual slicing. The legacy `to_date_str` helper remains for workflows that still
  need strings.
- **Upstream Ask:** Clarify whether future Polars releases will expose format-tolerant
  string parsing (or restore implicit ISO parsing) so the helper can be retired.
- **Status:** TODO(#datetime-parsing-roadmap) — file upstream issue documenting the need
  for format-agnostic parsing and reference our helper as an interim workaround.

## Python 3.8 Compatibility Shim
- **Issue:** Mock Spark 3.1.0 requires a few Python 3.9+ niceties (``typing.TypeAlias``,
  subscription on ``collections.abc`` classes, optional DuckDB backend imports) to import
  cleanly.
- **Local Mitigation:** ``sitecustomize.py`` now backfills ``TypeAlias``, injects
  ``__class_getitem__`` on the affected ABCs, and stubs ``mock_spark.backend.duckdb`` for
  environments without the optional dependency.
- **Upstream Ask:** Coordinate with maintainers on official Python 3.8 support status and
  surface any planned deprecations.
- **Status:** TODO(#python38-support) — open tracking ticket summarising the shim and exit
  criteria so we can remove it once upstream clarifies timelines.

## Documentation Examples
- **Issue:** Documentation tests execute `python3 examples/...` which resolves to the global
  interpreter and imports the pip-installed `mock_spark`. This diverges from the workspace
  version.
- **Proposed Action:** Decide whether to ship a CLI wrapper that sets `PYTHONPATH` before
  running examples or update tests to invoke `sys.executable` instead of `python3`.

