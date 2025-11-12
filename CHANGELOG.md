# Changelog

## 3.2.0 — 2025-11-12

### Changed
- Raised the minimum supported Python version to 3.9 and aligned Black, Ruff, and mypy
  targets so local tooling matches the published wheel.
- Removed the Python 3.8 compatibility shim in `mock_spark.compat.datetime` in favour of
  native typing support, simplifying downstream imports.
- Standardised type hints on built-in generics (`list[str]`, `dict[str, Any]`) and
  `collections.abc` protocols across the codebase, eliminating leftover `typing` fallbacks.
- Adopted `ruff format` as the canonical formatter, bringing the entire repository in line with
  the Ruff style guide.

### Documentation
- Updated the README to call out the Python 3.9 baseline and refreshed the “Recent Updates”
  section with the typing/tooling improvements delivered in 3.2.0.

## 3.1.0 — 2025-11-07

### Added
- Schema reconciliation for Delta `mergeSchema=true` appends on the Polars backend,
  preventing null-type collisions while preserving legacy data.
- Datetime compatibility helpers in `mock_spark.compat.datetime` for producing
  stable string outputs when downstream code expects substrings.
- Configurable backend selection via constructor overrides, the
  `MOCK_SPARK_BACKEND` environment variable, or `SparkSession.builder.config`.
- Regression tests covering schema evolution, datetime normalisation, backend
  selection, and compatibility helpers.
- Protocol-based DataFrame mixins (`SupportsDataFrameOps`) enabling structural typing and
  a clean mypy run across 260 modules.
- Ruff lint configuration and cast/typing cleanups so that `ruff check` passes repository-wide.

## 3.0.0 — 2025-09-12

### Added
- Polars backend as the new default execution engine, delivering thread-safe, high-performance
  DataFrame operations without JVM dependencies.
- Parquet-based table persistence with `saveAsTable`, including catalog synchronisation and
  cross-session durability via `db_path`.
- Comprehensive backend selection via environment variables, builder configuration, and constructor overrides.
- New documentation covering backend architecture, migration guidance from v2.x, and configuration options.

### Changed
- Migrated window functions, joins, aggregations, and lazy evaluation to Polars-powered implementations
  while maintaining PySpark-compatible APIs.
- Updated test harness and CI scripts to exercise the Polars backend, increasing the regression suite to
  600+ passing tests.

### Removed
- Legacy DuckDB-backed SQL translation layer (`sqlglot` dependency, Mock* prefixed classes) in favour of
  the unified protocol-based backend architecture.

### Documentation
- Introduced `docs/backend_selection.md` describing backend options, environment
  overrides, and troubleshooting tips.
- Documented merge-schema limitations and datetime helper usage in
  `docs/known_issues.md`.

### Known Issues
- Documentation example tests invoke the globally installed `mock_spark`
  distribution. When a different version is installed in `site-packages`, the
  example scripts exit early with `ImportError`. Align the executable path or
  install the local wheel before running documentation fixtures.

