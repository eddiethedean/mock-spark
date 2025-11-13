# Changelog

## 3.7.0 — 2025-01-XX

### Added
- Full SQL DDL/DML support: `CREATE TABLE`, `DROP TABLE`, `INSERT INTO`, `UPDATE`, and `DELETE FROM` statements are now fully implemented in the SQL executor.
- Enhanced SQL parser with comprehensive support for DDL statements including column definitions, `IF NOT EXISTS`, and `IF EXISTS` clauses.
- Support for `INSERT INTO ... VALUES (...)` with multiple rows and `INSERT INTO ... SELECT ...` sub-queries.
- `UPDATE ... SET ... WHERE ...` statements with Python-based expression evaluation for WHERE conditions and SET clauses.
- `DELETE FROM ... WHERE ...` statements with Python-based condition evaluation.

### Changed
- SQL executor now handles DDL/DML operations by directly interacting with the storage backend, bypassing DataFrame expression translation for complex SQL operations.
- Improved error handling in SQL operations with proper exception types and messages.

### Fixed
- Fixed recursion error in `DataFrame._project_schema_with_operations` by using `_schema` directly instead of the `schema` property.
- Fixed `UnboundLocalError` in SQL executor by removing shadowing local imports of `StructType`.
- Removed unused imports and improved code quality with ruff linting fixes.

### Documentation
- Updated SQL executor docstrings to reflect full DDL/DML implementation status.
- README "Recent Updates" highlights the new SQL DDL/DML capabilities.

## 3.6.0 — 2025-11-13

### Added
- Feature-flagged profiling utilities in `mock_spark.utils.profiling`, with Polars execution and
  expression hot paths instrumented via lightweight decorators.
- Optional native pandas backend selection through `MOCK_SPARK_PANDAS_MODE`, including a benchmarking
  harness at `scripts/benchmark_pandas_fallback.py`.

### Changed
- The query optimizer now supports adaptive execution simulation, inserting configurable
  `REPARTITION` operations when skew metrics indicate imbalanced workloads.

### Documentation
- Published performance guides covering hot-path profiling (`docs/performance/profiling.md`) and
  pandas fallback benchmarking (`docs/performance/pandas_fallback.md`).
- README “Recent Updates” highlights the profiling, adaptive execution, and pandas backend features.

## 3.5.0 — 2025-11-13

### Added
- Session-aware helper functions in `mock_spark.functions`: `current_catalog`, `current_database`,
  `current_schema`, and `current_user`, plus a dynamic `call_function` dispatcher.
- Regression tests covering the new helpers and dynamic dispatch, ensuring PySpark-compatible error
  handling.
- Pure-Python statistical fallbacks (`percentile`, `covariance`) to remove the dependency on native
  wheels when running documentation and compatibility suites.

### Changed
- The Polars storage backend and `UnifiedStorageManager` now track the active schema so
  `setCurrentDatabase` updates propagate end-to-end.
- `SparkContext.sparkUser()` mirrors PySpark’s context helper, allowing the new literal functions to
  surface the current user.

### Documentation
- README and quick-start docs updated for version 3.5.0 and the session-aware catalogue features.
- Internal upgrade summary documents the stability improvements and successful full-suite run.

## 3.4.0 — 2025-11-12

### Changed
- Standardised local workflows around `bash tests/run_all_tests.sh`, Ruff, and MyPy via updated Makefile targets and `install.sh`.
- Introduced GitHub Actions CI that enforces linting, type-checking, and full-suite coverage on every push and pull request.
- Refreshed development docs to reflect the consolidated tooling commands.

### Added
- Published `plans/typing_delta_roadmap.md`, outlining phased mypy cleanup and Delta feature milestones for the next release cycle.

### Documentation
- README “Recent Updates” highlights the 3.4.0 workflow improvements and roadmap visibility.

## 3.3.0 — 2025-11-12

### Added
- Consolidated release metadata so `pyproject.toml`, `mock_spark/__init__.py`, and published wheels all advertise version `3.3.0`.
- Documented the renumbering from the legacy 3.x preview series to the semantic 0.x roadmap, keeping downstream consumers aligned with public messaging.
- Updated README badges and compatibility tables to reflect the curated 396-test suite and PySpark 3.2–3.5 coverage.

### Changed
- Finalised the migration to Python 3.9-native typing throughout the Polars executor,
  DataFrame reader/writer, schema manager, and Delta helpers so that `mypy mock_spark`
  now completes without suppressions.
- Consolidated type-only imports behind `TYPE_CHECKING` guards, reducing import
  overhead while keeping tooling visibility intact.

### Fixed
- Ensured Python-evaluated projection columns always materialise with string aliases,
  preventing accidental `None` column names when fallback expressions run outside Polars.
- Normalised optional alias handling inside the Delta merge builder, avoiding runtime
  `None` lookups when accessing assignment metadata.

### Documentation
- README “Recent Updates” highlights the metadata realignment for 3.3.0 and the clean `mypy` status.
- Refreshed version references to 3.3.0 across project metadata.

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

