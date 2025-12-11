# Changelog

## 3.11.0 — 2025-12-10

### Added
- Lazy evaluation for session-aware functions (`current_database()`, `current_schema()`, `current_user()`, `current_catalog()`)
  - Literals now resolve session state at evaluation time, not creation time, matching PySpark behavior
  - Session-aware functions properly reflect the active session's catalog state during DataFrame operations
- Session validation for all functions requiring an active SparkSession
  - Functions now validate session availability at creation time (matching PySpark error behavior)
  - Improved error messages for missing session scenarios
- Comprehensive test coverage for session isolation and validation
  - `test_sparkcontext_validation.py` - validates session dependency requirements
  - `test_column_availability.py` - tests column materialization behavior
  - `test_fixture_compatibility.py` - verifies fixture/setup compatibility
  - `test_function_api_compatibility.py` - validates function API signatures
  - `test_type_strictness.py` - tests strict type checking for datetime functions

### Changed
- Session-aware functions now use lazy literal resolution via resolver functions
- Expression evaluator and Polars translator updated to resolve lazy literals during evaluation
- Improved type annotations: replaced `callable` with `Callable[[], Any]` for better mypy compatibility
- Enhanced `SparkColumnNotFoundError` with optional custom message support

### Fixed
- Fixed `test_current_helpers_are_session_isolated` to properly capture original session before `newSession()`
- Fixed type checking issues in `TransformationOperations` mixin for `_validate_operation_types` method
- Fixed strict type validation for `to_timestamp()` and `to_date()` to accept both StringType and native types (TimestampType/DateType)
- Fixed column availability tracking to correctly update after DataFrame materialization
- Fixed lazy literal evaluation to resolve session state dynamically at evaluation time

### Testing
- All 1209 tests passing (46 skipped)
- All files pass mypy type checking with Python 3.11
- All files pass ruff format and lint checks
- Code coverage: 50% overall

## 3.10.0 — 2025-01-XX

### Added
- Comprehensive type safety improvements across the codebase
- Improved protocol type definitions with Union types instead of Any
- Enhanced type annotations for better IDE support and static analysis

### Changed
- Aligned mypy.ini configuration with pyproject.toml settings
- Replaced `Any` type aliases with proper Union types for `ColumnExpression`, `AggregateExpression`
- Improved protocol method signatures to use `ColumnExpression` instead of `Any`
- Enhanced TypeConverter return types from `Any` to Union types
- Removed module-level error ignoring for `display.operations`, `joins.operations`, and `operations.misc`

### Fixed
- Fixed type ignore comments with proper type narrowing in datetime functions
- Fixed SQLAlchemy helper function return types
- Fixed PySpark compatibility layer typing issues
- Fixed `timestamp_seconds()` to preserve Literal objects correctly
- Improved type safety in display, join, and misc operations modules

### Testing
- All tests passing (1095 passed, 47 skipped)
- All modified files pass mypy type checking
- Code formatted and linted with ruff

## 3.9.1 — 2025-01-XX

### Fixed
- Fixed timezone handling in `from_unixtime()` and `timestamp_seconds()` functions to interpret Unix timestamps as UTC and convert to local timezone, matching PySpark behavior.
- Fixed CI performance test job to handle cases where no performance tests are found (exit code 5).

### Changed
- Improved CI test execution with parallel test runs using pytest-xdist, significantly reducing CI execution time.
- Added `pytest-xdist>=3.0.0` to dev dependencies for parallel test execution.

### Testing
- All compatibility tests now passing with proper timezone handling.
- CI tests now run in parallel across 4 test groups (unit, compatibility, performance, documentation).

## 3.9.0 — 2025-12-02

### Added
- Complete implementation of all 11 window functions with proper partitioning and ordering support:
  - `row_number()`, `rank()`, `dense_rank()` - ranking functions
  - `cume_dist()`, `percent_rank()` - distribution functions
  - `lag()`, `lead()` - offset functions
  - `first_value()`, `last_value()`, `nth_value()` - value functions
  - `ntile()` - bucket function
- Python fallback mechanism for window functions not natively supported in Polars backend (cume_dist, percent_rank, nth_value, ntile).
- Enhanced window function evaluation with proper tie handling for rank-based calculations.

### Fixed
- Fixed `nth_value()` to return NULL for rows before the nth position, matching PySpark behavior.
- Fixed `cume_dist()` and `percent_rank()` calculations to correctly handle ties using rank-based calculations.
- Fixed window function results alignment when DataFrame is sorted after evaluation.
- Fixed mypy type error in `MiscellaneousOperations` by accessing columns via schema instead of direct property access.
- Fixed syntax errors in `window_execution.py` that prevented proper module import.

### Changed
- Window functions now use Python evaluation fallback when Polars backend doesn't support them, ensuring correct PySpark-compatible behavior.
- Improved window function partitioning and ordering logic to handle edge cases (single-row partitions, ties, etc.).

### Testing
- All 11 window function compatibility tests now passing (previously 7 passing, 4 skipped).
- Full test suite: 1088 tests passing with 47 expected skips.

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

