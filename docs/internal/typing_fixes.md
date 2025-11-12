# Typing Remediation Tracker

## Baseline (2025-11-07)

Command: `mypy mock_spark tests` (captured in `mypy-baseline.log`)

### Error Themes

- **Type aliases & helpers** – invalid runtime aliases and missing generics in
  `mock_spark/compat/datetime.py`, `mock_spark/backend/polars/schema_registry.py`,
  `tests/tools/*`, etc. (~35 findings)
- **Interface contract drift** – mixins overriding abstract methods with
  incompatible signatures across `mock_spark/core/interfaces`,
  `mock_spark/functions/core`, and `mock_spark/dataframe/*`. (>120 findings)
- **Optional / None handling** – unguarded optionals and union misuse in
  `mock_spark/dataframe/lazy.py`, `grouped/base.py`, and the expression
  evaluator. (~40 findings)
- **Missing annotations & attr-defined** – untyped vars/functions and mistaken
  attribute usage in storage/materializer modules and tests. (~15 findings)

### Next Steps

1. Normalize aliases and helper annotations.
2. Align interface and mixin signatures with their abstractions.
3. Harden optional handling hotspots.
4. Iterate on residual errors and wire mypy into CI.

Status will be updated as each theme is addressed.

## Progress Log

- **2025-11-07** — Normalised type aliases in `mock_spark/compat/datetime.py`
  (now using standard 3.9 generics without runtime fallbacks) and cleaned helper annotations in
  `tests/tools/` and `mock_spark/backend/polars/{schema_registry,storage}.py`.
  Refreshed `mypy-baseline.log` after changes (still pending interface/optional
  work).

