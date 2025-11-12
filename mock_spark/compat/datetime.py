"""Datetime compatibility helpers for Mock Spark.

These helpers normalise date and timestamp outputs when running on the
Polars-backed Mock Spark engine, while acting as no-ops under real PySpark.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, MutableMapping, Sequence
from datetime import date, datetime
from typing import Union, cast

try:  # Python <3.10 support
    from typing import TypeAlias  # type: ignore[attr-defined]
except ImportError:  # pragma: no cover - fallback for older versions
    from typing_extensions import TypeAlias

try:  # pragma: no cover - optional dependency
    from pyspark.sql.column import Column as PySparkColumn  # type: ignore
except Exception:  # pragma: no cover - PySpark not available in Mock Spark env
    PySparkColumn = None

from mock_spark.functions import Functions as F
from mock_spark.functions.base import Column, ColumnOperation
from mock_spark.spark_types import Row

ColumnLike: TypeAlias = Union[str, Column, ColumnOperation]
RowLike: TypeAlias = Union[Row, Mapping[str, object]]


def _is_pyspark_column(column: object) -> bool:
    if PySparkColumn is None:
        return False
    return isinstance(column, PySparkColumn)


def _ensure_to_date_operation(column: ColumnLike) -> ColumnOperation:
    if isinstance(column, ColumnOperation):
        if getattr(column, "operation", None) == "to_date":
            return column
        raise TypeError(
            "Expected a to_date ColumnOperation when passing ColumnOperation to to_date_str"
        )
    return F.to_date(column)


def _ensure_to_timestamp_operation(
    column: ColumnLike, fmt: str | None
) -> ColumnOperation:
    if isinstance(column, ColumnOperation):
        if getattr(column, "operation", None) == "to_timestamp":
            return column
        raise TypeError(
            "Expected a to_timestamp ColumnOperation when passing ColumnOperation to to_timestamp_str"
        )
    if fmt is None:
        return F.to_timestamp(column)
    return F.to_timestamp(column, fmt)


def _derive_to_date_alias(column: ColumnLike) -> str:
    if isinstance(column, ColumnOperation):
        if getattr(column, "operation", None) == "to_date":
            return column.name
        return f"to_date({column.name})"
    if isinstance(column, Column):
        return f"to_date({column.name})"
    return f"to_date({column})"


def _java_to_polars_date_format(fmt: str) -> str:
    mapping = {
        "yyyy": "%Y",
        "yy": "%y",
        "MM": "%m",
        "dd": "%d",
    }
    result = fmt
    for java_token, polars_token in sorted(mapping.items(), key=lambda item: len(item[0]), reverse=True):
        result = result.replace(java_token, polars_token)
    return result


def to_date_str(
    column: ColumnLike, fmt: str = "yyyy-MM-dd"
) -> ColumnOperation | ColumnLike:
    """Ensure ``to_date`` results are represented as ISO-8601 strings.

    When running under PySpark, the function returns the original column to avoid
    behavioural drift. Under Mock Spark, the column is formatted using
    ``date_format`` while preserving the original alias.
    """

    if _is_pyspark_column(column):  # type: ignore[arg-type]
        return column

    to_date_op = _ensure_to_date_operation(column)
    formatted = F.date_format(cast("Column", to_date_op), fmt)
    return formatted.alias(to_date_op.name)


def to_timestamp_str(
    column: ColumnLike,
    fmt: str = "yyyy-MM-dd HH:mm:ss",
    source_format: str | None = None,
) -> ColumnOperation | ColumnLike:
    """Format ``to_timestamp`` outputs as strings when using Mock Spark."""

    if _is_pyspark_column(column):  # type: ignore[arg-type]
        return column

    to_timestamp_op = _ensure_to_timestamp_operation(column, source_format)
    formatted = F.date_format(cast("Column", to_timestamp_op), fmt)
    return formatted.alias(to_timestamp_op.name)


def to_date_typed(
    column: ColumnLike,
    fmt: str = "yyyy-MM-dd",
    *,
    source_format: str | None = None,
) -> ColumnOperation | ColumnLike:
    """Return a typed ``date`` column while tolerating Mock Spark quirks.

    Real PySpark already returns proper ``date`` objects for ISO-formatted strings,
    so we delegate to the native ``F.to_date`` implementation there. Under Mock Spark
    we normalise the input via :func:`to_date_str` (which produces a consistent ISO
    string backed by ``date_format``) and then round-trip it through ``to_date`` with
    an explicit format specifier. When a ``source_format`` is provided we parse the
    input via ``to_timestamp`` first (allowing datetime strings that include time
    components) before casting back to a date. This mirrors the behaviour of real
    Spark without requiring downstream code to fall back to manual string slicing.
    """

    if _is_pyspark_column(column):  # type: ignore[arg-type]
        try:
            from pyspark.sql import functions as pyspark_f  # type: ignore
        except Exception:  # pragma: no cover - PySpark not available
            return column
        if source_format:
            return pyspark_f.to_date(pyspark_f.to_timestamp(column, source_format))  # type: ignore[arg-type]
        return pyspark_f.to_date(column)  # type: ignore[arg-type]

    base_alias = _derive_to_date_alias(column)

    if source_format:
        timestamp_op = _ensure_to_timestamp_operation(column, source_format)
        typed = timestamp_op.cast("date")
        return typed.alias(base_alias)

    if isinstance(column, ColumnOperation) and getattr(column, "operation", None) == "to_date":
        typed = column
    else:
        if fmt is not None:
            polars_fmt = _java_to_polars_date_format(fmt)
            typed = F.to_date(column, polars_fmt)
        else:
            typed = F.to_date(column)
    # Preserve the original alias/name to avoid churn in downstream expectations.
    return typed.alias(base_alias)


def normalize_date_value(value: object) -> str | None:
    """Convert Python ``date`` values to ISO strings while leaving others intact."""

    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value.isoformat()
    return value  # type: ignore[return-value]


def normalize_timestamp_value(
    value: object, fmt: str = "%Y-%m-%d %H:%M:%S"
) -> str | None:
    """Convert Python ``datetime`` values to formatted strings."""

    if value is None:
        return None
    if isinstance(value, datetime):
        return value.strftime(fmt)
    return value  # type: ignore[return-value]


def normalize_collected_datetimes(
    rows: Sequence[RowLike],
    *,
    date_columns: Iterable[str] | None = None,
    timestamp_columns: Iterable[str] | None = None,
    timestamp_format: str = "%Y-%m-%d %H:%M:%S",
) -> list[MutableMapping[str, str | None]]:
    """Normalise collected Row objects containing date/timestamp values.

    Returns a list of dictionaries to make downstream assertions (especially in
    snapshot-based tests) stable across engines.
    """

    date_cols = list(date_columns or [])
    ts_cols = list(timestamp_columns or [])

    normalized: list[MutableMapping[str, str | None]] = []

    for row in rows:
        if isinstance(row, Mapping):
            data = dict(row)
        else:
            try:
                data = row.asDict(recursive=True)
            except TypeError:
                data = row.asDict()

        for col in date_cols:
            if col in data:
                data[col] = normalize_date_value(data.get(col))
        for col in ts_cols:
            if col in data:
                data[col] = normalize_timestamp_value(data.get(col), timestamp_format)

        normalized.append(data)

    return normalized
