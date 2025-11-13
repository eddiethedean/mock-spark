"""
Lightweight stub or optional delegation to real pandas for Mock Spark testing.

By default Mock Spark uses an internal stub to avoid pulling the heavy pandas +
NumPy dependency tree.  Set the `MOCK_SPARK_PANDAS_MODE` environment variable to
`native` (or `auto` when pandas is installed) to delegate to the real pandas
package instead.
"""

from __future__ import annotations

import importlib
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Sequence
else:  # pragma: no cover - runtime placeholders
    Iterable = Iterator = Sequence = object

__all__ = ["DataFrame", "concat", "get_backend", "using_native_pandas"]


def _load_native_pandas() -> Any:
    """Attempt to import the real pandas package."""

    return importlib.import_module("pandas")


_mode = os.getenv("MOCK_SPARK_PANDAS_MODE", "stub").strip().lower()
_native_module: Any | None = None
_BACKEND = "stub"
__version__ = "0.0.0-mock"

if _mode in {"native", "real"}:
    try:
        _native_module = _load_native_pandas()
    except ModuleNotFoundError as exc:  # pragma: no cover - explicit failure path
        raise ModuleNotFoundError(
            "MOCK_SPARK_PANDAS_MODE is set to 'native' but pandas is not installed. "
            "Install it via `pip install mock-spark[pandas]`."
        ) from exc
    else:
        _BACKEND = "native"
elif _mode == "auto":
    try:
        _native_module = _load_native_pandas()
    except ModuleNotFoundError:
        _native_module = None
    else:  # pragma: no cover - only exercised when pandas is available in CI
        _BACKEND = "native"
else:
    _native_module = None
    _BACKEND = "stub"


if _native_module is not None:
    DataFrame = _native_module.DataFrame  # type: ignore[attr-defined]
    concat = _native_module.concat  # type: ignore[attr-defined]
    __version__ = getattr(_native_module, "__version__", "unknown")
else:

    @dataclass
    class _RowAdapter:
        data: dict

        def __getitem__(self, key: str) -> object:
            return self.data[key]

    class _ILocAdapter:
        def __init__(self, rows: Sequence[dict]):
            self._rows = rows

        def __getitem__(self, index):
            if isinstance(index, slice):
                return [_RowAdapter(row) for row in self._rows[index]]
            return _RowAdapter(self._rows[index])

    class DataFrame:
        """
        Extremely small subset of pandas.DataFrame required for tests.
        """

        def __init__(self, data: object = None, columns: Sequence[str] | None = None):
            if data is None:
                data = []

            if isinstance(data, DataFrame):
                rows = [dict(row) for row in data._rows]
                inferred_columns = list(data._columns)
            elif isinstance(data, list):
                if data and isinstance(data[0], dict):
                    rows = [dict(row) for row in data]
                    inferred_columns = list(rows[0].keys())
                elif columns is not None:
                    rows = []
                    for entry in data:
                        rows.append(
                            {col: entry[idx] for idx, col in enumerate(columns)}
                        )
                    inferred_columns = list(columns)
                else:
                    raise ValueError(
                        "columns must be provided when constructing DataFrame from sequences"
                    )
            elif isinstance(data, dict):
                inferred_columns = (
                    list(columns) if columns is not None else list(data.keys())
                )
                max_len = max((len(values) for values in data.values()), default=0)
                rows = []
                for i in range(max_len):
                    row = {}
                    for col in inferred_columns:
                        values = data.get(col, [])
                        row[col] = values[i] if i < len(values) else None
                    rows.append(row)
            else:
                raise TypeError(
                    f"Unsupported data type for stub DataFrame: {type(data)!r}"
                )

            if columns is None:
                columns = inferred_columns

            self._columns = list(columns)
            self._rows = [{col: row.get(col) for col in self._columns} for row in rows]

        def to_dict(self, orient: str = "records") -> list[dict]:
            if orient != "records":
                raise NotImplementedError(
                    "stub pandas.DataFrame only supports records orient"
                )
            return [dict(row) for row in self._rows]

        @property
        def columns(self) -> list[str]:
            return list(self._columns)

        def __len__(self) -> int:
            return len(self._rows)

        @property
        def iloc(self) -> _ILocAdapter:
            return _ILocAdapter(self._rows)

        def __iter__(self) -> Iterator[dict]:
            return iter(self._rows)

    def concat(
        objects: Iterable[DataFrame],
        ignore_index: bool = False,
    ) -> DataFrame:
        rows = []
        columns: list[str] | None = None
        for df in objects:
            if not isinstance(df, DataFrame):
                raise TypeError("stub pandas.concat expects DataFrame instances")
            if columns is None:
                columns = list(df._columns)
            rows.extend(df._rows)
        if columns is None:
            columns = []
        result = DataFrame(rows, columns=columns)
        if ignore_index:
            # Index semantics are ignored in this stub implementation.
            pass
        return result


def get_backend() -> str:
    """Return the active pandas backend (`'stub'` or `'native'`)."""

    return _BACKEND


def using_native_pandas() -> bool:
    """Convenience boolean for checking whether native pandas is active."""

    return _BACKEND == "native"
