"""Python 3.8 compatibility shim for Mock Spark.

The interpreter loads this module automatically (via ``sitecustomize``) if it is
present on ``PYTHONPATH``. We use it to backfill minimal features that Mock Spark
expects from Python 3.9+ so that the package continues to bootstrap under Python
3.8 installations that cannot upgrade yet.
"""

from __future__ import annotations

from collections import abc as collections_abc
import importlib
import os
import sys
from types import ModuleType
from typing import Iterable, MutableMapping, Tuple

_ABC_TARGETS: Tuple[str, ...] = (
    "Mapping",
    "MutableMapping",
    "Sequence",
    "MutableSequence",
    "Set",
    "MutableSet",
)


def _patch_typing(typing_module: ModuleType) -> None:
    """Ensure ``typing.TypeAlias`` exists (Python <3.10 compatibility)."""

    if hasattr(typing_module, "TypeAlias"):
        return

    try:
        from typing_extensions import TypeAlias as _TypeAlias  # type: ignore
    except Exception:  # pragma: no cover - typing_extensions missing
        class _TypeAlias(str):  # type: ignore[override]
            """Fallback sentinel used when typing_extensions is unavailable."""

            def __repr__(self) -> str:
                return "TypeAlias"

        typing_module.TypeAlias = _TypeAlias  # type: ignore[attr-defined]
    else:
        typing_module.TypeAlias = _TypeAlias  # type: ignore[attr-defined]


def _install_class_getitem(target: type) -> None:
    if hasattr(target, "__class_getitem__"):
        return

    def _class_getitem(cls: type, _: object) -> type:
        return cls

    target.__class_getitem__ = classmethod(_class_getitem)  # type: ignore[attr-defined]


def _patch_collections_abc(
    module: ModuleType, targets: Iterable[str] = _ABC_TARGETS
) -> None:
    """Add ``__class_getitem__`` to selected ABCs when missing."""

    for name in targets:
        target = getattr(module, name, None)
        if target is not None:
            _install_class_getitem(target)


def _ensure_duckdb_stub(modules: MutableMapping[str, ModuleType]) -> None:
    """Provide a lightweight stub for ``mock_spark.backend.duckdb`` if absent."""

    if "mock_spark.backend.duckdb" in modules:
        return

    try:
        importlib.import_module("mock_spark.backend.duckdb")
    except ModuleNotFoundError:
        stub = ModuleType("mock_spark.backend.duckdb")
        stub.__all__ = []  # type: ignore[attr-defined]
        modules["mock_spark.backend.duckdb"] = stub

        backend_module = modules.get("mock_spark.backend")
        if backend_module is not None:
            setattr(backend_module, "duckdb", stub)


def _apply_shim(
    *,
    typing_module: ModuleType | None = None,
    collections_module: ModuleType | None = None,
    modules: MutableMapping[str, ModuleType] | None = None,
    version_info: tuple[int, ...] | None = None,
) -> None:
    """Apply the compatibility shims when running on Python 3.8."""

    if version_info is None:
        version_info = sys.version_info
    if version_info >= (3, 9):
        # Newer versions natively support the features we patch in.
        return

    import typing as typing_mod  # Local import to avoid shadowing

    typing_module = typing_module if typing_module is not None else typing_mod
    collections_module = (
        collections_module if collections_module is not None else collections_abc
    )
    modules = modules if modules is not None else sys.modules

    _patch_typing(typing_module)
    _patch_collections_abc(collections_module)
    _ensure_duckdb_stub(modules)


# Execute automatically unless explicitly disabled (useful for tests).
if not os.environ.get("MOCK_SPARK_DISABLE_PY38_SHIM"):
    _apply_shim()


__all__ = [
    "_apply_shim",
    "_patch_typing",
    "_patch_collections_abc",
    "_ensure_duckdb_stub",
]

