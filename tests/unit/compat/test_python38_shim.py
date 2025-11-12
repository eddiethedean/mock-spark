"""Unit tests for the Python 3.8 compatibility shim."""

from __future__ import annotations

import types

import pytest

import sitecustomize


def test_apply_shim_backfills_missing_features(monkeypatch):
    fake_typing = types.SimpleNamespace()

    class FakeMapping:
        pass

    fake_collections = types.SimpleNamespace(Mapping=FakeMapping)
    modules: dict[str, types.ModuleType] = {}

    def raise_module_not_found(name: str) -> None:
        raise ModuleNotFoundError(name)

    monkeypatch.setattr(
        sitecustomize.importlib, "import_module", raise_module_not_found
    )

    sitecustomize._apply_shim(
        typing_module=fake_typing,
        collections_module=fake_collections,
        modules=modules,
        version_info=(3, 8, 18),
    )

    assert hasattr(fake_typing, "TypeAlias")
    assert fake_collections.Mapping[int, str] is FakeMapping
    assert "mock_spark.backend.duckdb" in modules


def test_patch_collections_is_idempotent():
    class Sample:
        pass

    sitecustomize._patch_collections_abc(
        types.SimpleNamespace(Mapping=Sample), targets=["Mapping"]
    )
    # Second invocation should not fail after attribute exists.
    sitecustomize._patch_collections_abc(
        types.SimpleNamespace(Mapping=Sample), targets=["Mapping"]
    )

    assert Sample[str] is Sample


@pytest.mark.parametrize("version_info", [(3, 9, 0), (3, 10, 12), (3, 11, 9)])
def test_apply_shim_skips_newer_pythons(version_info):
    fake_typing = types.SimpleNamespace()

    class FakeMapping:
        pass

    fake_collections = types.SimpleNamespace(Mapping=FakeMapping)
    modules: dict[str, types.ModuleType] = {}

    sitecustomize._apply_shim(
        typing_module=fake_typing,
        collections_module=fake_collections,
        modules=modules,
        version_info=version_info,
    )

    assert not hasattr(fake_typing, "TypeAlias")
    assert not hasattr(FakeMapping, "__class_getitem__")
    assert "mock_spark.backend.duckdb" not in modules

