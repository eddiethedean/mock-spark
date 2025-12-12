"""Tests for backend selection and configuration."""

import os

import pytest

from mock_spark import SparkSession
from mock_spark.backend.factory import BackendFactory


@pytest.fixture(autouse=True)
def reset_singleton():
    original_env = os.environ.get("MOCK_SPARK_BACKEND")
    try:
        SparkSession._singleton_session = None
        yield
    finally:
        SparkSession._singleton_session = None
        if original_env is None:
            os.environ.pop("MOCK_SPARK_BACKEND", None)
        else:
            os.environ["MOCK_SPARK_BACKEND"] = original_env


def test_default_backend_is_polars():
    spark = SparkSession()
    assert spark.backend_type == "polars"
    assert BackendFactory.get_backend_type(spark._storage) == "polars"


def test_env_var_overrides_backend(monkeypatch):
    monkeypatch.setenv("MOCK_SPARK_BACKEND", "memory")
    spark = SparkSession()
    assert spark.backend_type == "memory"
    assert BackendFactory.get_backend_type(spark._storage) == "memory"


def test_builder_config_overrides_backend():
    spark = SparkSession.builder.config("spark.mock.backend", "file").getOrCreate()
    assert spark.backend_type == "file"
    assert BackendFactory.get_backend_type(spark._storage) == "file"


def test_invalid_backend_raises_value_error():
    with pytest.raises(ValueError) as excinfo:
        SparkSession(backend_type="unknown")
    assert "Unsupported backend type" in str(excinfo.value)
