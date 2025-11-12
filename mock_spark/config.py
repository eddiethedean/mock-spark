"""Global configuration helpers for Mock Spark."""

import os
from typing import Optional

DEFAULT_BACKEND = "polars"
ENV_BACKEND_KEY = "MOCK_SPARK_BACKEND"


def resolve_backend_type(explicit_backend: Optional[str] = None) -> str:
    """Resolve the backend type using overrides, environment variables, and defaults."""

    candidate = explicit_backend or os.getenv(ENV_BACKEND_KEY) or DEFAULT_BACKEND
    candidate_normalized = candidate.strip().lower()

    from mock_spark.backend.factory import BackendFactory

    BackendFactory.validate_backend_type(candidate_normalized)
    return candidate_normalized
