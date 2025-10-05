"""
Test factories for Mock Spark.

This module provides comprehensive test factories for creating
various test scenarios including DataFrame, Session, Function, and Integration tests.

Key Features:
    - DataFrame test scenarios (performance, stress, join, window)
    - Session test scenarios (tables, configs, storage)
    - Function test scenarios (arithmetic, comparison, string, aggregate)
    - Integration test scenarios (E2E pipelines, performance benchmarks)
    - Error handling test scenarios

Example:
    >>> from mock_spark.testing.factories import DataFrameTestFactory
    >>> df = DataFrameTestFactory.create_stress_test_dataframe(session)
    >>> stress_data = df.collect()
"""

# Import from the new modular structure
from .factories import (
    DataFrameTestFactory,
    SessionTestFactory,
    FunctionTestFactory,
    IntegrationTestFactory,
)
