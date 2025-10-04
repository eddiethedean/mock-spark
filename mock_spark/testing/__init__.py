"""
Testing utilities for Mock Spark.

This module provides comprehensive testing infrastructure including fixtures,
generators, simulators, and mock utilities for testing Mock Spark components.
"""

from .fixtures import *
from .generators import *
from .simulators import *
from .mocks import *
from .factories import *

__all__ = [
    # Fixtures
    "MockSparkSessionFixture",
    "DataFrameFixture",
    "SchemaFixture",
    
    # Generators
    "DataGenerator",
    "TestDataBuilder",
    "RealisticDataGenerator",
    "EdgeCaseDataGenerator",
    
    # Simulators
    "ErrorSimulator",
    "PerformanceSimulator",
    "MemorySimulator",
    
    # Mocks
    "MockDataFrameFactory",
    "MockSessionFactory",
    "MockFunctionFactory",
    "MockStorageFactory",
    "MockSchemaFactory",
    
    # Factories
    "DataFrameTestFactory",
    "SessionTestFactory",
    "FunctionTestFactory",
    "IntegrationTestFactory",
    
    # Convenience functions
    "create_test_session",
    "create_test_dataframe",
    "create_test_storage",
    "create_test_data",
    "create_realistic_data",
    "create_edge_case_data",
    "create_comprehensive_test_session",
    "create_benchmark_test_suite",
]
