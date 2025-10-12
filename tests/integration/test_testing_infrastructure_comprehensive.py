"""
Comprehensive Testing Infrastructure Tests for Mock Spark

This module provides comprehensive tests for testing utilities, fixtures,
data generation, and mock objects to improve coverage and validate testing infrastructure.
"""

import pytest
import tempfile
import os
from unittest.mock import patch, MagicMock, Mock
from mock_spark import MockSparkSession, F
from mock_spark.testing.factories import (
    DataFrameTestFactory,
    SessionTestFactory,
    FunctionTestFactory,
    IntegrationTestFactory,
)
from mock_spark.testing.mocks import (
    MockDataFrameFactory,
    MockSessionFactory,
    MockFunctionFactory,
    MockStorageFactory,
    MockSchemaFactory,
)
from mock_spark.testing.simulators import ErrorSimulator, PerformanceSimulator
from mock_spark.testing.generators import DataGenerator
from mock_spark.testing.fixtures import (
    mock_spark_session,
    simple_dataframe,
    empty_dataframe,
    large_dataframe,
    dataframe_with_nulls,
)
from mock_spark.spark_types import (
    MockStructType,
    MockStructField,
    StringType,
    IntegerType,
    DoubleType,
    BooleanType,
)
from mock_spark.data_generation import MockDataGenerator as CoreDataGenerator


class TestTestingInfrastructureComprehensive:
    """Comprehensive testing infrastructure tests."""

    def test_mock_dataframe_factory_comprehensive(self):
        """Test comprehensive MockDataFrameFactory functionality."""
        factory = MockDataFrameFactory()
        spark = MockSparkSession()

        # Test simple DataFrame creation
        simple_df = factory.create_simple_dataframe(spark)
        assert simple_df.count() > 0
        assert "id" in simple_df.columns
        assert "name" in simple_df.columns

        # Test empty DataFrame creation
        empty_df = factory.create_empty_dataframe(spark)
        assert empty_df.count() == 0

        # Test DataFrame with nulls
        null_df = factory.create_dataframe_with_nulls(spark)
        assert null_df.count() > 0

        # Test large DataFrame creation
        large_df = factory.create_large_dataframe(spark, 1000)
        assert large_df.count() == 1000

    def test_mock_session_factory_comprehensive(self):
        """Test comprehensive MockSessionFactory functionality."""
        factory = MockSessionFactory()

        # Test default session creation
        session = factory.create_default_session()
        assert session is not None
        assert hasattr(session, "createDataFrame")

        # Test session with custom config
        config = {"spark.app.name": "test_app"}
        custom_session = factory.create_session_with_config(config)
        assert custom_session is not None

        # Test session cleanup
        custom_session.stop()

    def test_mock_function_factory_comprehensive(self):
        """Test comprehensive MockFunctionFactory functionality."""
        factory = MockFunctionFactory()

        # Test column creation
        col_func = factory.create_column("test_col")
        assert col_func is not None

        # Test literal creation
        lit_func = factory.create_literal("test_value")
        assert lit_func is not None

    def test_mock_storage_factory_comprehensive(self):
        """Test comprehensive MockStorageFactory functionality."""
        factory = MockStorageFactory()

        # Test memory storage creation
        memory_storage = factory.create_memory_storage()
        assert memory_storage is not None

        # Test storage operations
        schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType()),
            ]
        )
        memory_storage.create_table("default", "test_table", schema)
        assert memory_storage.table_exists("default", "test_table")

    def test_mock_schema_factory_comprehensive(self):
        """Test comprehensive MockSchemaFactory functionality."""
        factory = MockSchemaFactory()

        # Test simple schema creation
        simple_schema = factory.create_simple_schema()
        assert simple_schema is not None
        assert hasattr(simple_schema, "fields")

    def test_data_generator_comprehensive(self):
        """Test comprehensive DataGenerator functionality."""
        generator = DataGenerator()

        # Test string generation
        string_val = generator.generate_string()
        assert isinstance(string_val, str)
        assert len(string_val) > 0

        # Test integer generation
        int_val = generator.generate_integer()
        assert isinstance(int_val, int)

        # Test long generation
        long_val = generator.generate_long()
        assert isinstance(long_val, int)

        # Test double generation
        double_val = generator.generate_double()
        assert isinstance(double_val, float)

        # Test boolean generation
        bool_val = generator.generate_boolean()
        assert isinstance(bool_val, bool)

    def test_dataframe_test_factory_comprehensive(self):
        """Test comprehensive DataFrameTestFactory functionality."""
        factory = DataFrameTestFactory()
        spark = MockSparkSession()

        # Test performance test DataFrame
        perf_df = factory.create_performance_test_dataframe(spark)
        assert perf_df.count() > 0

        # Test stress test DataFrame
        stress_df = factory.create_stress_test_dataframe(spark)
        assert stress_df.count() > 0

    def test_error_simulator_comprehensive(self):
        """Test comprehensive ErrorSimulator functionality."""
        simulator = ErrorSimulator()

        # Test error simulation creation
        assert simulator is not None

        # Test error rule addition
        simulator.add_error_rule("test_operation", ValueError, "Test error")
        assert "test_operation" in simulator.error_rules

        # Test error simulation - use "always" condition to ensure error is raised
        simulator.add_error_rule("always", ValueError, "Always error")
        error = simulator.should_raise_error("any_operation")
        assert error is not None
        assert isinstance(error, ValueError)

    def test_performance_simulator_comprehensive(self):
        """Test comprehensive PerformanceSimulator functionality."""
        simulator = PerformanceSimulator()

        # Test performance simulation creation
        assert simulator is not None

        # Test performance rule addition
        simulator.add_slowdown("test_operation", 0.1)
        assert "test_operation" in simulator.performance_rules

        # Test operation simulation
        def test_func():
            return "test_result"

        result = simulator.simulate_operation("test_operation", test_func)
        assert result == "test_result"

    def test_mock_objects_comprehensive(self):
        """Test comprehensive mock objects functionality."""
        # Test that we can create mock objects through factories
        spark = MockSparkSession()
        factory = MockDataFrameFactory()

        # Test DataFrame creation through factory
        df = factory.create_simple_dataframe(spark)
        assert df is not None
        assert hasattr(df, "count")
        assert hasattr(df, "columns")

        # Test column creation through factory
        func_factory = MockFunctionFactory()
        col = func_factory.create_column("test_col")
        assert col is not None
        assert hasattr(col, "name")
        assert hasattr(col, "alias")

    def test_testing_utility_integration(self):
        """Test integration of testing utilities."""
        # Test factory integration
        session_factory = MockSessionFactory()
        df_factory = MockDataFrameFactory()

        session = session_factory.create_default_session()
        df = df_factory.create_simple_dataframe(session)

        assert df is not None
        assert df.count() > 0

    def test_performance_testing_utilities(self):
        """Test performance testing utilities."""
        perf_sim = PerformanceSimulator()

        # Test performance rule configuration
        perf_sim.add_slowdown("test_op", 0.01)

        def test_function():
            return "success"

        result = perf_sim.simulate_operation("test_op", test_function)
        assert result == "success"

    def test_error_simulation_utilities(self):
        """Test error simulation utilities."""
        error_sim = ErrorSimulator()

        # Test error rule configuration
        error_sim.add_error_rule("always", ValueError, "Test error")

        error = error_sim.should_raise_error("any_operation")
        assert error is not None
        assert isinstance(error, ValueError)
