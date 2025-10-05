"""
Data generation utilities for Mock Spark.

This module provides comprehensive data generation utilities that allow
creation of test data based on schemas, with support for various data
types, corruption simulation, and realistic data patterns.

Key Features:
    - Schema-based data generation
    - Support for all Mock Spark data types
    - Data corruption simulation for error testing
    - Realistic data patterns and distributions
    - Configurable data generation parameters

Example:
    >>> from mock_spark import MockSparkSession, StringType, IntegerType
    >>> from mock_spark.data_generation import MockDataGenerator
    >>> from mock_spark.spark_types import MockStructType, MockStructField
    >>> 
    >>> spark = MockSparkSession("test")
    >>> schema = MockStructType([
    ...     MockStructField("name", StringType()),
    ...     MockStructField("age", IntegerType())
    ... ])
    >>> data = MockDataGenerator.create_test_data(schema, num_rows=100)
"""

# Import from the new modular structure
from .data_generation import (
    MockDataGenerator,
    MockDataGeneratorBuilder,
    create_test_data,
    create_corrupted_data,
    create_realistic_data,
)
