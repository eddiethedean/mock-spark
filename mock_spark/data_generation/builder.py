"""
Data generator builder for Mock Spark.

This module provides the MockDataGeneratorBuilder class for creating
common data generation scenarios using the builder pattern.
"""

from typing import List, Dict, Any
from ..spark_types import MockStructType
from .generator import MockDataGenerator


class MockDataGeneratorBuilder:
    """Builder pattern for creating common data generation scenarios.

    Provides convenient methods for setting up common data generation
    scenarios without requiring manual configuration.

    Example:
        >>> builder = MockDataGeneratorBuilder(schema)
        >>> data = (builder
        ...     .num_rows(1000)
        ...     .realistic()
        ...     .corruption_rate(0.05)
        ...     .build())
    """

    def __init__(self, schema: MockStructType):
        """Initialize MockDataGeneratorBuilder.

        Args:
            schema: MockStructType defining the data structure.
        """
        self.schema = schema
        self.num_rows = 100
        self.seed = 42
        self.corruption_rate = 0.0
        self.realistic = False

    def with_num_rows(self, count: int) -> "MockDataGeneratorBuilder":
        """Set number of rows to generate.

        Args:
            count: Number of rows to generate.
        """
        self.num_rows = count
        return self

    def with_seed(self, seed_value: int) -> "MockDataGeneratorBuilder":
        """Set random seed for reproducible data.

        Args:
            seed_value: Random seed value.
        """
        self.seed = seed_value
        return self

    def with_corruption_rate(self, rate: float) -> "MockDataGeneratorBuilder":
        """Set corruption rate for error testing.

        Args:
            rate: Fraction of data to corrupt (0.0 to 1.0).
        """
        self.corruption_rate = rate
        return self

    def with_realistic(self, enabled: bool = True) -> "MockDataGeneratorBuilder":
        """Enable realistic data generation."""
        self.realistic = enabled
        return self

    def build(self) -> MockDataGenerator:
        """Build the MockDataGenerator with all configured settings.

        Returns:
            Configured MockDataGenerator instance.
        """
        # MockDataGenerator is a static class, so we return a new instance
        # with the configured schema and seed
        generator = MockDataGenerator()
        # Store configuration for use in generation methods
        setattr(generator, "_schema", self.schema)
        setattr(generator, "_seed", self.seed)
        setattr(generator, "_corruption_rate", self.corruption_rate)
        setattr(generator, "_realistic", self.realistic)
        return generator
