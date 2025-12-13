"""
Data generation utilities for Sparkless.

This module provides comprehensive data generation capabilities including
schema-based generation, data corruption simulation, and realistic data patterns.
"""

from .generator import MockDataGenerator
from .builder import MockDataGeneratorBuilder
from .convenience import create_test_data, create_corrupted_data, create_realistic_data

__all__ = [
    "MockDataGenerator",
    "MockDataGeneratorBuilder",
    "create_test_data",
    "create_corrupted_data",
    "create_realistic_data",
]
