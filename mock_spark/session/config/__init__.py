"""
Configuration management module for Mock Spark.

This module provides configuration management for Mock Spark,
including session configuration, runtime settings, and
environment-specific configurations.

Components:
    - Configuration: Main configuration management
    - ConfigBuilder: Configuration builder pattern
    - EnvironmentConfig: Environment-specific settings
"""

from .configuration import Configuration, ConfigBuilder, SparkConfig

__all__ = [
    "Configuration",
    "ConfigBuilder",
    "SparkConfig",
]
