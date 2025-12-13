"""
Configuration management module for Sparkless.

This module provides configuration management for Sparkless,
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
