"""
Mock SparkSession implementation for Mock Spark.

This module provides a complete mock implementation of PySpark's SparkSession
that behaves identically to the real SparkSession for testing and development.
It includes session management, DataFrame creation, SQL operations, and catalog
management without requiring a JVM or actual Spark installation.

Key Features:
    - Complete PySpark SparkSession API compatibility
    - DataFrame creation from various data sources
    - SQL query parsing and execution
    - Catalog operations (databases, tables)
    - Configuration management
    - Session lifecycle management

Example:
    >>> from mock_spark.sql import SparkSession
    >>> spark = SparkSession("MyApp")
    >>> data = [{"name": "Alice", "age": 25}]
    >>> df = spark.createDataFrame(data)
    >>> df.show()
    DataFrame[1 rows, 2 columns]
    age name
    25    Alice
    >>> spark.sql("CREATE DATABASE test")
"""

# Import from the new modular structure
