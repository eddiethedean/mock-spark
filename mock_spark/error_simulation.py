"""
Error simulation framework for Mock Spark.

This module provides a comprehensive error simulation framework that allows
testing of error scenarios by injecting specific exceptions into Mock Spark
operations. Supports rule-based error injection and conditional error raising.

Key Features:
    - Rule-based error injection
    - Conditional error raising based on method arguments
    - Support for all major Mock Spark operations
    - Easy integration with existing test frameworks
    - Comprehensive error simulation examples

Example:
    >>> from mock_spark import MockSparkSession
    >>> from mock_spark.error_simulation import MockErrorSimulator
    >>> 
    >>> spark = MockSparkSession("test")
    >>> error_sim = MockErrorSimulator(spark)
    >>> 
    >>> # Simulate table not found error
    >>> error_sim.add_rule("table", lambda name: "nonexistent" in name, AnalysisException("Table not found"))
    >>> 
    >>> with pytest.raises(AnalysisException):
    ...     spark.table("nonexistent.table")
"""

from typing import Any, Callable, Dict, List, Optional, Union
from .errors import AnalysisException, IllegalArgumentException, PySparkValueError


class MockErrorSimulator:
    """Error simulation framework for Mock Spark operations.

    Provides rule-based error injection that allows testing of error scenarios
    by raising specific exceptions when certain conditions are met.

    Attributes:
        spark_session: The MockSparkSession instance to simulate errors on.
        error_rules: Dictionary mapping method names to lists of (condition, exception) tuples.

    Example:
        >>> error_sim = MockErrorSimulator(spark)
        >>> error_sim.add_rule("table", lambda name: "nonexistent" in name, AnalysisException("Table not found"))
        >>> error_sim.add_rule("createDataFrame", lambda data, schema: len(data) > 1000, PySparkValueError("Too much data"))
    """

    def __init__(self, spark_session):
        """Initialize MockErrorSimulator.

        Args:
            spark_session: MockSparkSession instance to simulate errors on.
        """
        self.spark_session = spark_session
        self.error_rules: Dict[str, List[tuple]] = {}
        self._original_methods = {}

    def add_rule(self, method_name: str, condition: Callable, exception: Exception):
        """Add an error rule for a specific method.

        Args:
            method_name: Name of the method to apply the rule to.
            condition: Function that takes method arguments and returns True if error should be raised.
            exception: Exception to raise when condition is True.

        Example:
            >>> error_sim.add_rule("table", lambda name: "nonexistent" in name, AnalysisException("Table not found"))
        """
        if method_name not in self.error_rules:
            self.error_rules[method_name] = []
        self.error_rules[method_name].append((condition, exception))

    def remove_rule(
        self, method_name: str, condition: Optional[Callable[..., bool]] = None
    ) -> None:
        """Remove error rules for a method.

        Args:
            method_name: Name of the method to remove rules from.
            condition: Specific condition to remove. If None, removes all rules for the method.
        """
        if method_name not in self.error_rules:
            return

        if condition is None:
            self.error_rules[method_name] = []
        else:
            self.error_rules[method_name] = [
                (cond, exc)
                for cond, exc in self.error_rules[method_name]
                if cond != condition
            ]

    def clear_rules(self, method_name: Optional[str] = None) -> None:
        """Clear all error rules.

        Args:
            method_name: Specific method to clear rules for. If None, clears all rules.
        """
        if method_name:
            self.error_rules[method_name] = []
        else:
            self.error_rules.clear()

    def should_raise_error(
        self, method_name: str, *args, **kwargs
    ) -> Optional[Exception]:
        """Check if an error should be raised for a method call.

        Args:
            method_name: Name of the method being called.
            *args: Positional arguments passed to the method.
            **kwargs: Keyword arguments passed to the method.

        Returns:
            Exception to raise if conditions are met, None otherwise.
        """
        if method_name not in self.error_rules:
            return None

        for condition, exception in self.error_rules[method_name]:
            try:
                if condition(*args, **kwargs):
                    return exception  # type: ignore[no-any-return]
            except Exception:
                # If condition evaluation fails, skip this rule
                continue
        return None

    def enable_error_simulation(self):
        """Enable error simulation by wrapping methods."""
        # Store original methods
        self._original_methods = {
            "createDataFrame": self.spark_session.createDataFrame,
            "table": self.spark_session.table,
            "sql": self.spark_session.sql,
        }

        # Wrap methods with error simulation
        self.spark_session.createDataFrame = self._wrap_method("createDataFrame")
        self.spark_session.table = self._wrap_method("table")
        self.spark_session.sql = self._wrap_method("sql")

    def disable_error_simulation(self):
        """Disable error simulation by restoring original methods."""
        for method_name, original_method in self._original_methods.items():
            setattr(self.spark_session, method_name, original_method)
        self._original_methods.clear()

    def _wrap_method(self, method_name: str):
        """Wrap a method with error simulation logic."""
        original_method = getattr(self.spark_session, method_name)

        def wrapped_method(*args, **kwargs):
            # Check if error should be raised
            error = self.should_raise_error(method_name, *args, **kwargs)
            if error:
                raise error

            # Call original method
            return original_method(*args, **kwargs)

        return wrapped_method


class MockErrorSimulatorBuilder:
    """Builder pattern for creating common error simulation scenarios.

    Provides convenient methods for setting up common error testing scenarios
    without requiring manual rule creation.

    Example:
        >>> builder = MockErrorSimulatorBuilder(spark)
        >>> error_sim = (builder
        ...     .table_not_found("nonexistent.*")
        ...     .data_too_large(1000)
        ...     .invalid_schema("invalid.*")
        ...     .build())
    """

    def __init__(self, spark_session):
        """Initialize MockErrorSimulatorBuilder.

        Args:
            spark_session: MockSparkSession instance to build error simulation for.
        """
        self.spark_session = spark_session
        self.error_sim = MockErrorSimulator(spark_session)

    def table_not_found(self, pattern: str = "nonexistent.*"):
        """Add rule for table not found errors.

        Args:
            pattern: Pattern to match table names that should raise errors.
        """
        import re

        pattern_re = re.compile(pattern.replace("*", ".*"))
        self.error_sim.add_rule(
            "table",
            lambda name: pattern_re.match(name) is not None,
            AnalysisException(f"Table or view not found: {pattern}"),
        )
        return self

    def data_too_large(self, max_rows: int = 1000):
        """Add rule for data too large errors.

        Args:
            max_rows: Maximum number of rows allowed in createDataFrame.
        """
        self.error_sim.add_rule(
            "createDataFrame",
            lambda data, schema=None: len(data) > max_rows,
            PySparkValueError(f"Data too large: {max_rows} rows maximum"),
        )
        return self

    def invalid_schema(self, pattern: str = "invalid.*"):
        """Add rule for invalid schema errors.

        Args:
            pattern: Pattern to match schema names that should raise errors.
        """
        import re

        pattern_re = re.compile(pattern.replace("*", ".*"))
        self.error_sim.add_rule(
            "createDataFrame",
            lambda data, schema=None: schema is not None
            and hasattr(schema, "fields")
            and any(pattern_re.match(field.name) for field in schema.fields),
            AnalysisException(f"Invalid schema: {pattern}"),
        )
        return self

    def sql_syntax_error(self, pattern: str = "INVALID.*"):
        """Add rule for SQL syntax errors.

        Args:
            pattern: Pattern to match SQL queries that should raise errors.
        """
        import re

        pattern_re = re.compile(pattern.replace("*", ".*"))
        self.error_sim.add_rule(
            "sql",
            lambda query: pattern_re.match(query.upper()) is not None,
            AnalysisException(f"SQL syntax error: {pattern}"),
        )
        return self

    def build(self) -> MockErrorSimulator:
        """Build the error simulator with all configured rules.

        Returns:
            MockErrorSimulator instance with all rules applied.
        """
        return self.error_sim


# Convenience functions for common error scenarios
def create_table_not_found_simulator(
    spark_session, table_pattern: str = "nonexistent.*"
):
    """Create a simulator that raises table not found errors.

    Args:
        spark_session: MockSparkSession instance.
        table_pattern: Pattern for table names that should raise errors.

    Returns:
        MockErrorSimulator configured for table not found errors.
    """
    return (
        MockErrorSimulatorBuilder(spark_session).table_not_found(table_pattern).build()
    )


def create_data_too_large_simulator(spark_session, max_rows: int = 1000):
    """Create a simulator that raises data too large errors.

    Args:
        spark_session: MockSparkSession instance.
        max_rows: Maximum number of rows allowed.

    Returns:
        MockErrorSimulator configured for data too large errors.
    """
    return MockErrorSimulatorBuilder(spark_session).data_too_large(max_rows).build()


def create_sql_error_simulator(spark_session, error_pattern: str = "INVALID.*"):
    """Create a simulator that raises SQL syntax errors.

    Args:
        spark_session: MockSparkSession instance.
        error_pattern: Pattern for SQL queries that should raise errors.

    Returns:
        MockErrorSimulator configured for SQL syntax errors.
    """
    return (
        MockErrorSimulatorBuilder(spark_session).sql_syntax_error(error_pattern).build()
    )
