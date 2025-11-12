"""
Protocol interfaces for SparkSession services.

This module defines Protocol interfaces that provide type safety
and clear contracts for service implementations.
"""

from typing import Protocol, Any, Optional, Union
from mock_spark.spark_types import StructType
from mock_spark.dataframe import DataFrame
from mock_spark.session.config import SparkConfig


class IDataFrameFactory(Protocol):
    """Protocol for DataFrame factory services."""

    def create_dataframe(
        self,
        data: list[dict[str, Any]],
        schema: Optional[Union[StructType, list[str], str]],
        engine_config: SparkConfig,
        storage: Any,
    ) -> DataFrame:
        """Create a DataFrame with validation and coercion."""
        ...

    def _handle_schema_inference(
        self, data: list[dict[str, Any]], schema: Optional[Any]
    ) -> tuple[StructType, list[dict[str, Any]]]:
        """Handle schema inference or conversion."""
        ...

    def _apply_validation_and_coercion(
        self,
        data: list[dict[str, Any]],
        schema: StructType,
        engine_config: SparkConfig,
    ) -> list[dict[str, Any]]:
        """Apply validation and type coercion."""
        ...


class ISQLParameterBinder(Protocol):
    """Protocol for SQL parameter binding services."""

    def bind_parameters(
        self, query: str, args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> str:
        """Bind parameters to SQL query safely."""
        ...

    def _format_param(self, value: Any) -> str:
        """Format a parameter value for SQL."""
        ...


class ISessionLifecycleManager(Protocol):
    """Protocol for session lifecycle management services."""

    def stop_session(self, storage: Any, performance_tracker: Any) -> None:
        """Stop session and clean up resources."""
        ...

    def cleanup_resources(self, storage: Any) -> None:
        """Clean up storage and other resources."""
        ...


class IMockingCoordinator(Protocol):
    """Protocol for mocking coordination services."""

    def setup_mock_impl(
        self,
        method_name: str,
        side_effect: Optional[Any] = None,
        return_value: Optional[Any] = None,
    ) -> Any:
        """Set up a mock implementation for a method."""
        ...

    def reset_all_mocks(self, original_impls: dict[str, Any]) -> dict[str, Any]:
        """Reset all mocks to original implementations."""
        ...

    def add_error_rule(self, method_name: str, condition: Any, exception: Any) -> None:
        """Add an error simulation rule."""
        ...

    def check_error_rules(
        self, method_name: str, *args: Any, **kwargs: Any
    ) -> Optional[Exception]:
        """Check if error should be raised."""
        ...

    def clear_error_rules(self) -> None:
        """Clear all error simulation rules."""
        ...
