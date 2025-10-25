"""
Protocol interfaces for MockSparkSession services.

This module defines Protocol interfaces that provide type safety
and clear contracts for service implementations.
"""

from typing import Protocol, List, Dict, Any, Optional, Tuple, Union
from mock_spark.spark_types import MockStructType
from mock_spark.dataframe import MockDataFrame
from mock_spark.session.config import MockSparkConfig


class IDataFrameFactory(Protocol):
    """Protocol for DataFrame factory services."""

    def create_dataframe(
        self,
        data: List[Dict[str, Any]],
        schema: Optional[Union[MockStructType, List[str], str]],
        engine_config: MockSparkConfig,
        storage: Any,
    ) -> MockDataFrame:
        """Create a DataFrame with validation and coercion."""
        ...

    def _handle_schema_inference(
        self, data: List[Dict[str, Any]], schema: Optional[Any]
    ) -> Tuple[MockStructType, List[Dict[str, Any]]]:
        """Handle schema inference or conversion."""
        ...

    def _apply_validation_and_coercion(
        self,
        data: List[Dict[str, Any]],
        schema: MockStructType,
        engine_config: MockSparkConfig,
    ) -> List[Dict[str, Any]]:
        """Apply validation and type coercion."""
        ...


class ISQLParameterBinder(Protocol):
    """Protocol for SQL parameter binding services."""

    def bind_parameters(
        self, query: str, args: Tuple[Any, ...], kwargs: Dict[str, Any]
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

    def reset_all_mocks(self, original_impls: Dict[str, Any]) -> Dict[str, Any]:
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
