"""
Protocol interfaces for MockDataFrame handlers.

This module defines type-safe protocols for the specialized handler classes
that implement the Single Responsibility Principle for MockDataFrame operations.
"""

from typing import Protocol, List, Dict, Any, Tuple, Iterator, TYPE_CHECKING

if TYPE_CHECKING:
    from ..spark_types import MockRow, MockStructType


class WindowFunctionHandler(Protocol):
    """Protocol for window function evaluation."""

    def evaluate_window_functions(
        self, data: List[Dict[str, Any]], window_functions: List[Tuple[Any, ...]]
    ) -> List[Dict[str, Any]]:
        """Evaluate window functions across all rows."""
        ...

    def _evaluate_lag_lead(
        self,
        result_data: List[Dict[str, Any]],
        window_func: Any,
        col_name: str,
        is_lead: bool,
    ) -> None:
        """Evaluate LAG/LEAD functions."""
        ...

    def _apply_ordering_to_indices(
        self, data: List[Dict[str, Any]], indices: List[int], order_by_cols: List[Any]
    ) -> List[int]:
        """Apply ordering to row indices."""
        ...

    def _apply_lag_lead_to_partition(
        self,
        result_data: List[Dict[str, Any]],
        partition_indices: List[int],
        window_func: Any,
        col_name: str,
        is_lead: bool,
    ) -> None:
        """Apply LAG/LEAD to a partition."""
        ...

    def _evaluate_rank_functions(
        self, result_data: List[Dict[str, Any]], window_func: Any, col_name: str
    ) -> None:
        """Evaluate RANK/DENSE_RANK functions."""
        ...

    def _apply_rank_to_partition(
        self,
        result_data: List[Dict[str, Any]],
        partition_indices: List[int],
        window_func: Any,
        col_name: str,
    ) -> None:
        """Apply ranking to a partition."""
        ...

    def _evaluate_aggregate_window_functions(
        self, result_data: List[Dict[str, Any]], window_func: Any, col_name: str
    ) -> None:
        """Evaluate aggregate window functions (SUM, AVG, etc.)."""
        ...

    def _apply_aggregate_to_partition(
        self,
        result_data: List[Dict[str, Any]],
        partition_indices: List[int],
        window_func: Any,
        col_name: str,
    ) -> None:
        """Apply aggregate functions to a partition."""
        ...


class CollectionHandler(Protocol):
    """Protocol for collection operations."""

    def collect(
        self, data: List[Dict[str, Any]], schema: "MockStructType"
    ) -> List["MockRow"]:
        """Convert data to MockRow objects."""
        ...

    def take(
        self, data: List[Dict[str, Any]], schema: "MockStructType", n: int
    ) -> List["MockRow"]:
        """Take first n rows."""
        ...

    def head(
        self, data: List[Dict[str, Any]], schema: "MockStructType", n: int = 1
    ) -> Any:
        """Get first row(s)."""
        ...

    def tail(
        self, data: List[Dict[str, Any]], schema: "MockStructType", n: int = 1
    ) -> Any:
        """Get last n rows."""
        ...

    def to_local_iterator(
        self,
        data: List[Dict[str, Any]],
        schema: "MockStructType",
        prefetch: bool = False,
    ) -> Iterator["MockRow"]:
        """Return iterator over rows."""
        ...


class ValidationHandler(Protocol):
    """Protocol for data validation."""

    def validate_column_exists(
        self, schema: "MockStructType", column_name: str, operation: str
    ) -> None:
        """Validate single column exists."""
        ...

    def validate_columns_exist(
        self, schema: "MockStructType", column_names: List[str], operation: str
    ) -> None:
        """Validate multiple columns exist."""
        ...

    def validate_filter_expression(
        self,
        schema: "MockStructType",
        condition: Any,
        operation: str,
        has_pending_joins: bool = False,
    ) -> None:
        """Validate filter expression."""
        ...

    def validate_expression_columns(
        self,
        schema: "MockStructType",
        expression: Any,
        operation: str,
        in_lazy_materialization: bool = False,
    ) -> None:
        """Validate columns in expression exist."""
        ...


class ConditionHandler(Protocol):
    """Protocol for condition evaluation."""

    def apply_condition(
        self, data: List[Dict[str, Any]], condition: Any
    ) -> List[Dict[str, Any]]:
        """Filter data based on condition."""
        ...

    def evaluate_condition(self, row: Dict[str, Any], condition: Any) -> bool:
        """Evaluate condition for a single row."""
        ...

    def evaluate_column_expression(
        self, row: Dict[str, Any], column_expression: Any
    ) -> Any:
        """Evaluate column expression."""
        ...

    def evaluate_case_when(self, row: Dict[str, Any], case_when_obj: Any) -> Any:
        """Evaluate CASE WHEN expression."""
        ...

    def _evaluate_case_when_condition(
        self, row: Dict[str, Any], condition: Any
    ) -> bool:
        """Helper for case when condition evaluation."""
        ...
