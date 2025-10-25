"""
Query Optimizer for Mock-Spark.

Provides comprehensive query optimization including filter pushdown,
column pruning, join optimization, and memory management.
"""

from typing import List, Dict, Any, Optional, Set, cast
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum


class OperationType(Enum):
    """Types of DataFrame operations."""

    SELECT = "select"
    FILTER = "filter"
    JOIN = "join"
    GROUP_BY = "group_by"
    ORDER_BY = "order_by"
    LIMIT = "limit"
    WITH_COLUMN = "with_column"
    DROP = "drop"
    UNION = "union"
    WINDOW = "window"


@dataclass
class Operation:
    """Represents a DataFrame operation in the query plan."""

    type: OperationType
    columns: List[str]
    predicates: List[Dict[str, Any]]
    join_conditions: List[Dict[str, Any]]
    group_by_columns: List[str]
    order_by_columns: List[str]
    limit_count: Optional[int]
    window_specs: List[Dict[str, Any]]
    metadata: Dict[str, Any]

    def __post_init__(self) -> None:
        if self.predicates is None:
            self.predicates = []
        if self.join_conditions is None:
            self.join_conditions = []
        if self.group_by_columns is None:
            self.group_by_columns = []
        if self.order_by_columns is None:
            self.order_by_columns = []
        if self.window_specs is None:
            self.window_specs = []
        if self.metadata is None:
            self.metadata = {}


class OptimizationRule(ABC):
    """Base class for optimization rules."""

    @abstractmethod
    def apply(self, operations: List[Operation]) -> List[Operation]:
        """Apply optimization rule to operations."""
        pass

    @abstractmethod
    def can_apply(self, operations: List[Operation]) -> bool:
        """Check if rule can be applied to operations."""
        pass


class FilterPushdownRule(OptimizationRule):
    """Push filters as early as possible in the query plan."""

    def apply(self, operations: List[Operation]) -> List[Operation]:
        """Move filter operations before other operations when possible."""
        if not self.can_apply(operations):
            return operations

        optimized = []
        filters = []

        for op in operations:
            if op.type == OperationType.FILTER:
                filters.extend(op.predicates)
            else:
                # If we have filters and this is a non-filter operation,
                # try to push filters before it
                if filters and self._can_push_filters_before(op):
                    # Create filter operation
                    filter_op = Operation(
                        type=OperationType.FILTER,
                        columns=[],
                        predicates=filters.copy(),
                        join_conditions=[],
                        group_by_columns=[],
                        order_by_columns=[],
                        limit_count=None,
                        window_specs=[],
                        metadata={},
                    )
                    optimized.append(filter_op)
                    filters = []

                optimized.append(op)

        # Add any remaining filters at the end
        if filters:
            filter_op = Operation(
                type=OperationType.FILTER,
                columns=[],
                predicates=filters,
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={},
            )
            optimized.append(filter_op)

        return optimized

    def can_apply(self, operations: List[Operation]) -> bool:
        """Check if filter pushdown can be applied."""
        filter_ops = [op for op in operations if op.type == OperationType.FILTER]
        return len(filter_ops) > 0

    def _can_push_filters_before(self, op: Operation) -> bool:
        """Check if filters can be pushed before this operation."""
        # Can't push filters before GROUP_BY, ORDER_BY, or WINDOW
        return op.type not in [
            OperationType.GROUP_BY,
            OperationType.ORDER_BY,
            OperationType.WINDOW,
        ]


class ColumnPruningRule(OptimizationRule):
    """Remove unnecessary columns from the query plan."""

    def apply(self, operations: List[Operation]) -> List[Operation]:
        """Remove columns that are not needed in the final result."""
        if not self.can_apply(operations):
            return operations

        # Find all columns that are actually needed
        needed_columns = self._find_needed_columns(operations)

        optimized = []
        for op in operations:
            if op.type == OperationType.SELECT:
                # Only select needed columns
                needed_for_op = [col for col in op.columns if col in needed_columns]
                if needed_for_op:
                    optimized_op = Operation(
                        type=op.type,
                        columns=needed_for_op,
                        predicates=op.predicates,
                        join_conditions=op.join_conditions,
                        group_by_columns=op.group_by_columns,
                        order_by_columns=op.order_by_columns,
                        limit_count=op.limit_count,
                        window_specs=op.window_specs,
                        metadata=op.metadata,
                    )
                    optimized.append(optimized_op)
            else:
                optimized.append(op)

        return optimized

    def can_apply(self, operations: List[Operation]) -> bool:
        """Check if column pruning can be applied."""
        select_ops = [op for op in operations if op.type == OperationType.SELECT]
        return len(select_ops) > 0

    def _find_needed_columns(self, operations: List[Operation]) -> Set[str]:
        """Find all columns that are actually needed."""
        needed = set()

        # Start from the end and work backwards
        for op in reversed(operations):
            if op.type == OperationType.SELECT:
                needed.update(op.columns)
            elif op.type == OperationType.FILTER:
                # Add columns used in filter predicates
                for pred in op.predicates:
                    if "column" in pred:
                        needed.add(pred["column"])
            elif op.type == OperationType.GROUP_BY:
                needed.update(op.group_by_columns)
            elif op.type == OperationType.ORDER_BY:
                needed.update(op.order_by_columns)
            elif op.type == OperationType.JOIN:
                # Add columns used in join conditions
                for condition in op.join_conditions:
                    if "left_column" in condition:
                        needed.add(condition["left_column"])
                    if "right_column" in condition:
                        needed.add(condition["right_column"])

        return needed


class JoinOptimizationRule(OptimizationRule):
    """Optimize join operations for better performance."""

    def apply(self, operations: List[Operation]) -> List[Operation]:
        """Reorder joins and optimize join conditions."""
        if not self.can_apply(operations):
            return operations

        optimized = []
        join_ops = []
        other_ops = []

        # Separate join operations from others
        for op in operations:
            if op.type == OperationType.JOIN:
                join_ops.append(op)
            else:
                other_ops.append(op)

        # Optimize join order (simple heuristic: smaller tables first)
        if join_ops:
            optimized_joins = self._optimize_join_order(join_ops)
            optimized.extend(optimized_joins)

        optimized.extend(other_ops)
        return optimized

    def can_apply(self, operations: List[Operation]) -> bool:
        """Check if join optimization can be applied."""
        join_ops = [op for op in operations if op.type == OperationType.JOIN]
        return len(join_ops) > 0

    def _optimize_join_order(self, join_ops: List[Operation]) -> List[Operation]:
        """Optimize the order of join operations."""

        # Simple heuristic: sort by estimated size (metadata)
        def get_estimated_size(op: Operation) -> int:
            return cast(int, op.metadata.get("estimated_size", 1000))  # Default size

        return sorted(join_ops, key=get_estimated_size)


class PredicatePushdownRule(OptimizationRule):
    """Push predicates down to reduce data early."""

    def apply(self, operations: List[Operation]) -> List[Operation]:
        """Push predicates as early as possible."""
        if not self.can_apply(operations):
            return operations

        # Collect all predicates
        all_predicates = []
        for op in operations:
            if op.predicates:
                all_predicates.extend(op.predicates)

        # Push predicates to the earliest possible operation
        optimized = []
        predicates_pushed = False

        for op in operations:
            if not predicates_pushed and self._can_push_predicates_to(op):
                # Add predicates to this operation
                optimized_op = Operation(
                    type=op.type,
                    columns=op.columns,
                    predicates=all_predicates,
                    join_conditions=op.join_conditions,
                    group_by_columns=op.group_by_columns,
                    order_by_columns=op.order_by_columns,
                    limit_count=op.limit_count,
                    window_specs=op.window_specs,
                    metadata=op.metadata,
                )
                optimized.append(optimized_op)
                predicates_pushed = True
            else:
                optimized.append(op)

        return optimized

    def can_apply(self, operations: List[Operation]) -> bool:
        """Check if predicate pushdown can be applied."""
        return any(op.predicates for op in operations)

    def _can_push_predicates_to(self, op: Operation) -> bool:
        """Check if predicates can be pushed to this operation."""
        # Can push to SELECT, FILTER, but not to GROUP_BY, ORDER_BY, WINDOW
        return op.type in [OperationType.SELECT, OperationType.FILTER]


class ProjectionPushdownRule(OptimizationRule):
    """Push column projections as early as possible."""

    def apply(self, operations: List[Operation]) -> List[Operation]:
        """Push column selections as early as possible."""
        if not self.can_apply(operations):
            return operations

        # Find all columns that will be needed
        needed_columns = self._find_final_columns(operations)

        optimized = []
        for op in operations:
            if op.type == OperationType.SELECT:
                # Only select columns that will be needed
                optimized_op = Operation(
                    type=op.type,
                    columns=list(needed_columns),
                    predicates=op.predicates,
                    join_conditions=op.join_conditions,
                    group_by_columns=op.group_by_columns,
                    order_by_columns=op.order_by_columns,
                    limit_count=op.limit_count,
                    window_specs=op.window_specs,
                    metadata=op.metadata,
                )
                optimized.append(optimized_op)
            else:
                optimized.append(op)

        return optimized

    def can_apply(self, operations: List[Operation]) -> bool:
        """Check if projection pushdown can be applied."""
        return any(op.type == OperationType.SELECT for op in operations)

    def _find_final_columns(self, operations: List[Operation]) -> Set[str]:
        """Find columns that will be in the final result."""
        # Start from the last SELECT operation
        for op in reversed(operations):
            if op.type == OperationType.SELECT:
                return set(op.columns)
        return set()


class QueryOptimizer:
    """Main query optimizer that applies multiple optimization rules."""

    def __init__(self) -> None:
        """Initialize optimizer with default rules."""
        self.rules = [
            FilterPushdownRule(),
            ColumnPruningRule(),
            JoinOptimizationRule(),
            PredicatePushdownRule(),
            ProjectionPushdownRule(),
        ]

    def optimize(self, operations: List[Operation]) -> List[Operation]:
        """Apply all optimization rules to the query plan."""
        optimized = operations.copy()

        # Apply rules in sequence
        for rule in self.rules:
            if rule.can_apply(optimized):
                optimized = rule.apply(optimized)

        return optimized

    def add_rule(self, rule: OptimizationRule) -> None:
        """Add a custom optimization rule."""
        self.rules.append(rule)

    def remove_rule(self, rule_class: type) -> None:
        """Remove a rule by class."""
        self.rules = [rule for rule in self.rules if not isinstance(rule, rule_class)]

    def get_optimization_stats(
        self, original: List[Operation], optimized: List[Operation]
    ) -> Dict[str, Any]:
        """Get statistics about the optimization."""
        return {
            "original_operations": len(original),
            "optimized_operations": len(optimized),
            "operations_reduced": len(original) - len(optimized),
            "optimization_ratio": len(optimized) / len(original) if original else 1.0,
            "rules_applied": len(self.rules),
        }
