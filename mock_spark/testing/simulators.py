"""
Simulation utilities for Mock Spark testing.

This module provides utilities for simulating various conditions during testing,
including errors, performance issues, and memory constraints.
"""

import time
import random
import psutil
from typing import Dict, Any, List, Optional, Callable
from contextlib import contextmanager


class ErrorSimulator:
    """Simulator for various error conditions during testing."""

    def __init__(self) -> None:
        self.error_rules: Dict[str, Dict[str, Any]] = {}
        self.error_count = 0

    def add_error_rule(self, condition: str, exception_class: type, message: Optional[str] = None) -> None:
        """Add an error rule that triggers under specific conditions."""
        if message is None:
            message = f"Simulated error: {condition}"

        self.error_rules[condition] = {
            "exception": exception_class,
            "message": message,
            "count": 0,
        }

    def remove_error_rule(self, condition: str) -> None:
        """Remove an error rule."""
        if condition in self.error_rules:
            del self.error_rules[condition]

    def clear_error_rules(self) -> None:
        """Clear all error rules."""
        self.error_rules.clear()
        self.error_count = 0

    def should_raise_error(self, operation: str, **kwargs: Any) -> Optional[Exception]:
        """Check if an error should be raised for the given operation."""
        for condition, rule in self.error_rules.items():
            if self._evaluate_condition(condition, operation, **kwargs):
                rule["count"] += 1
                self.error_count += 1
                exception_class = rule["exception"]
                if isinstance(exception_class, type) and issubclass(exception_class, Exception):
                    exception_instance = exception_class(rule["message"])
                    return exception_instance
                else:
                    return None
        return None

    def _evaluate_condition(self, condition: str, operation: str, **kwargs: Any) -> bool:
        """Evaluate if a condition is met."""
        # Simple condition evaluation - can be extended
        if condition == "always":
            return True
        elif condition == "never":
            return False
        elif condition.startswith("operation:"):
            target_op = condition.split(":", 1)[1]
            return operation == target_op
        elif condition.startswith("count:"):
            target_count = int(condition.split(":", 1)[1])
            return self.error_count >= target_count
        elif condition.startswith("random:"):
            probability = float(condition.split(":", 1)[1])
            return random.random() < probability
        else:
            return False

    def get_error_stats(self) -> Dict[str, Any]:
        """Get statistics about triggered errors."""
        return {
            "total_errors": self.error_count,
            "rules": {k: v["count"] for k, v in self.error_rules.items()},
        }


class PerformanceSimulator:
    """Simulator for performance testing and benchmarking."""

    def __init__(self) -> None:
        self.performance_rules: Dict[str, Dict[str, Any]] = {}
        self.measurements: List[Dict[str, Any]] = []

    def add_slowdown(self, operation: str, delay_seconds: float) -> None:
        """Add a slowdown rule for a specific operation."""
        self.performance_rules[operation] = {"type": "slowdown", "delay": delay_seconds}

    def add_memory_limit(self, limit_mb: int) -> None:
        """Add a memory limit for testing memory constraints."""
        self.performance_rules["memory"] = {
            "type": "memory_limit",
            "limit_mb": limit_mb,
        }

    def simulate_operation(self, operation: str, func: Callable, *args: Any, **kwargs: Any) -> Any:
        """Simulate an operation with performance modifications."""
        start_time = time.time()

        # Apply slowdown if configured
        if operation in self.performance_rules:
            rule = self.performance_rules[operation]
            if rule["type"] == "slowdown":
                time.sleep(rule["delay"])

        # Check memory limit
        if "memory" in self.performance_rules:
            rule = self.performance_rules["memory"]
            current_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
            if current_memory > rule["limit_mb"]:
                raise MemoryError(
                    f"Memory limit exceeded: {current_memory:.1f}MB > {rule['limit_mb']}MB"
                )

        # Execute the function
        result = func(*args, **kwargs)

        # Record performance metrics
        end_time = time.time()
        duration = end_time - start_time

        self.measurements.append(
            {
                "operation": operation,
                "duration": duration,
                "timestamp": start_time,
                "memory_mb": psutil.Process().memory_info().rss / 1024 / 1024,
            }
        )

        return result

    def get_performance_stats(self) -> Dict[str, Any]:
        """Get performance statistics."""
        if not self.measurements:
            return {"total_operations": 0, "avg_duration": 0, "max_duration": 0}

        durations = [m["duration"] for m in self.measurements]
        memory_usage = [m["memory_mb"] for m in self.measurements]

        return {
            "total_operations": len(self.measurements),
            "avg_duration": sum(durations) / len(durations),
            "max_duration": max(durations),
            "min_duration": min(durations),
            "avg_memory_mb": sum(memory_usage) / len(memory_usage),
            "max_memory_mb": max(memory_usage),
            "operations": self.measurements,
        }

    def clear_measurements(self) -> None:
        """Clear performance measurements."""
        self.measurements.clear()


class MemorySimulator:
    """Simulator for memory-related testing scenarios."""

    def __init__(self) -> None:
        self.memory_usage: List[Dict[str, Any]] = []
        self.peak_memory = 0.0

    def get_current_memory_mb(self) -> float:
        """Get current memory usage in MB."""
        memory_bytes = psutil.Process().memory_info().rss
        return float(memory_bytes) / 1024 / 1024

    def record_memory_usage(self, operation: str = "unknown") -> None:
        """Record current memory usage."""
        current_memory = self.get_current_memory_mb()
        self.memory_usage.append(
            {
                "operation": operation,
                "memory_mb": current_memory,
                "timestamp": time.time(),
            }
        )
        self.peak_memory = max(self.peak_memory, current_memory)

    def simulate_memory_pressure(self, target_mb: int) -> str:
        """Simulate memory pressure by allocating memory."""
        # This is a simple simulation - in real scenarios you'd allocate actual memory
        self.record_memory_usage("memory_pressure_simulation")
        return f"Simulated memory pressure: {target_mb}MB"

    def get_memory_stats(self) -> Dict[str, Any]:
        """Get memory usage statistics."""
        if not self.memory_usage:
            return {"peak_memory_mb": 0, "current_memory_mb": 0, "measurements": []}

        current_memory = self.get_current_memory_mb()
        return {
            "peak_memory_mb": self.peak_memory,
            "current_memory_mb": current_memory,
            "measurements": self.memory_usage,
        }


# Context managers for easy use in tests
@contextmanager
def error_simulation(condition: str, exception_class: type, message: Optional[str] = None) -> Any:
    """Context manager for error simulation."""
    simulator = ErrorSimulator()
    simulator.add_error_rule(condition, exception_class, message)
    try:
        yield simulator
    finally:
        simulator.clear_error_rules()


@contextmanager
def performance_simulation(operation: Optional[str] = None, delay_seconds: float = 0.1) -> Any:
    """Context manager for performance simulation."""
    simulator = PerformanceSimulator()
    if operation and delay_seconds > 0:
        simulator.add_slowdown(operation, delay_seconds)
    try:
        yield simulator
    finally:
        pass  # Keep measurements for analysis


@contextmanager
def memory_simulation() -> Any:
    """Context manager for memory simulation."""
    simulator = MemorySimulator()
    try:
        yield simulator
    finally:
        pass  # Keep measurements for analysis


# Global simulators for easy access
error_simulator = ErrorSimulator()
performance_simulator = PerformanceSimulator()
memory_simulator = MemorySimulator()
