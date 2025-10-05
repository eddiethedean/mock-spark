"""
Function test factory for Mock Spark.

This module provides the FunctionTestFactory class for creating
comprehensive function test scenarios and test cases.
"""

from typing import List, Dict, Any


class FunctionTestFactory:
    """Factory for creating comprehensive function test scenarios."""

    @staticmethod
    def create_arithmetic_test_expressions() -> List[Dict[str, Any]]:
        """Create arithmetic expression test cases."""
        return [
            {"left": 10, "operator": "+", "right": 5, "expected": 15},
            {"left": 10, "operator": "-", "right": 3, "expected": 7},
            {"left": 4, "operator": "*", "right": 6, "expected": 24},
            {"left": 15, "operator": "/", "right": 3, "expected": 5.0},
            {"left": 17, "operator": "%", "right": 5, "expected": 2},
        ]

    @staticmethod
    def create_comparison_test_expressions() -> List[Dict[str, Any]]:
        """Create comparison expression test cases."""
        return [
            {"left": 10, "operator": "==", "right": 10, "expected": True},
            {"left": 10, "operator": "!=", "right": 5, "expected": True},
            {"left": 10, "operator": ">", "right": 5, "expected": True},
            {"left": 10, "operator": ">=", "right": 10, "expected": True},
            {"left": 5, "operator": "<", "right": 10, "expected": True},
            {"left": 5, "operator": "<=", "right": 5, "expected": True},
        ]

    @staticmethod
    def create_string_function_test_cases() -> List[Dict[str, Any]]:
        """Create string function test cases."""
        return [
            {"function": "upper", "input": "hello", "expected": "HELLO"},
            {"function": "lower", "input": "WORLD", "expected": "world"},
            {"function": "length", "input": "test", "expected": 4},
            {"function": "trim", "input": "  spaces  ", "expected": "spaces"},
        ]

    @staticmethod
    def create_aggregate_function_test_cases() -> List[Dict[str, Any]]:
        """Create aggregate function test cases."""
        return [
            {"function": "count", "data": [1, 2, 3, 4, 5], "expected": 5},
            {"function": "sum", "data": [1, 2, 3, 4, 5], "expected": 15},
            {"function": "avg", "data": [1, 2, 3, 4, 5], "expected": 3.0},
            {"function": "max", "data": [1, 2, 3, 4, 5], "expected": 5},
            {"function": "min", "data": [1, 2, 3, 4, 5], "expected": 1},
        ]
