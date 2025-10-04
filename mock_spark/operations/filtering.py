"""
Filtering operations for MockDataFrame.

This module contains the implementation of filtering operations
including filter, where, and other conditional operations.
"""

from typing import Any, Dict, List, Optional, Union
from mock_spark.functions import MockColumn, MockColumnOperation


class FilteringOperations:
    """Handles filtering operations for DataFrames."""
    
    def __init__(self, dataframe):
        """Initialize with a reference to the DataFrame."""
        self.df = dataframe
    
    def filter(self, condition: Union[MockColumnOperation, MockColumn]) -> "MockDataFrame":
        """Filter rows based on condition.
        
        Args:
            condition: Filter condition to apply.
        
        Returns:
            New MockDataFrame with filtered rows.
        """
        if isinstance(condition, MockColumn):
            # Simple column reference - return all non-null rows
            filtered_data = [
                row for row in self.df.data if row.get(condition.name) is not None
            ]
        else:
            # Apply condition logic
            filtered_data = self._apply_condition(self.df.data, condition)
        
        from mock_spark.dataframe import MockDataFrame
        return MockDataFrame(filtered_data, self.df.schema, self.df.storage)
    
    def where(self, condition: Union[MockColumnOperation, MockColumn]) -> "MockDataFrame":
        """Alias for filter method.
        
        Args:
            condition: Filter condition to apply.
        
        Returns:
            New MockDataFrame with filtered rows.
        """
        return self.filter(condition)
    
    def _apply_condition(self, data: List[Dict[str, Any]], condition: MockColumnOperation) -> List[Dict[str, Any]]:
        """Apply a condition to filter data.
        
        Args:
            data: List of data rows.
            condition: Condition to apply.
        
        Returns:
            Filtered list of data rows.
        """
        filtered_data = []
        
        for row in data:
            if self._evaluate_condition(row, condition):
                filtered_data.append(row)
        
        return filtered_data
    
    def _evaluate_condition(self, row: Dict[str, Any], condition: MockColumnOperation) -> bool:
        """Evaluate a condition for a single row.
        
        Args:
            row: Data row to evaluate.
            condition: Condition to evaluate.
        
        Returns:
            True if condition is met, False otherwise.
        """
        try:
            if hasattr(condition, 'operation') and hasattr(condition, 'column'):
                operation = condition.operation
                column = condition.column
                
                if operation == "==":
                    return self._evaluate_equality(row, column, condition.value)
                elif operation == "!=":
                    return not self._evaluate_equality(row, column, condition.value)
                elif operation == ">":
                    return self._evaluate_comparison(row, column, condition.value, ">")
                elif operation == ">=":
                    return self._evaluate_comparison(row, column, condition.value, ">=")
                elif operation == "<":
                    return self._evaluate_comparison(row, column, condition.value, "<")
                elif operation == "<=":
                    return self._evaluate_comparison(row, column, condition.value, "<=")
                elif operation == "&":
                    return self._evaluate_logical_and(row, column, condition.value)
                elif operation == "|":
                    return self._evaluate_logical_or(row, column, condition.value)
                elif operation == "isnull":
                    return self._evaluate_isnull(row, column)
                elif operation == "isnotnull":
                    return self._evaluate_isnotnull(row, column)
                elif operation == "isin":
                    return self._evaluate_isin(row, column, condition.value)
                elif operation == "between":
                    return self._evaluate_between(row, column, condition.value)
                else:
                    # For other operations, try to evaluate as expression
                    result = self.df._evaluate_column_expression(row, condition)
                    return bool(result) if result is not None else False
            
            elif hasattr(condition, 'name'):
                # Simple column reference - check if value is truthy
                value = row.get(condition.name)
                return bool(value) if value is not None else False
            
            else:
                # Fallback - try to evaluate as expression
                result = self.df._evaluate_column_expression(row, condition)
                return bool(result) if result is not None else False
                
        except Exception:
            # If evaluation fails, return False
            return False
    
    def _evaluate_equality(self, row: Dict[str, Any], column: MockColumn, value: Any) -> bool:
        """Evaluate equality condition."""
        col_value = row.get(column.name)
        if col_value is None or value is None:
            return col_value is None and value is None
        return col_value == value
    
    def _evaluate_comparison(self, row: Dict[str, Any], column: MockColumn, value: Any, operator: str) -> bool:
        """Evaluate comparison condition."""
        col_value = row.get(column.name)
        if col_value is None or value is None:
            return False
        
        try:
            if operator == ">":
                return col_value > value
            elif operator == ">=":
                return col_value >= value
            elif operator == "<":
                return col_value < value
            elif operator == "<=":
                return col_value <= value
            else:
                return False
        except TypeError:
            # If comparison fails, return False
            return False
    
    def _evaluate_logical_and(self, row: Dict[str, Any], left: MockColumnOperation, right: MockColumnOperation) -> bool:
        """Evaluate logical AND condition."""
        left_result = self._evaluate_condition(row, left)
        right_result = self._evaluate_condition(row, right)
        return left_result and right_result
    
    def _evaluate_logical_or(self, row: Dict[str, Any], left: MockColumnOperation, right: MockColumnOperation) -> bool:
        """Evaluate logical OR condition."""
        left_result = self._evaluate_condition(row, left)
        right_result = self._evaluate_condition(row, right)
        return left_result or right_result
    
    def _evaluate_isnull(self, row: Dict[str, Any], column: MockColumn) -> bool:
        """Evaluate IS NULL condition."""
        return row.get(column.name) is None
    
    def _evaluate_isnotnull(self, row: Dict[str, Any], column: MockColumn) -> bool:
        """Evaluate IS NOT NULL condition."""
        return row.get(column.name) is not None
    
    def _evaluate_isin(self, row: Dict[str, Any], column: MockColumn, values: List[Any]) -> bool:
        """Evaluate IN condition."""
        col_value = row.get(column.name)
        return col_value in values if col_value is not None else False
    
    def _evaluate_between(self, row: Dict[str, Any], column: MockColumn, bounds: tuple) -> bool:
        """Evaluate BETWEEN condition."""
        if len(bounds) != 2:
            return False
        
        col_value = row.get(column.name)
        if col_value is None:
            return False
        
        lower_bound, upper_bound = bounds
        try:
            return lower_bound <= col_value <= upper_bound
        except TypeError:
            return False
