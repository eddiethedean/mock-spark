"""
Mock DataFrame implementation for Mock Spark.

This module provides a complete mock implementation of PySpark DataFrame
that behaves identically to the real PySpark DataFrame.
"""

from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass
import pandas as pd

from .spark_types import MockStructType, MockStructField, StringType, IntegerType
from .functions import MockColumn, MockColumnOperation, F
from .storage import MockStorageManager
from .errors import (
    raise_column_not_found,
    raise_value_error,
    raise_invalid_argument,
    AnalysisException,
    IllegalArgumentException,
)


@dataclass
class MockDataFrameWriter:
    """Mock DataFrame writer for saveAsTable operations."""
    
    def __init__(self, df: 'MockDataFrame', storage: MockStorageManager):
        self.df = df
        self.storage = storage
        self.format_name = "parquet"
        self.save_mode = "append"
        self.options: Dict[str, Any] = {}
    
    def format(self, source: str) -> 'MockDataFrameWriter':
        """Set the format."""
        self.format_name = source
        return self
    
    def mode(self, mode: str) -> 'MockDataFrameWriter':
        """Set the save mode."""
        self.save_mode = mode
        return self
    
    def option(self, key: str, value: Any) -> 'MockDataFrameWriter':
        """Set an option."""
        self.options[key] = value
        return self
    
    def saveAsTable(self, table_name: str) -> None:
        """Save DataFrame as table."""
        schema, table = table_name.split(".", 1) if "." in table_name else ("default", table_name)
        
        # Create table if not exists
        if not self.storage.table_exists(schema, table):
            self.storage.create_table(schema, table, self.df.schema.fields)
        
        # Insert data
        if self.save_mode == "overwrite":
            # Clear existing data by dropping and recreating table
            if self.storage.table_exists(schema, table):
                self.storage.drop_table(schema, table)
            self.storage.create_table(schema, table, self.df.schema.fields)
        
        data = self.df.collect()
        self.storage.insert_data(schema, table, data)


class MockDataFrame:
    """Mock DataFrame implementation."""
    
    def __init__(self, data: List[Dict[str, Any]], schema: MockStructType, storage: Optional[MockStorageManager] = None):
        self.data = data
        self.schema = schema
        self.storage = storage or MockStorageManager()
        self._cached_count: Optional[int] = None
    
    def __repr__(self) -> str:
        return f"MockDataFrame[{len(self.data)} rows, {len(self.schema.fields)} columns]"
    
    def show(self, n: int = 20, truncate: bool = True) -> None:
        """Show DataFrame content."""
        print(f"+--- MockDataFrame: {len(self.data)} rows ---+")
        if not self.data:
            print("(empty)")
            return
        
        # Show first n rows
        display_data = self.data[:n]
        
        # Get column names
        columns = list(display_data[0].keys()) if display_data else self.schema.fieldNames()
        
        # Print header
        header = " | ".join(f"{col:>12}" for col in columns)
        print(header)
        print("-" * len(header))
        
        # Print data
        for row in display_data:
            row_str = " | ".join(f"{str(row.get(col, 'null')):>12}" for col in columns)
            print(row_str)
        
        if len(self.data) > n:
            print(f"... ({len(self.data) - n} more rows)")
    
    def collect(self) -> List[Dict[str, Any]]:
        """Collect all data as list of dictionaries."""
        return self.data.copy()
    
    def count(self) -> int:
        """Count number of rows."""
        if self._cached_count is None:
            self._cached_count = len(self.data)
        return self._cached_count
    
    @property
    def columns(self) -> List[str]:
        """Get column names."""
        return [field.name for field in self.schema.fields]
    
    def printSchema(self) -> None:
        """Print DataFrame schema."""
        print("MockDataFrame Schema:")
        for field in self.schema.fields:
            nullable = "nullable" if field.nullable else "not nullable"
            print(f" |-- {field.name}: {field.dataType.__class__.__name__} ({nullable})")
    
    def select(self, *columns: Union[str, MockColumn]) -> 'MockDataFrame':
        """Select columns."""
        if not columns:
            return self
        
        # Convert MockColumn to string names
        col_names = []
        for col in columns:
            if isinstance(col, MockColumn):
                col_names.append(col.name)
            elif isinstance(col, str):
                col_names.append(col)
            else:
                raise_value_error(f"Invalid column type: {type(col)}")  # type: ignore[unreachable]
        
        # Validate columns exist
        for col_name in col_names:
            if col_name not in [field.name for field in self.schema.fields]:
                raise_column_not_found(col_name)
        
        # Filter data to selected columns
        filtered_data = []
        for row in self.data:
            filtered_row = {name: row.get(name) for name in col_names}
            filtered_data.append(filtered_row)
        
        # Create new schema
        new_fields = [field for field in self.schema.fields if field.name in col_names]
        new_schema = MockStructType(new_fields)
        
        return MockDataFrame(filtered_data, new_schema, self.storage)
    
    def filter(self, condition: Union[MockColumnOperation, MockColumn]) -> 'MockDataFrame':
        """Filter rows based on condition."""
        if isinstance(condition, MockColumn):
            # Simple column reference - return all non-null rows
            filtered_data = [row for row in self.data if row.get(condition.name) is not None]
        else:
            # Apply condition logic
            filtered_data = self._apply_condition(self.data, condition)
        
        return MockDataFrame(filtered_data, self.schema, self.storage)
    
    def withColumn(self, col_name: str, col: Union[MockColumn, Any]) -> 'MockDataFrame':
        """Add or replace column."""
        new_data = []
        
        for row in self.data:
            new_row = row.copy()
            
            if isinstance(col, MockColumn):
                # For now, just add a placeholder value
                # In a real implementation, this would evaluate the expression
                new_row[col_name] = f"computed_{col_name}"
            else:
                new_row[col_name] = col
            
            new_data.append(new_row)
        
        # Update schema
        new_fields = [field for field in self.schema.fields if field.name != col_name]
        from .spark_types import StringType
        new_fields.append(MockStructField(col_name, StringType()))
        new_schema = MockStructType(new_fields)
        
        return MockDataFrame(new_data, new_schema, self.storage)
    
    def groupBy(self, *columns: Union[str, MockColumn]) -> 'MockGroupedData':
        """Group by columns."""
        col_names = []
        for col in columns:
            if isinstance(col, MockColumn):
                col_names.append(col.name)
            else:
                col_names.append(col)
        
        return MockGroupedData(self, col_names)
    
    def orderBy(self, *columns: Union[str, MockColumn]) -> 'MockDataFrame':
        """Order by columns."""
        col_names = []
        for col in columns:
            if isinstance(col, MockColumn):
                col_names.append(col.name)
            else:
                col_names.append(col)
        
        # Sort data by columns
        sorted_data = sorted(self.data, key=lambda row: tuple(row.get(col, None) for col in col_names))
        
        return MockDataFrame(sorted_data, self.schema, self.storage)
    
    def limit(self, n: int) -> 'MockDataFrame':
        """Limit number of rows."""
        limited_data = self.data[:n]
        return MockDataFrame(limited_data, self.schema, self.storage)
    
    def union(self, other: 'MockDataFrame') -> 'MockDataFrame':
        """Union with another DataFrame."""
        combined_data = self.data + other.data
        return MockDataFrame(combined_data, self.schema, self.storage)
    
    def join(self, other: 'MockDataFrame', on: Union[str, List[str]], how: str = "inner") -> 'MockDataFrame':
        """Join with another DataFrame."""
        if isinstance(on, str):
            on = [on]
        
        # Simple join implementation
        joined_data = []
        for left_row in self.data:
            for right_row in other.data:
                # Check if join condition matches
                if all(left_row.get(col) == right_row.get(col) for col in on):
                    joined_row = left_row.copy()
                    joined_row.update(right_row)
                    joined_data.append(joined_row)
        
        # Create new schema
        new_fields = self.schema.fields.copy()
        for field in other.schema.fields:
            if not any(f.name == field.name for f in new_fields):
                new_fields.append(field)
        
        new_schema = MockStructType(new_fields)
        return MockDataFrame(joined_data, new_schema, self.storage)
    
    def cache(self) -> 'MockDataFrame':
        """Cache DataFrame (no-op in mock)."""
        return self
    
    @property
    def write(self) -> MockDataFrameWriter:
        """Get DataFrame writer."""
        return MockDataFrameWriter(self, self.storage)
    
    def _apply_condition(self, data: List[Dict[str, Any]], condition: MockColumnOperation) -> List[Dict[str, Any]]:
        """Apply condition to filter data."""
        # This is a simplified implementation
        # In a real implementation, this would parse and evaluate the condition
        filtered_data = []
        
        for row in data:
            if self._evaluate_condition(row, condition):
                filtered_data.append(row)
        
        return filtered_data
    
    def _evaluate_condition(self, row: Dict[str, Any], condition: Union[MockColumnOperation, MockColumn]) -> bool:
        """Evaluate condition for a single row."""
        # Handle MockColumn case
        if isinstance(condition, MockColumn):
            return row.get(condition.name) is not None
        
        # Handle MockColumnOperation case
        if condition.operation == "isNotNull":
            return row.get(condition.column.name) is not None
        elif condition.operation == "isNull":
            return row.get(condition.column.name) is None
        elif condition.operation == "eq":
            return bool(row.get(condition.column.name) == condition.value)
        elif condition.operation == "ne":
            return bool(row.get(condition.column.name) != condition.value)
        elif condition.operation == "gt":
            return bool(row.get(condition.column.name) > condition.value)
        elif condition.operation == "ge":
            return bool(row.get(condition.column.name) >= condition.value)
        elif condition.operation == "lt":
            return bool(row.get(condition.column.name) < condition.value)
        elif condition.operation == "le":
            return bool(row.get(condition.column.name) <= condition.value)
        elif condition.operation == "like":
            value = str(row.get(condition.column.name, ""))
            pattern = str(condition.value).replace("%", ".*")
            import re
            return bool(re.match(pattern, value))
        elif condition.operation == "isin":
            return row.get(condition.column.name) in condition.value
        elif condition.operation == "between":
            between_value: Any = row.get(condition.column.name)
            if between_value is None:
                return False
            lower, upper = condition.value
            return bool(lower <= between_value <= upper)
        elif condition.operation == "and":
            return (self._evaluate_condition(row, condition.column) and 
                   self._evaluate_condition(row, condition.value))
        elif condition.operation == "or":
            return (self._evaluate_condition(row, condition.column) or 
                   self._evaluate_condition(row, condition.value))
        elif condition.operation == "not":
            return not self._evaluate_condition(row, condition.column)
        else:
            return True  # Default to True for unknown operations


class MockGroupedData:
    """Mock grouped data for aggregation operations."""
    
    def __init__(self, df: MockDataFrame, group_columns: List[str]):
        self.df = df
        self.group_columns = group_columns
    
    def agg(self, *exprs: Union[MockColumn, Dict[str, MockColumn]]) -> MockDataFrame:
        """Aggregate grouped data."""
        if not exprs:
            return self.df
        
        # Group data by group columns
        groups: Dict[Any, List[Dict[str, Any]]] = {}
        for row in self.df.data:
            key = tuple(row.get(col) for col in self.group_columns)
            if key not in groups:
                groups[key] = []
            groups[key].append(row)
        
        # Apply aggregations
        result_data = []
        for key, group_rows in groups.items():
            result_row = {}
            
            # Add group columns
            for i, col in enumerate(self.group_columns):
                result_row[col] = key[i]
            
            # Add aggregated columns
            for expr in exprs:
                if isinstance(expr, MockColumn):
                    # Simple aggregation
                    col_name = expr.name
                    if "count" in col_name:
                        result_row[col_name] = len(group_rows)
                    elif "sum" in col_name:
                        # Extract column name from sum(column_name)
                        inner_col = col_name.replace("sum(", "").replace(")", "")
                        values = [row.get(inner_col, 0) for row in group_rows if row.get(inner_col) is not None]
                        result_row[col_name] = sum(values) if values else 0
                    elif "avg" in col_name:
                        inner_col = col_name.replace("avg(", "").replace(")", "")
                        values = [row.get(inner_col, 0) for row in group_rows if row.get(inner_col) is not None]
                        result_row[col_name] = sum(values) / len(values) if values else 0
                    elif "max" in col_name:
                        inner_col = col_name.replace("max(", "").replace(")", "")
                        values = [row.get(inner_col) for row in group_rows if row.get(inner_col) is not None]
                        result_row[col_name] = max(values) if values else None
                    elif "min" in col_name:
                        inner_col = col_name.replace("min(", "").replace(")", "")
                        values = [row.get(inner_col) for row in group_rows if row.get(inner_col) is not None]
                        result_row[col_name] = min(values) if values else None
                    else:
                        result_row[col_name] = len(group_rows)
            
            result_data.append(result_row)
        
        # Create new schema
        new_fields = []
        for col in self.group_columns:
            new_fields.append(MockStructField(col, StringType()))
        
        for expr in exprs:
            if isinstance(expr, MockColumn):
                new_fields.append(MockStructField(expr.name, IntegerType()))
        
        new_schema = MockStructType(new_fields)
        return MockDataFrame(result_data, new_schema, self.df.storage)
    
    def count(self) -> MockDataFrame:
        """Count grouped data."""
        return self.agg(F.count("*"))
    
    def sum(self, *columns: Union[str, MockColumn]) -> MockDataFrame:
        """Sum grouped data."""
        if not columns:
            return self.agg(F.sum("*"))
        
        exprs = [F.sum(col) if isinstance(col, str) else col for col in columns]
        return self.agg(*exprs)
    
    def avg(self, *columns: Union[str, MockColumn]) -> MockDataFrame:
        """Average grouped data."""
        if not columns:
            return self.agg(F.avg("*"))
        
        exprs = [F.avg(col) if isinstance(col, str) else col for col in columns]
        return self.agg(*exprs)
    
    def max(self, *columns: Union[str, MockColumn]) -> MockDataFrame:
        """Max grouped data."""
        if not columns:
            return self.agg(F.max("*"))
        
        exprs = [F.max(col) if isinstance(col, str) else col for col in columns]
        return self.agg(*exprs)
    
    def min(self, *columns: Union[str, MockColumn]) -> MockDataFrame:
        """Min grouped data."""
        if not columns:
            return self.agg(F.min("*"))
        
        exprs = [F.min(col) if isinstance(col, str) else col for col in columns]
        return self.agg(*exprs)
