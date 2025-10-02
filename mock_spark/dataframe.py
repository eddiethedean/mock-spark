"""
Mock DataFrame implementation for Mock Spark.

This module provides a complete mock implementation of PySpark DataFrame
that behaves identically to the real PySpark DataFrame.
"""

from typing import Any, Dict, List, Optional, Union, Tuple
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
    
    @property
    def saveMode(self) -> str:
        """Get the current save mode (PySpark compatibility)."""
        return self.save_mode
    
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
    
    def collect(self) -> List[Any]:
        """Collect all data as list of Row objects."""
        from .spark_types import MockRow
        return [MockRow(row) for row in self.data]
    
    def toPandas(self) -> "pandas.DataFrame":
        """Convert to pandas DataFrame."""
        import pandas as pd
        return pd.DataFrame(self.data)
    
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
        
        # Import MockLiteral and MockAggregateFunction to check for special columns
        from .functions import MockLiteral, MockAggregateFunction
        
        # Check if this is an aggregation operation
        has_aggregation = any(
            isinstance(col, MockAggregateFunction) or 
            (isinstance(col, MockColumn) and col.name.startswith(("count(", "sum(", "avg(", "max(", "min(", "countDistinct(")))
            for col in columns
        )
        
        if has_aggregation:
            # Handle aggregation - return single row
            return self._handle_aggregation_select(columns)
        
        # Process columns and handle literals
        col_names = []
        literal_columns = {}
        literal_objects = {}  # Store MockLiteral objects for type information
        
        for col in columns:
            if isinstance(col, MockColumn):
                col_names.append(col.name)
            elif isinstance(col, str):
                col_names.append(col)
            elif isinstance(col, MockLiteral):
                # Handle literal columns
                literal_name = col.name
                col_names.append(literal_name)
                literal_columns[literal_name] = col.value
                literal_objects[literal_name] = col  # Store the MockLiteral object
            elif hasattr(col, 'operation') and hasattr(col, 'column'):
                # Handle MockColumnOperation (e.g., col + 1, upper(col))
                col_names.append(col.name)
            elif hasattr(col, 'name'):  # Support other column-like objects
                col_names.append(col.name)
            else:
                raise_value_error(f"Invalid column type: {type(col)}")  # type: ignore[unreachable]
        
        # Validate non-literal columns exist (skip validation for MockColumnOperation and function calls)
        for col_name in col_names:
            if (col_name not in [field.name for field in self.schema.fields] and 
                col_name not in literal_columns and
                not any(hasattr(col, 'operation') and hasattr(col, 'column') and col.name == col_name for col in columns) and
                not col_name.startswith(("upper(", "lower(", "length(", "abs(", "round(", "count(", "sum(", "avg(", "max(", "min(", "countDistinct("))):
                raise_column_not_found(col_name)
        
        # Filter data to selected columns and add literal values
        filtered_data = []
        for row in self.data:
            filtered_row = {}
            for i, col in enumerate(columns):
                if isinstance(col, MockColumn):
                    col_name = col.name
                    if col_name in literal_columns:
                        # Add literal value
                        filtered_row[col_name] = literal_columns[col_name]
                    elif col_name.startswith(("upper(", "lower(", "length(", "abs(", "round(")):
                        # Handle function calls
                        evaluated_value = self._evaluate_column_expression(row, col)
                        filtered_row[col_name] = evaluated_value
                    else:
                        # Add existing column value
                        filtered_row[col_name] = row[col_name]
                elif isinstance(col, str):
                    col_name = col
                    if col_name in literal_columns:
                        # Add literal value
                        filtered_row[col_name] = literal_columns[col_name]
                    else:
                        # Add existing column value
                        filtered_row[col_name] = row[col_name]
                elif hasattr(col, 'operation') and hasattr(col, 'column'):
                    # Handle MockColumnOperation (e.g., upper(col), length(col))
                    col_name = col.name
                    evaluated_value = self._evaluate_column_expression(row, col)
                    filtered_row[col_name] = evaluated_value
                elif hasattr(col, 'name'):
                    col_name = col.name
                    if col_name in literal_columns:
                        # Add literal value
                        filtered_row[col_name] = literal_columns[col_name]
                    else:
                        # Add existing column value
                        filtered_row[col_name] = row[col_name]
            filtered_data.append(filtered_row)
        
        # Create new schema
        new_fields = []
        for i, col in enumerate(columns):
            if isinstance(col, MockLiteral):
                # Handle MockLiteral directly
                col_name = col.name
                new_fields.append(MockStructField(col_name, col.column_type))
            elif isinstance(col, MockColumn):
                col_name = col.name
                # Check if this is a function call first
                if col_name.startswith(("abs(", "round(", "upper(", "lower(", "length(")):
                    from .spark_types import DoubleType, StringType, LongType, IntegerType
                    if col_name.startswith("abs("):
                        new_fields.append(MockStructField(col_name, LongType()))  # abs() returns LongType for integers
                    elif col_name.startswith("round("):
                        new_fields.append(MockStructField(col_name, DoubleType()))
                    elif col_name.startswith("length("):
                        new_fields.append(MockStructField(col_name, IntegerType()))  # length() returns IntegerType
                    elif col_name.startswith(("upper(", "lower(")):
                        new_fields.append(MockStructField(col_name, StringType()))
                    else:
                        new_fields.append(MockStructField(col_name, StringType()))
                elif col_name in literal_columns:
                    # Create field for literal column with correct type
                    if col_name in literal_objects:
                        # Use the MockLiteral object's column_type
                        literal_obj = literal_objects[col_name]
                        new_fields.append(MockStructField(col_name, literal_obj.column_type))
                    else:
                        # Fallback to type inference
                        from .spark_types import convert_python_type_to_mock_type, IntegerType
                        literal_value = literal_columns[col_name]
                        if isinstance(literal_value, int):
                            literal_type = IntegerType()
                        else:
                            literal_type = convert_python_type_to_mock_type(type(literal_value))
                        new_fields.append(MockStructField(col_name, literal_type))
                else:
                    # Use existing field
                    for field in self.schema.fields:
                        if field.name == col_name:
                            new_fields.append(field)
                            break
            elif isinstance(col, str):
                col_name = col
                if col_name in literal_columns:
                    # Create field for literal column with correct type
                    from .spark_types import convert_python_type_to_mock_type
                    literal_value = literal_columns[col_name]
                    literal_type = convert_python_type_to_mock_type(type(literal_value))
                    new_fields.append(MockStructField(col_name, literal_type))
                else:
                    # Use existing field
                    for field in self.schema.fields:
                        if field.name == col_name:
                            new_fields.append(field)
                            break
            elif hasattr(col, 'operation') and hasattr(col, 'column'):
                # Handle MockColumnOperation (e.g., upper(col), length(col))
                col_name = col.name
                from .spark_types import StringType, LongType, DoubleType
                if col.operation in ["upper", "lower"]:
                    new_fields.append(MockStructField(col_name, StringType()))
                elif col.operation == "length":
                    new_fields.append(MockStructField(col_name, LongType()))
                elif col.operation in ["+", "-", "*", "%"]:
                    # Arithmetic operations - use LongType for integer arithmetic to match PySpark
                    new_fields.append(MockStructField(col_name, LongType()))
                elif col.operation == "/":
                    # Division operation - use DoubleType for decimal results
                    new_fields.append(MockStructField(col_name, DoubleType()))
                else:
                    # Default to StringType for other operations
                    new_fields.append(MockStructField(col_name, StringType()))
            elif isinstance(col, MockColumn) and col.name.startswith(("abs(", "round(", "upper(", "lower(", "length(")):
                # Handle function calls like abs(column), round(column), upper(column), etc.
                col_name = col.name
                from .spark_types import DoubleType, StringType, LongType
                if col.name.startswith(("abs(", "round(")):
                    new_fields.append(MockStructField(col_name, DoubleType()))
                elif col.name.startswith("length("):
                    new_fields.append(MockStructField(col_name, LongType()))
                elif col.name.startswith(("upper(", "lower(")):
                    new_fields.append(MockStructField(col_name, StringType()))
                else:
                    new_fields.append(MockStructField(col_name, StringType()))
            elif hasattr(col, 'name'):
                col_name = col.name
                if col_name in literal_columns:
                    # Create field for literal column with correct type
                    from .spark_types import convert_python_type_to_mock_type
                    literal_value = literal_columns[col_name]
                    literal_type = convert_python_type_to_mock_type(type(literal_value))
                    new_fields.append(MockStructField(col_name, literal_type))
                else:
                    # Use existing field
                    for field in self.schema.fields:
                        if field.name == col_name:
                            new_fields.append(field)
                            break
        
        new_schema = MockStructType(new_fields)
        return MockDataFrame(filtered_data, new_schema, self.storage)
    
    def _handle_aggregation_select(self, columns: List[Union[str, MockColumn]]) -> 'MockDataFrame':
        """Handle aggregation select operations."""
        from .functions import MockAggregateFunction
        from .spark_types import LongType, DoubleType
        
        result_row = {}
        new_fields = []
        
        for col in columns:
            if isinstance(col, MockAggregateFunction):
                func_name = col.function_name
                col_name = col.column_name
                
                if func_name == "count":
                    if col_name is None or col_name == "*":
                        agg_col_name = "count(1)"
                    else:
                        agg_col_name = f"count({col_name})"
                    result_row[agg_col_name] = len(self.data)
                    new_fields.append(MockStructField(agg_col_name, LongType()))
                elif func_name == "sum":
                    agg_col_name = f"sum({col_name})"
                    values = [row.get(col_name, 0) for row in self.data if row.get(col_name) is not None]
                    result_row[agg_col_name] = sum(values) if values else 0
                    new_fields.append(MockStructField(agg_col_name, DoubleType()))
                elif func_name == "avg":
                    agg_col_name = f"avg({col_name})"
                    values = [row.get(col_name, 0) for row in self.data if row.get(col_name) is not None]
                    result_row[agg_col_name] = sum(values) / len(values) if values else 0
                    new_fields.append(MockStructField(agg_col_name, DoubleType()))
                elif func_name == "max":
                    agg_col_name = f"max({col_name})"
                    values = [row.get(col_name) for row in self.data if row.get(col_name) is not None]
                    result_row[agg_col_name] = max(values) if values else None
                    new_fields.append(MockStructField(agg_col_name, DoubleType()))
                elif func_name == "min":
                    agg_col_name = f"min({col_name})"
                    values = [row.get(col_name) for row in self.data if row.get(col_name) is not None]
                    result_row[agg_col_name] = min(values) if values else None
                    new_fields.append(MockStructField(agg_col_name, DoubleType()))
                elif func_name == "countDistinct":
                    agg_col_name = f"countDistinct({col_name})"
                    values = [row.get(col_name) for row in self.data if row.get(col_name) is not None]
                    result_row[agg_col_name] = len(set(values)) if values else 0
                    new_fields.append(MockStructField(agg_col_name, LongType()))
            elif isinstance(col, MockColumn) and col.name.startswith(("count(", "sum(", "avg(", "max(", "min(", "countDistinct(")):
                # Handle MockColumn with function names
                col_name = col.name
                if col_name.startswith("count("):
                    if col_name == "count(1)":
                        result_row[col_name] = len(self.data)
                    else:
                        # Extract column name from count(column)
                        inner_col = col_name[6:-1]
                        result_row[col_name] = len(self.data)
                    new_fields.append(MockStructField(col_name, LongType()))
                elif col_name.startswith("sum("):
                    inner_col = col_name[4:-1]
                    values = [row.get(inner_col, 0) for row in self.data if row.get(inner_col) is not None]
                    result_row[col_name] = sum(values) if values else 0
                    new_fields.append(MockStructField(col_name, DoubleType()))
                elif col_name.startswith("avg("):
                    inner_col = col_name[4:-1]
                    values = [row.get(inner_col, 0) for row in self.data if row.get(inner_col) is not None]
                    result_row[col_name] = sum(values) / len(values) if values else 0
                    new_fields.append(MockStructField(col_name, DoubleType()))
                elif col_name.startswith("max("):
                    inner_col = col_name[4:-1]
                    values = [row.get(inner_col) for row in self.data if row.get(inner_col) is not None]
                    result_row[col_name] = max(values) if values else None
                    new_fields.append(MockStructField(col_name, DoubleType()))
                elif col_name.startswith("min("):
                    inner_col = col_name[4:-1]
                    values = [row.get(inner_col) for row in self.data if row.get(inner_col) is not None]
                    result_row[col_name] = min(values) if values else None
                    new_fields.append(MockStructField(col_name, DoubleType()))
                elif col_name.startswith("countDistinct("):
                    inner_col = col_name[13:-1]
                    values = [row.get(inner_col) for row in self.data if row.get(inner_col) is not None]
                    result_row[col_name] = len(set(values)) if values else 0
                    new_fields.append(MockStructField(col_name, LongType()))
        
        new_schema = MockStructType(new_fields)
        return MockDataFrame([result_row], new_schema, self.storage)
    
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
        sort_orders = []
        
        for col in columns:
            if isinstance(col, MockColumn):
                col_names.append(col.name)
                sort_orders.append(True)  # Default ascending
            elif hasattr(col, 'operation') and hasattr(col, 'column'):
                # Handle MockColumnOperation (e.g., col.desc())
                if col.operation == "desc":
                    col_names.append(col.column.name)
                    sort_orders.append(False)  # Descending
                elif col.operation == "asc":
                    col_names.append(col.column.name)
                    sort_orders.append(True)  # Ascending
                else:
                    col_names.append(col.column.name)
                    sort_orders.append(True)  # Default ascending
            else:
                col_names.append(col)
                sort_orders.append(True)  # Default ascending
        
        # Sort data by columns with proper ordering
        def sort_key(row):
            key_values = []
            for i, col in enumerate(col_names):
                value = row.get(col, None)
                # Handle None values for sorting
                if value is None:
                    value = float('inf') if sort_orders[i] else float('-inf')
                key_values.append(value)
            return tuple(key_values)
        
        sorted_data = sorted(self.data, key=sort_key, reverse=any(not order for order in sort_orders))
        
        return MockDataFrame(sorted_data, self.schema, self.storage)
    
    def limit(self, n: int) -> 'MockDataFrame':
        """Limit number of rows."""
        limited_data = self.data[:n]
        return MockDataFrame(limited_data, self.schema, self.storage)
    
    def take(self, n: int) -> List[Dict[str, Any]]:
        """Take first n rows as list of dictionaries."""
        return self.data[:n]
    
    @property
    def dtypes(self) -> List[Tuple[str, str]]:
        """Get column names and their data types."""
        return [(field.name, field.dataType.__class__.__name__) for field in self.schema.fields]
    
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
        elif condition.operation == "==":
            return bool(row.get(condition.column.name) == condition.value)
        elif condition.operation == "!=":
            return bool(row.get(condition.column.name) != condition.value)
        elif condition.operation == ">":
            return bool(row.get(condition.column.name) > condition.value)
        elif condition.operation == ">=":
            return bool(row.get(condition.column.name) >= condition.value)
        elif condition.operation == "<":
            return bool(row.get(condition.column.name) < condition.value)
        elif condition.operation == "<=":
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
    
    def _evaluate_column_expression(self, row: Dict[str, Any], column: Union[MockColumn, MockColumnOperation]) -> Any:
        """Evaluate a column expression for a single row."""
        if isinstance(column, MockColumn):
            # Check if this is a function call (e.g., abs(age), round(salary), upper(name))
            if column.name.startswith("abs("):
                col_name = column.name[4:-1]  # Extract column name from abs(column)
                value = self._get_column_value(row, col_name)
                return abs(value) if value is not None else None
            elif column.name.startswith("round("):
                # Handle round(column, scale) - extract column and scale
                parts = column.name[6:-1].split(", ")
                col_name = parts[0]
                scale = int(parts[1]) if len(parts) > 1 else 0
                value = self._get_column_value(row, col_name)
                return round(value, scale) if value is not None else None
            elif column.name.startswith("upper("):
                col_name = column.name[6:-1]  # Extract column name from upper(column)
                value = self._get_column_value(row, col_name)
                return str(value).upper() if value is not None else None
            elif column.name.startswith("lower("):
                col_name = column.name[6:-1]  # Extract column name from lower(column)
                value = self._get_column_value(row, col_name)
                return str(value).lower() if value is not None else None
            elif column.name.startswith("length("):
                col_name = column.name[7:-1]  # Extract column name from length(column)
                value = self._get_column_value(row, col_name)
                return len(str(value)) if value is not None else None
            else:
                return row.get(column.name)
        
        if isinstance(column, MockColumnOperation):
            if column.operation == "upper":
                value = self._get_column_value(row, column.column.name)
                return str(value).upper() if value is not None else None
            elif column.operation == "lower":
                value = self._get_column_value(row, column.column.name)
                return str(value).lower() if value is not None else None
            elif column.operation == "length":
                value = self._get_column_value(row, column.column.name)
                return len(str(value)) if value is not None else None
            elif column.operation == "+":
                left_val = self._get_column_value(row, column.column.name)
                right_val = column.value
                return left_val + right_val if left_val is not None else None
            elif column.operation == "-":
                left_val = self._get_column_value(row, column.column.name)
                right_val = column.value
                return left_val - right_val if left_val is not None else None
            elif column.operation == "*":
                left_val = self._get_column_value(row, column.column.name)
                right_val = column.value
                return left_val * right_val if left_val is not None else None
            elif column.operation == "/":
                left_val = self._get_column_value(row, column.column.name)
                right_val = column.value
                return left_val / right_val if left_val is not None and right_val != 0 else None
            elif column.operation == "%":
                left_val = self._get_column_value(row, column.column.name)
                right_val = column.value
                return left_val % right_val if left_val is not None and right_val != 0 else None
            else:
                return self._get_column_value(row, column.column.name)
        
        return None
    
    def _get_column_value(self, row: Dict[str, Any], col_name: str) -> Any:
        """Get a column value, handling MockColumnOperation objects."""
        value = row.get(col_name)
        if isinstance(value, MockColumnOperation):
            # Recursively evaluate MockColumnOperation
            return self._evaluate_column_expression(row, value)
        return value


class MockGroupedData:
    """Mock grouped data for aggregation operations."""
    
    def __init__(self, df: MockDataFrame, group_columns: List[str]):
        self.df = df
        self.group_columns = group_columns
    
    def agg(self, *exprs: Union[MockColumn, Dict[str, MockColumn]]) -> MockDataFrame:
        """Aggregate grouped data."""
        if not exprs:
            return self.df
        
        # Import MockAggregateFunction
        from .functions import MockAggregateFunction
        
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
                if isinstance(expr, MockAggregateFunction):
                    # Handle MockAggregateFunction
                    func_name = expr.function_name
                    col_name = expr.column_name
                    
                    if func_name == "count":
                        if col_name is None or col_name == "*":
                            agg_col_name = "count(1)"
                        else:
                            agg_col_name = f"count({col_name})"
                        result_row[agg_col_name] = len(group_rows)
                    elif func_name == "sum":
                        agg_col_name = f"sum({col_name})"
                        values = [row.get(col_name, 0) for row in group_rows if row.get(col_name) is not None]
                        result_row[agg_col_name] = sum(values) if values else 0
                    elif func_name == "avg":
                        agg_col_name = f"avg({col_name})"
                        values = [row.get(col_name, 0) for row in group_rows if row.get(col_name) is not None]
                        result_row[agg_col_name] = sum(values) / len(values) if values else 0
                    elif func_name == "max":
                        agg_col_name = f"max({col_name})"
                        values = [row.get(col_name) for row in group_rows if row.get(col_name) is not None]
                        result_row[agg_col_name] = max(values) if values else None
                    elif func_name == "min":
                        agg_col_name = f"min({col_name})"
                        values = [row.get(col_name) for row in group_rows if row.get(col_name) is not None]
                        result_row[agg_col_name] = min(values) if values else None
                elif isinstance(expr, MockColumn):
                    # Handle MockColumn (legacy support)
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
            if isinstance(expr, MockAggregateFunction):
                func_name = expr.function_name
                col_name = expr.column_name
                
                if func_name == "count":
                    if col_name is None or col_name == "*":
                        agg_col_name = "count(1)"
                    else:
                        agg_col_name = f"count({col_name})"
                elif func_name in ["sum", "avg", "max", "min"]:
                    agg_col_name = f"{func_name}({col_name})"
                else:
                    agg_col_name = f"{func_name}()"
                
                new_fields.append(MockStructField(agg_col_name, IntegerType()))
            elif isinstance(expr, MockColumn):
                new_fields.append(MockStructField(expr.name, IntegerType()))
        
        new_schema = MockStructType(new_fields)
        return MockDataFrame(result_data, new_schema, self.df.storage)
    
    def count(self) -> MockDataFrame:
        """Count grouped data."""
        # For the count() method, we want to return just "count" as the column name
        # This is different from agg(F.count("*")) which returns "count(1)"
        
        # Group data by group columns (same logic as agg method)
        groups: Dict[Any, List[Dict[str, Any]]] = {}
        for row in self.df.data:
            key = tuple(row.get(col) for col in self.group_columns)
            if key not in groups:
                groups[key] = []
            groups[key].append(row)
        
        result_data = []
        for key, group_rows in groups.items():
            result_row = {}
            
            # Add group columns
            for i, col in enumerate(self.group_columns):
                result_row[col] = key[i]
            
            # Add count
            result_row["count"] = len(group_rows)
            result_data.append(result_row)
        
        # Create schema
        new_fields = []
        for field in self.df.schema.fields:
            if field.name in self.group_columns:
                new_fields.append(field)
        new_fields.append(MockStructField("count", IntegerType()))
        
        new_schema = MockStructType(new_fields)
        return MockDataFrame(result_data, new_schema, self.df.storage)
    
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
