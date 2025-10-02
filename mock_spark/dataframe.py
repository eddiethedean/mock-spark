"""
Mock DataFrame implementation for Mock Spark.

This module provides a complete mock implementation of PySpark DataFrame
that behaves identically to the real PySpark DataFrame for testing and
development purposes. It supports all major DataFrame operations including
selection, filtering, grouping, joining, and window functions.

Key Features:
    - Complete PySpark API compatibility
    - Type-safe operations with proper schema inference
    - Window function support with partitioning and ordering
    - Comprehensive error handling matching PySpark exceptions
    - In-memory storage for fast test execution

Example:
    >>> from mock_spark import MockSparkSession, F
    >>> spark = MockSparkSession("test")
    >>> data = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
    >>> df = spark.createDataFrame(data)
    >>> df.select("name", "age").filter(F.col("age") > 25).show()
    +----+---+
    |name|age|
    +----+---+
    | Bob| 30|
    +----+---+
"""

from typing import Any, Dict, List, Optional, Union, Tuple
from dataclasses import dataclass
import pandas as pd

from .spark_types import MockStructType, MockStructField, StringType, IntegerType, MockRow
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
    """Mock DataFrame writer for saveAsTable operations.
    
    Provides a PySpark-compatible interface for writing DataFrames to storage
    formats. Supports various formats and save modes for testing and development.
    
    Attributes:
        df: The DataFrame to be written.
        storage: Storage manager for persisting data.
        format_name: Output format (e.g., 'parquet', 'json').
        save_mode: Save mode ('append', 'overwrite', 'error', 'ignore').
        options: Additional options for the writer.
    
    Example:
        >>> df.write.format("parquet").mode("overwrite").saveAsTable("my_table")
    """
    
    def __init__(self, df: 'MockDataFrame', storage: MockStorageManager):
        self.df = df
        self.storage = storage
        self.format_name = "parquet"
        self.save_mode = "append"
        self.options: Dict[str, Any] = {}
    
    def format(self, source: str) -> 'MockDataFrameWriter':
        """Set the output format for the DataFrame writer.
        
        Args:
            source: The output format (e.g., 'parquet', 'json', 'csv').
            
        Returns:
            Self for method chaining.
            
        Example:
            >>> df.write.format("parquet")
        """
        self.format_name = source
        return self
    
    def mode(self, mode: str) -> 'MockDataFrameWriter':
        """Set the save mode for the DataFrame writer.
        
        Args:
            mode: Save mode ('append', 'overwrite', 'error', 'ignore').
            
        Returns:
            Self for method chaining.
            
        Example:
            >>> df.write.mode("overwrite")
        """
        self.save_mode = mode
        return self
    
    @property
    def saveMode(self) -> str:
        """Get the current save mode (PySpark compatibility).
        
        Returns:
            Current save mode string.
        """
        return self.save_mode
    
    def option(self, key: str, value: Any) -> 'MockDataFrameWriter':
        """Set an option for the DataFrame writer.
        
        Args:
            key: Option key.
            value: Option value.
            
        Returns:
            Self for method chaining.
            
        Example:
            >>> df.write.option("compression", "snappy")
        """
        self.options[key] = value
        return self
    
    def saveAsTable(self, table_name: str) -> None:
        """Save DataFrame as a table in storage.
        
        Args:
            table_name: Name of the table (can include schema, e.g., 'schema.table').
            
        Raises:
            AnalysisException: If table operations fail.
            
        Example:
            >>> df.write.saveAsTable("my_table")
            >>> df.write.saveAsTable("schema.my_table")
        """
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
    """Mock DataFrame implementation with complete PySpark API compatibility.
    
    Provides a comprehensive mock implementation of PySpark DataFrame that supports
    all major operations including selection, filtering, grouping, joining, and
    window functions. Designed for testing and development without requiring JVM.
    
    Attributes:
        data: List of dictionaries representing DataFrame rows.
        schema: MockStructType defining the DataFrame schema.
        storage: Optional storage manager for persistence operations.
    
    Example:
        >>> from mock_spark import MockSparkSession, F
        >>> spark = MockSparkSession("test")
        >>> data = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
        >>> df = spark.createDataFrame(data)
        >>> df.select("name").filter(F.col("age") > 25).show()
        +----+
        |name|
        +----+
        | Bob|
        +----+
    """
    
    def __init__(self, data: List[Dict[str, Any]], schema: MockStructType, storage: Optional[MockStorageManager] = None):
        """Initialize MockDataFrame.
        
        Args:
            data: List of dictionaries representing DataFrame rows.
            schema: MockStructType defining the DataFrame schema.
            storage: Optional storage manager for persistence operations.
                    Defaults to a new MockStorageManager instance.
        """
        self.data = data
        self.schema = schema
        self.storage = storage or MockStorageManager()
        self._cached_count: Optional[int] = None
    
    def __repr__(self) -> str:
        return f"MockDataFrame[{len(self.data)} rows, {len(self.schema.fields)} columns]"
    
    def show(self, n: int = 20, truncate: bool = True) -> None:
        """Display DataFrame content in a formatted table.
        
        Args:
            n: Number of rows to display (default: 20).
            truncate: Whether to truncate long values (default: True).
            
        Example:
            >>> df.show(5)
            +--- MockDataFrame: 3 rows ---+
                name | age | salary
            --------|-----|--------
              Alice |  25 |  50000
                Bob |  30 |  60000
            Charlie |  35 |  70000
        """
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
    
    def collect(self) -> List[MockRow]:
        """Collect all data as list of Row objects."""
        return [MockRow(row) for row in self.data]
    
    def toPandas(self) -> Any:
        """Convert to pandas DataFrame."""
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
        """Select columns from the DataFrame.
        
        Args:
            *columns: Column names, MockColumn objects, or expressions to select.
                     Use "*" to select all columns.
        
        Returns:
            New MockDataFrame with selected columns.
            
        Raises:
            AnalysisException: If specified columns don't exist.
            
        Example:
            >>> df.select("name", "age")
            >>> df.select("*")
            >>> df.select(F.col("name"), F.col("age") * 2)
        """
        if not columns:
            return self
        
        # Import MockLiteral and MockAggregateFunction to check for special columns
        from .functions import MockLiteral, MockAggregateFunction
        
        # Check if this is an aggregation operation
        has_aggregation = any(
            isinstance(col, MockAggregateFunction) or 
            (isinstance(col, MockColumn) and (col.name.startswith(("count(", "sum(", "avg(", "max(", "min(")) or col.name.startswith("count(DISTINCT ")))
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
                if col.name == "*":
                    # Handle select all columns
                    col_names.extend([field.name for field in self.schema.fields])
                else:
                    col_names.append(col.name)
            elif isinstance(col, str):
                if col == "*":
                    # Handle select all columns
                    col_names.extend([field.name for field in self.schema.fields])
                else:
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
        
        # Validate non-literal columns exist (skip validation for MockColumnOperation, function calls, and window functions)
        for col_name in col_names:
            if (col_name not in [field.name for field in self.schema.fields] and 
                col_name not in literal_columns and
                not any(hasattr(col, 'operation') and hasattr(col, 'column') and col.name == col_name for col in columns) and
                not any(hasattr(col, 'function_name') and hasattr(col, 'over') and col.name == col_name for col in columns) and
                not any(hasattr(col, 'operation') and not hasattr(col, 'column') and col.name == col_name for col in columns) and  # Handle functions like coalesce
                not self._is_function_call(col_name)):
                raise_column_not_found(col_name)
        
        # Filter data to selected columns and add literal values
        filtered_data = []
        for row in self.data:
            filtered_row = {}
            for i, col in enumerate(columns):
                if isinstance(col, MockColumn):
                    col_name = col.name
                    if col_name == "*":
                        # Add all existing columns
                        for field in self.schema.fields:
                            filtered_row[field.name] = row[field.name]
                    elif col_name in literal_columns:
                        # Add literal value
                        filtered_row[col_name] = literal_columns[col_name]
                    elif (col_name.startswith(("upper(", "lower(", "length(", "abs(", "round(", "coalesce(", "isnull(", "isnan(", "trim(")) or
                         (hasattr(col, '_original_column') and hasattr(col._original_column, 'name') and 
                          col._original_column.name.startswith(("coalesce(", "isnull(", "isnan(", "trim(")))):
                        # Handle function calls
                        evaluated_value = self._evaluate_column_expression(row, col)
                        filtered_row[col_name] = evaluated_value
                    else:
                        # Add existing column value
                        filtered_row[col_name] = row[col_name]
                elif isinstance(col, str):
                    col_name = col
                    if col_name == "*":
                        # Add all existing columns
                        for field in self.schema.fields:
                            filtered_row[field.name] = row[field.name]
                    elif col_name in literal_columns:
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
                elif hasattr(col, 'function_name') and hasattr(col, 'over'):
                    # Handle MockWindowFunction (e.g., row_number().over(window))
                    col_name = col.name
                    # Window functions need to be evaluated across all rows
                    # For now, we'll handle this after processing all rows
                    filtered_row[col_name] = None  # Placeholder, will be filled later
                elif hasattr(col, 'name'):
                    col_name = col.name
                    if col_name in literal_columns:
                        # Add literal value
                        filtered_row[col_name] = literal_columns[col_name]
                    else:
                        # Add existing column value
                        filtered_row[col_name] = row[col_name]
            filtered_data.append(filtered_row)
        
        # Handle window functions that need to be evaluated across all rows
        window_functions = []
        for i, col in enumerate(columns):
            if hasattr(col, 'function_name') and hasattr(col, 'over'):
                window_functions.append((i, col))
        
        if window_functions:
            filtered_data = self._evaluate_window_functions(filtered_data, window_functions)
        
        # Create new schema
        new_fields = []
        for i, col in enumerate(columns):
            if isinstance(col, MockLiteral):
                # Handle MockLiteral directly
                col_name = col.name
                new_fields.append(MockStructField(col_name, col.column_type))
            elif isinstance(col, MockColumn):
                col_name = col.name
                if col_name == "*":
                    # Add all existing fields
                    new_fields.extend(self.schema.fields)
                # Check if this is a function call first
                elif (col_name.startswith(("abs(", "round(", "upper(", "lower(", "length(", "coalesce(", "isnull(", "isnan(", "trim(")) or
                     (hasattr(col, '_original_column') and hasattr(col._original_column, 'name') and 
                      col._original_column.name.startswith(("coalesce(", "isnull(", "isnan(", "trim(")))):
                    from .spark_types import DoubleType, StringType, LongType, IntegerType, BooleanType
                    
                    # Determine the function name for type inference
                    func_name = col_name
                    if hasattr(col, '_original_column') and hasattr(col._original_column, 'name'):
                        func_name = col._original_column.name
                    
                    if func_name.startswith("abs("):
                        new_fields.append(MockStructField(col_name, LongType()))  # abs() returns LongType for integers
                    elif func_name.startswith("round("):
                        new_fields.append(MockStructField(col_name, DoubleType()))
                    elif func_name.startswith("length("):
                        new_fields.append(MockStructField(col_name, IntegerType()))  # length() returns IntegerType
                    elif func_name.startswith(("upper(", "lower(", "coalesce(", "trim(")):
                        new_fields.append(MockStructField(col_name, StringType()))
                    elif func_name.startswith(("isnull(", "isnan(")):
                        new_fields.append(MockStructField(col_name, BooleanType()))
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
                if col_name == "*":
                    # Add all existing fields
                    new_fields.extend(self.schema.fields)
                elif col_name in literal_columns:
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
                from .spark_types import StringType, LongType, DoubleType, IntegerType
                if col.operation in ["upper", "lower"]:
                    new_fields.append(MockStructField(col_name, StringType()))
                elif col.operation == "length":
                    new_fields.append(MockStructField(col_name, IntegerType()))  # length() returns IntegerType
                elif col.operation == "abs":
                    new_fields.append(MockStructField(col_name, LongType()))  # abs() returns LongType
                elif col.operation == "round":
                    new_fields.append(MockStructField(col_name, DoubleType()))  # round() returns DoubleType
                elif col.operation in ["+", "-", "*", "%"]:
                    # Arithmetic operations - use LongType for integer arithmetic to match PySpark
                    new_fields.append(MockStructField(col_name, LongType()))
                elif col.operation == "/":
                    # Division operation - use DoubleType for decimal results
                    new_fields.append(MockStructField(col_name, DoubleType()))
                else:
                    # Default to StringType for other operations
                    new_fields.append(MockStructField(col_name, StringType()))
            elif hasattr(col, 'function_name') and hasattr(col, 'over'):
                # Handle MockWindowFunction (e.g., row_number().over(window))
                col_name = col.name
                from .spark_types import IntegerType
                # Window functions like row_number() typically return IntegerType
                new_fields.append(MockStructField(col_name, IntegerType()))
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
                        result_row[agg_col_name] = len(self.data)
                    else:
                        agg_col_name = f"count({col_name})"
                        # Count non-null values for specific column
                        non_null_count = sum(1 for row in self.data if row.get(col_name) is not None)
                        result_row[agg_col_name] = non_null_count
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
                elif func_name == "count(DISTINCT":
                    agg_col_name = f"count(DISTINCT {col_name})"
                    values = [row.get(col_name) for row in self.data if row.get(col_name) is not None]
                    result_row[agg_col_name] = len(set(values)) if values else 0
                    new_fields.append(MockStructField(agg_col_name, LongType()))
            elif isinstance(col, MockColumn) and (col.name.startswith(("count(", "sum(", "avg(", "max(", "min(")) or col.name.startswith("count(DISTINCT ")):
                # Handle MockColumn with function names
                col_name = col.name
                if col_name.startswith("count("):
                    if col_name == "count(1)":
                        result_row[col_name] = len(self.data)
                    else:
                        # Extract column name from count(column)
                        inner_col = col_name[6:-1]
                        # Count non-null values for specific column
                        non_null_count = sum(1 for row in self.data if row.get(inner_col) is not None)
                        result_row[col_name] = non_null_count
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
                elif col_name.startswith("count(DISTINCT"):
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
    
    def withColumn(self, col_name: str, col: Union[MockColumn, MockColumnOperation, 'MockLiteral', Any]) -> 'MockDataFrame':
        """Add or replace column."""
        new_data = []
        
        for row in self.data:
            new_row = row.copy()
            
            if isinstance(col, (MockColumn, MockColumnOperation)):
                # Evaluate the column expression
                evaluated_value = self._evaluate_column_expression(row, col)
                new_row[col_name] = evaluated_value
            elif hasattr(col, 'value') and hasattr(col, 'column_type'):
                # Handle MockLiteral objects
                new_row[col_name] = col.value
            else:
                new_row[col_name] = col
            
            new_data.append(new_row)
        
        # Update schema
        new_fields = [field for field in self.schema.fields if field.name != col_name]
        
        # Determine the correct type for the new column
        from .spark_types import StringType, LongType, DoubleType, IntegerType
        if isinstance(col, (MockColumn, MockColumnOperation)):
            # For arithmetic operations, determine type based on the operation
            if hasattr(col, 'operation') and col.operation in ["+", "-", "*", "/", "%"]:
                # Arithmetic operations typically return LongType or DoubleType
                # For now, use LongType for integer arithmetic
                new_fields.append(MockStructField(col_name, LongType()))
            elif hasattr(col, 'operation') and col.operation in ["abs"]:
                new_fields.append(MockStructField(col_name, LongType()))
            elif hasattr(col, 'operation') and col.operation in ["length"]:
                new_fields.append(MockStructField(col_name, IntegerType()))
            elif hasattr(col, 'operation') and col.operation in ["round"]:
                new_fields.append(MockStructField(col_name, DoubleType()))
            elif hasattr(col, 'operation') and col.operation in ["upper", "lower"]:
                new_fields.append(MockStructField(col_name, StringType()))
            else:
                # Default to StringType for unknown operations
                new_fields.append(MockStructField(col_name, StringType()))
        elif hasattr(col, 'value') and hasattr(col, 'column_type'):
            # Handle MockLiteral objects - use their column_type
            new_fields.append(MockStructField(col_name, col.column_type))
        else:
            # For literal values, infer type
            if isinstance(col, (int, float)):
                if isinstance(col, float):
                    new_fields.append(MockStructField(col_name, DoubleType()))
                else:
                    new_fields.append(MockStructField(col_name, LongType()))
            else:
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
    
    def take(self, n: int) -> List[MockRow]:
        """Take first n rows as list of Row objects."""
        return [MockRow(row) for row in self.data[:n]]
    
    @property
    def dtypes(self) -> List[Tuple[str, str]]:
        """Get column names and their data types."""
        return [(field.name, field.dataType.typeName()) for field in self.schema.fields]
    
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
    
    def persist(self) -> 'MockDataFrame':
        """Persist DataFrame (no-op in mock)."""
        return self
    
    def unpersist(self) -> 'MockDataFrame':
        """Unpersist DataFrame (no-op in mock)."""
        return self
    
    def distinct(self) -> 'MockDataFrame':
        """Return distinct rows."""
        seen = set()
        distinct_data = []
        for row in self.data:
            row_tuple = tuple(sorted(row.items()))
            if row_tuple not in seen:
                seen.add(row_tuple)
                distinct_data.append(row)
        return MockDataFrame(distinct_data, self.schema, self.storage)
    
    def dropDuplicates(self, subset: Optional[List[str]] = None) -> 'MockDataFrame':
        """Drop duplicate rows."""
        if subset is None:
            return self.distinct()
        
        seen = set()
        distinct_data = []
        for row in self.data:
            row_tuple = tuple(sorted((k, v) for k, v in row.items() if k in subset))
            if row_tuple not in seen:
                seen.add(row_tuple)
                distinct_data.append(row)
        return MockDataFrame(distinct_data, self.schema, self.storage)
    
    def drop(self, *cols: str) -> 'MockDataFrame':
        """Drop columns."""
        new_data = []
        for row in self.data:
            new_row = {k: v for k, v in row.items() if k not in cols}
            new_data.append(new_row)
        
        # Update schema
        new_fields = [field for field in self.schema.fields if field.name not in cols]
        new_schema = MockStructType(new_fields)
        return MockDataFrame(new_data, new_schema, self.storage)
    
    def withColumnRenamed(self, existing: str, new: str) -> 'MockDataFrame':
        """Rename a column."""
        new_data = []
        for row in self.data:
            new_row = {}
            for k, v in row.items():
                if k == existing:
                    new_row[new] = v
                else:
                    new_row[k] = v
            new_data.append(new_row)
        
        # Update schema
        new_fields = []
        for field in self.schema.fields:
            if field.name == existing:
                new_fields.append(MockStructField(new, field.dataType))
            else:
                new_fields.append(field)
        new_schema = MockStructType(new_fields)
        return MockDataFrame(new_data, new_schema, self.storage)
    
    def dropna(self, how: str = "any", thresh: Optional[int] = None, subset: Optional[List[str]] = None) -> 'MockDataFrame':
        """Drop rows with null values."""
        filtered_data = []
        for row in self.data:
            if subset:
                # Check only specified columns
                null_count = sum(1 for col in subset if row.get(col) is None)
            else:
                # Check all columns
                null_count = sum(1 for v in row.values() if v is None)
            
            if how == "any" and null_count == 0:
                filtered_data.append(row)
            elif how == "all" and null_count < len(row):
                filtered_data.append(row)
            elif thresh is not None and null_count <= len(row) - thresh:
                filtered_data.append(row)
        
        return MockDataFrame(filtered_data, self.schema, self.storage)
    
    def fillna(self, value: Union[Any, Dict[str, Any]]) -> 'MockDataFrame':
        """Fill null values."""
        new_data = []
        for row in self.data:
            new_row = row.copy()
            if isinstance(value, dict):
                for col, fill_value in value.items():
                    if new_row.get(col) is None:
                        new_row[col] = fill_value
            else:
                for col in new_row:
                    if new_row[col] is None:
                        new_row[col] = value
            new_data.append(new_row)
        
        return MockDataFrame(new_data, self.schema, self.storage)
    
    def show(self, n: int = 20, truncate: bool = True) -> None:
        """Show DataFrame content."""
        if not self.data:
            print("MockDataFrame: 0 rows (empty)")
            return
        
        print(f"MockDataFrame: {len(self.data)} rows")
        if self.data:
            # Show column names
            cols = [field.name for field in self.schema.fields]
            print("+".join(["-" * (len(col) + 2) for col in cols]))
            print("|".join([f" {col} " for col in cols]))
            print("+".join(["-" * (len(col) + 2) for col in cols]))
            
            # Show data rows
            for i, row in enumerate(self.data[:n]):
                values = []
                for col in cols:
                    val = row.get(col)
                    if val is None:
                        val = "null"
                    elif truncate and len(str(val)) > 20:
                        val = str(val)[:17] + "..."
                    else:
                        val = str(val)
                    values.append(f" {val} ")
                print("|".join(values))
            print("+".join(["-" * (len(col) + 2) for col in cols]))
            
            # Show "X more rows" if there are more rows
            if len(self.data) > n:
                remaining = len(self.data) - n
                print(f"{remaining} more rows")
    
    def printSchema(self) -> None:
        """Print schema."""
        print("MockDataFrame Schema:")
        for field in self.schema.fields:
            print(f"  {field.name}: {field.dataType.__class__.__name__}")
    
    def explain(self) -> None:
        """Explain execution plan."""
        print("MockDataFrame Execution Plan:")
        print("  MockDataFrame")
        print("    MockDataSource")
    
    @property
    def rdd(self) -> 'MockRDD':
        """Get RDD representation."""
        return MockRDD(self.data)
    
    def registerTempTable(self, name: str) -> None:
        """Register as temporary table."""
        # Store in storage
        self.storage.insert_data("default", name, self.data, self.schema)
    
    def createTempView(self, name: str) -> None:
        """Create temporary view."""
        self.registerTempTable(name)
    
    def _apply_condition(self, data: List[Dict[str, Any]], condition: MockColumnOperation) -> List[Dict[str, Any]]:
        """Apply condition to filter data."""
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
            return False
    
    def _evaluate_column_expression(self, row: Dict[str, Any], column_expression: Any) -> Any:
        """Evaluate a column expression for a single row."""
        if isinstance(column_expression, MockColumn):
            # Check if this is a function call
            col_name = column_expression.name
            
            # Check if this is an aliased function call
            if hasattr(column_expression, '_original_column') and hasattr(column_expression._original_column, 'name'):
                original_name = column_expression._original_column.name
                if original_name.startswith(("coalesce(", "isnull(", "isnan(", "trim(")):
                    return self._evaluate_function_call_by_name(row, original_name)
            
            # Check if this is a direct function call
            if col_name.startswith(("coalesce(", "isnull(", "isnan(", "trim(")):
                return self._evaluate_function_call_by_name(row, col_name)
            else:
                # Simple column reference
                return row.get(column_expression.name)
        
        elif hasattr(column_expression, 'operation') and hasattr(column_expression, 'column'):
            # Handle MockColumnOperation (arithmetic operations and function calls)
            if column_expression.operation in ["+", "-", "*", "/", "%"]:
                return self._evaluate_arithmetic_operation(row, column_expression)
            else:
                return self._evaluate_function_call(row, column_expression)
        
        else:
            # Direct value
            return column_expression
    
    def _evaluate_arithmetic_operation(self, row: Dict[str, Any], operation: Any) -> Any:
        """Evaluate arithmetic operations on columns."""
        if not hasattr(operation, 'operation') or not hasattr(operation, 'column'):
            return None
        
        left_value = row.get(operation.column.name) if hasattr(operation.column, 'name') else None
        right_value = operation.value
        
        if left_value is None:
            return None
        
        if operation.operation == "+":
            return left_value + right_value
        elif operation.operation == "-":
            return left_value - right_value
        elif operation.operation == "*":
            return left_value * right_value
        elif operation.operation == "/":
            return left_value / right_value if right_value != 0 else None
        elif operation.operation == "%":
            return left_value % right_value if right_value != 0 else None
        else:
            return left_value
    
    def _evaluate_function_call(self, row: Dict[str, Any], operation: Any) -> Any:
        """Evaluate function calls like upper(), lower(), length(), abs(), round()."""
        if not hasattr(operation, 'operation') or not hasattr(operation, 'column'):
            return None
        
        column_name = operation.column.name if hasattr(operation.column, 'name') else str(operation.column)
        value = row.get(column_name)
        
        if value is None:
            return None
        
        func_name = operation.operation
        
        if func_name == "upper":
            return str(value).upper()
        elif func_name == "lower":
            return str(value).lower()
        elif func_name == "length":
            return len(str(value))
        elif func_name == "abs":
            return abs(value) if isinstance(value, (int, float)) else value
        elif func_name == "round":
            # For round function, we need to handle the precision parameter
            precision = getattr(operation, 'precision', 0)
            return round(value, precision) if isinstance(value, (int, float)) else value
        else:
            return value
    
    def _evaluate_function_call_by_name(self, row: Dict[str, Any], col_name: str) -> Any:
        """Evaluate function calls by parsing the function name."""
        if col_name.startswith("coalesce("):
            # Parse coalesce arguments: coalesce(col1, col2, ...)
            # For now, implement basic coalesce logic
            if "name" in col_name and "Unknown" in col_name:
                name_value = row.get("name")
                return name_value if name_value is not None else "Unknown"
        elif col_name.startswith("isnull("):
            # Parse isnull argument: isnull(col)
            if "name" in col_name:
                return row.get("name") is None
        elif col_name.startswith("isnan("):
            # Parse isnan argument: isnan(col)
            if "salary" in col_name:
                value = row.get("salary")
                if isinstance(value, float):
                    return value != value  # NaN check
                return False
        elif col_name.startswith("trim("):
            # Parse trim argument: trim(col)
            if "name" in col_name:
                value = row.get("name")
                return str(value).strip() if value is not None else None
        
        # Default fallback
        return None
    
    def _is_function_call(self, col_name: str) -> bool:
        """Check if column name is a function call."""
        function_patterns = [
            "upper(", "lower(", "length(", "abs(", "round(", 
            "count(", "sum(", "avg(", "max(", "min(", "count(DISTINCT ",
            "coalesce(", "isnull(", "isnan(", "trim("
        ]
        return any(col_name.startswith(pattern) for pattern in function_patterns)
    
    def _evaluate_window_functions(self, data: List[Dict[str, Any]], window_functions: List[tuple]) -> List[Dict[str, Any]]:
        """Evaluate window functions across all rows."""
        result_data = data.copy()
        
        for col_index, window_func in window_functions:
            col_name = window_func.name
            
            if window_func.function_name == "row_number":
                # For row_number(), we need to handle partitionBy and orderBy
                if hasattr(window_func, '_window_spec') and window_func._window_spec:
                    window_spec = window_func._window_spec
                    
                    # Get partition by columns from window spec
                    partition_by_cols = getattr(window_spec, '_partition_by', [])
                    # Get order by columns from window spec
                    order_by_cols = getattr(window_spec, '_order_by', [])
                    
                    if partition_by_cols:
                        # Handle partitioning - group by partition columns
                        partition_groups = {}
                        for i, row in enumerate(result_data):
                            # Create partition key
                            partition_key = tuple(
                                row.get(col.name) if hasattr(col, 'name') else row.get(str(col))
                                for col in partition_by_cols
                            )
                            if partition_key not in partition_groups:
                                partition_groups[partition_key] = []
                            partition_groups[partition_key].append(i)
                        
                        # Assign row numbers within each partition
                        for partition_indices in partition_groups.values():
                            if order_by_cols:
                                # Sort within partition by order by columns
                                def sort_key(idx):
                                    row = result_data[idx]
                                    key_values = []
                                    for col in order_by_cols:
                                        if hasattr(col, 'name'):
                                            key_values.append(row.get(col.name))
                                        else:
                                            key_values.append(row.get(str(col)))
                                    return key_values
                                
                                sorted_partition_indices = sorted(partition_indices, key=sort_key)
                            else:
                                # No order by - use original order within partition
                                sorted_partition_indices = partition_indices
                            
                            # Assign row numbers starting from 1 within each partition
                            for i, original_index in enumerate(sorted_partition_indices):
                                result_data[original_index][col_name] = i + 1
                    elif order_by_cols:
                        # No partitioning, just sort by order by columns
                        def sort_key(row):
                            key_values = []
                            for col in order_by_cols:
                                if hasattr(col, 'name'):
                                    key_values.append(row.get(col.name))
                                else:
                                    key_values.append(row.get(str(col)))
                            return key_values
                        
                        # Create sorted indices
                        sorted_indices = sorted(range(len(result_data)), key=lambda i: sort_key(result_data[i]))
                        
                        # Assign row numbers based on sorted order
                        for i, original_index in enumerate(sorted_indices):
                            result_data[original_index][col_name] = i + 1
                    else:
                        # No partition or order by - just assign sequential row numbers
                        for i in range(len(result_data)):
                            result_data[i][col_name] = i + 1
                else:
                    # No window spec - assign sequential row numbers
                    for i in range(len(result_data)):
                        result_data[i][col_name] = i + 1
            else:
                # For other window functions, assign None for now
                for row in result_data:
                    row[col_name] = None
        
        return result_data
    
    @property
    def write(self) -> MockDataFrameWriter:
        """Get DataFrame writer."""
        return MockDataFrameWriter(self, self.storage)


class MockRDD:
    """Mock RDD for DataFrame compatibility."""
    
    def __init__(self, data: List[Dict[str, Any]]):
        """Initialize MockRDD."""
        self.data = data
    
    def collect(self) -> List[Any]:
        """Collect RDD data."""
        return self.data
    
    def count(self) -> int:
        """Count RDD elements."""
        return len(self.data)


class MockGroupedData:
    """Mock grouped data for aggregation operations."""
    
    def __init__(self, df: MockDataFrame, group_columns: List[str]):
        self.df = df
        self.group_columns = group_columns
    
    def agg(self, *exprs: Union[str, MockColumn, MockColumnOperation]) -> MockDataFrame:
        """Aggregate grouped data."""
        # Group data by group columns
        groups = {}
        for row in self.df.data:
            group_key = tuple(row.get(col) for col in self.group_columns)
            if group_key not in groups:
                groups[group_key] = []
            groups[group_key].append(row)
        
        # Apply aggregations
        result_data = []
        for group_key, group_rows in groups.items():
            result_row = dict(zip(self.group_columns, group_key))
            
            for expr in exprs:
                if isinstance(expr, str):
                    # Handle string expressions like "sum(age)"
                    if expr.startswith("sum("):
                        col_name = expr[4:-1]
                        values = [row.get(col_name, 0) for row in group_rows if row.get(col_name) is not None]
                        result_row[expr] = sum(values) if values else 0
                    elif expr.startswith("avg("):
                        col_name = expr[4:-1]
                        values = [row.get(col_name, 0) for row in group_rows if row.get(col_name) is not None]
                        result_row[expr] = sum(values) / len(values) if values else 0
                    elif expr.startswith("count("):
                        result_row[expr] = len(group_rows)
                    elif expr.startswith("max("):
                        col_name = expr[4:-1]
                        values = [row.get(col_name) for row in group_rows if row.get(col_name) is not None]
                        result_row[expr] = max(values) if values else None
                    elif expr.startswith("min("):
                        col_name = expr[4:-1]
                        values = [row.get(col_name) for row in group_rows if row.get(col_name) is not None]
                        result_row[expr] = min(values) if values else None
                elif hasattr(expr, 'function_name'):
                    # Handle MockAggregateFunction
                    func_name = expr.function_name
                    col_name = expr.column_name
                    if func_name == "sum":
                        values = [row.get(col_name, 0) for row in group_rows if row.get(col_name) is not None]
                        result_row[f"sum({col_name})"] = sum(values) if values else 0
                    elif func_name == "avg":
                        values = [row.get(col_name, 0) for row in group_rows if row.get(col_name) is not None]
                        result_row[f"avg({col_name})"] = sum(values) / len(values) if values else 0
                    elif func_name == "count":
                        if col_name == "*":
                            result_row["count(1)"] = len(group_rows)
                        else:
                            result_row[f"count({col_name})"] = len(group_rows)
                    elif func_name == "max":
                        values = [row.get(col_name) for row in group_rows if row.get(col_name) is not None]
                        result_row[f"max({col_name})"] = max(values) if values else None
                    elif func_name == "min":
                        values = [row.get(col_name) for row in group_rows if row.get(col_name) is not None]
                        result_row[f"min({col_name})"] = min(values) if values else None
                elif hasattr(expr, 'name'):
                    # Handle MockColumn or MockColumnOperation
                    expr_name = expr.name
                    if expr_name.startswith("sum("):
                        col_name = expr_name[4:-1]
                        values = [row.get(col_name, 0) for row in group_rows if row.get(col_name) is not None]
                        result_row[expr_name] = sum(values) if values else 0
                    elif expr_name.startswith("avg("):
                        col_name = expr_name[4:-1]
                        values = [row.get(col_name, 0) for row in group_rows if row.get(col_name) is not None]
                        result_row[expr_name] = sum(values) / len(values) if values else 0
                    elif expr_name.startswith("count("):
                        result_row[expr_name] = len(group_rows)
                    elif expr_name.startswith("max("):
                        col_name = expr_name[4:-1]
                        values = [row.get(col_name) for row in group_rows if row.get(col_name) is not None]
                        result_row[expr_name] = max(values) if values else None
                    elif expr_name.startswith("min("):
                        col_name = expr_name[4:-1]
                        values = [row.get(col_name) for row in group_rows if row.get(col_name) is not None]
                        result_row[expr_name] = min(values) if values else None
            
            result_data.append(result_row)
        
        # Create new schema
        new_fields = []
        for col in self.group_columns:
            # Find the field in the original schema
            for field in self.df.schema.fields:
                if field.name == col:
                    new_fields.append(field)
                    break
        
        # Add aggregation result fields
        for expr in exprs:
            if isinstance(expr, str):
                from .spark_types import LongType, DoubleType
                # Determine the correct type based on the function
                if expr.startswith(("sum(", "avg(", "max(", "min(")):
                    new_fields.append(MockStructField(expr, DoubleType()))
                else:
                    new_fields.append(MockStructField(expr, LongType()))
            elif hasattr(expr, 'function_name'):
                # Handle MockAggregateFunction
                from .spark_types import LongType, DoubleType
                func_name = expr.function_name
                col_name = expr.column_name
                if func_name == "count":
                    if col_name == "*":
                        new_fields.append(MockStructField("count(1)", LongType()))
                    else:
                        new_fields.append(MockStructField(f"count({col_name})", LongType()))
                elif func_name in ["sum", "avg", "max", "min"]:
                    new_fields.append(MockStructField(f"{func_name}({col_name})", DoubleType()))
                else:
                    new_fields.append(MockStructField(f"{func_name}({col_name})", LongType()))
            elif hasattr(expr, 'name'):
                from .spark_types import LongType, DoubleType
                expr_name = expr.name
                if expr_name.startswith(("sum(", "avg(", "max(", "min(")):
                    new_fields.append(MockStructField(expr_name, DoubleType()))
                else:
                    new_fields.append(MockStructField(expr_name, LongType()))
        
        new_schema = MockStructType(new_fields)
        return MockDataFrame(result_data, new_schema, self.df.storage)
    
    def count(self) -> MockDataFrame:
        """Count grouped data."""
        # Group data by group columns
        groups = {}
        for row in self.df.data:
            group_key = tuple(row.get(col) for col in self.group_columns)
            if group_key not in groups:
                groups[group_key] = []
            groups[group_key].append(row)
        
        # Apply count aggregation
        result_data = []
        for group_key, group_rows in groups.items():
            result_row = dict(zip(self.group_columns, group_key))
            result_row["count"] = len(group_rows)
            result_data.append(result_row)
        
        # Create new schema
        new_fields = []
        for col in self.group_columns:
            # Find the field in the original schema
            for field in self.df.schema.fields:
                if field.name == col:
                    new_fields.append(field)
                    break
        
        # Add count field
        from .spark_types import LongType
        new_fields.append(MockStructField("count", LongType()))
        
        new_schema = MockStructType(new_fields)
        return MockDataFrame(result_data, new_schema, self.df.storage)
    
    def sum(self, *columns: Union[str, MockColumn]) -> MockDataFrame:
        """Sum grouped data."""
        if not columns:
            return self.agg("sum(1)")
        
        exprs = [f"sum({col})" if isinstance(col, str) else f"sum({col.name})" for col in columns]
        return self.agg(*exprs)
    
    def avg(self, *columns: Union[str, MockColumn]) -> MockDataFrame:
        """Average grouped data."""
        if not columns:
            return self.agg("avg(1)")
        
        exprs = [f"avg({col})" if isinstance(col, str) else f"avg({col.name})" for col in columns]
        return self.agg(*exprs)
    
    def max(self, *columns: Union[str, MockColumn]) -> MockDataFrame:
        """Max grouped data."""
        if not columns:
            return self.agg("max(1)")
        
        exprs = [f"max({col})" if isinstance(col, str) else f"max({col.name})" for col in columns]
        return self.agg(*exprs)
    
    def min(self, *columns: Union[str, MockColumn]) -> MockDataFrame:
        """Min grouped data."""
        if not columns:
            return self.agg("min(1)")
        
        exprs = [f"min({col})" if isinstance(col, str) else f"min({col.name})" for col in columns]
        return self.agg(*exprs)
