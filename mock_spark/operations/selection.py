"""
Selection operations for MockDataFrame.

This module contains the implementation of column selection operations
including select, withColumn, drop, and rename operations.
"""

from typing import Any, Dict, List, Optional, Union
from mock_spark.spark_types import MockStructType, MockStructField, StringType, IntegerType
from mock_spark.functions import MockColumn, MockLiteral, MockAggregateFunction
from mock_spark.errors import raise_column_not_found, raise_value_error


class SelectionOperations:
    """Handles column selection operations for DataFrames."""
    
    def __init__(self, dataframe):
        """Initialize with a reference to the DataFrame."""
        self.df = dataframe
    
    def select(self, *columns: Union[str, MockColumn]) -> "MockDataFrame":
        """Select columns from the DataFrame.
        
        Args:
            *columns: Column names, MockColumn objects, or expressions to select.
                     Use "*" to select all columns.
        
        Returns:
            New MockDataFrame with selected columns.
        
        Raises:
            AnalysisException: If specified columns don't exist.
        """
        from .spark_types import LongType
        
        if not columns:
            return self.df
        
        # Check if this is an aggregation operation
        has_aggregation = any(
            isinstance(col, MockAggregateFunction)
            or (
                isinstance(col, MockColumn)
                and (
                    col.name.startswith(("count(", "sum(", "avg(", "max(", "min("))
                    or col.name.startswith("count(DISTINCT ")
                )
            )
            for col in columns
        )
        
        if has_aggregation:
            # Handle aggregation - return single row
            return self._handle_aggregation_select(list(columns))
        
        # Process columns and handle literals
        col_names = []
        literal_columns: Dict[str, Any] = {}
        literal_objects: Dict[str, MockLiteral] = {}
        
        for col in columns:
            if isinstance(col, str):
                if col == "*":
                    # Handle select all columns
                    col_names.extend([field.name for field in self.df.schema.fields])
                else:
                    col_names.append(col)
            elif isinstance(col, MockLiteral):
                # Handle literal columns
                literal_name = col.name
                col_names.append(literal_name)
                literal_columns[literal_name] = col.value
                literal_objects[literal_name] = col
            elif isinstance(col, MockColumn):
                if col.name == "*":
                    # Handle select all columns
                    col_names.extend([field.name for field in self.df.schema.fields])
                else:
                    col_names.append(col.name)
            elif hasattr(col, "operation") and hasattr(col, "column"):
                # Handle MockColumnOperation (e.g., col + 1, upper(col))
                col_names.append(col.name)
            elif hasattr(col, "name"):  # Support other column-like objects
                col_names.append(col.name)
            else:
                raise_value_error(f"Invalid column type: {type(col)}")
        
        # Validate non-literal columns exist
        for col_name in col_names:
            if (
                col_name not in [field.name for field in self.df.schema.fields]
                and col_name not in literal_columns
                and not any(
                    hasattr(col, "operation")
                    and hasattr(col, "column")
                    and hasattr(col, "name")
                    and col.name == col_name
                    for col in columns
                )
                and not any(
                    hasattr(col, "function_name")
                    and hasattr(col, "over")
                    and hasattr(col, "name")
                    and col.name == col_name
                    for col in columns
                )
                and not any(
                    hasattr(col, "operation")
                    and not hasattr(col, "column")
                    and hasattr(col, "name")
                    and col.name == col_name
                    for col in columns
                )  # Handle functions like coalesce
                and not any(
                    hasattr(col, "conditions")
                    and hasattr(col, "name")
                    and col.name == col_name
                    for col in columns
                )  # Handle MockCaseWhen objects
                and not self._is_function_call(col_name)
            ):
                raise_column_not_found(col_name)
        
        # Filter data to selected columns and add literal values
        filtered_data = []
        for row in self.df.data:
            filtered_row = {}
            for i, col in enumerate(columns):
                if isinstance(col, str):
                    col_name = col
                    if col_name == "*":
                        # Add all existing columns
                        for field in self.df.schema.fields:
                            filtered_row[field.name] = row[field.name]
                    elif col_name in literal_columns:
                        # Add literal value
                        filtered_row[col_name] = literal_columns[col_name]
                    elif col_name in ("current_timestamp()", "current_date()"):
                        # Handle timestamp and date functions
                        if col_name == "current_timestamp()":
                            import datetime
                            filtered_row[col_name] = datetime.datetime.now()
                        elif col_name == "current_date()":
                            import datetime
                            filtered_row[col_name] = datetime.date.today()
                    else:
                        # Add existing column value
                        filtered_row[col_name] = row[col_name]
                elif hasattr(col, "operation") and hasattr(col, "column"):
                    # Handle MockColumnOperation (e.g., upper(col), length(col))
                    col_name = col.name
                    evaluated_value = self.df._evaluate_column_expression(row, col)
                    filtered_row[col_name] = evaluated_value
                elif hasattr(col, "conditions"):
                    # Handle MockCaseWhen objects
                    col_name = col.name
                    evaluated_value = self.df._evaluate_case_when(row, col)
                    filtered_row[col_name] = evaluated_value
                elif isinstance(col, MockColumn):
                    col_name = col.name
                    if col_name == "*":
                        # Add all existing columns
                        for field in self.df.schema.fields:
                            filtered_row[field.name] = row[field.name]
                    elif col_name in literal_columns:
                        # Add literal value
                        filtered_row[col_name] = literal_columns[col_name]
                    elif self._is_function_call(col_name):
                        # Handle function calls
                        evaluated_value = self.df._evaluate_column_expression(row, col)
                        filtered_row[col_name] = evaluated_value
                    else:
                        # Handle aliased columns - get value from original column name
                        if (
                            hasattr(col, "_original_column")
                            and col._original_column is not None
                        ):
                            # This is an aliased column, get value from original column
                            original_name = col._original_column.name
                            if original_name in (
                                "current_timestamp()",
                                "current_date()",
                            ):
                                # Handle timestamp and date functions
                                if original_name == "current_timestamp()":
                                    import datetime
                                    filtered_row[col_name] = datetime.datetime.now()
                                elif original_name == "current_date()":
                                    import datetime
                                    filtered_row[col_name] = datetime.date.today()
                            else:
                                filtered_row[col_name] = row[original_name]
                        elif col_name in ("current_timestamp()", "current_date()"):
                            # Handle timestamp and date functions
                            if col_name == "current_timestamp()":
                                import datetime
                                filtered_row[col_name] = datetime.datetime.now()
                            elif col_name == "current_date()":
                                import datetime
                                filtered_row[col_name] = datetime.date.today()
                        else:
                            # Add existing column value
                            filtered_row[col_name] = row[col_name]
                elif hasattr(col, "function_name") and hasattr(col, "over"):
                    # Handle MockWindowFunction (e.g., row_number().over(window))
                    col_name = col.name
                    # Window functions need to be evaluated across all rows
                    # For now, we'll handle this in the main DataFrame class
                    evaluated_value = self.df._evaluate_window_function(col, self.df.data)
                    filtered_row[col_name] = evaluated_value
                else:
                    # Handle other column types
                    col_name = col.name if hasattr(col, "name") else str(col)
                    filtered_row[col_name] = row.get(col_name, None)
            
            filtered_data.append(filtered_row)
        
        # Create new schema for selected columns
        new_schema_fields = []
        for col_name in col_names:
            if col_name in literal_columns:
                # Add literal column to schema
                literal_obj = literal_objects[col_name]
                field_type = literal_obj.dataType if hasattr(literal_obj, 'dataType') else StringType()
                new_schema_fields.append(MockStructField(col_name, field_type, True))
            elif col_name in [field.name for field in self.df.schema.fields]:
                # Add existing column to schema
                original_field = next(f for f in self.df.schema.fields if f.name == col_name)
                new_schema_fields.append(original_field)
            else:
                # Add computed column to schema (default to StringType)
                new_schema_fields.append(MockStructField(col_name, StringType(), True))
        
        new_schema = MockStructType(new_schema_fields)
        
        # Create new DataFrame with selected data and schema
        from mock_spark.dataframe import MockDataFrame
        return MockDataFrame(filtered_data, new_schema, self.df.storage)
    
    def _handle_aggregation_select(self, columns: List[Union[str, MockColumn]]) -> "MockDataFrame":
        """Handle aggregation operations in select."""
        from mock_spark.dataframe import MockDataFrame
        from .spark_types import LongType, DoubleType, StringType
        
        # Evaluate aggregations
        aggregated_data = {}
        aggregated_schema_fields = []
        
        for col in columns:
            if isinstance(col, str):
                col_name = col
                if col_name.startswith("count("):
                    if col_name == "count(*)" or col_name == "count(1)":
                        aggregated_data[col_name] = len(self.df.data)
                        aggregated_schema_fields.append(MockStructField(col_name, LongType(), False))
                    else:
                        # Count non-null values in a specific column
                        column_name = col_name[6:-1]  # Remove "count(" and ")"
                        if column_name.startswith("DISTINCT "):
                            column_name = column_name[9:]  # Remove "DISTINCT "
                            unique_values = set()
                            for row in self.df.data:
                                value = row.get(column_name)
                                if value is not None:
                                    unique_values.add(value)
                            aggregated_data[col_name] = len(unique_values)
                        else:
                            non_null_count = sum(1 for row in self.df.data if row.get(column_name) is not None)
                            aggregated_data[col_name] = non_null_count
                        aggregated_schema_fields.append(MockStructField(col_name, LongType(), False))
                elif col_name.startswith("sum("):
                    column_name = col_name[4:-1]  # Remove "sum(" and ")"
                    total = 0
                    for row in self.df.data:
                        value = row.get(column_name)
                        if value is not None:
                            total += value
                    aggregated_data[col_name] = total
                    aggregated_schema_fields.append(MockStructField(col_name, DoubleType(), True))
                elif col_name.startswith("avg("):
                    column_name = col_name[4:-1]  # Remove "avg(" and ")"
                    values = [row.get(column_name) for row in self.df.data if row.get(column_name) is not None]
                    if values:
                        aggregated_data[col_name] = sum(values) / len(values)
                    else:
                        aggregated_data[col_name] = None
                    aggregated_schema_fields.append(MockStructField(col_name, DoubleType(), True))
                elif col_name.startswith("max("):
                    column_name = col_name[4:-1]  # Remove "max(" and ")"
                    values = [row.get(column_name) for row in self.df.data if row.get(column_name) is not None]
                    if values:
                        aggregated_data[col_name] = max(values)
                    else:
                        aggregated_data[col_name] = None
                    aggregated_schema_fields.append(MockStructField(col_name, StringType(), True))
                elif col_name.startswith("min("):
                    column_name = col_name[4:-1]  # Remove "min(" and ")"
                    values = [row.get(column_name) for row in self.df.data if row.get(column_name) is not None]
                    if values:
                        aggregated_data[col_name] = min(values)
                    else:
                        aggregated_data[col_name] = None
                    aggregated_schema_fields.append(MockStructField(col_name, StringType(), True))
            elif isinstance(col, MockAggregateFunction):
                # Handle MockAggregateFunction objects
                col_name = col.name
                evaluated_value = col.evaluate(self.df.data)
                aggregated_data[col_name] = evaluated_value
                aggregated_schema_fields.append(MockStructField(col_name, col.dataType, True))
        
        new_schema = MockStructType(aggregated_schema_fields)
        return MockDataFrame([aggregated_data], new_schema, self.df.storage)
    
    def _is_function_call(self, col_name: str) -> bool:
        """Check if a column name represents a function call."""
        function_prefixes = (
            "upper(", "lower(", "length(", "abs(", "round(", "coalesce(",
            "isnull(", "isnan(", "trim(", "ceil(", "floor(", "sqrt(",
            "regexp_replace(", "split(", "to_date(", "to_timestamp(",
            "hour(", "day(", "month(", "year(", "current_timestamp()", "current_date()"
        )
        return col_name.startswith(function_prefixes)
    
    def withColumn(self, colName: str, col: MockColumn) -> "MockDataFrame":
        """Add or replace a column in the DataFrame.
        
        Args:
            colName: Name of the new column.
            col: MockColumn expression for the new column.
        
        Returns:
            New MockDataFrame with the added/replaced column.
        """
        # Get all existing column names
        existing_columns = [field.name for field in self.df.schema.fields]
        
        if colName in existing_columns:
            # Replace existing column
            return self._replace_column(colName, col)
        else:
            # Add new column
            return self._add_column(colName, col)
    
    def _replace_column(self, colName: str, col: MockColumn) -> "MockDataFrame":
        """Replace an existing column."""
        from mock_spark.dataframe import MockDataFrame
        
        new_data = []
        for row in self.df.data:
            new_row = row.copy()
            evaluated_value = self.df._evaluate_column_expression(row, col)
            new_row[colName] = evaluated_value
            new_data.append(new_row)
        
        # Create new schema with replaced column
        new_schema_fields = []
        for field in self.df.schema.fields:
            if field.name == colName:
                # Replace with new column type
                new_field = MockStructField(colName, col.dataType if hasattr(col, 'dataType') else StringType(), True)
                new_schema_fields.append(new_field)
            else:
                new_schema_fields.append(field)
        
        new_schema = MockStructType(new_schema_fields)
        return MockDataFrame(new_data, new_schema, self.df.storage)
    
    def _add_column(self, colName: str, col: MockColumn) -> "MockDataFrame":
        """Add a new column."""
        from mock_spark.dataframe import MockDataFrame
        
        new_data = []
        for row in self.df.data:
            new_row = row.copy()
            evaluated_value = self.df._evaluate_column_expression(row, col)
            new_row[colName] = evaluated_value
            new_data.append(new_row)
        
        # Create new schema with added column
        new_schema_fields = list(self.df.schema.fields)
        new_field = MockStructField(colName, col.dataType if hasattr(col, 'dataType') else StringType(), True)
        new_schema_fields.append(new_field)
        
        new_schema = MockStructType(new_schema_fields)
        return MockDataFrame(new_data, new_schema, self.df.storage)
    
    def drop(self, *cols: str) -> "MockDataFrame":
        """Drop specified columns from the DataFrame.
        
        Args:
            *cols: Column names to drop.
        
        Returns:
            New MockDataFrame without the specified columns.
        """
        from mock_spark.dataframe import MockDataFrame
        
        # Validate that all columns exist
        existing_columns = [field.name for field in self.df.schema.fields]
        for col in cols:
            if col not in existing_columns:
                raise_column_not_found(col)
        
        # Filter data to exclude dropped columns
        filtered_data = []
        for row in self.df.data:
            filtered_row = {k: v for k, v in row.items() if k not in cols}
            filtered_data.append(filtered_row)
        
        # Create new schema without dropped columns
        new_schema_fields = [field for field in self.df.schema.fields if field.name not in cols]
        new_schema = MockStructType(new_schema_fields)
        
        return MockDataFrame(filtered_data, new_schema, self.df.storage)
    
    def withColumnRenamed(self, existing: str, new: str) -> "MockDataFrame":
        """Rename a column in the DataFrame.
        
        Args:
            existing: Current column name.
            new: New column name.
        
        Returns:
            New MockDataFrame with renamed column.
        """
        from mock_spark.dataframe import MockDataFrame
        
        # Validate that the existing column exists
        existing_columns = [field.name for field in self.df.schema.fields]
        if existing not in existing_columns:
            raise_column_not_found(existing)
        
        # Create new data with renamed column
        new_data = []
        for row in self.df.data:
            new_row = {}
            for key, value in row.items():
                if key == existing:
                    new_row[new] = value
                else:
                    new_row[key] = value
            new_data.append(new_row)
        
        # Create new schema with renamed column
        new_schema_fields = []
        for field in self.df.schema.fields:
            if field.name == existing:
                new_field = MockStructField(new, field.dataType, field.nullable)
                new_schema_fields.append(new_field)
            else:
                new_schema_fields.append(field)
        
        new_schema = MockStructType(new_schema_fields)
        return MockDataFrame(new_data, new_schema, self.df.storage)
