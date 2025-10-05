"""
DataFrame operations module for Mock Spark.

This module contains the core DataFrame operations including selection,
filtering, column manipulation, and other fundamental operations.
"""

from typing import Any, Dict, List, Optional, Union, Tuple
import datetime

from ...spark_types import (
    MockStructType,
    MockStructField,
    MockRow,
    StringType,
    LongType,
    DoubleType,
    IntegerType,
    BooleanType,
)
from ...functions import MockColumn, MockColumnOperation, F, MockLiteral, MockAggregateFunction
from ...core.exceptions import (
    AnalysisException,
    IllegalArgumentException,
    PySparkValueError,
)
from ...core.exceptions.analysis import ColumnNotFoundException
from .dataframe import MockDataFrame


class DataFrameOperations:
    """Mixin class providing DataFrame operations functionality."""

    def select(self, *columns: Union[str, MockColumn, MockLiteral, Any]) -> "MockDataFrame":
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
                    col_names.extend([field.name for field in self.schema.fields])
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
                    col_names.extend([field.name for field in self.schema.fields])
                else:
                    col_names.append(col.name)
            elif hasattr(col, "operation") and hasattr(col, "column"):
                # Handle MockColumnOperation (e.g., col + 1, upper(col))
                col_names.append(col.name)
            elif hasattr(col, "function_name") and hasattr(col, "window_spec"):
                # Handle MockWindowFunction (e.g., rank().over(window))
                col_names.append(col.name)
            elif hasattr(col, "name"):  # Support other column-like objects
                col_names.append(col.name)
            else:
                raise PySparkValueError(f"Invalid column type: {type(col)}")

        # Validate non-literal columns exist
        for col_name in col_names:
            if (
                col_name not in [field.name for field in self.schema.fields]
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
                    and hasattr(col, "window_spec")
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
                )
                and not any(
                    hasattr(col, "conditions") and hasattr(col, "name") and col.name == col_name
                    for col in columns
                )
                and not self._is_function_call(col_name)
            ):
                raise ColumnNotFoundException(col_name)

        # Filter data to selected columns and add literal values
        filtered_data = []
        for row in self.data:
            filtered_row = {}
            for i, col in enumerate(columns):
                if isinstance(col, str):
                    col_name = col
                    if col_name == "*":
                        # Add all existing columns
                        for field in self.schema.fields:
                            filtered_row[field.name] = row[field.name]
                    elif col_name in literal_columns:
                        # Add literal value
                        filtered_row[col_name] = literal_columns[col_name]
                    elif col_name in ("current_timestamp()", "current_date()"):
                        # Handle timestamp and date functions
                        if col_name == "current_timestamp()":
                            filtered_row[col_name] = datetime.datetime.now()
                        elif col_name == "current_date()":
                            filtered_row[col_name] = datetime.date.today()
                    else:
                        # Add existing column value
                        filtered_row[col_name] = row[col_name]
                elif hasattr(col, "operation") and hasattr(col, "column"):
                    # Handle MockColumnOperation (e.g., upper(col), length(col))
                    col_name = col.name
                    evaluated_value = self._evaluate_column_expression(row, col)
                    filtered_row[col_name] = evaluated_value
                elif hasattr(col, "conditions"):
                    # Handle MockCaseWhen objects
                    col_name = col.name
                    evaluated_value = self._evaluate_case_when(row, col)
                    filtered_row[col_name] = evaluated_value
                elif isinstance(col, MockColumn):
                    col_name = col.name
                    if col_name == "*":
                        # Add all existing columns
                        for field in self.schema.fields:
                            filtered_row[field.name] = row[field.name]
                    elif hasattr(col, "_original_column") and col._original_column is not None:
                        # Alias of an existing column: copy value under alias name
                        original_name = col._original_column.name
                        filtered_row[col_name] = row.get(original_name)
                    elif col_name in literal_columns:
                        # Add literal value
                        filtered_row[col_name] = literal_columns[col_name]
                    else:
                        # Handle function calls and expressions
                        evaluated_value = self._evaluate_column_expression(row, col)
                        filtered_row[col_name] = evaluated_value

            filtered_data.append(filtered_row)

        # Create new schema
        new_fields = []
        for col in columns:
            if isinstance(col, str):
                if col == "*":
                    # Add all existing fields
                    new_fields.extend(self.schema.fields)
                else:
                    # Find the field in the original schema
                    for field in self.schema.fields:
                        if field.name == col:
                            new_fields.append(field)
                            break
            elif isinstance(col, MockLiteral):
                # Add field for literal
                new_fields.append(MockStructField(col.name, col.column_type))
            elif isinstance(col, MockColumn):
                if col.name == "*":
                    # Add all existing fields
                    new_fields.extend(self.schema.fields)
                elif hasattr(col, "_original_column") and col._original_column is not None:
                    # Alias of an existing column: create new field with alias name
                    original_field = None
                    for field in self.schema.fields:
                        if field.name == col._original_column.name:
                            original_field = field
                            break
                    if original_field:
                        new_fields.append(MockStructField(col.name, original_field.dataType))
                else:
                    # Handle function calls and expressions - default to StringType
                    new_fields.append(MockStructField(col.name, StringType()))
            elif hasattr(col, "operation") and hasattr(col, "column"):
                # Handle MockColumnOperation - determine type based on operation
                if col.operation in ["+", "-", "*", "/", "%"]:
                    new_fields.append(MockStructField(col.name, LongType()))
                elif col.operation in ["abs"]:
                    new_fields.append(MockStructField(col.name, LongType()))
                elif col.operation in ["length"]:
                    new_fields.append(MockStructField(col.name, IntegerType()))
                elif col.operation in ["round"]:
                    new_fields.append(MockStructField(col.name, DoubleType()))
                elif col.operation in ["upper", "lower"]:
                    new_fields.append(MockStructField(col.name, StringType()))
                else:
                    new_fields.append(MockStructField(col.name, StringType()))
            elif hasattr(col, "conditions"):
                # Handle MockCaseWhen objects - default to StringType
                new_fields.append(MockStructField(col.name, StringType()))
            elif hasattr(col, "function_name") and hasattr(col, "window_spec"):
                # Handle MockWindowFunction - default to LongType for ranking functions
                new_fields.append(MockStructField(col.name, LongType()))

        new_schema = MockStructType(new_fields)
        return MockDataFrame(filtered_data, new_schema, self.storage)

    def filter(self, condition: Union[MockColumnOperation, MockColumn]) -> "MockDataFrame":
        """Filter rows based on condition."""
        if isinstance(condition, MockColumn):
            # Simple column reference - return all non-null rows
            filtered_data = [row for row in self.data if row.get(condition.name) is not None]
        else:
            # Apply condition logic
            filtered_data = self._apply_condition(self.data, condition)

        return MockDataFrame(filtered_data, self.schema, self.storage)

    def withColumn(
        self,
        col_name: str,
        col: Union[MockColumn, MockColumnOperation, MockLiteral, Any],
    ) -> "MockDataFrame":
        """Add or replace column."""
        new_data = []

        for row in self.data:
            new_row = row.copy()

            if isinstance(col, (MockColumn, MockColumnOperation)):
                # Evaluate the column expression
                evaluated_value = self._evaluate_column_expression(row, col)
                new_row[col_name] = evaluated_value
            elif hasattr(col, "value") and hasattr(col, "column_type"):
                # Handle MockLiteral objects
                new_row[col_name] = col.value
            else:
                new_row[col_name] = col

            new_data.append(new_row)

        # Update schema
        new_fields = [field for field in self.schema.fields if field.name != col_name]

        # Determine the correct type for the new column
        if isinstance(col, (MockColumn, MockColumnOperation)):
            # For arithmetic operations, determine type based on the operation
            if hasattr(col, "operation") and col.operation in ["+", "-", "*", "/", "%"]:
                # Arithmetic operations typically return LongType or DoubleType
                new_fields.append(MockStructField(col_name, LongType()))
            elif hasattr(col, "operation") and col.operation in ["abs"]:
                new_fields.append(MockStructField(col_name, LongType()))
            elif hasattr(col, "operation") and col.operation in ["length"]:
                new_fields.append(MockStructField(col_name, IntegerType()))
            elif hasattr(col, "operation") and col.operation in ["round"]:
                new_fields.append(MockStructField(col_name, DoubleType()))
            elif hasattr(col, "operation") and col.operation in ["upper", "lower"]:
                new_fields.append(MockStructField(col_name, StringType()))
            else:
                # Default to StringType for unknown operations
                new_fields.append(MockStructField(col_name, StringType()))
        elif hasattr(col, "value") and hasattr(col, "column_type"):
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

    def drop(self, *cols: str) -> "MockDataFrame":
        """Drop specified columns."""
        # Validate that all columns exist
        for col in cols:
            if col not in [field.name for field in self.schema.fields]:
                raise ColumnNotFoundException(col)

        # Filter out dropped columns
        new_fields = [field for field in self.schema.fields if field.name not in cols]
        new_schema = MockStructType(new_fields)

        # Create new data without dropped columns
        new_data = []
        for row in self.data:
            new_row = {k: v for k, v in row.items() if k not in cols}
            new_data.append(new_row)

        return MockDataFrame(new_data, new_schema, self.storage)

    def withColumnRenamed(self, existing: str, new: str) -> "MockDataFrame":
        """Rename a column."""
        if existing not in [field.name for field in self.schema.fields]:
            raise ColumnNotFoundException(existing)

        # Create new schema with renamed field
        new_fields = []
        for field in self.schema.fields:
            if field.name == existing:
                new_fields.append(MockStructField(new, field.dataType))
            else:
                new_fields.append(field)

        new_schema = MockStructType(new_fields)

        # Create new data with renamed column
        new_data = []
        for row in self.data:
            new_row = {}
            for k, v in row.items():
                if k == existing:
                    new_row[new] = v
                else:
                    new_row[k] = v
            new_data.append(new_row)

        return MockDataFrame(new_data, new_schema, self.storage)

    def dropna(
        self,
        how: str = "any",
        thresh: Optional[int] = None,
        subset: Optional[List[str]] = None,
    ) -> "MockDataFrame":
        """Drop rows with null values."""
        if subset is None:
            subset = [field.name for field in self.schema.fields]

        filtered_data = []
        for row in self.data:
            if how == "any":
                # Drop if any column in subset is null
                if not any(row.get(col) is None for col in subset):
                    filtered_data.append(row)
            elif how == "all":
                # Drop if all columns in subset are null
                if not all(row.get(col) is None for col in subset):
                    filtered_data.append(row)

        return MockDataFrame(filtered_data, self.schema, self.storage)

    def fillna(self, value: Union[Any, Dict[str, Any]]) -> "MockDataFrame":
        """Fill null values with specified value."""
        new_data = []
        for row in self.data:
            new_row = row.copy()
            if isinstance(value, dict):
                # Fill specific columns
                for col, fill_value in value.items():
                    if new_row.get(col) is None:
                        new_row[col] = fill_value
            else:
                # Fill all null values
                for col in new_row:
                    if new_row[col] is None:
                        new_row[col] = value
            new_data.append(new_row)

        return MockDataFrame(new_data, self.schema, self.storage)

    def distinct(self) -> "MockDataFrame":
        """Return distinct rows."""
        seen = set()
        unique_data = []
        for row in self.data:
            row_tuple = tuple(sorted(row.items()))
            if row_tuple not in seen:
                seen.add(row_tuple)
                unique_data.append(row)

        return MockDataFrame(unique_data, self.schema, self.storage)

    def dropDuplicates(self, subset: Optional[List[str]] = None) -> "MockDataFrame":
        """Drop duplicate rows."""
        if subset is None:
            subset = [field.name for field in self.schema.fields]

        seen = set()
        unique_data = []
        for row in self.data:
            row_tuple = tuple(sorted((k, v) for k, v in row.items() if k in subset))
            if row_tuple not in seen:
                seen.add(row_tuple)
                unique_data.append(row)

        return MockDataFrame(unique_data, self.schema, self.storage)

    def selectExpr(self, *exprs: str) -> "MockDataFrame":
        """Select columns using SQL expressions."""
        # This is a simplified implementation
        # In a full implementation, this would parse SQL expressions
        columns = []
        for expr in exprs:
            if " as " in expr:
                # Handle aliases like "name as n"
                col_name, alias = expr.split(" as ", 1)
                col_name = col_name.strip()
                alias = alias.strip()
                # Create a MockColumn with alias
                from ...functions import F

                columns.append(F.col(col_name).alias(alias))
            else:
                # Simple column reference
                columns.append(expr.strip())

        return self.select(*columns)

    # Helper methods for operations
    def _apply_condition(self, data: List[Dict[str, Any]], condition: Any) -> List[Dict[str, Any]]:
        """Apply a condition to filter data."""
        filtered_data = []
        for row in data:
            if self._evaluate_condition(row, condition):
                filtered_data.append(row)
        return filtered_data

    def _evaluate_condition(self, row: Dict[str, Any], condition: Any) -> bool:
        """Evaluate a condition against a row."""
        if isinstance(condition, MockColumnOperation):
            return self._evaluate_column_operation(row, condition)
        elif isinstance(condition, MockColumn):
            # Simple column reference - check if not null
            return row.get(condition.name) is not None
        else:
            return bool(condition)

    def _evaluate_column_operation(self, row: Dict[str, Any], operation: Any) -> Any:
        """Evaluate a column operation against a row."""
        if hasattr(operation, "operation"):
            if operation.operation == "==":
                left_val = self._get_column_value(row, operation.column)
                right_val = self._get_column_value(row, operation.value)
                return left_val == right_val
            elif operation.operation == "!=":
                left_val = self._get_column_value(row, operation.column)
                right_val = self._get_column_value(row, operation.value)
                return left_val != right_val
            elif operation.operation == ">":
                left_val = self._get_column_value(row, operation.column)
                right_val = self._get_column_value(row, operation.value)
                return left_val > right_val
            elif operation.operation == "<":
                left_val = self._get_column_value(row, operation.column)
                right_val = self._get_column_value(row, operation.value)
                return left_val < right_val
            elif operation.operation == ">=":
                left_val = self._get_column_value(row, operation.column)
                right_val = self._get_column_value(row, operation.value)
                return left_val >= right_val
            elif operation.operation == "<=":
                left_val = self._get_column_value(row, operation.column)
                right_val = self._get_column_value(row, operation.value)
                return left_val <= right_val
            elif operation.operation == "&":
                left_val = self._evaluate_column_operation(row, operation.column)
                right_val = self._evaluate_column_operation(row, operation.value)
                return bool(left_val) and bool(right_val)
            elif operation.operation == "|":
                left_val = self._evaluate_column_operation(row, operation.column)
                right_val = self._evaluate_column_operation(row, operation.value)
                return bool(left_val) or bool(right_val)
            elif operation.operation == "~":
                val = self._evaluate_column_operation(row, operation.column)
                return not bool(val)
            else:
                # Handle other operations
                return self._evaluate_column_expression(row, operation)
        else:
            return self._evaluate_column_expression(row, operation)

    def _evaluate_column_expression(self, row: Dict[str, Any], column_expression: Any) -> Any:
        """Evaluate a column expression against a row."""
        if isinstance(column_expression, str):
            return row.get(column_expression)
        elif isinstance(column_expression, MockColumn):
            return self._evaluate_mock_column(row, column_expression)
        elif isinstance(column_expression, MockLiteral):
            return column_expression.value
        else:
            return column_expression

    def _evaluate_mock_column(self, row: Dict[str, Any], column: MockColumn) -> Any:
        """Evaluate a MockColumn expression against a row."""
        if column.name in row:
            return row[column.name]
        else:
            # Handle function calls and expressions
            return self._evaluate_column_expression(row, column)

    def _get_column_value(self, row: Dict[str, Any], column: Any) -> Any:
        """Get value from a row for a given column."""
        if isinstance(column, str):
            return row.get(column)
        elif isinstance(column, MockColumn):
            return self._evaluate_mock_column(row, column)
        elif isinstance(column, MockLiteral):
            return column.value
        else:
            return column

    def _is_function_call(self, col_name: str) -> bool:
        """Check if a column name represents a function call."""
        return col_name.endswith("()") or "(" in col_name

    def _evaluate_case_when(self, row: Dict[str, Any], case_when_obj) -> Any:
        """Evaluate a case-when expression against a row."""
        # This is a simplified implementation
        # In a full implementation, this would handle complex case-when logic
        return None

    def _handle_aggregation_select(self, columns: List[Any]) -> "MockDataFrame":
        """Handle aggregation operations in select."""
        # This is a simplified implementation
        # In a full implementation, this would handle complex aggregations
        return MockDataFrame([], self.schema, self.storage)
