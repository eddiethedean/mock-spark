"""
DataFrame transformation operations for Mock Spark.

This module provides transformation operations like select, filter, withColumn, etc.
"""

from typing import Any, Dict, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from .dataframe_refactored import MockDataFrame
from ..functions import MockColumn, MockColumnOperation, MockLiteral
from ..core.exceptions.operation import MockSparkColumnNotFoundError
from ..core.exceptions.analysis import ColumnNotFoundException


class DataFrameTransformer:
    """Handles DataFrame transformation operations (select, filter, withColumn, etc.)."""

    def __init__(self, dataframe: "MockDataFrame"):
        """Initialize transformer.

        Args:
            dataframe: The DataFrame instance to transform
        """
        self.dataframe = dataframe

    def select(
        self, *columns: Union[str, MockColumn, MockLiteral, Any]
    ) -> "MockDataFrame":
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
            return self.dataframe

        # Validate column names eagerly (even in lazy mode) to match PySpark behavior
        # But skip validation if there are pending join operations (columns might come from other DF)
        has_pending_joins = any(
            op[0] == "join" for op in self.dataframe._operations_queue
        )

        if not has_pending_joins:
            for col in columns:
                if isinstance(col, str) and col != "*":
                    # Check if column exists
                    if col not in self.dataframe.columns:
                        raise MockSparkColumnNotFoundError(col, self.dataframe.columns)
                elif isinstance(col, MockColumn):
                    if hasattr(col, "operation"):
                        # Complex expression - validate column references
                        self._validate_expression_columns(col, "select")
                    else:
                        # Simple column reference - validate
                        if col.name not in self.dataframe.columns:
                            raise MockSparkColumnNotFoundError(
                                col.name, self.dataframe.columns
                            )
                elif isinstance(col, MockColumnOperation):
                    # Complex expression - validate column references
                    # Skip validation for function operations that will be evaluated later
                    if not (
                        hasattr(col, "operation")
                        and col.operation in ["months_between", "datediff"]
                    ):
                        self._validate_expression_columns(col, "select")

        # Always use lazy evaluation
        return self.dataframe._queue_op("select", columns)

    def filter(
        self, condition: Union[MockColumnOperation, MockColumn, MockLiteral]
    ) -> "MockDataFrame":
        """Filter rows based on condition.

        Args:
            condition: Filter condition

        Returns:
            New MockDataFrame with filtered rows
        """
        # Pre-validation: validate filter expression
        self._validate_filter_expression(condition, "filter")

        return self.dataframe._queue_op("filter", condition)

    def withColumn(
        self,
        col_name: str,
        col: Union[MockColumn, MockColumnOperation, MockLiteral, Any],
    ) -> "MockDataFrame":
        """Add or replace column.

        Args:
            col_name: Name of the column to add/replace
            col: Column expression

        Returns:
            New MockDataFrame with the new column
        """
        # Validate column references in expressions
        if isinstance(col, MockColumn) and not hasattr(col, "operation"):
            # Simple column reference - validate
            self._validate_column_exists(col.name, "withColumn")
        elif isinstance(col, MockColumnOperation):
            # Complex expression - validate column references
            self._validate_expression_columns(col, "withColumn")
        # For MockLiteral and other cases, skip validation

        return self.dataframe._queue_op("withColumn", (col_name, col))

    def withColumns(
        self,
        colsMap: Dict[str, Union[MockColumn, MockColumnOperation, MockLiteral, Any]],
    ) -> "MockDataFrame":
        """Add or replace multiple columns.

        Args:
            colsMap: Dictionary mapping column names to expressions

        Returns:
            New MockDataFrame with the new columns
        """
        # Validate all column expressions
        for col_name, col_expr in colsMap.items():
            if isinstance(col_expr, MockColumn) and not hasattr(col_expr, "operation"):
                # Simple column reference - validate
                self._validate_column_exists(col_expr.name, "withColumns")
            elif isinstance(col_expr, MockColumnOperation):
                # Complex expression - validate column references
                self._validate_expression_columns(col_expr, "withColumns")

        return self.dataframe._queue_op("withColumns", colsMap)

    def withColumnRenamed(self, existing: str, new: str) -> "MockDataFrame":
        """Rename a column.

        Args:
            existing: Current column name
            new: New column name

        Returns:
            New MockDataFrame with renamed column
        """
        # Validate existing column exists
        self._validate_column_exists(existing, "withColumnRenamed")

        return self.dataframe._queue_op("withColumnRenamed", (existing, new))

    def withColumnsRenamed(self, colsMap: Dict[str, str]) -> "MockDataFrame":
        """Rename multiple columns.

        Args:
            colsMap: Dictionary mapping old names to new names

        Returns:
            New MockDataFrame with renamed columns
        """
        # Validate all existing columns exist
        for existing_col in colsMap.keys():
            self._validate_column_exists(existing_col, "withColumnsRenamed")

        return self.dataframe._queue_op("withColumnsRenamed", colsMap)

    def orderBy(self, *columns: Union[str, MockColumn]) -> "MockDataFrame":
        """Order by columns.

        Args:
            *columns: Columns to order by

        Returns:
            New MockDataFrame ordered by the specified columns
        """
        return self.dataframe._queue_op("orderBy", columns)

    def selectExpr(self, *exprs: str) -> "MockDataFrame":
        """Select columns or expressions using SQL-like syntax.

        Args:
            *exprs: SQL expressions to select

        Returns:
            New MockDataFrame with selected expressions
        """
        # For now, delegate to regular select - full SQL parsing would be implemented here
        # This is a simplified version that handles basic column references
        columns = []
        for expr in exprs:
            if expr == "*":
                columns.append("*")
            elif expr in self.dataframe.columns:
                columns.append(expr)
            else:
                # For complex expressions, we'd need a SQL parser
                # For now, treat as literal column name
                columns.append(expr)

        return self.select(*columns)

    def _validate_column_exists(self, col_name: str, operation: str) -> None:
        """Validate that a column exists.

        Args:
            col_name: Column name to validate
            operation: Operation being performed (for error messages)

        Raises:
            ColumnNotFoundException: If column doesn't exist
        """
        if col_name not in self.dataframe.columns:
            raise ColumnNotFoundException(col_name)

    def _validate_expression_columns(self, expr: Any, operation: str) -> None:
        """Validate column references in expressions.

        Args:
            expr: Expression to validate
            operation: Operation being performed (for error messages)

        Raises:
            ColumnNotFoundException: If referenced columns don't exist
        """
        if isinstance(expr, MockColumn):
            if not hasattr(expr, "operation"):
                # Simple column reference
                self._validate_column_exists(expr.name, operation)
            else:
                # Complex expression - recursively validate
                if hasattr(expr, "column"):
                    self._validate_expression_columns(expr.column, operation)
                if hasattr(expr, "value") and isinstance(expr.value, MockColumn):
                    self._validate_expression_columns(expr.value, operation)
        elif isinstance(expr, MockColumnOperation):
            # Recursively validate nested expressions
            if hasattr(expr, "column"):
                self._validate_expression_columns(expr.column, operation)
            if hasattr(expr, "value") and isinstance(
                expr.value, (MockColumn, MockColumnOperation)
            ):
                self._validate_expression_columns(expr.value, operation)

    def _validate_filter_expression(self, condition: Any, operation: str) -> None:
        """Validate filter expression.

        Args:
            condition: Filter condition to validate
            operation: Operation being performed (for error messages)

        Raises:
            ColumnNotFoundException: If referenced columns don't exist
        """
        self._validate_expression_columns(condition, operation)
