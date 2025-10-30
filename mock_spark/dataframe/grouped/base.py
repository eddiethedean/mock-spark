"""
Base grouped data implementation for Mock Spark.

This module provides the core MockGroupedData class for DataFrame aggregation
operations, maintaining compatibility with PySpark's GroupedData interface.
"""

from typing import Any, List, Dict, Union, Tuple, TYPE_CHECKING, Optional, Set
import statistics

from ...functions import (
    MockColumn,
    MockColumnOperation,
    MockAggregateFunction,
)
from ...core.exceptions.analysis import AnalysisException

if TYPE_CHECKING:
    from ..dataframe import MockDataFrame
    from .rollup import MockRollupGroupedData
    from .cube import MockCubeGroupedData
    from .pivot import MockPivotGroupedData


class MockGroupedData:
    """Mock grouped data for aggregation operations.

    Provides grouped data functionality for DataFrame aggregation operations,
    maintaining compatibility with PySpark's GroupedData interface.
    """

    def __init__(self, df: "MockDataFrame", group_columns: List[str]):
        """Initialize MockGroupedData.

        Args:
            df: The DataFrame being grouped.
            group_columns: List of column names to group by.
        """
        self.df = df
        self.group_columns = group_columns

    def agg(
        self, *exprs: Union[str, MockColumn, MockColumnOperation, MockAggregateFunction]
    ) -> "MockDataFrame":
        """Aggregate grouped data.

        Args:
            *exprs: Aggregation expressions.

        Returns:
            New MockDataFrame with aggregated results.
        """
        from ...functions.core.literals import MockLiteral

        # Materialize the DataFrame if it has queued operations
        if self.df._operations_queue:
            self.df = self.df._materialize_if_lazy()

        # Group data by group columns
        groups: Dict[Any, List[Dict[str, Any]]] = {}
        for row in self.df.data:
            group_key = tuple(row.get(col) for col in self.group_columns)
            if group_key not in groups:
                groups[group_key] = []
            groups[group_key].append(row)

        # Track which result keys are from count/rank functions (non-nullable)
        non_nullable_keys = set()

        # Apply aggregations
        result_data = []
        for group_key, group_rows in groups.items():
            result_row = dict(zip(self.group_columns, group_key))

            for expr in exprs:
                if isinstance(expr, str):
                    # Handle string expressions like "sum(age)"
                    result_key, result_value = self._evaluate_string_expression(
                        expr, group_rows
                    )
                    # Check if this is a count function
                    if expr.startswith("count("):
                        non_nullable_keys.add(result_key)
                    result_row[result_key] = result_value
                elif hasattr(expr, "function_name"):
                    # Handle MockAggregateFunction
                    from typing import cast
                    from ...functions import MockAggregateFunction

                    result_key, result_value = self._evaluate_aggregate_function(
                        cast(MockAggregateFunction, expr), group_rows
                    )
                    # Check if this is a count function
                    if expr.function_name == "count":
                        non_nullable_keys.add(result_key)
                    result_row[result_key] = result_value
                elif isinstance(expr, MockLiteral):
                    # For literals in aggregation, just use their value
                    lit_expr = expr
                    # Use the literal's name as key if it has an alias
                    result_key = getattr(lit_expr, "name", str(lit_expr.value))
                    result_row[result_key] = lit_expr.value
                elif hasattr(expr, "name"):
                    # Handle MockColumn or MockColumnOperation
                    result_key, result_value = self._evaluate_column_expression(
                        expr, group_rows
                    )
                    result_row[result_key] = result_value

            result_data.append(result_row)

        # Create result DataFrame with proper schema
        from ..dataframe import MockDataFrame
        from ...spark_types import (
            MockStructType,
            MockStructField,
            StringType,
            LongType,
            DoubleType,
            BooleanType,
            MockDataType,
        )

        # Create schema based on the first result row and expression types
        if result_data:
            fields = []

            # Track which expressions are literals for proper nullable inference
            literal_keys: Set[str] = set()
            for expr in exprs:
                if isinstance(expr, MockLiteral):
                    lit_key = getattr(expr, "name", str(expr.value))
                    literal_keys.add(lit_key)

            for key, value in result_data[0].items():
                if key in self.group_columns:
                    # Use existing schema for group columns
                    for field in self.df.schema.fields:
                        if field.name == key:
                            fields.append(field)
                            break
                else:
                    # Determine if this is a literal value
                    is_literal = key in literal_keys

                    # Count functions, window ranking functions, and boolean functions are non-nullable in PySpark
                    # Other aggregations and literals are non-nullable
                    is_count_function = key in non_nullable_keys or any(
                        key.startswith(func)
                        for func in [
                            "count(",
                            "count(1)",
                            "count(DISTINCT",
                            "count_if",
                            "row_number",
                            "rank",
                            "dense_rank",
                            "row_num",
                            "dept_row_num",
                            "global_row",
                            "dept_row",
                            "dept_rank",
                        ]
                    )
                    is_boolean_function = any(
                        key.startswith(func)
                        for func in ["coalesced_", "is_null_", "is_nan_"]
                    )
                    nullable = not (
                        is_literal or is_count_function or is_boolean_function
                    )

                    if isinstance(value, bool):
                        data_type = BooleanType(nullable=nullable)
                        fields.append(
                            MockStructField(key, data_type, nullable=nullable)
                        )
                    elif isinstance(value, str):
                        str_data_type: MockDataType = StringType(nullable=nullable)
                        fields.append(
                            MockStructField(key, str_data_type, nullable=nullable)
                        )
                    elif isinstance(value, float):
                        float_data_type: MockDataType = DoubleType(nullable=nullable)
                        fields.append(
                            MockStructField(key, float_data_type, nullable=nullable)
                        )
                    else:
                        long_data_type: MockDataType = LongType(nullable=nullable)
                        fields.append(
                            MockStructField(key, long_data_type, nullable=nullable)
                        )
            schema = MockStructType(fields)
            return MockDataFrame(result_data, schema)
        else:
            # Empty result
            return MockDataFrame(result_data, MockStructType([]))

    def _evaluate_string_expression(
        self, expr: str, group_rows: List[Dict[str, Any]]
    ) -> Tuple[str, Any]:
        """Evaluate string aggregation expression.

        Args:
            expr: String expression to evaluate.
            group_rows: Rows in the group.

        Returns:
            Tuple of (result_key, result_value).
        """
        if expr.startswith("sum("):
            col_name = expr[4:-1]
            # Validate column exists using ValidationHandler
            from ...dataframe.validation_handler import ValidationHandler

            validator = ValidationHandler()
            validator.validate_column_exists(self.df.schema, col_name, "aggregation")
            values = [
                row.get(col_name, 0)
                for row in group_rows
                if row.get(col_name) is not None
            ]
            return expr, sum(values) if values else 0
        elif expr.startswith("avg("):
            col_name = expr[4:-1]
            # Validate column exists using ValidationHandler
            from ...dataframe.validation_handler import ValidationHandler

            validator = ValidationHandler()
            validator.validate_column_exists(self.df.schema, col_name, "aggregation")
            values = [
                row.get(col_name, 0)
                for row in group_rows
                if row.get(col_name) is not None
            ]
            return expr, sum(values) / len(values) if values else 0
        elif expr.startswith("count("):
            return expr, len(group_rows)
        elif expr.startswith("max("):
            col_name = expr[4:-1]
            # Validate column exists using ValidationHandler
            from ...dataframe.validation_handler import ValidationHandler

            validator = ValidationHandler()
            validator.validate_column_exists(self.df.schema, col_name, "aggregation")
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            return expr, max(values) if values else None
        elif expr.startswith("min("):
            col_name = expr[4:-1]
            # Validate column exists using ValidationHandler
            from ...dataframe.validation_handler import ValidationHandler

            validator = ValidationHandler()
            validator.validate_column_exists(self.df.schema, col_name, "aggregation")
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            return expr, min(values) if values else None
        else:
            return expr, None

    def _evaluate_aggregate_function(
        self, expr: MockAggregateFunction, group_rows: List[Dict[str, Any]]
    ) -> Tuple[str, Any]:
        """Evaluate MockAggregateFunction.

        Args:
            expr: Aggregate function to evaluate.
            group_rows: Rows in the group.

        Returns:
            Tuple of (result_key, result_value).
        """
        func_name = expr.function_name
        col_name = (
            getattr(expr, "column_name", "") if hasattr(expr, "column_name") else ""
        )

        # Check if the function has an alias set
        has_alias = expr.name != expr._generate_name()
        alias_name = expr.name if has_alias else None

        if func_name == "sum":
            # If the aggregate targets an expression (e.g., cast or arithmetic), evaluate per-row
            if hasattr(expr, "column") and hasattr(expr.column, "operation"):
                values = []
                for row_data in group_rows:
                    try:
                        expr_result = self.df._evaluate_column_expression(
                            row_data, expr.column
                        )
                        if expr_result is not None:
                            # Coerce booleans to ints to mirror Spark when user casts
                            if isinstance(expr_result, bool):
                                expr_result = 1 if expr_result else 0
                            # Convert numeric-looking strings
                            if isinstance(expr_result, str):
                                try:
                                    expr_result = (
                                        float(expr_result)
                                        if "." in expr_result
                                        else int(expr_result)
                                    )
                                except ValueError:
                                    continue
                            values.append(expr_result)
                    except (ValueError, TypeError, AttributeError):
                        pass
                result_key = alias_name if alias_name else f"sum({col_name})"
                return result_key, sum(values) if values else 0
            # Simple column: validate and sum
            if (
                col_name
                and not any(
                    op in col_name
                    for op in [
                        "+",
                        "-",
                        "*",
                        "/",
                        "(",
                        ")",
                        "extract",
                        "TRY_CAST",
                        "AS",
                    ]
                )
                and col_name not in [field.name for field in self.df.schema.fields]
            ):
                available_columns = [field.name for field in self.df.schema.fields]
                from ...core.exceptions.operation import MockSparkColumnNotFoundError

                raise MockSparkColumnNotFoundError(col_name, available_columns)
            values = []
            for row in group_rows:
                val = row.get(col_name)
                if val is not None:
                    if isinstance(val, bool):
                        val = 1 if val else 0
                    if isinstance(val, str):
                        try:
                            val = float(val) if "." in val else int(val)
                        except ValueError:
                            continue
                    values.append(val)
            result_key = alias_name if alias_name else f"sum({col_name})"
            return result_key, sum(values) if values else 0
        elif func_name == "avg":
            # Expression-aware avg
            if hasattr(expr, "column") and hasattr(expr.column, "operation"):
                values = []
                for row_data in group_rows:
                    try:
                        expr_result = self.df._evaluate_column_expression(
                            row_data, expr.column
                        )
                        if expr_result is not None:
                            if isinstance(expr_result, bool):
                                expr_result = 1 if expr_result else 0
                            if isinstance(expr_result, str):
                                try:
                                    expr_result = (
                                        float(expr_result)
                                        if "." in expr_result
                                        else int(expr_result)
                                    )
                                except ValueError:
                                    continue
                            values.append(expr_result)
                    except (ValueError, TypeError, AttributeError):
                        pass
                result_key = alias_name if alias_name else f"avg({col_name})"
                return result_key, (sum(values) / len(values)) if values else None
            # Simple column: validate and average
            if (
                col_name
                and not any(
                    op in col_name
                    for op in [
                        "+",
                        "-",
                        "*",
                        "/",
                        "(",
                        ")",
                        "extract",
                        "TRY_CAST",
                        "AS",
                    ]
                )
                and col_name not in [field.name for field in self.df.schema.fields]
            ):
                available_columns = [field.name for field in self.df.schema.fields]
                from ...core.exceptions.operation import MockSparkColumnNotFoundError

                raise MockSparkColumnNotFoundError(col_name, available_columns)
            values = []
            for row in group_rows:
                val = row.get(col_name)
                if val is not None:
                    if isinstance(val, bool):
                        val = 1 if val else 0
                    if isinstance(val, str):
                        try:
                            val = float(val) if "." in val else int(val)
                        except ValueError:
                            continue
                    values.append(val)
            result_key = alias_name if alias_name else f"avg({col_name})"
            return result_key, (sum(values) / len(values)) if values else None
        elif func_name == "count":
            if col_name == "*" or col_name == "":
                # For count(*), use alias if available, otherwise use the function's generated name
                result_key = alias_name if alias_name else expr._generate_name()
                return result_key, len(group_rows)
            else:
                result_key = alias_name if alias_name else f"count({col_name})"
                return result_key, len(group_rows)
        elif func_name == "max":
            # Check if this is a complex expression (MockColumnOperation)
            if hasattr(expr, "column") and hasattr(expr.column, "operation"):
                # Evaluate the expression for each row
                values = []
                for row_data in group_rows:
                    try:
                        expr_result = self.df._evaluate_column_expression(
                            row_data, expr.column
                        )
                        if expr_result is not None:
                            values.append(expr_result)
                    except (ValueError, TypeError, AttributeError):
                        pass
                result_key = alias_name if alias_name else f"max({col_name})"
                return result_key, max(values) if values else None
            else:
                # Simple column reference
                values = [
                    row.get(col_name)
                    for row in group_rows
                    if row.get(col_name) is not None
                ]
                result_key = alias_name if alias_name else f"max({col_name})"
                return result_key, max(values) if values else None
        elif func_name == "min":
            # Check if this is a complex expression (MockColumnOperation)
            if hasattr(expr, "column") and hasattr(expr.column, "operation"):
                # Evaluate the expression for each row
                values = []
                for row_data in group_rows:
                    try:
                        expr_result = self.df._evaluate_column_expression(
                            row_data, expr.column
                        )
                        if expr_result is not None:
                            values.append(expr_result)
                    except (ValueError, TypeError, AttributeError):
                        pass
                result_key = alias_name if alias_name else f"min({col_name})"
                return result_key, min(values) if values else None
            else:
                # Simple column reference
                values = [
                    row.get(col_name)
                    for row in group_rows
                    if row.get(col_name) is not None
                ]
                result_key = alias_name if alias_name else f"min({col_name})"
                return result_key, min(values) if values else None
        elif func_name == "collect_list":
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            result_key = alias_name if alias_name else f"collect_list({col_name})"
            return result_key, values
        elif func_name == "collect_set":
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            result_key = alias_name if alias_name else f"collect_set({col_name})"
            return result_key, list(set(values))
        elif func_name == "first":
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            result_key = alias_name if alias_name else f"first({col_name})"
            return result_key, values[0] if values else None
        elif func_name == "last":
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            result_key = alias_name if alias_name else f"last({col_name})"
            return result_key, values[-1] if values else None
        elif func_name == "stddev":
            values = [
                row.get(col_name)
                for row in group_rows
                if row.get(col_name) is not None
                and isinstance(row.get(col_name), (int, float))
            ]
            result_key = alias_name if alias_name else f"stddev({col_name})"
            if values:
                return result_key, statistics.stdev(values) if len(values) > 1 else 0.0
            else:
                return result_key, None
        elif func_name == "variance":
            values = [
                row.get(col_name)
                for row in group_rows
                if row.get(col_name) is not None
                and isinstance(row.get(col_name), (int, float))
            ]
            result_key = alias_name if alias_name else f"variance({col_name})"
            if values:
                return result_key, (
                    statistics.variance(values) if len(values) > 1 else 0.0
                )
            else:
                return result_key, None
        elif func_name == "skewness":
            values = [
                row.get(col_name)
                for row in group_rows
                if row.get(col_name) is not None
                and isinstance(row.get(col_name), (int, float))
            ]
            result_key = alias_name if alias_name else f"skewness({col_name})"
            if values and len(values) > 2:
                mean_val = statistics.mean(values)
                std_val = statistics.stdev(values)
                if std_val > 0:
                    skewness = sum((x - mean_val) ** 3 for x in values) / (
                        len(values) * std_val**3
                    )
                    return result_key, skewness
                else:
                    return result_key, 0.0
            else:
                return result_key, None
        elif func_name == "kurtosis":
            values = [
                row.get(col_name)
                for row in group_rows
                if row.get(col_name) is not None
                and isinstance(row.get(col_name), (int, float))
            ]
            result_key = alias_name if alias_name else f"kurtosis({col_name})"
            if values and len(values) > 3:
                mean_val = statistics.mean(values)
                std_val = statistics.stdev(values)
                if std_val > 0:
                    kurtosis = (
                        sum((x - mean_val) ** 4 for x in values)
                        / (len(values) * std_val**4)
                        - 3
                    )
                    return result_key, kurtosis
                else:
                    return result_key, 0.0
            else:
                return result_key, None
        elif func_name == "bool_and":
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            result_key = alias_name if alias_name else f"bool_and({col_name})"
            return result_key, all(values) if values else None
        elif func_name == "bool_or":
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            result_key = alias_name if alias_name else f"bool_or({col_name})"
            return result_key, any(values) if values else None
        elif func_name == "max_by":
            # max_by(col, ord) - return col value where ord is maximum
            if expr.ord_column is None:
                return alias_name if alias_name else f"max_by({col_name})", None
            ord_col_name = (
                expr.ord_column.name
                if hasattr(expr.ord_column, "name")
                else str(expr.ord_column)
            )
            if group_rows:
                max_row = max(
                    group_rows, key=lambda r: r.get(ord_col_name, float("-inf"))
                )
                result_key = alias_name if alias_name else f"max_by({col_name})"
                return result_key, max_row.get(col_name)
            return alias_name if alias_name else f"max_by({col_name})", None
        elif func_name == "min_by":
            # min_by(col, ord) - return col value where ord is minimum
            if expr.ord_column is None:
                return alias_name if alias_name else f"min_by({col_name})", None
            ord_col_name = (
                expr.ord_column.name
                if hasattr(expr.ord_column, "name")
                else str(expr.ord_column)
            )
            if group_rows:
                min_row = min(
                    group_rows, key=lambda r: r.get(ord_col_name, float("inf"))
                )
                result_key = alias_name if alias_name else f"min_by({col_name})"
                return result_key, min_row.get(col_name)
            return alias_name if alias_name else f"min_by({col_name})", None
        elif func_name == "count_if":
            # count_if(condition) - count where condition is true
            # The column might be a condition expression (e.g., col > 20)
            if expr.column is not None and hasattr(expr.column, "operation"):
                # This is a condition expression - evaluate it for each row
                true_count = 0
                for row in group_rows:
                    # Evaluate the condition expression
                    cond_expr = expr.column
                    if (
                        hasattr(cond_expr, "column")
                        and hasattr(cond_expr, "operation")
                        and hasattr(cond_expr, "value")
                    ):
                        col_val = row.get(
                            cond_expr.column.name
                            if hasattr(cond_expr.column, "name")
                            else cond_expr.column
                        )
                        comp_val = (
                            cond_expr.value.value
                            if hasattr(cond_expr.value, "value")
                            else cond_expr.value
                        )

                        # Evaluate the condition based on the operation
                        if cond_expr.operation == ">":
                            if col_val is not None and col_val > comp_val:
                                true_count += 1
                        elif cond_expr.operation == "<":
                            if col_val is not None and col_val < comp_val:
                                true_count += 1
                        elif cond_expr.operation == ">=":
                            if col_val is not None and col_val >= comp_val:
                                true_count += 1
                        elif cond_expr.operation == "<=":
                            if col_val is not None and col_val <= comp_val:
                                true_count += 1
                        elif cond_expr.operation == "==":
                            if col_val is not None and col_val == comp_val:
                                true_count += 1
                result_key = alias_name if alias_name else "count_if"
                return result_key, true_count
            else:
                # Simple boolean column
                values = [
                    row.get(col_name)
                    for row in group_rows
                    if row.get(col_name) is not None
                ]
                true_count = sum(
                    1 for v in values if v is True or v == 1 or str(v).lower() == "true"
                )
                result_key = alias_name if alias_name else f"count_if({col_name})"
                return result_key, true_count
        elif func_name == "any_value":
            # any_value(col) - return any non-null value (non-deterministic)
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            result_key = alias_name if alias_name else f"any_value({col_name})"
            return result_key, values[0] if values else None
        elif func_name == "mean":
            # mean(col) - alias for avg
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            result_key = alias_name if alias_name else f"mean({col_name})"
            return result_key, statistics.mean(values) if values else None
        elif func_name == "approx_count_distinct":
            # approx_count_distinct(col) - approximate distinct count
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            distinct_count = len(set(values))
            result_key = (
                alias_name if alias_name else f"approx_count_distinct({col_name})"
            )
            return result_key, distinct_count
        elif func_name == "countDistinct":
            # countDistinct(col) - exact distinct count
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            distinct_count = len(set(values))
            result_key = alias_name if alias_name else f"countDistinct({col_name})"
            return result_key, distinct_count
        elif func_name == "stddev_pop":
            # stddev_pop(col) - population standard deviation
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            result_key = alias_name if alias_name else f"stddev_pop({col_name})"
            return result_key, statistics.pstdev(values) if len(values) > 0 else None
        elif func_name == "stddev_samp":
            # stddev_samp(col) - sample standard deviation
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            result_key = alias_name if alias_name else f"stddev_samp({col_name})"
            return result_key, statistics.stdev(values) if len(values) > 1 else None
        elif func_name == "var_pop":
            # var_pop(col) - population variance
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            result_key = alias_name if alias_name else f"var_pop({col_name})"
            return result_key, statistics.pvariance(values) if len(values) > 0 else None
        elif func_name == "var_samp":
            # var_samp(col) - sample variance
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            result_key = alias_name if alias_name else f"var_samp({col_name})"
            return result_key, statistics.variance(values) if len(values) > 1 else None
        elif func_name == "covar_pop":
            # covar_pop(col1, col2) - population covariance
            # Get both columns
            if hasattr(expr, "ord_column") and expr.ord_column is not None:
                col2_name = (
                    expr.ord_column.name
                    if hasattr(expr.ord_column, "name")
                    else str(expr.ord_column)
                )
                values1 = [
                    row.get(col_name)
                    for row in group_rows
                    if row.get(col_name) is not None and row.get(col2_name) is not None
                ]
                values2 = [
                    row.get(col2_name)
                    for row in group_rows
                    if row.get(col_name) is not None and row.get(col2_name) is not None
                ]

                if len(values1) > 0 and len(values2) > 0:
                    # Mypy has limitations with statistics.mean and list comprehensions
                    mean1 = statistics.mean(values1)  # type: ignore[type-var]
                    mean2 = statistics.mean(values2)  # type: ignore[type-var]
                    if mean1 is not None and mean2 is not None:
                        covar = sum(
                            (x1 - mean1) * (x2 - mean2)
                            for x1, x2 in zip(values1, values2)
                        ) / len(values1)
                    else:
                        covar = 0.0
                    result_key = (
                        alias_name
                        if alias_name
                        else f"covar_pop({col_name}, {col2_name})"
                    )
                    return result_key, covar
                else:
                    result_key = alias_name if alias_name else f"covar_pop({col_name})"
                    return result_key, None
            else:
                result_key = alias_name if alias_name else f"covar_pop({col_name})"
                return result_key, None
        else:
            result_key = alias_name if alias_name else f"{func_name}({col_name})"
            return result_key, None

    def _evaluate_column_expression(
        self,
        expr: Union[MockColumn, MockColumnOperation],
        group_rows: List[Dict[str, Any]],
    ) -> Tuple[str, Any]:
        """Evaluate MockColumn or MockColumnOperation.

        Args:
            expr: Column expression to evaluate.
            group_rows: Rows in the group.

        Returns:
            Tuple of (result_key, result_value).
        """
        expr_name = expr.name
        if expr_name.startswith("sum("):
            col_name = expr_name[4:-1]
            values = [
                row.get(col_name, 0)
                for row in group_rows
                if row.get(col_name) is not None
            ]
            return expr_name, sum(values) if values else 0
        elif expr_name.startswith("avg("):
            col_name = expr_name[4:-1]
            values = [
                row.get(col_name, 0)
                for row in group_rows
                if row.get(col_name) is not None
            ]
            return expr_name, sum(values) / len(values) if values else 0
        elif expr_name.startswith("count("):
            return expr_name, len(group_rows)
        elif expr_name.startswith("max("):
            col_name = expr_name[4:-1]
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            return expr_name, max(values) if values else None
        elif expr_name.startswith("min("):
            col_name = expr_name[4:-1]
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            return expr_name, min(values) if values else None
        else:
            return expr_name, None

    def sum(self, *columns: Union[str, MockColumn]) -> "MockDataFrame":
        """Sum grouped data.

        Args:
            *columns: Columns to sum.

        Returns:
            MockDataFrame with sum aggregations.
        """
        if not columns:
            return self.agg("sum(1)")

        exprs = [
            f"sum({col})" if isinstance(col, str) else f"sum({col.name})"
            for col in columns
        ]
        return self.agg(*exprs)

    def avg(self, *columns: Union[str, MockColumn]) -> "MockDataFrame":
        """Average grouped data.

        Args:
            *columns: Columns to average.

        Returns:
            MockDataFrame with average aggregations.
        """
        if not columns:
            return self.agg("avg(1)")

        exprs = [
            f"avg({col})" if isinstance(col, str) else f"avg({col.name})"
            for col in columns
        ]
        return self.agg(*exprs)

    def count(self, *columns: Union[str, MockColumn]) -> "MockDataFrame":
        """Count grouped data.

        Args:
            *columns: Columns to count.

        Returns:
            MockDataFrame with count aggregations.
        """
        if not columns:
            # Use MockAggregateFunction for count(*) to get proper naming
            from ...functions.aggregate import AggregateFunctions

            return self.agg(AggregateFunctions.count())

        exprs = [
            f"count({col})" if isinstance(col, str) else f"count({col.name})"
            for col in columns
        ]
        return self.agg(*exprs)

    def max(self, *columns: Union[str, MockColumn]) -> "MockDataFrame":
        """Max grouped data.

        Args:
            *columns: Columns to get max of.

        Returns:
            MockDataFrame with max aggregations.
        """
        if not columns:
            return self.agg("max(1)")

        exprs = [
            f"max({col})" if isinstance(col, str) else f"max({col.name})"
            for col in columns
        ]
        return self.agg(*exprs)

    def min(self, *columns: Union[str, MockColumn]) -> "MockDataFrame":
        """Min grouped data.

        Args:
            *columns: Columns to get min of.

        Returns:
            MockDataFrame with min aggregations.
        """
        if not columns:
            return self.agg("min(1)")

        exprs = [
            f"min({col})" if isinstance(col, str) else f"min({col.name})"
            for col in columns
        ]
        return self.agg(*exprs)

    def count_distinct(self, *columns: Union[str, MockColumn]) -> "MockDataFrame":
        """Count distinct values in columns.

        Args:
            *columns: Columns to count distinct values for.

        Returns:
            MockDataFrame with count distinct results.
        """
        from ...functions import count_distinct

        exprs = []
        for col in columns:
            if isinstance(col, MockColumn):
                exprs.append(count_distinct(col))
            else:
                exprs.append(count_distinct(col))

        return self.agg(*exprs)

    def collect_set(self, *columns: Union[str, MockColumn]) -> "MockDataFrame":
        """Collect unique values into a set.

        Args:
            *columns: Columns to collect unique values for.

        Returns:
            MockDataFrame with collect_set results.
        """
        from ...functions import collect_set

        exprs = []
        for col in columns:
            if isinstance(col, MockColumn):
                exprs.append(collect_set(col))
            else:
                exprs.append(collect_set(col))

        return self.agg(*exprs)

    def first(self, *columns: Union[str, MockColumn]) -> "MockDataFrame":
        """Get first value in each group.

        Args:
            *columns: Columns to get first values for.

        Returns:
            MockDataFrame with first values.
        """
        from ...functions import first

        exprs = []
        for col in columns:
            if isinstance(col, MockColumn):
                exprs.append(first(col))
            else:
                exprs.append(first(col))

        return self.agg(*exprs)

    def last(self, *columns: Union[str, MockColumn]) -> "MockDataFrame":
        """Get last value in each group.

        Args:
            *columns: Columns to get last values for.

        Returns:
            MockDataFrame with last values.
        """
        from ...functions import last

        exprs = []
        for col in columns:
            if isinstance(col, MockColumn):
                exprs.append(last(col))
            else:
                exprs.append(last(col))

        return self.agg(*exprs)

    def stddev(self, *columns: Union[str, MockColumn]) -> "MockDataFrame":
        """Calculate standard deviation.

        Args:
            *columns: Columns to calculate standard deviation for.

        Returns:
            MockDataFrame with standard deviation results.
        """
        from ...functions import stddev

        exprs = []
        for col in columns:
            if isinstance(col, MockColumn):
                exprs.append(stddev(col))
            else:
                exprs.append(stddev(col))

        return self.agg(*exprs)

    def variance(self, *columns: Union[str, MockColumn]) -> "MockDataFrame":
        """Calculate variance.

        Args:
            *columns: Columns to calculate variance for.

        Returns:
            MockDataFrame with variance results.
        """
        from ...functions import variance

        exprs = []
        for col in columns:
            if isinstance(col, MockColumn):
                exprs.append(variance(col))
            else:
                exprs.append(variance(col))

        return self.agg(*exprs)

    def rollup(self, *columns: Union[str, MockColumn]) -> "MockRollupGroupedData":
        """Create rollup grouped data for hierarchical grouping.

        Args:
            *columns: Columns to rollup.

        Returns:
            MockRollupGroupedData for hierarchical grouping.
        """
        from .rollup import MockRollupGroupedData

        col_names = []
        for col in columns:
            if isinstance(col, MockColumn):
                col_names.append(col.name)
            else:
                col_names.append(col)

        # Validate that all columns exist
        for col_name in col_names:
            if col_name not in [field.name for field in self.df.schema.fields]:
                raise AnalysisException(f"Column '{col_name}' does not exist")

        return MockRollupGroupedData(self.df, col_names)

    def cube(self, *columns: Union[str, MockColumn]) -> "MockCubeGroupedData":
        """Create cube grouped data for multi-dimensional grouping.

        Args:
            *columns: Columns to cube.

        Returns:
            MockCubeGroupedData for multi-dimensional grouping.
        """
        from .cube import MockCubeGroupedData

        col_names = []
        for col in columns:
            if isinstance(col, MockColumn):
                col_names.append(col.name)
            else:
                col_names.append(col)

        # Validate that all columns exist
        for col_name in col_names:
            if col_name not in [field.name for field in self.df.schema.fields]:
                raise AnalysisException(f"Column '{col_name}' does not exist")

        return MockCubeGroupedData(self.df, col_names)

    def pivot(
        self, pivot_col: str, values: Optional[List[Any]] = None
    ) -> "MockPivotGroupedData":
        """Create pivot grouped data.

        Args:
            pivot_col: Column to pivot on.
            values: Optional list of pivot values. If None, uses all unique values.

        Returns:
            MockPivotGroupedData for pivot operations.
        """
        from .pivot import MockPivotGroupedData

        # Validate that pivot column exists
        if pivot_col not in [field.name for field in self.df.schema.fields]:
            raise AnalysisException(f"Column '{pivot_col}' does not exist")

        # If values not provided, get unique values from pivot column
        if values is None:
            values = list(
                set(
                    row.get(pivot_col)
                    for row in self.df.data
                    if row.get(pivot_col) is not None
                )
            )
            values.sort()  # Sort for consistent ordering

        return MockPivotGroupedData(self.df, self.group_columns, pivot_col, values)

    def applyInPandas(self, func: Any, schema: Any) -> "MockDataFrame":
        """Apply a Python native function to each group using pandas DataFrames.

        The function should take a pandas DataFrame and return a pandas DataFrame.
        For each group, the group data is passed as a pandas DataFrame to the function
        and the returned pandas DataFrame is used to construct the output rows.

        Args:
            func: A function that takes a pandas DataFrame and returns a pandas DataFrame.
            schema: The schema of the output DataFrame (StructType or DDL string).

        Returns:
            MockDataFrame: Result of applying the function to each group.

        Example:
            >>> def normalize(pdf):
            ...     pdf['normalized'] = (pdf['value'] - pdf['value'].mean()) / pdf['value'].std()
            ...     return pdf
            >>> df.groupBy("category").applyInPandas(normalize, schema="category string, value double, normalized double")
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "pandas is required for applyInPandas. "
                "Install it with: pip install 'mock-spark[pandas]'"
            )

        # Materialize DataFrame if lazy
        if self.df._operations_queue:
            df = self.df._materialize_if_lazy()
        else:
            df = self.df

        # Group data by group columns
        groups: Dict[Any, List[Dict[str, Any]]] = {}
        for row in df.data:
            group_key = tuple(row.get(col) for col in self.group_columns)
            if group_key not in groups:
                groups[group_key] = []
            groups[group_key].append(row)

        # Apply function to each group
        result_pdfs = []
        for group_rows in groups.values():
            # Convert group to pandas DataFrame
            group_pdf = pd.DataFrame(group_rows)

            # Apply function
            result_pdf = func(group_pdf)

            if not isinstance(result_pdf, pd.DataFrame):
                raise TypeError(
                    f"Function must return a pandas DataFrame, got {type(result_pdf).__name__}"
                )

            result_pdfs.append(result_pdf)

        # Concatenate all results
        result_data: List[Dict[str, Any]] = []
        if result_pdfs:
            combined_pdf = pd.concat(result_pdfs, ignore_index=True)
            # Convert to records and ensure string keys
            result_data = [
                {str(k): v for k, v in row.items()}
                for row in combined_pdf.to_dict("records")
            ]

        # Parse schema
        from ...spark_types import MockStructType
        from ...core.schema_inference import infer_schema_from_data

        result_schema: MockStructType
        if isinstance(schema, str):
            # For DDL string, use schema inference from result data
            # (DDL parsing is complex, so we rely on inference for now)
            result_schema = (
                infer_schema_from_data(result_data) if result_data else self.df.schema
            )
        elif isinstance(schema, MockStructType):
            result_schema = schema
        else:
            # Try to infer schema from result data
            result_schema = (
                infer_schema_from_data(result_data) if result_data else self.df.schema
            )

        from ..dataframe import MockDataFrame as MDF

        storage: Any = getattr(self.df, "storage", None)
        return MDF(result_data, result_schema, storage)

    def transform(self, func: Any) -> "MockDataFrame":
        """Apply a function to each group and return a DataFrame with the same schema.

        This is similar to applyInPandas but preserves the original schema.
        The function should take a pandas DataFrame and return a pandas DataFrame
        with the same columns (though it may add computed columns).

        Args:
            func: A function that takes a pandas DataFrame and returns a pandas DataFrame.

        Returns:
            MockDataFrame: Result of applying the function to each group.

        Example:
            >>> def add_group_stats(pdf):
            ...     pdf['group_mean'] = pdf['value'].mean()
            ...     pdf['group_std'] = pdf['value'].std()
            ...     return pdf
            >>> df.groupBy("category").transform(add_group_stats)
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "pandas is required for transform. "
                "Install it with: pip install 'mock-spark[pandas]'"
            )

        # Materialize DataFrame if lazy
        if self.df._operations_queue:
            df = self.df._materialize_if_lazy()
        else:
            df = self.df

        # Group data by group columns
        groups: Dict[Any, List[Dict[str, Any]]] = {}
        group_indices: Dict[Any, List[int]] = {}  # Track original indices

        for idx, row in enumerate(df.data):
            group_key = tuple(row.get(col) for col in self.group_columns)
            if group_key not in groups:
                groups[group_key] = []
                group_indices[group_key] = []
            groups[group_key].append(row)
            group_indices[group_key].append(idx)

        # Apply function to each group and preserve order
        result_rows: List[Dict[str, Any]] = [{}] * len(df.data)

        for group_key, group_rows in groups.items():
            # Convert group to pandas DataFrame
            group_pdf = pd.DataFrame(group_rows)

            # Apply function
            transformed_pdf = func(group_pdf)

            if not isinstance(transformed_pdf, pd.DataFrame):
                raise TypeError(
                    f"Function must return a pandas DataFrame, got {type(transformed_pdf).__name__}"
                )

            # Put transformed rows back in their original positions
            transformed_rows = transformed_pdf.to_dict("records")
            for idx, transformed_row in zip(group_indices[group_key], transformed_rows):
                # Convert hashable keys to strings for type safety
                result_rows[idx] = {str(k): v for k, v in transformed_row.items()}

        # Use the same schema as the original DataFrame
        # (or extend it if new columns were added)
        from ...core.schema_inference import infer_schema_from_data

        result_schema = (
            infer_schema_from_data(result_rows) if result_rows else df.schema
        )

        from ..dataframe import MockDataFrame as MDF

        storage: Any = getattr(self.df, "storage", None)
        return MDF(result_rows, result_schema, storage)
