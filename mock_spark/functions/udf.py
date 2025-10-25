"""
User-Defined Function (UDF) implementation for Mock Spark.

This module provides the UserDefinedFunction class for wrapping Python
functions to use in DataFrame transformations.
"""

from typing import Any, Callable, Union, Optional
from mock_spark.functions.core.column import MockColumn
from mock_spark.functions.core.operations import MockColumnOperation


class UserDefinedFunction:
    """User-defined function wrapper (all PySpark versions).

    Wraps a Python function to be used in DataFrame transformations.
    Supports marking as nondeterministic and applying to columns.

    Example:
        >>> def upper_case(s):
        ...     return s.upper()
        >>> udf_func = UserDefinedFunction(upper_case, StringType())
        >>> df.select(udf_func("name").alias("upper_name"))
    """

    def __init__(
        self,
        func: Callable[..., Any],
        returnType: Any,
        name: Optional[str] = None,
        evalType: str = "SQL",
    ):
        """Initialize UserDefinedFunction.

        Args:
            func: Python function to wrap
            returnType: Return data type
            name: Optional function name
            evalType: Evaluation type ("SQL" or "PANDAS")
        """
        self.func = func
        self.returnType = returnType
        self.evalType = evalType
        self._name = name
        self._deterministic = True
        self._is_pandas_udf = evalType == "PANDAS"

    def asNondeterministic(self) -> "UserDefinedFunction":
        """Mark UDF as nondeterministic.

        Nondeterministic UDFs may return different results for the same input.
        This affects query optimization and caching.

        Returns:
            Self with nondeterministic flag set
        """
        self._deterministic = False
        return self

    def __call__(self, *cols: Union[str, MockColumn]) -> MockColumnOperation:
        """Apply UDF to columns.

        Args:
            *cols: Column names or Column objects

        Returns:
            MockColumnOperation representing the UDF application
        """
        # Convert string column names to MockColumn objects
        column_objs = []
        for col in cols:
            if isinstance(col, str):
                column_objs.append(MockColumn(col))
            else:
                column_objs.append(col)

        # Create the first column operation
        if not column_objs:
            raise ValueError("UDF requires at least one column argument")

        first_col = column_objs[0]
        # Get column name safely
        col_name = getattr(first_col, "name", str(first_col))
        op = MockColumnOperation(
            first_col, "udf", name=self._name or f"udf({col_name})"
        )
        op._udf_func = self.func  # type: ignore
        op._udf_return_type = self.returnType  # type: ignore
        op._udf_cols = column_objs  # type: ignore
        op._is_pandas_udf = self._is_pandas_udf  # type: ignore

        return op
