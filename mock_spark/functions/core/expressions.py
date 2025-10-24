"""
Expression functions for Mock Spark.

This module provides the F namespace functions and expression utilities
for creating column expressions and transformations.
"""

from typing import Any, Union, List, TYPE_CHECKING
from .column import MockColumn, MockColumnOperation
from .literals import MockLiteral

if TYPE_CHECKING:
    from ..conditional import MockCaseWhen


class ExpressionFunctions:
    """Expression functions for creating column expressions."""

    @staticmethod
    def col(name: str) -> MockColumn:
        """Create a column reference.

        Delegates to canonical MockColumn constructor.

        Args:
            name: Column name.

        Returns:
            MockColumn instance.
        """
        return MockColumn(name)

    @staticmethod
    def lit(value: Any) -> MockLiteral:
        """Create a literal value.

        Delegates to canonical MockLiteral constructor.

        Args:
            value: Literal value.

        Returns:
            MockLiteral instance.
        """
        return MockLiteral(value)

    @staticmethod
    def when(condition: MockColumnOperation, value: Any) -> "MockCaseWhen":
        """Start a CASE WHEN expression.

        Delegates to canonical MockCaseWhen constructor.

        Args:
            condition: Condition to evaluate.
            value: Value if condition is true.

        Returns:
            MockCaseWhen instance.
        """
        from ..conditional import MockCaseWhen

        return MockCaseWhen(None, condition, value)

    @staticmethod
    def coalesce(
        *columns: Union[MockColumn, MockColumnOperation, str],
    ) -> MockColumnOperation:
        """Return the first non-null value from a list of columns.

        Args:
            *columns: Columns to check for non-null values.

        Returns:
            MockColumnOperation for coalesce.
        """
        col_refs: List[Union[MockColumn, MockColumnOperation]] = []
        for col in columns:
            if isinstance(col, str):
                col_refs.append(MockColumn(col))
            else:
                col_refs.append(col)

        return MockColumnOperation(None, "coalesce", col_refs)

    @staticmethod
    def isnull(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Check if column value is null.

        Args:
            column: Column to check.

        Returns:
            MockColumnOperation for isnull.
        """
        if isinstance(column, str):
            column = MockColumn(column)
        return column.isnull()

    @staticmethod
    def isnotnull(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Check if column value is not null.

        Args:
            column: Column to check.

        Returns:
            MockColumnOperation for isnotnull.
        """
        if isinstance(column, str):
            column = MockColumn(column)
        return column.isnotnull()

    @staticmethod
    def isnan(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Check if column value is NaN.

        Args:
            column: Column to check.

        Returns:
            MockColumnOperation for isnan.
        """
        if isinstance(column, str):
            column = MockColumn(column)
        return MockColumnOperation(column, "isnan", None)

    @staticmethod
    def isnotnan(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Check if column value is not NaN.

        Args:
            column: Column to check.

        Returns:
            MockColumnOperation for isnotnan.
        """
        if isinstance(column, str):
            column = MockColumn(column)
        return MockColumnOperation(column, "isnotnan", None)

    @staticmethod
    def expr(expr: str) -> MockColumnOperation:
        """Create a column expression from SQL string.

        Args:
            expr: SQL expression string.

        Returns:
            MockColumnOperation for the expression.
        """
        return MockColumnOperation(None, "expr", expr)

    @staticmethod
    def array(*columns: Union[MockColumn, str]) -> MockColumnOperation:
        """Create an array from columns.

        Args:
            *columns: Columns to include in array.

        Returns:
            MockColumnOperation for array.
        """
        col_refs = []
        for col in columns:
            if isinstance(col, str):
                col_refs.append(MockColumn(col))
            else:
                col_refs.append(col)

        return MockColumnOperation(None, "array", col_refs)

    @staticmethod
    def struct(*columns: Union[MockColumn, str]) -> MockColumnOperation:
        """Create a struct from columns.

        Args:
            *columns: Columns to include in struct.

        Returns:
            MockColumnOperation for struct.
        """
        col_refs = []
        for col in columns:
            if isinstance(col, str):
                col_refs.append(MockColumn(col))
            else:
                col_refs.append(col)

        return MockColumnOperation(None, "struct", col_refs)

    @staticmethod
    def greatest(*columns: Union[MockColumn, str]) -> MockColumnOperation:
        """Return the greatest value among columns.

        Args:
            *columns: Columns to compare.

        Returns:
            MockColumnOperation for greatest.
        """
        col_refs = []
        for col in columns:
            if isinstance(col, str):
                col_refs.append(MockColumn(col))
            else:
                col_refs.append(col)

        return MockColumnOperation(None, "greatest", col_refs)

    @staticmethod
    def least(*columns: Union[MockColumn, str]) -> MockColumnOperation:
        """Return the least value among columns.

        Args:
            *columns: Columns to compare.

        Returns:
            MockColumnOperation for least.
        """
        col_refs = []
        for col in columns:
            if isinstance(col, str):
                col_refs.append(MockColumn(col))
            else:
                col_refs.append(col)

        return MockColumnOperation(None, "least", col_refs)

    @staticmethod
    def when_otherwise(
        condition: MockColumnOperation, value: Any, otherwise: Any
    ) -> "MockCaseWhen":
        """Create a complete CASE WHEN expression.

        Args:
            condition: Condition to evaluate.
            value: Value if condition is true.
            otherwise: Default value.

        Returns:
            MockCaseWhen instance.
        """
        from ..conditional import MockCaseWhen

        case_when = MockCaseWhen(None, condition, value)
        return case_when.otherwise(otherwise)
