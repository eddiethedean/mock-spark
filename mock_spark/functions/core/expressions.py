"""
Expression functions for Mock Spark.

This module provides the F namespace functions and expression utilities
for creating column expressions and transformations.
"""

from typing import Any, Union, TYPE_CHECKING
from .column import Column, ColumnOperation
from .literals import Literal

if TYPE_CHECKING:
    from ..conditional import CaseWhen


class ExpressionFunctions:
    """Expression functions for creating column expressions."""

    @staticmethod
    def col(name: str) -> Column:
        """Create a column reference.

        Delegates to canonical Column constructor.

        Args:
            name: Column name.

        Returns:
            Column instance.
        """
        return Column(name)

    @staticmethod
    def lit(value: Any) -> Literal:
        """Create a literal value.

        Delegates to canonical Literal constructor.

        Args:
            value: Literal value.

        Returns:
            Literal instance.
        """
        return Literal(value)

    @staticmethod
    def when(condition: ColumnOperation, value: Any) -> "CaseWhen":
        """Start a CASE WHEN expression.

        Delegates to canonical CaseWhen constructor.

        Args:
            condition: Condition to evaluate.
            value: Value if condition is true.

        Returns:
            CaseWhen instance.
        """
        from ..conditional import CaseWhen

        return CaseWhen(None, condition, value)

    @staticmethod
    def coalesce(
        *columns: Union[Column, ColumnOperation, str],
    ) -> ColumnOperation:
        """Return the first non-null value from a list of columns.

        Args:
            *columns: Columns to check for non-null values.

        Returns:
            ColumnOperation for coalesce.
        """
        col_refs: list[Union[Column, ColumnOperation]] = []
        for col in columns:
            if isinstance(col, str):
                col_refs.append(Column(col))
            else:
                col_refs.append(col)

        return ColumnOperation(None, "coalesce", col_refs)

    @staticmethod
    def isnull(column: Union[Column, str]) -> ColumnOperation:
        """Check if column value is null.

        Args:
            column: Column to check.

        Returns:
            ColumnOperation for isnull.
        """
        if isinstance(column, str):
            column = Column(column)
        return column.isnull()

    @staticmethod
    def isnotnull(column: Union[Column, str]) -> ColumnOperation:
        """Check if column value is not null.

        Args:
            column: Column to check.

        Returns:
            ColumnOperation for isnotnull.
        """
        if isinstance(column, str):
            column = Column(column)
        return column.isnotnull()

    @staticmethod
    def isnan(column: Union[Column, str]) -> ColumnOperation:
        """Check if column value is NaN.

        Args:
            column: Column to check.

        Returns:
            ColumnOperation for isnan.
        """
        if isinstance(column, str):
            column = Column(column)
        return ColumnOperation(column, "isnan", None)

    @staticmethod
    def isnotnan(column: Union[Column, str]) -> ColumnOperation:
        """Check if column value is not NaN.

        Args:
            column: Column to check.

        Returns:
            ColumnOperation for isnotnan.
        """
        if isinstance(column, str):
            column = Column(column)
        return ColumnOperation(column, "isnotnan", None)

    @staticmethod
    def expr(expr: str) -> ColumnOperation:
        """Create a column expression from SQL string.

        Args:
            expr: SQL expression string.

        Returns:
            ColumnOperation for the expression.
        """
        return ColumnOperation(None, "expr", expr)

    @staticmethod
    def array(*columns: Union[Column, str]) -> ColumnOperation:
        """Create an array from columns.

        Args:
            *columns: Columns to include in array.

        Returns:
            ColumnOperation for array.
        """
        col_refs = []
        for col in columns:
            if isinstance(col, str):
                col_refs.append(Column(col))
            else:
                col_refs.append(col)

        return ColumnOperation(None, "array", col_refs)

    @staticmethod
    def struct(*columns: Union[Column, str]) -> ColumnOperation:
        """Create a struct from columns.

        Args:
            *columns: Columns to include in struct.

        Returns:
            ColumnOperation for struct.
        """
        col_refs = []
        for col in columns:
            if isinstance(col, str):
                col_refs.append(Column(col))
            else:
                col_refs.append(col)

        return ColumnOperation(None, "struct", col_refs)

    @staticmethod
    def greatest(*columns: Union[Column, str]) -> ColumnOperation:
        """Return the greatest value among columns.

        Args:
            *columns: Columns to compare.

        Returns:
            ColumnOperation for greatest.
        """
        col_refs = []
        for col in columns:
            if isinstance(col, str):
                col_refs.append(Column(col))
            else:
                col_refs.append(col)

        return ColumnOperation(None, "greatest", col_refs)

    @staticmethod
    def least(*columns: Union[Column, str]) -> ColumnOperation:
        """Return the least value among columns.

        Args:
            *columns: Columns to compare.

        Returns:
            ColumnOperation for least.
        """
        col_refs = []
        for col in columns:
            if isinstance(col, str):
                col_refs.append(Column(col))
            else:
                col_refs.append(col)

        return ColumnOperation(None, "least", col_refs)

    @staticmethod
    def when_otherwise(
        condition: ColumnOperation, value: Any, otherwise: Any
    ) -> "CaseWhen":
        """Create a complete CASE WHEN expression.

        Args:
            condition: Condition to evaluate.
            value: Value if condition is true.
            otherwise: Default value.

        Returns:
            CaseWhen instance.
        """
        from ..conditional import CaseWhen

        case_when = CaseWhen(None, condition, value)
        return case_when.otherwise(otherwise)
