"""
Datetime functions for Mock Spark.

This module contains datetime functions including current_timestamp, to_date, etc.
"""

from typing import Any, Union, Optional
from mock_spark.functions.base import MockColumn, MockColumnOperation
from mock_spark.spark_types import MockDataType, StringType, DateType, TimestampType


class DateTimeFunctions:
    """Collection of datetime functions."""

    @staticmethod
    def current_timestamp() -> MockColumnOperation:
        """Get current timestamp.

        Returns:
            MockColumnOperation representing the current_timestamp function.
        """
        # Create a special column for functions without input
        from mock_spark.functions.base import MockColumn
        dummy_column = MockColumn("__current_timestamp__")
        operation = MockColumnOperation(dummy_column, "current_timestamp")
        operation.name = "current_timestamp()"
        return operation

    @staticmethod
    def current_date() -> MockColumnOperation:
        """Get current date.

        Returns:
            MockColumnOperation representing the current_date function.
        """
        # Create a special column for functions without input
        from mock_spark.functions.base import MockColumn
        dummy_column = MockColumn("__current_date__")
        operation = MockColumnOperation(dummy_column, "current_date")
        operation.name = "current_date()"
        return operation

    @staticmethod
    def to_date(column: Union[MockColumn, str], format: Optional[str] = None) -> MockColumnOperation:
        """Convert string to date.

        Args:
            column: The column to convert.
            format: Optional date format string.

        Returns:
            MockColumnOperation representing the to_date function.
        """
        if isinstance(column, str):
            column = MockColumn(column)
        
        operation = MockColumnOperation(column, "to_date", format)
        if format is not None:
            operation.name = f"to_date({column.name}, '{format}')"
        else:
            operation.name = f"to_date({column.name})"
        return operation

    @staticmethod
    def to_timestamp(column: Union[MockColumn, str], format: Optional[str] = None) -> MockColumnOperation:
        """Convert string to timestamp.

        Args:
            column: The column to convert.
            format: Optional timestamp format string.

        Returns:
            MockColumnOperation representing the to_timestamp function.
        """
        if isinstance(column, str):
            column = MockColumn(column)
        
        operation = MockColumnOperation(column, "to_timestamp", format)
        if format is not None:
            operation.name = f"to_timestamp({column.name}, '{format}')"
        else:
            operation.name = f"to_timestamp({column.name})"
        return operation

    @staticmethod
    def hour(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Extract hour from timestamp.

        Args:
            column: The column to extract hour from.

        Returns:
            MockColumnOperation representing the hour function.
        """
        if isinstance(column, str):
            column = MockColumn(column)
        
        operation = MockColumnOperation(column, "hour")
        operation.name = f"hour({column.name})"
        return operation

    @staticmethod
    def day(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Extract day from date/timestamp.

        Args:
            column: The column to extract day from.

        Returns:
            MockColumnOperation representing the day function.
        """
        if isinstance(column, str):
            column = MockColumn(column)
        
        operation = MockColumnOperation(column, "day")
        operation.name = f"day({column.name})"
        return operation

    @staticmethod
    def month(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Extract month from date/timestamp.

        Args:
            column: The column to extract month from.

        Returns:
            MockColumnOperation representing the month function.
        """
        if isinstance(column, str):
            column = MockColumn(column)
        
        operation = MockColumnOperation(column, "month")
        operation.name = f"month({column.name})"
        return operation

    @staticmethod
    def year(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Extract year from date/timestamp.

        Args:
            column: The column to extract year from.

        Returns:
            MockColumnOperation representing the year function.
        """
        if isinstance(column, str):
            column = MockColumn(column)
        
        operation = MockColumnOperation(column, "year")
        operation.name = f"year({column.name})"
        return operation

    @staticmethod
    def dayofweek(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Extract day of week from date/timestamp.

        Args:
            column: The column to extract day of week from.

        Returns:
            MockColumnOperation representing the dayofweek function.
        """
        if isinstance(column, str):
            column = MockColumn(column)
        
        operation = MockColumnOperation(column, "dayofweek")
        operation.name = f"dayofweek({column.name})"
        return operation

    @staticmethod
    def dayofyear(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Extract day of year from date/timestamp.

        Args:
            column: The column to extract day of year from.

        Returns:
            MockColumnOperation representing the dayofyear function.
        """
        if isinstance(column, str):
            column = MockColumn(column)
        
        operation = MockColumnOperation(column, "dayofyear")
        operation.name = f"dayofyear({column.name})"
        return operation

    @staticmethod
    def weekofyear(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Extract week of year from date/timestamp.

        Args:
            column: The column to extract week of year from.

        Returns:
            MockColumnOperation representing the weekofyear function.
        """
        if isinstance(column, str):
            column = MockColumn(column)
        
        operation = MockColumnOperation(column, "weekofyear")
        operation.name = f"weekofyear({column.name})"
        return operation

    @staticmethod
    def quarter(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Extract quarter from date/timestamp.

        Args:
            column: The column to extract quarter from.

        Returns:
            MockColumnOperation representing the quarter function.
        """
        if isinstance(column, str):
            column = MockColumn(column)
        
        operation = MockColumnOperation(column, "quarter")
        operation.name = f"quarter({column.name})"
        return operation
