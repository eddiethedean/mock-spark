"""
Datetime functions for Mock Spark.

This module provides comprehensive datetime functions that match PySpark's
datetime function API. Includes date/time conversion, extraction, and manipulation
operations for temporal data processing in DataFrames.

Key Features:
    - Complete PySpark datetime function API compatibility
    - Current date/time functions (current_timestamp, current_date)
    - Date conversion (to_date, to_timestamp)
    - Date extraction (year, month, day, hour, minute, second)
    - Date manipulation (dayofweek, dayofyear, weekofyear, quarter)
    - Type-safe operations with proper return types
    - Support for various date formats and time zones
    - Proper handling of date parsing and validation

Example:
    >>> from mock_spark import MockSparkSession, F
    >>> spark = MockSparkSession("test")
    >>> data = [{"timestamp": "2024-01-15 10:30:00", "date_str": "2024-01-15"}]
    >>> df = spark.createDataFrame(data)
    >>> df.select(
    ...     F.year(F.col("timestamp")),
    ...     F.month(F.col("timestamp")),
    ...     F.to_date(F.col("date_str"))
    ... ).show()
    +--- MockDataFrame: 1 rows ---+
    year(timestamp) | month(timestamp) | to_date(date_str)
    ------------------------------------------------------
    2024-01-15 10:30:00 | 2024-01-15 10:30:00 |   2024-01-15
"""

from typing import Union, Optional
from mock_spark.functions.base import MockColumn, MockColumnOperation


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
        operation = MockColumnOperation(
            dummy_column, "current_timestamp", name="current_timestamp()"
        )
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
        operation = MockColumnOperation(dummy_column, "current_date", name="current_date()")
        return operation

    @staticmethod
    def to_date(
        column: Union[MockColumn, str], format: Optional[str] = None
    ) -> MockColumnOperation:
        """Convert string to date.

        Args:
            column: The column to convert.
            format: Optional date format string.

        Returns:
            MockColumnOperation representing the to_date function.
        """
        if isinstance(column, str):
            column = MockColumn(column)

        name = (
            f"to_date({column.name}, '{format}')"
            if format is not None
            else f"to_date({column.name})"
        )
        operation = MockColumnOperation(column, "to_date", format, name=name)
        return operation

    @staticmethod
    def to_timestamp(
        column: Union[MockColumn, str], format: Optional[str] = None
    ) -> MockColumnOperation:
        """Convert string to timestamp.

        Args:
            column: The column to convert.
            format: Optional timestamp format string.

        Returns:
            MockColumnOperation representing the to_timestamp function.
        """
        if isinstance(column, str):
            column = MockColumn(column)

        name = (
            f"to_timestamp({column.name}, '{format}')"
            if format is not None
            else f"to_timestamp({column.name})"
        )
        operation = MockColumnOperation(column, "to_timestamp", format, name=name)
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

        operation = MockColumnOperation(column, "hour", name=f"hour({column.name})")
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

        operation = MockColumnOperation(column, "day", name=f"day({column.name})")
        name = f"day({column.name})"
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

        operation = MockColumnOperation(column, "month", name=f"month({column.name})")
        name = f"month({column.name})"
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

        operation = MockColumnOperation(column, "year", name=f"year({column.name})")
        name = f"year({column.name})"
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

        operation = MockColumnOperation(column, "dayofweek", name=f"dayofweek({column.name})")
        name = f"dayofweek({column.name})"
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

        operation = MockColumnOperation(column, "dayofyear", name=f"dayofyear({column.name})")
        name = f"dayofyear({column.name})"
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

        operation = MockColumnOperation(column, "weekofyear", name=f"weekofyear({column.name})")
        name = f"weekofyear({column.name})"
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

        operation = MockColumnOperation(column, "quarter", name=f"quarter({column.name})")
        name = f"quarter({column.name})"
        return operation

    @staticmethod
    def minute(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Extract minute from timestamp.

        Args:
            column: The column to extract minute from.

        Returns:
            MockColumnOperation representing the minute function.
        """
        if isinstance(column, str):
            column = MockColumn(column)

        operation = MockColumnOperation(column, "minute", name=f"minute({column.name})")
        return operation

    @staticmethod
    def second(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Extract second from timestamp.

        Args:
            column: The column to extract second from.

        Returns:
            MockColumnOperation representing the second function.
        """
        if isinstance(column, str):
            column = MockColumn(column)

        operation = MockColumnOperation(column, "second", name=f"second({column.name})")
        return operation

    @staticmethod
    def add_months(column: Union[MockColumn, str], num_months: int) -> MockColumnOperation:
        """Add months to date/timestamp.

        Args:
            column: The column to add months to.
            num_months: Number of months to add.

        Returns:
            MockColumnOperation representing the add_months function.
        """
        if isinstance(column, str):
            column = MockColumn(column)

        operation = MockColumnOperation(
            column,
            "add_months",
            num_months,
            name=f"add_months({column.name}, {num_months})",
        )
        return operation

    @staticmethod
    def months_between(
        column1: Union[MockColumn, str], column2: Union[MockColumn, str]
    ) -> MockColumnOperation:
        """Calculate months between two dates.

        Args:
            column1: The first date column.
            column2: The second date column.

        Returns:
            MockColumnOperation representing the months_between function.
        """
        if isinstance(column1, str):
            column1 = MockColumn(column1)
        if isinstance(column2, str):
            column2 = MockColumn(column2)

        operation = MockColumnOperation(
            column1,
            "months_between",
            column2,
            name=f"months_between({column1.name}, {column2.name})",
        )
        return operation

    @staticmethod
    def date_add(column: Union[MockColumn, str], days: int) -> MockColumnOperation:
        """Add days to date.

        Args:
            column: The column to add days to.
            days: Number of days to add.

        Returns:
            MockColumnOperation representing the date_add function.
        """
        if isinstance(column, str):
            column = MockColumn(column)

        operation = MockColumnOperation(
            column, "date_add", days, name=f"date_add({column.name}, {days})"
        )
        return operation

    @staticmethod
    def date_sub(column: Union[MockColumn, str], days: int) -> MockColumnOperation:
        """Subtract days from date.

        Args:
            column: The column to subtract days from.
            days: Number of days to subtract.

        Returns:
            MockColumnOperation representing the date_sub function.
        """
        if isinstance(column, str):
            column = MockColumn(column)

        operation = MockColumnOperation(
            column, "date_sub", days, name=f"date_sub({column.name}, {days})"
        )
        return operation
