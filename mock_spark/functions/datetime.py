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
from mock_spark.functions.core.literals import MockLiteral


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
        return operation

    @staticmethod
    def dayofmonth(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Extract day of month from date/timestamp (alias for day).

        Args:
            column: The column to extract day from.

        Returns:
            MockColumnOperation representing the dayofmonth function.
        """
        if isinstance(column, str):
            column = MockColumn(column)

        operation = MockColumnOperation(column, "day", name=f"dayofmonth({column.name})")
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

    @staticmethod
    def date_format(column: Union[MockColumn, str], format: str) -> MockColumnOperation:
        """Format date/timestamp as string.

        Args:
            column: The column to format.
            format: Date format string (e.g., 'yyyy-MM-dd').

        Returns:
            MockColumnOperation representing the date_format function.
        """
        if isinstance(column, str):
            column = MockColumn(column)

        operation = MockColumnOperation(
            column, "date_format", format, name=f"date_format({column.name}, '{format}')"
        )
        return operation

    @staticmethod
    def from_unixtime(
        column: Union[MockColumn, str], format: str = "yyyy-MM-dd HH:mm:ss"
    ) -> MockColumnOperation:
        """Convert unix timestamp to string.

        Args:
            column: The column with unix timestamp.
            format: Date format string (default: 'yyyy-MM-dd HH:mm:ss').

        Returns:
            MockColumnOperation representing the from_unixtime function.
        """
        if isinstance(column, str):
            column = MockColumn(column)

        operation = MockColumnOperation(
            column,
            "from_unixtime",
            format,
            name=f"from_unixtime({column.name}, '{format}')",
        )
        return operation

    @staticmethod
    def timestampadd(
        unit: str, quantity: Union[int, MockColumn], timestamp: Union[str, MockColumn]
    ) -> MockColumnOperation:
        """Add time units to a timestamp.

        Args:
            unit: Time unit (YEAR, QUARTER, MONTH, WEEK, DAY, HOUR, MINUTE, SECOND).
            quantity: Number of units to add (can be column or integer).
            timestamp: Timestamp column or literal.

        Returns:
            MockColumnOperation representing the timestampadd function.

        Example:
            >>> df.select(F.timestampadd("DAY", 7, F.col("created_at")))
            >>> df.select(F.timestampadd("HOUR", F.col("offset"), "2024-01-01"))
        """
        if isinstance(timestamp, str):
            timestamp = MockColumn(timestamp)

        # Handle quantity as column or literal
        if isinstance(quantity, MockColumn):
            quantity_str = quantity.name
        else:
            quantity_str = str(quantity)

        operation = MockColumnOperation(
            timestamp,
            "timestampadd",
            (unit, quantity),
            name=f"timestampadd('{unit}', {quantity_str}, {timestamp.name})",
        )
        return operation

    @staticmethod
    def timestampdiff(
        unit: str, start: Union[str, MockColumn], end: Union[str, MockColumn]
    ) -> MockColumnOperation:
        """Calculate difference between two timestamps.

        Args:
            unit: Time unit (YEAR, QUARTER, MONTH, WEEK, DAY, HOUR, MINUTE, SECOND).
            start: Start timestamp column or literal.
            end: End timestamp column or literal.

        Returns:
            MockColumnOperation representing the timestampdiff function.

        Example:
            >>> df.select(F.timestampdiff("DAY", F.col("start_date"), F.col("end_date")))
            >>> df.select(F.timestampdiff("HOUR", "2024-01-01", F.col("end_time")))
        """
        if isinstance(start, str):
            start = MockColumn(start)
        if isinstance(end, str):
            end = MockColumn(end)

        operation = MockColumnOperation(
            start,
            "timestampdiff",
            (unit, end),
            name=f"timestampdiff('{unit}', {start.name}, {end.name})",
        )
        return operation

    # Timezone Functions (PySpark 3.2+)

    @staticmethod
    def convert_timezone(
        sourceTz: str, targetTz: str, sourceTs: Union[MockColumn, str]
    ) -> MockColumnOperation:
        """Convert timestamp from source to target timezone."""
        if isinstance(sourceTs, str):
            sourceTs = MockColumn(sourceTs)

        return MockColumnOperation(
            sourceTs,
            "convert_timezone",
            (sourceTz, targetTz, sourceTs),
            name=f"convert_timezone('{sourceTz}', '{targetTz}', {sourceTs.name})",
        )

    @staticmethod
    def current_timezone() -> MockColumnOperation:
        """Get current timezone."""
        # Create a literal for functions without column input
        from mock_spark.functions.core.literals import MockLiteral
        dummy = MockLiteral(1)  # Use literal 1 as dummy input
        return MockColumnOperation(
            dummy,
            "current_timezone",
            name="current_timezone()",
        )

    @staticmethod
    def from_utc_timestamp(
        ts: Union[MockColumn, str], tz: str
    ) -> MockColumnOperation:
        """Convert UTC timestamp to given timezone."""
        if isinstance(ts, str):
            ts = MockColumn(ts)

        return MockColumnOperation(
            ts,
            "from_utc_timestamp",
            tz,
            name=f"from_utc_timestamp({ts.name}, '{tz}')",
        )

    @staticmethod
    def to_utc_timestamp(
        ts: Union[MockColumn, str], tz: str
    ) -> MockColumnOperation:
        """Convert timestamp from given timezone to UTC."""
        if isinstance(ts, str):
            ts = MockColumn(ts)

        return MockColumnOperation(
            ts,
            "to_utc_timestamp",
            tz,
            name=f"to_utc_timestamp({ts.name}, '{tz}')",
        )

    # Date/Time Part Functions (PySpark 3.2+)

    @staticmethod
    def date_part(field: str, source: Union[MockColumn, str]) -> MockColumnOperation:
        """Extract a field from a date/timestamp.

        Args:
            field: Field to extract (YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, etc.).
            source: Date/timestamp column.

        Returns:
            MockColumnOperation representing the date_part function.

        Example:
            >>> df.select(F.date_part("YEAR", F.col("date")))
        """
        if isinstance(source, str):
            source = MockColumn(source)

        return MockColumnOperation(
            source,
            "date_part",
            field,
            name=f"date_part('{field}', {source.name})",
        )

    @staticmethod
    def dayname(date: Union[MockColumn, str]) -> MockColumnOperation:
        """Get the name of the day of the week.

        Args:
            date: Date column.

        Returns:
            MockColumnOperation representing the dayname function.

        Example:
            >>> df.select(F.dayname(F.col("date")))
        """
        if isinstance(date, str):
            date = MockColumn(date)

        return MockColumnOperation(date, "dayname", name=f"dayname({date.name})")

    @staticmethod
    def make_date(
        year: Union[MockColumn, int],
        month: Union[MockColumn, int],
        day: Union[MockColumn, int]
    ) -> MockColumnOperation:
        """Construct a date from year, month, day integers (PySpark 3.0+).

        Args:
            year: Year column or integer
            month: Month column or integer (1-12)
            day: Day column or integer (1-31)

        Returns:
            MockColumnOperation representing the make_date function

        Example:
            >>> df.select(F.make_date(F.lit(2024), F.lit(3), F.lit(15)))
        """
        year_col: Union[MockColumn, MockLiteral]
        if isinstance(year, int):
            year_col = MockLiteral(year)
        elif isinstance(year, str):
            year_col = MockColumn(year)
        else:
            year_col = year  # type: ignore[assignment]

        return MockColumnOperation(
            year_col,  # type: ignore[arg-type]
            "make_date",
            value=(month, day),
            name=f"make_date({year_col.name if hasattr(year_col, 'name') else year_col})"
        )

    @staticmethod
    def date_trunc(format: str, timestamp: Union[MockColumn, str]) -> MockColumnOperation:
        """Truncate timestamp to specified unit (year, month, day, hour, etc.).

        Args:
            format: Truncation unit ('year', 'month', 'day', 'hour', 'minute', 'second')
            timestamp: Timestamp column to truncate

        Returns:
            MockColumnOperation representing the date_trunc function

        Example:
            >>> df.select(F.date_trunc('month', F.col('timestamp')))
        """
        if isinstance(timestamp, str):
            timestamp = MockColumn(timestamp)

        return MockColumnOperation(
            timestamp,
            "date_trunc",
            value=format,
            name=f"date_trunc({format}, {timestamp.name})"
        )

    @staticmethod
    def datediff(end: Union[MockColumn, str], start: Union[MockColumn, str]) -> MockColumnOperation:
        """Returns number of days between two dates.

        Args:
            end: End date column
            start: Start date column

        Returns:
            MockColumnOperation representing the datediff function

        Example:
            >>> df.select(F.datediff(F.col('end_date'), F.col('start_date')))
        """
        if isinstance(end, str):
            end = MockColumn(end)
        if isinstance(start, str):
            start = MockColumn(start)

        return MockColumnOperation(
            end,
            "datediff",
            value=start,
            name=f"datediff({end.name}, {start.name})"
        )

    @staticmethod
    def unix_timestamp(
        timestamp: Optional[Union[MockColumn, str]] = None,
        format: str = 'yyyy-MM-dd HH:mm:ss'
    ) -> MockColumnOperation:
        """Convert timestamp string to Unix timestamp (seconds since epoch).

        Args:
            timestamp: Timestamp column (optional, defaults to current timestamp)
            format: Date/time format string

        Returns:
            MockColumnOperation representing the unix_timestamp function

        Example:
            >>> df.select(F.unix_timestamp(F.col('timestamp'), 'yyyy-MM-dd'))
        """
        if timestamp is None:
            from mock_spark.functions.core.literals import MockLiteral
            timestamp = MockLiteral("current_timestamp")  # type: ignore[assignment]
        elif isinstance(timestamp, str):
            timestamp = MockColumn(timestamp)

        return MockColumnOperation(
            timestamp,  # type: ignore[arg-type]
            "unix_timestamp",
            value=format,
            name=f"unix_timestamp({timestamp.name if hasattr(timestamp, 'name') else 'current_timestamp'}, {format})"  # type: ignore[union-attr]
        )

    @staticmethod
    def last_day(date: Union[MockColumn, str]) -> MockColumnOperation:
        """Returns the last day of the month for a given date.

        Args:
            date: Date column

        Returns:
            MockColumnOperation representing the last_day function

        Example:
            >>> df.select(F.last_day(F.col('date')))
        """
        if isinstance(date, str):
            date = MockColumn(date)

        return MockColumnOperation(
            date,
            "last_day",
            name=f"last_day({date.name})"
        )

    @staticmethod
    def next_day(date: Union[MockColumn, str], dayOfWeek: str) -> MockColumnOperation:
        """Returns the first date which is later than the value of the date column that is on the specified day of the week.

        Args:
            date: Date column
            dayOfWeek: Day of week string (e.g., 'Mon', 'Monday')

        Returns:
            MockColumnOperation representing the next_day function

        Example:
            >>> df.select(F.next_day(F.col('date'), 'Monday'))
        """
        if isinstance(date, str):
            date = MockColumn(date)

        return MockColumnOperation(
            date,
            "next_day",
            value=dayOfWeek,
            name=f"next_day({date.name}, {dayOfWeek})"
        )

    @staticmethod
    def trunc(date: Union[MockColumn, str], format: str) -> MockColumnOperation:
        """Truncate date to specified unit (year, month, etc.).

        Args:
            date: Date column
            format: Truncation format ('year', 'yyyy', 'yy', 'month', 'mon', 'mm')

        Returns:
            MockColumnOperation representing the trunc function

        Example:
            >>> df.select(F.trunc(F.col('date'), 'year'))
        """
        if isinstance(date, str):
            date = MockColumn(date)

        return MockColumnOperation(
            date,
            "trunc",
            value=format,
            name=f"trunc({date.name}, {format})"
        )

    @staticmethod
    def timestamp_seconds(col: Union[MockColumn, str, int]) -> MockColumnOperation:
        """Convert seconds since epoch to timestamp (PySpark 3.1+).

        Args:
            col: Column or integer representing seconds since epoch

        Returns:
            MockColumnOperation representing the timestamp

        Example:
            >>> df.select(F.timestamp_seconds(F.col("seconds")))
        """
        if isinstance(col, str):
            col = MockColumn(col)
        elif isinstance(col, int):
            from mock_spark.functions.core.literals import MockLiteral
            col = MockLiteral(col)  # type: ignore[assignment]

        return MockColumnOperation(
            col,  # type: ignore[arg-type]
            "timestamp_seconds",
            name=f"timestamp_seconds({col})"
        )

    @staticmethod
    def weekday(col: Union[MockColumn, str]) -> MockColumnOperation:
        """Get the day of week as an integer (0 = Monday, 6 = Sunday) (PySpark 3.5+).

        Args:
            col: Column or column name containing date/timestamp values.

        Returns:
            MockColumnOperation representing the weekday function.
            
        Note:
            Returns 0 for Monday through 6 for Sunday.
        """
        column = MockColumn(col) if isinstance(col, str) else col
        return MockColumnOperation(column, "weekday", name=f"weekday({column.name})")

    @staticmethod
    def extract(field: str, source: Union[MockColumn, str]) -> MockColumnOperation:
        """Extract a field from a date/timestamp column (PySpark 3.5+).

        Args:
            field: The field to extract (YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, etc.)
            source: Column or column name containing date/timestamp values.

        Returns:
            MockColumnOperation representing the extract function.
            
        Example:
            >>> df.select(F.extract("YEAR", F.col("date")))
            >>> df.select(F.extract("MONTH", F.col("timestamp")))
        """
        column = MockColumn(source) if isinstance(source, str) else source
        return MockColumnOperation(
            column,
            "extract",
            value=field.upper(),
            name=f"extract({field}, {column.name})"
        )
