"""Unit tests for datetime compatibility helpers."""

from datetime import date, datetime

from mock_spark.compat import datetime as dt_compat
from mock_spark.functions import F
from mock_spark.functions.base import ColumnOperation
from mock_spark.spark_types import (
    Row,
    StructField,
    StructType,
    StringType,
    TimestampType,
)


def test_to_date_str_preserves_alias():
    column = dt_compat.to_date_str(F.to_date("event_date"))
    assert isinstance(column, ColumnOperation)
    assert column.name == "to_date(event_date)"


def test_to_date_str_keeps_custom_alias():
    column = F.to_date("event_date").alias("formatted_date")
    wrapped = dt_compat.to_date_str(column)
    assert wrapped.name == "formatted_date"


def test_to_timestamp_str_preserves_alias():
    base = F.to_timestamp("event_ts")
    column = dt_compat.to_timestamp_str(base)
    assert isinstance(column, ColumnOperation)
    assert column.name == base.name


def test_to_timestamp_str_keeps_custom_alias():
    column = F.to_timestamp("event_ts").alias("formatted_ts")
    wrapped = dt_compat.to_timestamp_str(column)
    assert wrapped.name == "formatted_ts"


def test_normalize_collected_datetimes_outputs_strings():
    schema = StructType(
        [
            StructField("event_date", StringType()),
            StructField("event_ts", TimestampType()),
        ]
    )
    row = Row(
        {
            "event_date": date(2024, 5, 9),
            "event_ts": datetime(2024, 5, 9, 12, 34, 56),
        },
        schema,
    )

    normalized = dt_compat.normalize_collected_datetimes(
        [row],
        date_columns=["event_date"],
        timestamp_columns=["event_ts"],
    )

    assert normalized[0]["event_date"] == "2024-05-09"
    assert normalized[0]["event_ts"] == "2024-05-09 12:34:56"
