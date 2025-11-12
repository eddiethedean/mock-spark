"""Regression tests for event_date extraction without substring fallbacks."""

from __future__ import annotations

from datetime import date

from mock_spark import F, SparkSession
from mock_spark.compat import datetime as dt_compat


def test_pipeline_uses_typed_dates_without_substring():
    spark = SparkSession("event_date_pipeline")
    try:
        df = spark.createDataFrame(
            [
                {"user_id": "u1", "event_ts": "2024-05-09 10:15:00"},
                {"user_id": "u1", "event_ts": "2024-05-09 18:45:00"},
                {"user_id": "u2", "event_ts": "2024-05-10 09:00:00"},
            ]
        )

        result = (
            df.withColumn(
                "event_date",
                dt_compat.to_date_typed("event_ts", source_format="yyyy-MM-dd HH:mm:ss"),
            )
            .groupBy("user_id", "event_date")
            .agg(F.count("*").alias("event_count"))
            .orderBy("user_id", "event_date")
            .collect()
        )

        assert result[0].event_date == date(2024, 5, 9)
        assert result[0].event_count == 2
        assert result[1].event_date == date(2024, 5, 10)
        assert result[1].event_count == 1
    finally:
        spark.stop()

