from sparkless.sql import SparkSession, functions as F


def test_groupby_agg_accepts_aggregate_function_objects() -> None:
    """BUG-022 regression: GroupedData.agg should accept AggregateFunction instances.

    Historically, strict validation in GroupedData.agg() rejected AggregateFunction
    objects even though PySpark accepts them in many contexts (e.g. F.first, F.last).
    This test ensures that passing AggregateFunction objects into agg() works and
    produces a sensible aggregated schema.
    """
    spark = SparkSession("Bug022GroupedAgg")
    try:
        df = spark.createDataFrame(
            [
                {"dept": "IT", "salary": 100},
                {"dept": "IT", "salary": 200},
                {"dept": "HR", "salary": 150},
            ]
        )

        # Use AggregateFunction-returning helpers directly
        result = df.groupBy("dept").agg(
            F.first("salary"),  # type: ignore[operator]
            F.last("salary"),  # type: ignore[operator]
        )

        # Should not raise, and should include both aggregates in schema
        assert "first(salary)" in result.columns or "first" in result.columns
        assert "last(salary)" in result.columns or "last" in result.columns
        assert result.count() == 2
    finally:
        spark.stop()
