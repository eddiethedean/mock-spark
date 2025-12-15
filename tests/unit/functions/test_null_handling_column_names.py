from sparkless.sql import SparkSession, functions as F


def test_coalesce_column_name_matches_expected() -> None:
    """BUG-018 regression: coalesce should use PySpark-compatible column naming.

    The parity JSON for null_handling/coalesce expects a single column named
    \"coalesce(salary, 0)\"; this test asserts that the DataFrame produced by
    the corresponding operation uses that exact column name.
    """
    spark = SparkSession("Bug018NullHandling")
    try:
        df = spark.createDataFrame(
            [
                {"id": 1, "name": "Alice", "age": 25, "salary": 50000.0},
                {"id": 2, "name": None, "age": 30, "salary": None},
            ]
        )

        result = df.select(F.coalesce(df.salary, F.lit(0)))
        assert result.columns == ["coalesce(salary, 0)"]
    finally:
        spark.stop()


