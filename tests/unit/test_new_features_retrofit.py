import pytest


def test_mockrow_index_and_key_access():
    from mock_spark.spark_types import (
        MockRow,
        MockStructType,
        MockStructField,
        StringType,
        LongType,
    )

    schema = MockStructType(
        [MockStructField("id", LongType()), MockStructField("name", StringType())]
    )
    row = MockRow({"id": 1, "name": "Alice"}, schema)

    assert row[0] == 1
    assert row[1] == "Alice"
    assert row["id"] == 1
    assert row["name"] == "Alice"

    d = row.asDict()
    assert list(d.keys()) == ["id", "name"]
    assert d["id"] == 1 and d["name"] == "Alice"


def test_create_dataframe_validation_and_coercion_strict_relaxed():
    from mock_spark import MockSparkSession
    from mock_spark.spark_types import (
        MockStructType,
        MockStructField,
        LongType,
        StringType,
    )

    schema = MockStructType(
        [MockStructField("id", LongType()), MockStructField("name", StringType())]
    )

    # relaxed: coercion should make it pass
    spark_relaxed = MockSparkSession(
        validation_mode="relaxed", enable_type_coercion=True
    )
    df_relaxed = spark_relaxed.createDataFrame([{"id": "1", "name": 10}], schema)
    assert df_relaxed.collect()[0]["id"] == 1
    assert df_relaxed.collect()[0]["name"] == "10"

    # strict: should raise on type mismatch
    spark_strict = MockSparkSession(
        validation_mode="strict", enable_type_coercion=False
    )
    with pytest.raises(Exception):
        spark_strict.createDataFrame([{"id": "1", "name": 10}], schema)


def test_lazy_evaluation_filter_and_withcolumn_materialization():
    from mock_spark import MockSparkSession, F

    spark = MockSparkSession()
    df = spark.createDataFrame(
        [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Carol"},
        ]
    )

    lazy_df = df.filter(F.col("id") > 1).withColumn("greeting", F.lit("hi"))

    # Ensure not materialized yet by checking original data length via eager df
    assert len(df.collect()) == 3

    mat = lazy_df.collect()
    assert len(mat) == 2
    names = [r["name"] for r in mat]
    assert names == ["Bob", "Carol"]
    greets = [r["greeting"] for r in mat]
    assert greets == ["hi", "hi"]
