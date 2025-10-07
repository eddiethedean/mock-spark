from typing import Any, Dict

from tests.compatibility.utils.comparison import compare_dataframes


def test_row_access_and_ordering_equivalence():
    # Mock
    from mock_spark import MockSparkSession

    mock = MockSparkSession("compat")
    data = [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
    ]
    mock_df = mock.createDataFrame(data)
    mock_rows = mock_df.collect()

    # PySpark
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("compat").getOrCreate()
    pyspark_df = spark.createDataFrame(data)
    pyspark_rows = pyspark_df.collect()

    # Index access
    assert mock_rows[0][0] == pyspark_rows[0][0]
    assert mock_rows[0][1] == pyspark_rows[0][1]

    # Key access
    assert mock_rows[0]["id"] == pyspark_rows[0]["id"]
    assert mock_rows[0]["name"] == pyspark_rows[0]["name"]

    # Dict ordering follows schema columns
    mock_keys = list(mock_rows[0].asDict().keys())
    py_keys = list(pyspark_rows[0].asDict().keys())
    assert mock_keys == py_keys == mock_df.columns

    spark.stop()


def test_lazy_materialization_equivalence():
    from mock_spark import MockSparkSession, F
    from pyspark.sql import SparkSession, functions as sf

    base_data = [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
        {"id": 3, "name": "Carol"},
    ]

    # Mock lazy pipeline
    mock = MockSparkSession("compat")
    mock_df = mock.createDataFrame(base_data)
    mock_lazy = mock_df.withLazy(True).filter(F.col("id") > 1).withColumn("greet", F.lit("hi"))

    # PySpark eager pipeline
    spark = SparkSession.builder.appName("compat").getOrCreate()
    py_df = spark.createDataFrame(base_data)
    py_eager = py_df.filter(sf.col("id") > 1).withColumn("greet", sf.lit("hi"))

    # Materialize mock and compare
    result = compare_dataframes(mock_lazy, py_eager, check_schema=True, check_data=True)
    assert result["equivalent"], result["errors"]

    spark.stop()
