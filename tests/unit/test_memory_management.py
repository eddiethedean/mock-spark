def test_memory_tracking_and_clear_cache():
    from mock_spark import MockSparkSession

    spark = MockSparkSession()
    assert spark.get_memory_usage() == 0

    df1 = spark.createDataFrame([{"a": 1, "b": 2}], ["a", "b"])  # 1 row, 2 cols
    df2 = spark.createDataFrame([{"a": 1, "b": 2}, {"a": 3, "b": 4}], ["a", "b"])  # 2 rows, 2 cols

    usage = spark.get_memory_usage()
    assert usage > 0

    spark.clear_cache()
    assert spark.get_memory_usage() == 0

