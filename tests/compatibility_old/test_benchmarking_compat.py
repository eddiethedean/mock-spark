def test_benchmarking_api_runs_and_returns_results():
    from mock_spark import MockSparkSession, functions as F
    from pyspark.sql import SparkSession, functions as sf

    base = [{"id": 1}, {"id": 2}, {"id": 3}]

    # Mock run
    mock = MockSparkSession("bench")
    mock_df = mock.createDataFrame(base)
    mock.benchmark_operation("mock_filter", lambda: mock_df.filter(F.col("id") > 1))
    mock_stats = mock.get_benchmark_results().get("mock_filter")
    assert mock_stats is not None
    assert mock_stats["duration_s"] >= 0

    # PySpark equivalent just to ensure the pipeline itself is valid
    spark = SparkSession.builder.appName("bench").getOrCreate()
    pys_df = spark.createDataFrame(base)
    pys_df.filter(sf.col("id") > 1).collect()
    spark.stop()
