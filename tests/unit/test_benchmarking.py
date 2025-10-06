def test_benchmark_operation_records_results():
    from mock_spark import MockSparkSession

    spark = MockSparkSession()
    df = spark.createDataFrame([{"x": i} for i in range(5)])

    def op():
        return df.filter(df.functions.col("x") >= 2) if hasattr(df, "functions") else df.filter(lambda r: r["x"] >= 2)  # fallback

    # Use a simple lambda over DataFrame to keep interface consistent
    result = spark.benchmark_operation("filter_ge_2", lambda: df.filter(df.functions.col("x")) if hasattr(df, "functions") else df)

    # Ensure results are recorded
    results = spark.get_benchmark_results()
    assert "filter_ge_2" in results
    assert results["filter_ge_2"]["duration_s"] >= 0
    assert results["filter_ge_2"]["memory_used_bytes"] >= 0
    assert results["filter_ge_2"]["result_size"] >= 1

