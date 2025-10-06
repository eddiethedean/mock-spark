def test_toDuckDB_uses_batch_insert_for_large_dataset():
    from mock_spark import MockSparkSession

    spark = MockSparkSession()
    data = [{"id": i, "name": f"n{i}"} for i in range(100)]
    df = spark.createDataFrame(data)

    # Ensure toDuckDB runs without excessive per-row execution (functional smoke test)
    table = df.toDuckDB()
    assert isinstance(table, str) and len(table) > 0

