def test_dataframe_test_helpers():
    from mock_spark import MockSparkSession
    from mock_spark.spark_types import MockStructType, MockStructField, LongType, StringType

    spark = MockSparkSession()
    schema = MockStructType(
        [MockStructField("id", LongType()), MockStructField("name", StringType())]
    )
    df = spark.createDataFrame([{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}], schema)

    # Helpers should not raise
    df.assert_has_columns(["id", "name"])
    df.assert_row_count(2)
    df.assert_schema_matches(schema)
    df.assert_data_equals([{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}])


def test_schema_string_generator():
    from mock_spark import MockSparkSession
    from mock_spark.testing.generators import create_dataframe_from_schema_string

    spark = MockSparkSession()
    df = create_dataframe_from_schema_string(
        spark, "id:int,name:string,active:boolean", row_count=5
    )
    assert df.count() == 5
    assert set(df.columns) == {"id", "name", "active"}


def test_plugin_hooks_modify_data():
    from mock_spark import MockSparkSession

    class TestPlugin:
        def before_create_dataframe(self, session, data, schema):
            # add a constant column to each row
            if isinstance(data, list) and data and isinstance(data[0], dict):
                data = [{**row, "added": 1} for row in data]
            # ensure schema includes the added column
            if isinstance(schema, list):
                if "added" not in schema:
                    schema = schema + ["added"]
            else:
                try:
                    from mock_spark.spark_types import MockStructField, LongType, MockStructType

                    if isinstance(schema, MockStructType):
                        names = [f.name for f in schema.fields]
                        if "added" not in names:
                            schema = MockStructType(
                                schema.fields + [MockStructField("added", LongType())]
                            )
                except Exception:
                    pass
            return data, schema

        def after_create_dataframe(self, session, df):
            # ensure the column exists via helper
            df.assert_has_columns(["added"])
            return df

    spark = MockSparkSession()
    spark.register_plugin(TestPlugin())

    df = spark.createDataFrame([{"x": 1}, {"x": 2}], ["x"])
    df.assert_has_columns(["x", "added"])
