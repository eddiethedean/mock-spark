def create_dataframe_from_schema_string(session, schema_string, row_count=10):
    """Create DataFrame from schema string like 'id:int,name:string'.

    Inlined from mock_spark.testing.generators to avoid dependency.
    """
    from mock_spark.spark_types import (
        MockStructType,
        MockStructField,
        LongType,
        DoubleType,
        BooleanType,
        StringType,
        DateType,
        TimestampType,
    )
    import random
    from datetime import datetime, timedelta

    def generate_date():
        """Generate a random date string."""
        start_date = datetime(2020, 1, 1)
        end_date = datetime(2023, 12, 31)
        delta = end_date - start_date
        random_days = random.randint(0, delta.days)
        random_date = start_date + timedelta(days=random_days)
        return random_date.strftime("%Y-%m-%d")

    def generate_timestamp():
        """Generate a random timestamp string."""
        start_date = datetime(2020, 1, 1)
        end_date = datetime(2023, 12, 31)
        delta = end_date - start_date
        random_seconds = random.randint(0, int(delta.total_seconds()))
        random_timestamp = start_date + timedelta(seconds=random_seconds)
        return random_timestamp.strftime("%Y-%m-%d %H:%M:%S")

    fields = []
    for part in schema_string.split(","):
        name, typ = [p.strip() for p in part.split(":", 1)]
        if typ in ("int", "integer", "long"):
            dtype = LongType()
        elif typ in ("double", "float"):
            dtype = DoubleType()
        elif typ in ("bool", "boolean"):
            dtype = BooleanType()
        elif typ in ("date",):
            dtype = DateType()
        elif typ in ("timestamp",):
            dtype = TimestampType()
        else:
            dtype = StringType()
        fields.append(MockStructField(name, dtype))

    schema = MockStructType(fields)

    data = []
    for i in range(row_count):
        row = {}
        for f in schema.fields:
            if isinstance(f.dataType, LongType):
                row[f.name] = i
            elif isinstance(f.dataType, DoubleType):
                row[f.name] = float(i)
            elif isinstance(f.dataType, BooleanType):
                row[f.name] = i % 2 == 0
            elif isinstance(f.dataType, DateType):
                row[f.name] = generate_date()
            elif isinstance(f.dataType, TimestampType):
                row[f.name] = generate_timestamp()
            else:
                row[f.name] = f"val_{i}"
        data.append(row)

    return session.createDataFrame(data, schema)


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
