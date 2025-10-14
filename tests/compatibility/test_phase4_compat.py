import pytest


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


def test_schema_string_generator_compat_columns_and_count():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession, types as T

    from mock_spark import MockSparkSession

    schema_str = "id:int,name:string,active:boolean"
    row_count = 7

    # Mock-Spark DataFrame
    mock = MockSparkSession()
    mock_df = create_dataframe_from_schema_string(mock, schema_str, row_count=row_count)

    # Build equivalent PySpark schema
    fields = []
    for part in schema_str.split(","):
        name, typ = [p.strip() for p in part.split(":", 1)]
        if typ in ("int", "integer", "long"):
            dtype = T.LongType()
        elif typ in ("double", "float"):
            dtype = T.DoubleType()
        elif typ in ("bool", "boolean"):
            dtype = T.BooleanType()
        else:
            dtype = T.StringType()
        fields.append(T.StructField(name, dtype, True))
    pys_schema = T.StructType(fields)

    # Create equivalent data using the same values from mock df
    data = [row.asDict() for row in mock_df.collect()]

    spark = SparkSession.builder.appName("phase4_compat").getOrCreate()
    pys_df = spark.createDataFrame(data=data, schema=pys_schema)

    # Compare columns and count
    assert set(mock_df.columns) == set(pys_df.columns)
    assert mock_df.count() == pys_df.count()

    spark.stop()
