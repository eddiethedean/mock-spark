import pytest


def test_schema_string_generator_compat_columns_and_count():
    pyspark = pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession, types as T

    from mock_spark import MockSparkSession
    from mock_spark.testing.generators import create_dataframe_from_schema_string

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
