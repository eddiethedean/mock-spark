"""
API parity tests for type casting operations.

Tests type conversions between MockSpark and PySpark
to ensure identical behavior and results.
"""

from tests.api_parity.conftest import ParityTestBase, compare_dataframes


class TestTypeCasting(ParityTestBase):
    """Test type casting operations for API parity."""

    def test_cast_to_string(self, mock_spark, pyspark_spark, numeric_data):
        """Test casting to string type."""
        # MockSpark
        mock_df = mock_spark.createDataFrame(numeric_data)
        mock_result = mock_df.select(
            "id", mock_df.value.cast("string").alias("value_str")
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(numeric_data)
        pyspark_result = pyspark_df.select(
            "id", pyspark_df.value.cast("string").alias("value_str")
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_cast_to_integer(self, mock_spark, pyspark_spark, numeric_data):
        """Test casting to integer type."""
        # MockSpark
        mock_df = mock_spark.createDataFrame(numeric_data)
        mock_result = mock_df.select("id", mock_df.value.cast("int").alias("value_int"))

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(numeric_data)
        pyspark_result = pyspark_df.select(
            "id", pyspark_df.value.cast("int").alias("value_int")
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_cast_to_long(self, mock_spark, pyspark_spark, numeric_data):
        """Test casting to long type."""
        # MockSpark
        mock_df = mock_spark.createDataFrame(numeric_data)
        mock_result = mock_df.select(
            "id", mock_df.value.cast("long").alias("value_long")
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(numeric_data)
        pyspark_result = pyspark_df.select(
            "id", pyspark_df.value.cast("long").alias("value_long")
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_cast_to_double(self, mock_spark, pyspark_spark, numeric_data):
        """Test casting to double type."""
        # MockSpark
        mock_df = mock_spark.createDataFrame(numeric_data)
        mock_result = mock_df.select(
            "id", mock_df.value.cast("double").alias("value_double")
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(numeric_data)
        pyspark_result = pyspark_df.select(
            "id", pyspark_df.value.cast("double").alias("value_double")
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_cast_to_float(self, mock_spark, pyspark_spark, numeric_data):
        """Test casting to float type."""
        # MockSpark
        mock_df = mock_spark.createDataFrame(numeric_data)
        mock_result = mock_df.select(
            "id", mock_df.value.cast("float").alias("value_float")
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(numeric_data)
        pyspark_result = pyspark_df.select(
            "id", pyspark_df.value.cast("float").alias("value_float")
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_cast_to_boolean(self, mock_spark, pyspark_spark, numeric_data):
        """Test casting to boolean type."""
        # MockSpark
        mock_df = mock_spark.createDataFrame(numeric_data)
        mock_result = mock_df.select(
            "id", mock_df.flag.cast("boolean").alias("flag_bool")
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(numeric_data)
        pyspark_result = pyspark_df.select(
            "id", pyspark_df.flag.cast("boolean").alias("flag_bool")
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_cast_string_to_numeric(self, mock_spark, pyspark_spark):
        """Test casting string to numeric types."""
        data = [
            {"id": 1, "value_str": "10.5"},
            {"id": 2, "value_str": "20.7"},
            {"id": 3, "value_str": "30.9"},
        ]

        # MockSpark
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.select(
            "id",
            mock_df.value_str.cast("double").alias("value_double"),
            mock_df.value_str.cast("int").alias("value_int"),
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(data)
        pyspark_result = pyspark_df.select(
            "id",
            pyspark_df.value_str.cast("double").alias("value_double"),
            pyspark_df.value_str.cast("int").alias("value_int"),
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_cast_with_null_handling(self, mock_spark, pyspark_spark):
        """Test casting with null values."""
        data = [
            {"id": 1, "value": 10.5, "text": "10.5"},
            {"id": 2, "value": None, "text": None},
            {"id": 3, "value": 30.9, "text": "30.9"},
        ]

        # MockSpark
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.select(
            "id",
            mock_df.value.cast("string").alias("value_str"),
            mock_df.text.cast("double").alias("text_double"),
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(data)
        pyspark_result = pyspark_df.select(
            "id",
            pyspark_df.value.cast("string").alias("value_str"),
            pyspark_df.text.cast("double").alias("text_double"),
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_cast_with_invalid_values(self, mock_spark, pyspark_spark):
        """Test casting with invalid values."""
        data = [
            {"id": 1, "text": "10.5"},
            {"id": 2, "text": "invalid"},
            {"id": 3, "text": "30.9"},
        ]

        # MockSpark
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.select(
            "id", mock_df.text.cast("double").alias("text_double")
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(data)
        pyspark_result = pyspark_df.select(
            "id", pyspark_df.text.cast("double").alias("text_double")
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_cast_boolean_to_numeric(self, mock_spark, pyspark_spark, numeric_data):
        """Test casting boolean to numeric types."""
        # MockSpark
        mock_df = mock_spark.createDataFrame(numeric_data)
        mock_result = mock_df.select(
            "id",
            mock_df.flag.cast("int").alias("flag_int"),
            mock_df.flag.cast("double").alias("flag_double"),
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(numeric_data)
        pyspark_result = pyspark_df.select(
            "id",
            pyspark_df.flag.cast("int").alias("flag_int"),
            pyspark_df.flag.cast("double").alias("flag_double"),
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_cast_numeric_to_boolean(self, mock_spark, pyspark_spark):
        """Test casting numeric to boolean types."""
        data = [
            {"id": 1, "value": 1.0},
            {"id": 2, "value": 0.0},
            {"id": 3, "value": 5.0},
            {"id": 4, "value": -1.0},
        ]

        # MockSpark
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.select(
            "id", mock_df.value.cast("boolean").alias("value_bool")
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(data)
        pyspark_result = pyspark_df.select(
            "id", pyspark_df.value.cast("boolean").alias("value_bool")
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_cast_with_precision_and_scale(self, mock_spark, pyspark_spark):
        """Test casting with precision and scale for decimal types."""
        data = [
            {"id": 1, "value": 10.567},
            {"id": 2, "value": 20.123},
            {"id": 3, "value": 30.999},
        ]

        # MockSpark
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.select(
            "id", mock_df.value.cast("decimal(10,2)").alias("value_decimal")
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(data)
        pyspark_result = pyspark_df.select(
            "id", pyspark_df.value.cast("decimal(10,2)").alias("value_decimal")
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_cast_date_types(self, mock_spark, pyspark_spark):
        """Test casting date and timestamp types."""
        data = [
            {"id": 1, "date_str": "2024-01-15", "timestamp_str": "2024-01-15 10:30:00"},
            {"id": 2, "date_str": "2024-01-16", "timestamp_str": "2024-01-16 14:45:00"},
        ]

        # MockSpark
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.select(
            "id",
            mock_df.date_str.cast("date").alias("date_col"),
            mock_df.timestamp_str.cast("timestamp").alias("timestamp_col"),
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(data)
        pyspark_result = pyspark_df.select(
            "id",
            pyspark_df.date_str.cast("date").alias("date_col"),
            pyspark_df.timestamp_str.cast("timestamp").alias("timestamp_col"),
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_cast_array_types(self, mock_spark, pyspark_spark, complex_data):
        """Test casting array types."""
        # MockSpark
        mock_df = mock_spark.createDataFrame(complex_data)
        mock_result = mock_df.select(
            "id", mock_df.scores.cast("string").alias("scores_str")
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(complex_data)
        pyspark_result = pyspark_df.select(
            "id", pyspark_df.scores.cast("string").alias("scores_str")
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_cast_map_types(self, mock_spark, pyspark_spark, complex_data):
        """Test casting map types."""
        # MockSpark
        mock_df = mock_spark.createDataFrame(complex_data)
        mock_result = mock_df.select(
            "id", mock_df.metadata.cast("string").alias("metadata_str")
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(complex_data)
        pyspark_result = pyspark_df.select(
            "id", pyspark_df.metadata.cast("string").alias("metadata_str")
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_multiple_casts_in_one_query(self, mock_spark, pyspark_spark, numeric_data):
        """Test multiple casts in one query."""
        # MockSpark
        mock_df = mock_spark.createDataFrame(numeric_data)
        mock_result = mock_df.select(
            "id",
            mock_df.value.cast("string").alias("value_str"),
            mock_df.value.cast("int").alias("value_int"),
            mock_df.value.cast("long").alias("value_long"),
            mock_df.flag.cast("int").alias("flag_int"),
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(numeric_data)
        pyspark_result = pyspark_df.select(
            "id",
            pyspark_df.value.cast("string").alias("value_str"),
            pyspark_df.value.cast("int").alias("value_int"),
            pyspark_df.value.cast("long").alias("value_long"),
            pyspark_df.flag.cast("int").alias("flag_int"),
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_cast_with_aliases(self, mock_spark, pyspark_spark, numeric_data):
        """Test casting with column aliases."""
        # MockSpark
        mock_df = mock_spark.createDataFrame(numeric_data)
        mock_result = mock_df.select(
            "id",
            mock_df.value.cast("string").alias("string_value"),
            mock_df.value.cast("int").alias("int_value"),
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(numeric_data)
        pyspark_result = pyspark_df.select(
            "id",
            pyspark_df.value.cast("string").alias("string_value"),
            pyspark_df.value.cast("int").alias("int_value"),
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_cast_in_expressions(self, mock_spark, pyspark_spark, numeric_data):
        """Test casting within expressions."""
        from mock_spark import F as MockF
        from pyspark.sql import functions as PySparkF

        # MockSpark
        mock_df = mock_spark.createDataFrame(numeric_data)
        mock_result = mock_df.select(
            "id",
            (mock_df.value.cast("int") + 10).alias("value_plus_10"),
            MockF.concat(mock_df.value.cast("string"), MockF.lit("_suffix")).alias(
                "value_with_suffix"
            ),
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(numeric_data)
        pyspark_result = pyspark_df.select(
            "id",
            (pyspark_df.value.cast("int") + 10).alias("value_plus_10"),
            PySparkF.concat(
                pyspark_df.value.cast("string"), PySparkF.lit("_suffix")
            ).alias("value_with_suffix"),
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)
