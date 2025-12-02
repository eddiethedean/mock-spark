"""
API parity tests for datetime operations.

Tests date/time functions between MockSpark and PySpark
to ensure identical behavior and results.
"""

from tests.api_parity.conftest import ParityTestBase, compare_dataframes


class TestDatetimeOperations(ParityTestBase):
    """Test datetime operations for API parity."""

    def test_hour_extraction(self, mock_spark, pyspark_spark, datetime_data):
        """Test hour extraction from timestamp."""
        from mock_spark import F as MockF
        from pyspark.sql import functions as PySparkF

        # MockSpark
        mock_df = mock_spark.createDataFrame(datetime_data)
        mock_result = mock_df.select(
            "timestamp", MockF.hour("timestamp").alias("extracted_hour")
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(datetime_data)
        pyspark_result = pyspark_df.select(
            "timestamp", PySparkF.hour("timestamp").alias("extracted_hour")
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_dayofweek_extraction(self, mock_spark, pyspark_spark, datetime_data):
        """Test dayofweek extraction from timestamp."""
        from mock_spark import F as MockF
        from pyspark.sql import functions as PySparkF

        # MockSpark
        mock_df = mock_spark.createDataFrame(datetime_data)
        mock_result = mock_df.select(
            "timestamp", MockF.dayofweek("timestamp").alias("day_of_week")
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(datetime_data)
        pyspark_result = pyspark_df.select(
            "timestamp", PySparkF.dayofweek("timestamp").alias("day_of_week")
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_to_timestamp_without_format(self, mock_spark, pyspark_spark):
        """Test to_timestamp without format string."""
        from mock_spark import F as MockF
        from pyspark.sql import functions as PySparkF

        data = [
            {"id": 1, "timestamp_str": "2024-01-15 10:30:00"},
            {"id": 2, "timestamp_str": "2024-01-16 14:45:00"},
        ]

        # MockSpark
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.select(
            "timestamp_str", MockF.to_timestamp("timestamp_str").alias("timestamp")
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(data)
        pyspark_result = pyspark_df.select(
            "timestamp_str", PySparkF.to_timestamp("timestamp_str").alias("timestamp")
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_to_timestamp_with_format(self, mock_spark, pyspark_spark):
        """Test to_timestamp with format string."""
        from mock_spark import F as MockF
        from pyspark.sql import functions as PySparkF

        data = [
            {"id": 1, "timestamp_str": "15/01/2024 10:30:00"},
            {"id": 2, "timestamp_str": "16/01/2024 14:45:00"},
        ]

        # MockSpark
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.select(
            "timestamp_str",
            MockF.to_timestamp("timestamp_str", "dd/MM/yyyy HH:mm:ss").alias(
                "timestamp"
            ),
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(data)
        pyspark_result = pyspark_df.select(
            "timestamp_str",
            PySparkF.to_timestamp("timestamp_str", "dd/MM/yyyy HH:mm:ss").alias(
                "timestamp"
            ),
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_to_date(self, mock_spark, pyspark_spark):
        """Test to_date function."""
        from mock_spark import F as MockF
        from pyspark.sql import functions as PySparkF

        data = [
            {"id": 1, "date_str": "2024-01-15"},
            {"id": 2, "date_str": "2024-01-16"},
        ]

        # MockSpark
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.select(
            "date_str", MockF.to_date("date_str").alias("date")
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(data)
        pyspark_result = pyspark_df.select(
            "date_str", PySparkF.to_date("date_str").alias("date")
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_current_timestamp(self, mock_spark, pyspark_spark, sample_data):
        """Test current_timestamp function."""
        from mock_spark import F as MockF
        from pyspark.sql import functions as PySparkF

        # MockSpark
        mock_df = mock_spark.createDataFrame(sample_data)
        mock_result = mock_df.select(
            "id", MockF.current_timestamp().alias("current_ts")
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(sample_data)
        pyspark_result = pyspark_df.select(
            "id", PySparkF.current_timestamp().alias("current_ts")
        )

        # Compare (just check that both have the same number of rows)
        assert mock_result.count() == pyspark_result.count()

    def test_current_date(self, mock_spark, pyspark_spark, sample_data):
        """Test current_date function."""
        from mock_spark import F as MockF
        from pyspark.sql import functions as PySparkF

        # MockSpark
        mock_df = mock_spark.createDataFrame(sample_data)
        mock_result = mock_df.select("id", MockF.current_date().alias("current_date"))

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(sample_data)
        pyspark_result = pyspark_df.select(
            "id", PySparkF.current_date().alias("current_date")
        )

        # Compare (just check that both have the same number of rows)
        assert mock_result.count() == pyspark_result.count()

    def test_date_format(self, mock_spark, pyspark_spark, datetime_data):
        """Test date_format function."""
        from mock_spark import F as MockF
        from pyspark.sql import functions as PySparkF

        # MockSpark
        mock_df = mock_spark.createDataFrame(datetime_data)
        mock_result = mock_df.select(
            "timestamp",
            MockF.date_format("timestamp", "yyyy-MM-dd").alias("formatted_date"),
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(datetime_data)
        pyspark_result = pyspark_df.select(
            "timestamp",
            PySparkF.date_format("timestamp", "yyyy-MM-dd").alias("formatted_date"),
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_year_extraction(self, mock_spark, pyspark_spark, datetime_data):
        """Test year extraction from timestamp."""
        from mock_spark import F as MockF
        from pyspark.sql import functions as PySparkF

        # MockSpark
        mock_df = mock_spark.createDataFrame(datetime_data)
        mock_result = mock_df.select("timestamp", MockF.year("timestamp").alias("year"))

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(datetime_data)
        pyspark_result = pyspark_df.select(
            "timestamp", PySparkF.year("timestamp").alias("year")
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_month_extraction(self, mock_spark, pyspark_spark, datetime_data):
        """Test month extraction from timestamp."""
        from mock_spark import F as MockF
        from pyspark.sql import functions as PySparkF

        # MockSpark
        mock_df = mock_spark.createDataFrame(datetime_data)
        mock_result = mock_df.select(
            "timestamp", MockF.month("timestamp").alias("month")
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(datetime_data)
        pyspark_result = pyspark_df.select(
            "timestamp", PySparkF.month("timestamp").alias("month")
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_day_extraction(self, mock_spark, pyspark_spark, datetime_data):
        """Test day extraction from timestamp."""
        from mock_spark import F as MockF
        from pyspark.sql import functions as PySparkF

        # MockSpark
        mock_df = mock_spark.createDataFrame(datetime_data)
        mock_result = mock_df.select("timestamp", MockF.day("timestamp").alias("day"))

        # PySpark - day is dayofmonth
        pyspark_df = pyspark_spark.createDataFrame(datetime_data)
        pyspark_result = pyspark_df.select(
            "timestamp", PySparkF.dayofmonth("timestamp").alias("day")
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_date_add(self, mock_spark, pyspark_spark, datetime_data):
        """Test date_add function."""
        from mock_spark import F as MockF
        from pyspark.sql import functions as PySparkF

        # MockSpark
        mock_df = mock_spark.createDataFrame(datetime_data)
        mock_result = mock_df.select(
            "date", MockF.date_add("date", 7).alias("date_plus_7")
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(datetime_data)
        pyspark_result = pyspark_df.select(
            "date", PySparkF.date_add("date", 7).alias("date_plus_7")
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_date_sub(self, mock_spark, pyspark_spark, datetime_data):
        """Test date_sub function."""
        from mock_spark import F as MockF
        from pyspark.sql import functions as PySparkF

        # MockSpark
        mock_df = mock_spark.createDataFrame(datetime_data)
        mock_result = mock_df.select(
            "date", MockF.date_sub("date", 7).alias("date_minus_7")
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(datetime_data)
        pyspark_result = pyspark_df.select(
            "date", PySparkF.date_sub("date", 7).alias("date_minus_7")
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_datediff(self, mock_spark, pyspark_spark, datetime_data):
        """Test datediff function."""
        from mock_spark import F as MockF
        from pyspark.sql import functions as PySparkF

        # MockSpark - datediff requires F.lit() for string literals
        mock_df = mock_spark.createDataFrame(datetime_data)
        mock_result = mock_df.select(
            "date",
            MockF.datediff("date", MockF.lit("2024-01-01")).alias("days_since_jan_1"),
        )

        # PySpark - datediff requires F.lit() for string literals
        pyspark_df = pyspark_spark.createDataFrame(datetime_data)
        pyspark_result = pyspark_df.select(
            "date",
            PySparkF.datediff("date", PySparkF.lit("2024-01-01")).alias(
                "days_since_jan_1"
            ),
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_timestamp_cast_to_long(self, mock_spark, pyspark_spark, datetime_data):
        """Test casting timestamp to long (epoch seconds)."""
        # MockSpark
        mock_df = mock_spark.createDataFrame(datetime_data)
        mock_result = mock_df.select(
            "timestamp", mock_df.timestamp.cast("long").alias("epoch_seconds")
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(datetime_data)
        pyspark_result = pyspark_df.select(
            "timestamp", pyspark_df.timestamp.cast("long").alias("epoch_seconds")
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_date_arithmetic(self, mock_spark, pyspark_spark, datetime_data):
        """Test date arithmetic operations."""
        from mock_spark import F as MockF
        from pyspark.sql import functions as PySparkF

        # MockSpark
        mock_df = mock_spark.createDataFrame(datetime_data)
        mock_result = mock_df.select(
            "date",
            MockF.date_add("date", 30).alias("date_plus_30"),
            MockF.date_sub("date", 15).alias("date_minus_15"),
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(datetime_data)
        pyspark_result = pyspark_df.select(
            "date",
            PySparkF.date_add("date", 30).alias("date_plus_30"),
            PySparkF.date_sub("date", 15).alias("date_minus_15"),
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_complex_datetime_operations(
        self, mock_spark, pyspark_spark, datetime_data
    ):
        """Test complex datetime operations combining multiple functions."""
        from mock_spark import F as MockF
        from pyspark.sql import functions as PySparkF

        # MockSpark
        mock_df = mock_spark.createDataFrame(datetime_data)
        mock_result = mock_df.select(
            "timestamp",
            MockF.year("timestamp").alias("year"),
            MockF.month("timestamp").alias("month"),
            MockF.day("timestamp").alias("day"),
            MockF.hour("timestamp").alias("hour"),
            MockF.dayofweek("timestamp").alias("day_of_week"),
        )

        # PySpark - day is dayofmonth
        pyspark_df = pyspark_spark.createDataFrame(datetime_data)
        pyspark_result = pyspark_df.select(
            "timestamp",
            PySparkF.year("timestamp").alias("year"),
            PySparkF.month("timestamp").alias("month"),
            PySparkF.dayofmonth("timestamp").alias("day"),
            PySparkF.hour("timestamp").alias("hour"),
            PySparkF.dayofweek("timestamp").alias("day_of_week"),
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)
